#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <semaphore.h>
#include <mysql/mysql.h>  

#define PRODUCER_COUNT 4
#define CONSUMER_COUNT 4
#define THREAD_COUNT (PRODUCER_COUNT + CONSUMER_COUNT)

static pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;
static void *producer(void *data_ptr);
static void *consumer(void *data_ptr);

sem_t empty_sem;
sem_t full_sem;

typedef struct
{
    char Timestamp[100];
    char IpAddress[100];
    char ProcessName[100];
    char Message[100];
}asffline;

FILE *fp;

int main()
{
    pthread_t threads[THREAD_COUNT];
    int data[THREAD_COUNT];
    int thread_id;
    sem_init(&full_sem, 0, 1);
    sem_init(&empty_sem, 0, 0);
    while(1)
    {
        fp = fopen("test.txt","a+");
        if(fp != NULL)
        {
            break;
        }
    }
    for(thread_id = 0; thread_id < PRODUCER_COUNT; thread_id++)
    {
        data[thread_id] = thread_id;
        printf("data[tid] is %d",data[thread_id]);
        pthread_create(&threads[thread_id], NULL, producer, &data[thread_id]);
    }

    for(; thread_id < THREAD_COUNT; thread_id++)
    {
        data[thread_id] = thread_id;
        pthread_create(&threads[thread_id], NULL, consumer, &data[thread_id]);
    }

    for(int i = 0; i < THREAD_COUNT; i++)
    {
        pthread_join(threads[i], NULL);
    }

    fclose(fp);
    return 0;
}

static inline long readFile(asffline * a, long pos, int thread_id)
{
    FILE * fptr; 
    int tid = thread_id;
    char * line = NULL;
    char word[100]={0};
    char message[100]={0};
    unsigned long position = pos;
    size_t len = 0;
    ssize_t read;
    char * token = NULL;
    char * rest = NULL;
    if (line)
        free(line);
    while(1)
    {
        fptr = fopen("sample_syslog", "r");
        if(fptr != NULL)
        {
            break;
        }
    }
    fseek(fptr,position,SEEK_CUR);
    read = getline(&line, &len, fptr);;
    if(read <= 1)
    {
        return position;
    }
    rest = line;
    token = strtok_r(rest," ", &rest);
    for(int i = 0; i <= 2; i++)
    {
        strcat(word,token);
        strcat(word," ");
        token = strtok_r(NULL," ", &rest);
    }
    memset(a->Timestamp,0,sizeof(a->Timestamp));
    strcpy(a->Timestamp,word);
    strcpy(a->IpAddress,token);
    token = strtok_r(NULL, " ", &rest);
    strcpy(a->ProcessName,token);
    token = strtok_r(NULL, " ", &rest);
    while(token != NULL)
    {
        strcat(message,token);
        strcat(message," ");
        token = strtok_r(NULL, " ", &rest);
    }
    strcpy(a->Message,message);
    position+= read;
    for(int x=1; x<= PRODUCER_COUNT;x++)
    {
        read = getline(&line, &len, fptr);
        position+=read;
    }
    fclose(fptr);
    return position;
}

static inline void writeFile(asffline * a)
{
    pthread_mutex_lock(&file_lock);
    fprintf(fp,"Timestamp: %s\n",a->Timestamp);
    fprintf(fp,"IP-Address: %s\n",a->IpAddress);
    fprintf(fp,"Process-Name: %s\n",a->ProcessName);
    fprintf(fp,"Message: %s\n",a->Message); 
    fflush(fp);
    pthread_mutex_unlock(&file_lock);
}

static inline long readAsf(asffline * a, long position, int thread_id)
{
    pthread_mutex_lock(&file_lock);
    int tid = thread_id;
    char * rest;
    char * line;
    size_t len = 0;
    ssize_t read;
    char * token = NULL;
    unsigned long pos = position;
    fseek(fp,pos,SEEK_SET);   
    read = getline(&line, &len, fp);
    
    rest = line;
    token = strtok_r(rest,"\n", &rest);
    memset(a->Timestamp,0,sizeof(a->Timestamp));
    strcpy(a->Timestamp,token);
    pos += read;
    read = getline(&line, &len, fp);
    rest = line;
    token = strtok_r(rest,"\n", &rest);
    strcpy(a->IpAddress,token);
    pos += read;

    read = getline(&line, &len, fp);
    rest = line;
    token = strtok_r(rest,"\n", &rest);
    strcpy(a->ProcessName, token);
    pos += read;

    read = getline(&line, &len, fp);
    rest = line;
    token = strtok_r(rest,"\n", &rest);
    memset(a->Message,0,sizeof(a->Message));
    strcpy(a->Message, token);
    pos += read;

    read = getline(&line, &len, fp);
    pos += read;
    for(int x=(THREAD_COUNT-PRODUCER_COUNT); x<tid;x++)
    {
       for(int j=1;j<=5;j++)
        {
            read = getline(&line, &len, fp);
            if(read <= 1)
            {
                pthread_mutex_unlock(&file_lock);
                return pos;
            }
            pos += read;
        }
    }
    if (line)
    {
        free(line);
    }
    pthread_mutex_unlock(&file_lock);

    return pos;
}

static inline void writeDb(asffline * a)
{ 
    MYSQL *conn = mysql_init(NULL); 
    mysql_real_connect(conn, "localhost", "root", "password", "SyslogDb", 0, NULL, 0);
    char query[500]; 
    sprintf(query,"INSERT INTO SyslogTable(TimeStamp,IpAddress,ProcessName,Message) VALUES('%s','%s','%s','%s')",a->Timestamp,a->IpAddress,a->ProcessName,a->Message);
    mysql_query(conn, query);
    mysql_close(conn);
}

static inline void deleteLine()
{
    pthread_mutex_lock(&file_lock);
    unsigned long post = 0L;
    char * line = NULL;
    ssize_t read;
    FILE *fptr;
    size_t len = 0;
    while(1)
    {
        fptr = fopen("temp.txt", "w");
        if(fptr != NULL)
        {
            break;
        }
    }
    for(int x = 1; x<= 5;x++)
    {
        read = getline(&line, &len, fp);
        post += read;
    }
    
    fseek(fp,post,SEEK_SET);
    
    while (!feof(fp)) 
    {
        read = getline(&line, &len, fp);
        if (!feof(fp)) 
        {
            fprintf(fptr, "%s", line);
        }
    }
    remove("test.txt");
    rename("temp.txt", "test.txt");
    pthread_mutex_unlock(&file_lock);
}

static void *producer(void *data_ptr)
{
    FILE * fptr2;
    while(1)
    {
        fptr2 = fopen("sample_syslog", "r");
        if(fptr2 != NULL)
        {
            break;
        }
    }
    char * line;
    size_t len = 0;
    ssize_t read;
    int thread_id = *(int *)data_ptr;
    asffline a;
    unsigned long position;
    for(int x=0;x<=thread_id;x++)
    {
        read = getline(&line, &len, fptr2);
        position += read;   
    }
    fclose(fptr2);
    while(1)
    {
        sleep(1);
        printf("Thread [%d] producing.\n", thread_id);
        sem_wait(&full_sem);
        position = readFile(&a, position, thread_id);
        if(position == 0)
        {
            sem_post(&empty_sem);
            continue;
        }
        writeFile(&a);
        printf("Thread [%d] finished producing.\n", thread_id);
        sem_post(&empty_sem);
    }
    return NULL;
}

static void *consumer(void *data_ptr)
{
    asffline as;
    int thread_id = *(int *)data_ptr;
    unsigned long pos = 0L;
    while(1)
    {

        printf("Thread [%d] consuming.\n", thread_id);
        sem_wait(&empty_sem);
        pos = readAsf(&as, pos, thread_id);
        if(pos == 0)
        {
            sem_post(&full_sem);
            continue;
        }
        writeDb(&as);
        deleteLine();
        printf("Thread [%d] finished consuming.\n", thread_id);
        sem_post(&full_sem);
        sleep(1);
    }
    return NULL;
}
