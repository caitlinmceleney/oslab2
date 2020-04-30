#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <ctype.h>
#include <unistd.h>

int global_count = 0;

//Struct to keep track of the ride info for each ride --> yellow has two extra fields
typedef struct rides
{
    char* vendorId;
    char* lpep_pickup_datetime;
    char* lpep_dropoff_datetime;
    char* store_and_fwd_flag;
    char* ratecodeId;
    char* yellowOnly;
    char* PULocationId;
    char* DOLocationId;
} rides_t;
//Arguments to pass in to count rides
typedef struct count_ride_args
{
    FILE *fp;
    char *pickupId;
    char *destId;
    int count;
    pthread_t tid;
}count_ride_args_t;

//trim the fbuffer to pass in
char *trim(char *var)
{
    char *end = var + strlen(var) - 1;
    char *beginning = var;

    while (isspace(*end))
    {
        *end = '\0';
        end--;
    }
    while(isspace(*beginning))
    {
        *beginning = '\0';
        beginning++;
    }
    memmove(var, beginning, end-beginning+2);
}

//count the green rides with different args than yellow --> csv
int green_count_rides(FILE *fp, char* pickupId, char* destId)
{
    int count = 0;
    char buffer[1024] = {""};
    rides_t ride = {0};
    while(fgets(buffer, 1024, fp))
    {
        char *tmp = buffer;
        ride.vendorId = strtok_r(tmp, ",", &tmp);
        ride.lpep_pickup_datetime = strtok_r(tmp, ",", &tmp);
        ride.lpep_dropoff_datetime = strtok_r(tmp, ",", &tmp);
        ride.store_and_fwd_flag = strtok_r(tmp, ",", &tmp);
        ride.ratecodeId = strtok_r(tmp, ",", &tmp);
        ride.PULocationId = strtok_r(tmp, ",", &tmp);
        ride.DOLocationId = strtok_r(tmp, ",", &tmp);
        //check that the pickupId and destId equal the requested one
        if(strcmp(ride.PULocationId, pickupId) == 0 && strcmp(ride.DOLocationId, destId) == 0){
            count++;
        }
        memset(buffer, 0, 1024);
        memset(&ride, 0, sizeof(rides_t));
    }
    return count;
}
//counting yellow rides
int yellow_count_rides(FILE *fp, char* pickupId, char* destId)
{
    int count = 0;
    char buffer[1024] = {""};
    rides_t ride = {0};
    while(fgets(buffer, 1024, fp))
    {
        char *tmp = buffer;
        ride.vendorId = strtok_r(tmp, ",", &tmp);
        ride.lpep_pickup_datetime = strtok_r(tmp, ",", &tmp);
        ride.lpep_dropoff_datetime = strtok_r(tmp, ",", &tmp);
        ride.store_and_fwd_flag = strtok_r(tmp, ",", &tmp);
        ride.ratecodeId = strtok_r(tmp, ",", &tmp);
        ride.yellowOnly  =  strtok_r(tmp, ",", &tmp);
        ride.yellowOnly  =  strtok_r(tmp, ",", &tmp);
        ride.PULocationId =  strtok_r(tmp, ",", &tmp);
        ride.DOLocationId = strtok_r(tmp, ",", &tmp);
        //check that pickupId and destId equal the requested one
        if(strcmp(ride.PULocationId, pickupId) == 0 && strcmp(ride.DOLocationId, destId) == 0){
            count++;
        }
        memset(buffer, 0, 1024);
        memset(&ride, 0, sizeof(rides_t));
    }
    return count;
}
//wrapper to pass the yellow ride data into pthreads_create
void *yellow_count_rides_wrapper(void *data)
{
    count_ride_args_t *arg = data;
    arg->count = yellow_count_rides(arg->fp, arg->pickupId, arg->destId);
    return NULL;
}
//wrapper to pass the yellow ride data into pthreads_create
void *green_count_rides_wrapper(void *data)
{
    count_ride_args_t *arg = data;
    arg->count = green_count_rides(arg->fp, arg->pickupId, arg->destId);
    return NULL;
}
//Version 0 - Single process, Single thread
void version_zero(FILE* fp, char* pickupId, char* destId)
{
    char fbuffer[100] = {0};
    while(fgets(fbuffer, 100, fp))
    {
        FILE *newFile = fopen(trim(fbuffer), "r");
        if(newFile)
        {
            if(strstr(fbuffer, "yellow") != NULL)
            {
                global_count += yellow_count_rides(newFile, pickupId, destId);
            }else
            {
                global_count += green_count_rides(newFile, pickupId, destId);
            }
            
        }else
        {
            break;
        }
    }
}
//Version 1 - Multiprocess, Single Thread
void version_one(FILE *fp, char *pickupId, char *destId)
{
    //pipe description array
    int pipefds[20] = {0};
    char fbuffer[100] = "";
    char readbuffer[100] = "";
    char writebuffer[100] = "";
    int i = 0;
    for (i = 0; i < 20; i += 2)
    {
        if (!fgets(fbuffer, 100, fp))
        {
            i-=2;
            break;
        }
        pipe(&pipefds[i]);
        int pid = fork();
        //if child
        if (pid==0)
        {
            //open new file
            FILE *newFile = fopen(trim(fbuffer), "r");
            if (newFile)
            {
                if (strstr(fbuffer, "yellow") != NULL)
                {
                    snprintf(writebuffer, 100, "%d", yellow_count_rides(newFile, pickupId, destId));
                    write(pipefds[i + 1], writebuffer, strlen(writebuffer));
                }
                else
                {
                    snprintf(writebuffer, 100, "%d", green_count_rides(newFile, pickupId, destId));
                    write(pipefds[i + 1], writebuffer, strlen(writebuffer));
                }
                //close the pipes
                close(pipefds[i]);
                close(pipefds[i+1]);
            }
            //error with newFile
            else
            {
                exit(1);
            }
            exit(0);
        }
    }
	//wait(NULL);
    for(int j = 0; j <=i; j+=2)
    {
        read(pipefds[j], readbuffer, sizeof(readbuffer));
        int count = atoi(readbuffer);
        global_count += count;
    }   
    return;
}

//One Process / Multithreaded
void version_two(FILE* fp, char* thispickupId, char* thisdestId)
{
    char fbuffer[100];
    int i = 0;
    void *retval = NULL;
    count_ride_args_t *rideinfo = 0;
    count_ride_args_t rideinfoarr[20] = {0};
    for (int i=0; i < 20; i++)
    {
        if (!fgets(fbuffer, 100, fp))
        {
            break;
        }
        rideinfo = &rideinfoarr[i];
        rideinfo -> pickupId = thispickupId;
        rideinfo -> destId = thisdestId;
        FILE *newFile = fopen(trim(fbuffer), "r");
        rideinfo -> fp = newFile;
        if(newFile)
        { 
         if(strstr(fbuffer, "yellow") != NULL)
            {                
                pthread_create(&rideinfo->tid, NULL, yellow_count_rides_wrapper, (void*)rideinfo);
            }else
            {
                pthread_create(&rideinfo->tid, NULL, green_count_rides_wrapper, (void*)rideinfo);
            }
            i++;
        }else
        {
            break;
        }
    }

    global_count = 0;
    for (int i=0; i < 20; i++)
    {
        rideinfo = &rideinfoarr[i];
      if(&rideinfo->count == 0){
        break;
      }
      else{
        pthread_join(rideinfo->tid, &retval);
       global_count += rideinfo->count;
      }
    }
}


int main(int argc, char** argv)
{
    char* filename = argv[1];
    char* pickupId = argv[2];
    char* destId = argv[3];
    char* versiontype = argv[4];
    FILE *fp = fopen(filename, "r");

    if(!fp)
    {
        printf("Error opening file\n");
        return 1;
    }
    if(strcmp(versiontype, "0") == 0)
    {
        version_zero(fp, pickupId, destId);
    }
    if(strcmp(versiontype, "1") == 0)
    {
        version_one(fp, pickupId, destId);
    }
    if(strcmp(versiontype, "2") == 0)
    {
        version_two(fp, pickupId, destId);
    }
    fclose(fp);

    printf("Number of Rides: %d\n", global_count);
    return 0;
}