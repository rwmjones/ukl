#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h> 
#include <netinet/in.h> 
#include <string.h> 
#include <pthread.h> 

#define QUEUE_SIZE 5

// __thread int counter = 10;
// __thread int nocounter;

void *printer(int * id);

int pmain(void){

	pthread_t thread_id[QUEUE_SIZE];
	int i = 0;


	for(i = 0; i < QUEUE_SIZE; i++){
		printukl("creating %d\n", i);
		if (pthread_create( &thread_id[i] , NULL , printer , (void *)i) < 0) {
			return 1;
		}	
	}

	for(i = 0; i < QUEUE_SIZE; i++){
		if (pthread_join(thread_id[i], NULL) < 0) {
			perror("could not join thread");
			return 1;
		}else{
			printukl("pthread_join worked for thread id %d\n",i);
		}
	}
	
	return 0;
}

void *printer(int * id)
{
	int i = 0;
	for(i = 0; i < 10; i++){
		printukl("Thread %d = A\n", id);
		printukl("Thread %d = quick\n", id);
		printukl("Thread %d = brown\n", id);
		printukl("Thread %d = fox\n", id);
		printukl("Thread %d = jumped\n", id);
		printukl("Thread %d = over\n", id);
		printukl("Thread %d = a\n", id);
		printukl("Thread %d = lazy\n", id);
		printukl("Thread %d = dog.\n", id);
	}
	return 0;
} 

int kmain(void){
        pthread_t thk;
        if (pthread_create (&thk, NULL, pmain, NULL) != 0)
        {
                printf("testmain create failed");
                return 1;
        }

        return 0;
}

