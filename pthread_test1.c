#include<stdlib.h>
#include<stdio.h>
#include<pthread.h>

#define N 10


void * thread_func(void * data){
	printf("Thread No. %d\n", (int)data);
}

int main(){
	int i = 0;
	pthread_t threads[N];

	printf("Creating %d Threads\n", N);
	for(i = 0; i < N; i++){
		pthread_create(&threads[i], NULL, thread_func, (void*) i);
	}

	for(i = 0; i < N; i++){
		printf("Joining thread %d\n", i);
		pthread_join(threads[i], NULL);
	}

	return 0;
}

