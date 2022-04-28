#include <sys/poll.h>
#include <netinet/in.h>
#include <sys/ioctl.h>

#ifndef MPC_TCP_H
#define MPC_TCP_H

#define TRUE 1
#define FALSE 0

int get_socket(unsigned int party_rank);

char *get_address(unsigned int rank);

// args: the number of parties, assume we have mapping between rank -> ip:port
int TCP_Init();

// set up initial MPC parties connections
int TCP_Connect(int dest);
int TCP_Accept(int source);

// Performs a standard-mode blocking send.
int TCP_Send(const void *buf, int count, int dest, int data_size);

//  Performs a standard-mode blocking receive.
int TCP_Recv(void *buf, int count, int source, int data_size);

// close all socket
int TCP_Finalize();

#endif
