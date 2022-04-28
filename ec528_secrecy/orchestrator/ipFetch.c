// Copyright Sophia Grace Delia sgdelia@bu.edu 2021

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <errno.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 8000 
#define PROTOCOL 0 // Protocol value for IP


int main(int argc, char *argv[]){
// Variables
struct sockaddr_in address;
struct sockaddr_in receivedFrom;
int sizeOfReceive = sizeof(receivedFrom);
int sizeOfAddress = sizeof(address);
char buffer[15];

address.sin_port = htons(PORT);
address.sin_family = AF_INET;
address.sin_addr.s_addr = INADDR_ANY; 

// Create a socket
// AF_INET sets ip for IPV4, SOCK_STREAM initializes for TCP connection
int testSock = socket(AF_INET, SOCK_STREAM, PROTOCOL);
// Test if socket successfully initialized
	if (testSock < 0) {
		perror("socket");
		exit (EXIT_FAILURE);
	} else 
		printf("successful socket initialization\n");
// Bind socket to address and port number, allows for "listening"
	int testBind = bind(testSock, (struct sockaddr*)&address, sizeof(address));
	if (testBind < 0) {
		perror("bind");
		exit (EXIT_FAILURE);
	} else
		printf("Successful port bind\n");
// Listen for connection
// Random number of pending connections for now
	int testListen = listen(testSock, 3);
	if (testListen < 0) {
		perror("listen");
		exit (EXIT_FAILURE);
	} else
		printf("Successful listen\n");
printf("Waiting for connection...\n");
// Accept connection
int testAccept;
int counter = 0;
char* returns[3];

//Allows for constant listening while listening for multiple parties
char ip0[15], ip1[15], ip2[15];
while (testAccept = accept(testSock, (struct sockaddr*)&address, (socklen_t*)&sizeOfAddress))
{
	printf("waiting for accept\n");
	if (testAccept < 0){
		perror("accept");
		exit(EXIT_FAILURE);
	} else {
		printf("accpted\n");
		struct sockaddr_in* pV4Addr = (struct sockaddr_in*)&address;
		struct in_addr ipAddr = pV4Addr->sin_addr;

		char client_ip[15];
		inet_ntop( AF_INET, &ipAddr, client_ip, 15);
		printf("got ip from connection\n");
		
		//stores all ip addresses together
		if (counter == 0){
			strcpy(ip0, client_ip);
			returns[counter] = ip0;
	
		}
		if (counter == 1){
                          strcpy(ip1, client_ip);
                         returns[counter] = ip1;
               }
		if (counter == 2) {
			strcpy(ip2, client_ip);
			returns[counter] = ip2;
		}

		printf("recieved ip: %s\n", returns[counter]);
		memset(client_ip, 0, 15);
		counter++;	
		printf("successful connection accept\n");
		if(counter == 3) {
			break;
		}
	}
}

printf("%s\n%s\n%s\n", returns[0], returns[1], returns[2]);
printf("%d %d %d\n", strlen(returns[0]), strlen(returns[1]), strlen(returns[2]));
//Start to create sockets to send all ip addresses to each party

for(int i = 0; i < 3; i++){

	struct sockaddr_in address;
	int sizeOfAddress = sizeof(address);

	int socket_desc = socket(AF_INET, SOCK_STREAM, 0);
	printf("socket created\n");
	address.sin_port = htons(PORT); // port to connect to
	address.sin_family = AF_INET; // use IPV4
	address.sin_addr.s_addr = inet_addr(returns[i]); // local IP address of client

	int connect_back = connect(socket_desc, (const struct sockaddr *)&address, (socklen_t)sizeof(address));
	printf("socket connected back to party\n");
	//Might still need to make sockets for each send
	int sendCount = 0;
	while (sendCount < 3){
		
		ssize_t n;
		const void* p = returns[sendCount];
		char *message;
	       	message	= returns[sendCount];
		int count = strlen(returns[sendCount]);
		uint32_t msg_size = htonl((uint32_t)count);
    		send(socket_desc, &msg_size, sizeof(msg_size), 0);
		printf("ip to send back: %s\n", message);
		n = send(socket_desc, message, count, 0);	
		sendCount++;
	}
	close(socket_desc);	
}

/*
// Code is a work in progress from here down
// Display message send (eventually written)
	read(testSock, buffer, sizeof(buffer));
	// End of program
*/	
	return 1;
} // end of main
