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
int sizeOfAddress = sizeof(address);

char *pseudoIP = "FAKEIP";

address.sin_port = htons(PORT); // port to connect to
address.sin_family = AF_INET; // use IPV4
address.sin_addr.s_addr = inet_addr("10.0.0.23"); // local IP address of client

// Create a socket

int clientSock = socket(AF_INET, SOCK_STREAM, PROTOCOL);
// test if successfully initialized
	if (clientSock < 0){
		perror("socket");
		exit (EXIT_FAILURE);
	} else 
		printf("Successful socket creation\n");

// connect client socket to server socket
int clientConnect = connect(clientSock, (struct sockaddr*)&address, sizeOfAddress);

// test if successfully initialized
	if (clientConnect < 0){
		perror("connect");
		exit (EXIT_FAILURE);
	} else 
		printf("Successful connection\n");
close(clientConnect);

// receive message
struct sockaddr_in receive;
int sizeofRec = sizeof(receive);

receive.sin_family = AF_INET;
receive.sin_port = htons(PORT);
receive.sin_addr.s_addr = INADDR_ANY;

int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
if(listen_sock < 0)
{
	perror("sock creation: ");
	exit(0);
}
printf("sock created\n");
int sock_bind = bind(listen_sock, (struct sockaddr *)&receive, sizeof(receive));
if (sock_bind < 0)
{
	perror("bind 1 fail: ");
	exit(0);
}
printf("successful bind\n");
int rec_listen = listen(listen_sock, 1);
if (rec_listen < 0)
{
	perror("listen 1 error: ");
	exit(0);
}
printf("listening\n");
int r_accept = accept(listen_sock, (struct sockaddr *)&receive, (socklen_t *)&sizeofRec);
if(r_accept < 0)
{
	perror("accept error");
	exit(0);
}
printf("accepted connection\n");

// Reading from listen_sock

// read in from buffer
int ipCount = 0;
// open file in write mode
FILE *fptr = fopen("ipAddress.txt", "w");
while (ipCount <3){

	// write to file
	uint32_t msg_size;
    	int valread = read(r_accept, &msg_size, sizeof(msg_size));
    	msg_size = ntohl(msg_size);
	char buffer[(int)msg_size];
	int ip = recv(r_accept, buffer, msg_size, 0);

	// error check
	if (ip <0){
		perror("read");
		exit(0);
	}

	// write to file
	fputs(buffer, fptr);
	fputc('\n', fptr);
	ipCount++;
}
// close file
fclose(fptr);

close(listen_sock);
return 0;
}
