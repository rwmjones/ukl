#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/poll.h>
#include <netinet/tcp.h>
#include "mpc_tcp.h"
#include "comm.h"
#include "config.h"

int succ_sock;
int pred_sock;

extern struct secrecy_config config;

int get_socket(unsigned int party_rank)
{
    if (party_rank == get_succ())
    {
        return succ_sock;
    }
    else
    {
        return pred_sock;
    }
}

/* get the IP address of given rank */
char *get_address(unsigned int rank)
{
    if (rank < config.num_parties)
    {
        return config.ip_list[rank];
    }
    else
    {
        printf("No such rank!");
        return NULL;
    }

    return NULL;
}

int TCP_Init()
{
    /* init party 0 last */
    if (get_rank() == 0)
    {
        TCP_Connect(get_succ());
        TCP_Accept(get_pred());
    }
    else
    {
        TCP_Accept(get_pred());
        TCP_Connect(get_succ());
    }

    return 0;
}

int TCP_Finalize(){

    close(succ_sock);
    close(pred_sock);

}

int TCP_Connect(int dest)
{

    int sock = 0, option = 1;
    struct sockaddr_in serv_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Socket creation error \n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(config.port);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, get_address(dest), &serv_addr.sin_addr) <= 0)
    {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    int result = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &option, sizeof(int));
    if (result)
    {
	perror("Error setting TCP_NODELAY ");
	return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("Connection Failed ");
        return -1;
    }

    succ_sock = sock;
}

int TCP_Accept(int source)
{

    int server_fd, new_socket, valread;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    int result = setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(int));
    if (result)
    {
	perror("Error setting TCP_NODELAY ");
	exit(EXIT_FAILURE);
    }


    // Forcefully attaching socket to the port
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(config.port);

    // Forcefully attaching socket to the port
    if (bind(server_fd, (struct sockaddr *)&address,
             sizeof(address)) < 0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 3) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address,
                             (socklen_t *)&addrlen)) < 0)
    {
        perror("accept");
        exit(EXIT_FAILURE);
    }

    pred_sock = new_socket;
}

int TCP_Send(const void *buf, int count, int dest, int data_size)
{

    ssize_t n;
    const void *p = buf;
    count = count * data_size;
    while (count > 0)
    {
        n = send(get_socket(dest), p, count, 0);
        if (n <= 0)
            return -1;
        p += n;
        count -= n;
    }

    return 0;
}

int TCP_Recv(void *buf, int count, int source, int data_size)
{
    ssize_t n;
    const void *p = buf;
    count = count * data_size;
    while (count > 0)
    {
        n = read(get_socket(source), p, count);
        if (n <= 0)
            return -1;
        p += n;
        count -= n;
    }

    return 0;
}
