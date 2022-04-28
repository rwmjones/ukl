#include <stdio.h>
#include <assert.h>

#include "test-utils.h"

int main(int argc, char **argv)
{
    char *ip = "127.0.0.1";

    establish_connection(ip, 8081);
    establish_connection(ip, 8082);
    establish_connection(ip, 8080);

    return 0;
}