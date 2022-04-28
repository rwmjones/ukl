#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "test-utils.h"

int main(int argc, char **argv)
{
    init(argc, argv);
    TCP_Finalize();
    return 0;
}