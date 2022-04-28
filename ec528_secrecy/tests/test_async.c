#include <stdio.h>
#include <assert.h>

#include "test-utils.h"

#define DEBUG 0
#define ROWS1 5
#define COLS1 2
#define ROWS2 4
#define COLS2 2

int main(int argc, char **argv)
{

    // initialize communication
    init(argc, argv);
    char buf[10];
    BShare sr;
    const int rank = get_rank();
    const int pred = get_pred();
    const int succ = get_succ();

    // r1: first relation with 5 rows, r2: second relation with 4 rows
    BShare r1s1[ROWS1][COLS1], r1s2[ROWS1][COLS1], r1s3[ROWS1][COLS1],
        r2s1[ROWS2][COLS2], r2s2[ROWS2][COLS2], r2s3[ROWS2][COLS2];
    r1s1[0][0] = 1;
    if (rank == 0)
    { // P1
        // Initialize input data and shares
        Data r1[ROWS1][COLS1] = {{1, 42}, {2, 42}, {3, 42}, {4, 42}, {5, 42}};
        Data r2[ROWS2][COLS2] = {{1, 99}, {3, 99}, {5, 99}, {7, 99}};

        init_sharing();

        // generate r1 shares
        for (int i = 0; i < ROWS1; i++)
        {
            for (int j = 0; j < COLS1; j++)
            {
                generate_bool_share(r1[i][j], &r1s1[i][j], &r1s2[i][j], &r1s3[i][j]);
            }
        }

        // generate r2 shares
        for (int i = 0; i < ROWS2; i++)
        {
            for (int j = 0; j < COLS2; j++)
            {
                generate_bool_share(r2[i][j], &r2s1[i][j], &r2s2[i][j], &r2s3[i][j]);
            }
        }

        TCP_Send(r1s1[0][0], 1, get_pred(), sizeof(BShare));
        TCP_Recv(r1s1[0][0], 1, get_succ(), sizeof(BShare));
        printf("receive %lld", r1s1[0][0]);
    }
    else if (rank == 1)
    {
        sr = exchange_shares(r1s1[0][0]);
        printf("receive %lld", sr);
    }
    else
    {
        sr = exchange_shares(r1s1[0][0]);
        printf("receive %lld", sr);
    }
}
