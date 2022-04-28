#include <stdio.h>
#include <assert.h>

#include "test-utils.h"

#define DEBUG 0
#define ROWS1 8
#define COLS1 4

int main(int argc, char** argv) {

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // Selected bits
  BShare r[ROWS1] = {1,1,1,0,1,1,1,0};

  // r1(user id1, user id2, score, score)
  BShare r1s1[ROWS1][COLS1], r1s2[ROWS1][COLS1], r1s3[ROWS1][COLS1];
  BShare rs[ROWS1];

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // test group_by_join
  #if DEBUG
    if (rank == 0) {
      printf("\nTesting mask\n");
    }
  #endif

  // allocate BShare tables
  BShareTable in1 = {-1, rank, ROWS1, 2*COLS1, 1};
  allocate_bool_shares_table(&in1);

  // copy shares into the BShareTables
  for (int i=0; i<ROWS1; i++) {
      in1.contents[i][0] = r1s1[i][0];
      in1.contents[i][1] = r1s2[i][0];
      in1.contents[i][2] = r1s1[i][1];
      in1.contents[i][3] = r1s2[i][1];
      in1.contents[i][4] = r1s1[i][2];
      in1.contents[i][5] = r1s2[i][2];
      in1.contents[i][6] = r1s1[i][3];
      in1.contents[i][7] = r1s2[i][3];
  }

  mask(&in1, rs, 3);

  // Open the group-by output
  Data out[ROWS1*COLS1], opened[ROWS1*COLS1];
  for (int i=0; i<in1.numRows; i++) {
    for (int j=0; j<in1.numCols; j++) {
      out[i*COLS1+j] = in1.contents[i][2*j];
    }
  }
  open_b_array(out, ROWS1*COLS1, opened);

  // assert and print result
  if (rank == 0) {
    #if DEBUG
    printf("Output (open): \n");
    #endif
    Data max = 0xFFFFFFFFFFFFFFFF;
    for (int i=0; i<ROWS1*COLS1; i+=COLS1) {
        // r = {1,1,1,0,1,1,1,0};
        // r1 = {{1, 10, 1, 1}, {1, 10, 3, 3}, {1, 10, 6, 6},
        //       {4, 11, 2, 12}, {4, 12, 7, 7},
        //       {6, 12, 11, 11}, {6, 12, 4, 4}, {6, 12, 2, 2}};
        Data gz[ROWS1][COLS1] = {{1, 10, 1, 1}, {1, 10, 3, 3},
                                 {1, 10, 6, 6}, {max, max, max, max},
                                 {4, 12, 7, 7}, {6, 12, 11, 11},
                                 {6, 12, 4, 4}, {max, max, max, max}};
        #if DEBUG
        printf("%lld %lld %lld %lld\n", opened[i], opened[i+1],
                                        opened[i+2], opened[i+3]);
        #endif
        if (i==12 || i==28) {
          assert(opened[i+0] == max);
          assert(opened[i+1] == max);
          assert(opened[i+2] == max);
          assert(opened[i+3] == max);
        }
        else {
          assert(opened[i+0] == gz[i/4][0]);
          assert(opened[i+1] == gz[i/4][1]);
          assert(opened[i+2] == gz[i/4][2]);
          assert(opened[i+3] == gz[i/4][3]);
        }
    }
    printf("TEST MASK: OK.\n");
  }

  // tear down communication
  TCP_Finalize();
  return 0;
}
