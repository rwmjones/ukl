#include <stdio.h>
#include <assert.h>

#include "test-utils.h"

#define DEBUG 0
#define ROWS1 5
#define COLS1 2

int main(int argc, char** argv) {

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // r1: a relation with 5 rows
  BShare r1s1[ROWS1][COLS1], r1s2[ROWS1][COLS1], r1s3[ROWS1][COLS1];

  if (rank == 0) { //P1
    // Initialize input data and shares
    Data r1[ROWS1][COLS1] = {{1, 42}, {2, 42}, {2, 45}, {2, 23432}, {7, 123}};

    init_sharing();

    // generate r1 shares
    for (int i=0; i<ROWS1; i++) {
        for (int j=0; j<COLS1; j++) {
            generate_bool_share(r1[i][j], &r1s1[i][j], &r1s2[i][j], &r1s3[i][j]);
        }
    }

    //Send shares to P2
    TCP_Send(&r1s2[0][0], 5 * 2, 1, sizeof(BShare));
    TCP_Send(&r1s3[0][0], 5 * 2, 1, sizeof(BShare));
    //Send shares to P3
    TCP_Send(&r1s3[0][0], 5 * 2, 2, sizeof(BShare));
    TCP_Send(&r1s1[0][0], 5 * 2, 2, sizeof(BShare));
  }
  else { // P2 and P3
    TCP_Recv(&r1s1[0][0], 5 * 2, 0, sizeof(BShare));
    TCP_Recv(&r1s2[0][0], 5 * 2, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // allocate BShare tables
  BShareTable table = {-1, rank, ROWS1, 2*COLS1, 1};
  allocate_bool_shares_table(&table);

  // copy shares into the BShareTables
  for (int i=0; i<ROWS1; i++) {
      table.contents[i][0] = r1s1[i][0];
      table.contents[i][1] = r1s2[i][0];
      table.contents[i][2] = r1s1[i][1];
      table.contents[i][3] = r1s2[i][1];
  }

  BitShare res[5]; // distinct array

  distinct(&table, 0, res);

  // reveal the result
  bool out[ROWS1]; /** TODO: only rank0 needs to allocate **/
  open_bit_array(res, ROWS1, out);

  // assert and print result
  if (rank == 0) {
      for (int i=0; i<ROWS1; i++) {
          #if DEBUG
            printf("[%d] Distinct: %d\n", i, out[i]);
          #endif
          if (i==0 || i==1 || i==4) {
              assert(out[i] == 1);
          }
          else {
              assert(out[i] == 0);
          }
      }
      printf("TEST DISTINCT: OK.\n");
  }

  #if DEBUG
    if (rank==0) {
      printf("Test distinct batch\n");
    }
  #endif

  // copy shares into the BShareTables
  for (int i=0; i<ROWS1; i++) {
      table.contents[i][0] = r1s1[i][0];
      table.contents[i][1] = r1s2[i][0];
      table.contents[i][2] = r1s1[i][1];
      table.contents[i][3] = r1s2[i][1];
  }

  distinct_batch(&table, 0, res, 3);

  // reveal the result
  open_bit_array(res, ROWS1, out);

  // assert and print result
  if (rank == 0) {
      for (int i=0; i<ROWS1; i++) {
          #if DEBUG
            printf("[%d] Distinct: %d\n", i, out[i]);
          #endif
          if (i==0 || i==1 || i==4) {
              assert(out[i] == 1);
          }
          else {
              assert(out[i] == 0);
          }
      }
      printf("TEST DISTINCT (BATCH): OK.\n");
  }

  // tear down communication
  TCP_Finalize();
  return 0;
}
