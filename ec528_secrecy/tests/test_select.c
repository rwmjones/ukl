#include <stdio.h>
#include <assert.h>

#include "test-utils.h"

#define DEBUG 0
#define ROWS 10
#define COLS 4
#define EQ_CONST 42

/**
 * SELECT * FROM r WHERE r.1 == 42
**/
int main(int argc, char** argv) {

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // r: relation with 10 rows, 4 columns
  BShare rs1[ROWS][COLS], rs2[ROWS][COLS], rs3[ROWS][COLS];

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  BShare res[ROWS]; // selection result

  // allocate BShare tables
  BShareTable in = {-1, rank, ROWS, 2*COLS, 1};
  allocate_bool_shares_table(&in);

  // copy shares into the BShareTables
  for (int i=0; i<ROWS; i++) {
    for (int j=0; j<COLS; j++) {
        in.contents[i][2*j] = rs1[i][j];
        in.contents[i][2*j+1] = rs2[i][j];
    }
  }

  // leftcol and rightcol point to the column indexes in the BShareTable
  // column 2 is at position 4 and column 3 is at position 6
  // because the BShareTable contains two share-columns per data column
  Predicate_B p = {EQ, NULL, NULL, 4, 6};

  select_b(in, p, res);

  // reveal the result
  Data out[ROWS]; /** TODO: only rank0 needs to allocate **/
  open_b_array(res, ROWS, out);

  // assert and print result
  if (rank == 0) {
      for (int i=0; i<ROWS; i++) {
          #if DEBUG
            printf("[%d] %lld\t", i, out[i]);
          #endif
          if (i<5) {
              assert(out[i] == 1);
          }
          else {
              assert(out[i] == 0);
          }
      }
      printf("TEST SELECT: OK.\n");
  }

  // leftcol and rightcol point to the column indexes in the BShareTable
  // Create secret-shared constant
  BShare cs1 = (rank==0 ? 43 : (rank==1 ? 0 : 0));
  BShare cs2 = (rank==0 ? 0 : (rank==1 ? 0 : 43));
  Predicate_B p2 = {GC, NULL, NULL, 2, -1, cs1, cs2};

  select_b(in, p2, res);

  // reveal the result
  Data out2[ROWS]; /** TODO: only rank0 needs to allocate **/
  open_b_array(res, ROWS, out2);
  // assert and print result
  if (rank == 0) {
      for (int i=0; i<ROWS; i++) {
          #if DEBUG
            printf("[%d] %lld\t", i, out2[i]);
          #endif
          if (i<=5) {
              assert(out2[i] == 0);
          }
          else {
              assert(out2[i] == 1);
          }
      }
      printf("TEST SELECT_CONST: OK.\n");
  }

  // tear down communication
  TCP_Finalize();
  return 0;
}
