#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define COLS 4

/**
 * Evaluates the performance selection followed by distinct (optimized).
 **/

int main(int argc, char** argv) {

  if (argc < 2) {
    printf("\n\nUsage: %s <NUM_ROWS>\n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS = atol(argv[argc - 1]); // input size

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS, 2*COLS, 1};
  allocate_bool_shares_table(&t1);

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1;
    generate_random_table(&r1, ROWS, COLS);

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS, 2*COLS, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS, 2*COLS, 1};
    allocate_bool_shares_table(&t13);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS*2*COLS, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS*2*COLS, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS*2*COLS, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  /* =======================================================
     Measure optimized select-distinct
  ======================================================== */
  /**
   * SELECT DISTINCT a
   * FROM t1
   * WHERE a = 'const'
  **/
  // start timer
  gettimeofday(&begin, 0);

  // sort on selected bit desc fist and then attribute
  unsigned int sort_att[2] = {2, 0};
  bool asc[2] = {0, 0};
  bitonic_sort_batch(&t1, sort_att, 2, asc, t1.numRows/2);

  // apply distinct on t1
  BitShare *res_distinct = malloc(ROWS*sizeof(BitShare));
  distinct_batch(&t1, 0, res_distinct, t1.numRows - 1);

  // exchange distinct result
  BitShare *res_distinct_remote = malloc(ROWS*sizeof(BitShare));
  exchange_bit_shares_array(res_distinct, res_distinct_remote, ROWS);

  // Open records where b_open = 1
  // b_open = b_distinct and b_selected
  BShare max=0xFFFFFFFFFFFFFFFF;
  BShare *b_open = malloc(ROWS*sizeof(BShare));
  assert(b_open !=NULL);
  BShare *b_open_remote = malloc(ROWS*sizeof(BShare));
  assert(b_open_remote !=NULL);

  for (int i=0; i< ROWS; i++) {
    b_open[i] = and_b(res_distinct[i], res_distinct_remote[i],
                          t1.contents[i][2], t1.contents[i][3],
                          get_next_rb()) & 1;
  }

  free(res_distinct); free(res_distinct_remote);
  exchange_shares_array(b_open, b_open_remote, ROWS);

  BShare *res = malloc(ROWS*sizeof(BShare));
  assert(res !=NULL);

  for (int i=0; i<ROWS; i++) {
    BShare b1 = -b_open[i];
    BShare b2 = -b_open_remote[i];
    // res = b_open * att + (1-b) * dummy
    res[i] = and_b(b1, b2,
                      t1.contents[i][0], t1.contents[i][1],
                      get_next_rb());
    res[i] ^= and_b(~b1, ~b2,
                      max, max,
                      get_next_rb());
  }
  free(t1.contents);
  free(b_open); free(b_open_remote);

  Data *open_res = malloc(ROWS*sizeof(Data));
  assert(open_res !=NULL);

  open_b_array(res, ROWS, open_res);
  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("%ld\tOPT select-distinct\t%.3f\n", ROWS, elapsed);
  }

  free(res); free(open_res);

  // tear down communication
  TCP_Finalize();
  return 0;
}
