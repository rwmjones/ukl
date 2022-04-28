#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define COLS 4

/**
 * Evaluates the performance selection followed by distinct (naive).
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

  // random bits required by group-by
  AShare *ra = malloc(2*(ROWS-1)*sizeof(AShare));
  BShare *rb = malloc(2*(ROWS-1)*sizeof(BShare));

  // initialize rand bits (all equal to 1)
  for (int i=0; i<ROWS; i++) {
    ra[i] = (unsigned int) 1;
    rb[i] = (unsigned int) 1;
  }

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
     Measure naive select-distinct
  ======================================================== */
  /**
   * SELECT DISTINCT a
   * FROM t1
   * WHERE a = 'const'
  **/
  // start timer
  gettimeofday(&begin, 0);

  // sort on attribute
  unsigned int sort_att[1] = {0};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, sort_att, 1, asc, t1.numRows/2);

  BShare *selected = malloc(ROWS*sizeof(BShare));
  assert(selected !=NULL);

  // copy selected bits from t1 into selected
  for (int i=0; i<ROWS; i++) {
    selected[i] = t1.contents[i][2];
  }

  // convert selected bits to arithmetic
  AShare *selected_a = malloc(ROWS*sizeof(BShare));
  assert(selected_a !=NULL);

  convert_single_bit_array(selected, ra, rb, ROWS, selected_a);

  // apply group-by-count on sorted output
  // the results are in join_selected_a
  group_by_count(&t1, 0, selected, selected_a, rb, ra);

  free(selected); free(ra); free(rb);

  BShare *res = malloc(ROWS*sizeof(BShare));
  assert(res !=NULL);

  // copy results for open
  for (int i=0; i<ROWS; i++) {
    res[i] = t1.contents[i][0];
  }

  free(t1.contents);

  Data *open_res = malloc(ROWS*sizeof(Data));
  assert(open_res !=NULL);

  open_b_array(res, ROWS, open_res);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("%ld\tNAIVE select-distinct\t%.3f\n", ROWS, elapsed);
  }

  free(res); free(open_res);

  // tear down communication
  TCP_Finalize();
  return 0;
}
