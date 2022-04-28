#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define COLS1 6
#define COLS2 4

/**
 * Evaluates the performance of optimized join-group-by-count.
 **/

int main(int argc, char** argv) {

  if (argc < 4) {
    printf("\n\nUsage: %s <NUM_ROWS_1> <NUM_ROWS_2> <BATCH_SIZE>\n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS1 = atol(argv[argc - 3]); // input1 size
  const long ROWS2 = atol(argv[argc - 2]); // input2 size
  const int BATCH_SIZE = atoi(argv[argc - 1]); // batch size for left input

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS1, 2*COLS1, 1};
  allocate_bool_shares_table(&t1);
  BShareTable t2 = {-1, rank, ROWS2, 2*COLS2, 2};
  allocate_bool_shares_table(&t2);

  BShare *rb_left = malloc(ROWS1*sizeof(BShare));
  AShare *ra_left = malloc(ROWS1*sizeof(AShare));
  BShare *rb_right = malloc(BATCH_SIZE*ROWS2*sizeof(BShare));
  AShare *ra_right = malloc(BATCH_SIZE*ROWS2*sizeof(AShare));

  // initialize rand bits (all equal to 1)
  for (int i=0; i<ROWS1; i++) {
    rb_left[i] = (unsigned int) 1;
    ra_left[i] = (unsigned int) 1;
  }

  for (int i=0; i<ROWS2; i++) {
    rb_right[i] = (unsigned int) 1;
    ra_right[i] = (unsigned int) 1;
  }

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1, r2;
    generate_random_table(&r1, ROWS1, COLS1);
    generate_random_table(&r2, ROWS2, COLS2);

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS1, 2*COLS1, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS1, 2*COLS1, 1};
    allocate_bool_shares_table(&t13);

    // t2 Bshare tables for P2, P3 (local to P1)
    BShareTable t22 = {-1, 1, ROWS2, 2*COLS2, 2};
    allocate_bool_shares_table(&t22);
    BShareTable t23 = {-1, 2, ROWS2, 2*COLS2, 2};
    allocate_bool_shares_table(&t23);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);
    // Generate boolean shares for r2
    generate_bool_share_tables(&r2, &t2, &t22, &t23);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS1*2*COLS1, 1, sizeof(BShare));
    TCP_Send(&(t22.contents[0][0]), ROWS2*2*COLS2, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS1*2*COLS1, 2, sizeof(BShare));
    TCP_Send(&(t23.contents[0][0]), ROWS2*2*COLS2, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
    free(r2.contents);
    free(t22.contents);
    free(t23.contents);

  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS1*2*COLS1, 0, sizeof(BShare));
    TCP_Recv(&(t2.contents[0][0]), ROWS2*2*COLS2, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  /* =======================================================
     Measure optimized join group-by count
  ======================================================== */
  /**
   * SELECT a, COUNT(a)
   * FROM t1, t2
   * WHERE t1.a=t2.a
   * GROUP BY a
  **/
  // start timer
  gettimeofday(&begin, 0);

  // sort left on group-by attribute
  unsigned int sort_att[1] = {0};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, sort_att, 1, asc, t1.numRows/2);

  // apply group-by-join in batches
  int num_batches = ROWS1 / BATCH_SIZE;
  for (int i=0; i<num_batches; i+=BATCH_SIZE) {
    // apply group-by-join
    group_by_join(&t1, &t2, i, i+BATCH_SIZE, 0, 0, 0, 2,
                rb_left, ra_left, rb_right, ra_right, 2, 4);
  }

  Data *open_res = malloc(ROWS1*sizeof(Data));
  assert(open_res !=NULL);
  AShare *out = malloc(ROWS1*sizeof(AShare));
  assert(out !=NULL);

  // reveal the group_by attribute and the aggregation (sum)
  for (int i=0; i<ROWS1; i++) {
    out[i] = t1.contents[i][2];
  }

  free(t1.contents);
  open_array(out, ROWS1, open_res);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("%ld\tOPT group-by-join\t%.3f\n", ROWS1, elapsed);
  }

  free(out); free(open_res);
  free(ra_right); free(ra_left); free(rb_left); free(rb_right);

  // tear down communication
  TCP_Finalize();
  return 0;
}
