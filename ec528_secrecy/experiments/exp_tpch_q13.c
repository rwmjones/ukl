#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define DEBUG 0
#define COLS_C 3
#define COLS_O 5
#define C 42

/**
 * Evaluates the performance of TPC-H Q13.
 **/

int main(int argc, char** argv) {

  if (argc < 4) {
    printf("\n\nUsage: %s <NUM_ROWS_CUSTOMER> <NUM_ROWS_ORDERS> <BATCH_SIZE>\n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS_C = atol(argv[argc - 3]); // input1 size
  const long ROWS_O = atol(argv[argc - 2]); // input2 size
  const int BATCH_SIZE = atol(argv[argc - 1]); //batch size for semi-join

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS_C, 2*COLS_C, 1};
  allocate_bool_shares_table(&t1);
  BShareTable t2 = {-1, rank, ROWS_O, 2*COLS_O, 2};
  allocate_bool_shares_table(&t2);

  BShare *rb_left = calloc(ROWS_C, sizeof(BShare));
  AShare *ra_left = calloc(ROWS_C, sizeof(AShare));
  BShare *rb_right = calloc(ROWS_O*BATCH_SIZE, sizeof(BShare));
  AShare *ra_right = calloc(ROWS_O*BATCH_SIZE, sizeof(AShare));

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1, r2;
    generate_random_table(&r1, ROWS_C, COLS_C);
    generate_random_table(&r2, ROWS_O, COLS_O);

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS_C, 2*COLS_C, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS_C, 2*COLS_C, 1};
    allocate_bool_shares_table(&t13);

    // t2 Bshare tables for P2, P3 (local to P1)
    BShareTable t22 = {-1, 1, ROWS_O, 2*COLS_O, 2};
    allocate_bool_shares_table(&t22);
    BShareTable t23 = {-1, 2, ROWS_O, 2*COLS_O, 2};
    allocate_bool_shares_table(&t23);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);
    // Generate boolean shares for r2
    generate_bool_share_tables(&r2, &t2, &t22, &t23);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS_C*2*COLS_C, 1, sizeof(BShare));
    TCP_Send(&(t22.contents[0][0]), ROWS_O*2*COLS_O, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS_C*2*COLS_C, 2, sizeof(BShare));
    TCP_Send(&(t23.contents[0][0]), ROWS_O*2*COLS_O, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
    free(r2.contents);
    free(t22.contents);
    free(t23.contents);

  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS_C*2*COLS_C, 0, sizeof(BShare));
    TCP_Recv(&(t2.contents[0][0]), ROWS_O*2*COLS_O, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  // start timer
  gettimeofday(&begin, 0);

  // STEP 1: SORT CUSTOMER on C_CUSTOMERKEY (0)
  #if DEBUG
    if (rank==0) {
      printf("ORDER-BY C_CUSTOMERKEY.\n");
    }
  #endif
  unsigned int att_index[1] = {0};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS_C/2);

  // STEP 2: SELECTION ON ORDERS
  #if DEBUG
    if (rank==0) {
      printf("1st SELECTION.\n");
    }
  #endif

  BShare *sel = malloc(ROWS_O*sizeof(BShare));
  assert(sel!=NULL);

  Predicate_B p = {EQ, NULL, NULL, 4, 6};
  select_b(t2, p, sel);

  // compute not selected
  for (int i=0; i<ROWS_O; i++) {
      sel[i] ^= 1;
  }

  /** Convert selected bits to arithmetic **/
  AShare *sel_a = malloc(ROWS_O*sizeof(AShare));
  assert(sel_a!=NULL);
  AShare *rem_sel_a = malloc(ROWS_O*sizeof(AShare));
  assert(rem_sel_a!=NULL);

  convert_single_bit_array(sel, ra_right, rb_right, ROWS_O, sel_a);
  exchange_a_shares_array(sel_a, rem_sel_a, ROWS_O);

  free(sel);

  // Copy arithmetic selection bits to columns 8, 9
  for (int i=0; i<ROWS_O; i++) {
    t2.contents[i][8] = sel_a[i];
    t2.contents[i][9] = rem_sel_a[i];
  }

  free(sel_a); free(rem_sel_a);

  // STEP 3: Fused group-by-cnt-join
  #if DEBUG
    if (rank==0) {
      printf("GROUP-BY & JOIN.\n");
    }
  #endif

  // init sum and count columns of the left table
  for (int i=0; i<ROWS_C; i++) {
    t1.contents[i][2] = 0;
    t1.contents[i][3] = 0;
    t1.contents[i][4] = rank % 2;
    t1.contents[i][5] = succ % 2;
  }

  // apply group-by-join in batches
  for (int i=0; i<ROWS_C; i+=BATCH_SIZE) {
    // apply group-by-join
    group_by_join(&t1, &t2, i, i+BATCH_SIZE, 0, 0, 2, 8,
                rb_left, ra_left, rb_right, ra_right, 2, 4);
  }

  free(ra_left); free(rb_left); free(ra_right); free(rb_right);

  AShare *c_counts = malloc(ROWS_C*sizeof(AShare));
  AShare *c_remote_counts = malloc(ROWS_C*sizeof(AShare));
  BShare *c_counts_b = malloc(ROWS_C*sizeof(BShare));
  BShare *c_remote_counts_b = malloc(ROWS_C*sizeof(BShare));

  for (int i=0; i<ROWS_C; i++) {
    c_counts[i] = t1.contents[i][2];
  }

  exchange_a_shares_array(c_counts, c_remote_counts, ROWS_C);

  // Convert c_count to binary **/
  convert_a_to_b_array(c_counts, c_remote_counts, c_counts_b, c_remote_counts_b, ROWS_C);

  // Copy binary c_counts to columns 2, 3
  for (int i=0; i<ROWS_C; i++) {
    t1.contents[i][2] = c_counts_b[i];
    t1.contents[i][3] = c_remote_counts_b[i];
  }

  free(c_counts); free(c_remote_counts); free(c_counts_b); free(c_remote_counts_b);

  // STEP 4: SORT ON C_COUNT (t1.2)
  #if DEBUG
    if (rank==0) {
      printf("ORDER-BY C_COUNT.\n");
    }
  #endif
  att_index[0] = 2;
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS_C/2);

  // STEP 5: GROUP-BY-COUNT on C_COUNT (t1.2)
  #if DEBUG
    if (rank==0) {
      printf("GROUP-BY on C_COUNT.\n");
    }
  #endif

  AShare *rand_a = calloc(2*(ROWS_C-1), sizeof(AShare));
  BShare *rand_b = calloc(2*(ROWS_C-1), sizeof(BShare));
  AShare *counters = malloc(ROWS_C*sizeof(AShare));
  AShare *remote_counters = malloc(ROWS_C*sizeof(AShare));

  // initialize counters
  for (int i=0; i<ROWS_C; i++) {
    counters[i] = rank % 2;
    remote_counters[i] = succ % 2;
  }

  unsigned key_indices[1] = {2};
  group_by_count_micro(&t1, key_indices, 1, counters, remote_counters,
                        rand_b, rand_a);

  free(rand_a); free(rand_b);

  BShare *counters_b = malloc(ROWS_C*sizeof(BShare));
  BShare *remote_counters_b = malloc(ROWS_C*sizeof(BShare));

  // Convert custdist to binary
  convert_a_to_b_array(counters, remote_counters, counters_b, remote_counters_b, ROWS_C);

  free(counters); free(remote_counters);

  // copy custdist (counters) to columns 4, 5
  for (int i=0; i<ROWS_C; i++) {
    t1.contents[i][4] = counters_b[i];
    t1.contents[i][5] = remote_counters_b[i];
  }

  // STEP 6: FINAL ORDER-BY custdist desc, c_count desc
  #if DEBUG
    if (rank==0) {
      printf("ORDER-BY CUSTDIST, C_COUNT.\n");
    }
  #endif
  unsigned int ind[2] = {4, 2};
  bool asc2[2] = {0, 0};
  bitonic_sort_batch(&t1, ind, 2, asc2, ROWS_C/2);

  // Open result
  BShare *s_result = malloc(2*ROWS_C*sizeof(BShare));
  Data *result = malloc(2*ROWS_C*sizeof(Data));
  for (int i=0; i<ROWS_C; i+=2) {
    s_result[i] = t1.contents[i][2]; // C_COUNT
    s_result[i+1] = t1.contents[i][4]; // CUSTDIST
  }
  open_b_array(s_result, 2*ROWS_C, result);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("\tTPCH-Q13\t%ld\t%ld\t%d\t%.3f\n", ROWS_C, ROWS_O, BATCH_SIZE, elapsed);
  }

  free(t1.contents); free(t2.contents); free(result); free(s_result);

  // tear down communication
  TCP_Finalize();
  return 0;
}
