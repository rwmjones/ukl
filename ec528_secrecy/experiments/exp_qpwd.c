#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define DEBUG 0
#define COLS1 3

/**
 * Evaluates the correctness of Q3 in the Senate paper (password reuse).
 **/

int main(int argc, char** argv) {

  if (argc < 3) {
    printf("\n\nUsage: %s <NUM_ROWS> <BATCH_SIZE>\n\n", argv[0]);
    return -1;
  }

  const long ROWS1 = atol(argv[argc - 2]);      // input size
  const long BATCH_SIZE = atol(argv[argc - 1]); // batch size

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS1, 2*COLS1, 1}; // {uid, pwd, cnt}
  allocate_bool_shares_table(&t1);

  BShare *rb = malloc(ROWS1*sizeof(BShare));
  assert(rb!=NULL);
  AShare *ra = malloc(ROWS1*sizeof(AShare));
  assert(ra!=NULL);
  BShare *rand_b = malloc(2*(ROWS1-1)*sizeof(BShare));
  assert(rand_b!=NULL);
  AShare *rand_a = malloc(2*(ROWS1-1)*sizeof(AShare));
  assert(rand_a!=NULL);

  // initialize rand bits for conversion (all equal to 0)
  for (int i=0; i<ROWS1; i++) {
    ra[i] = (unsigned int) 0;
    rb[i] = (unsigned int) 0;
  }

  // initialize rand bits for group-by (all equal to 0)
  for (int i=0; i<2*(ROWS1-1); i++) {
    rand_a[i] = (unsigned int) 0;
    rand_b[i] = (unsigned int) 0;
  }

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1;
    generate_random_table(&r1, ROWS1, COLS1);
    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS1, 2*COLS1, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS1, 2*COLS1, 1};
    allocate_bool_shares_table(&t13);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS1*2*COLS1, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS1*2*COLS1, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);

  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS1*2*COLS1, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  // start timer
  gettimeofday(&begin, 0);

  // STEP 1: SORT t1 on (uid,pwd) (attributes 1 and 2)
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  unsigned int att_index[2] = {0,2};
  bool asc[2] = {1,1};
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS1/2);

  // STEP 2: GROUP-BY-COUNT on (uid,pwd)
  #if DEBUG
    if (rank==0) {
      printf("Group-by.\n");
    }
  #endif
  unsigned key_indices[2] = {0,2};
  // Create 'selected' bit shares
  BShare sel_b = rank % 2;
  AShare sel_a = rank % 2;
  BShare* selected_b = malloc(ROWS1*sizeof(BShare));
  assert(selected_b!=NULL);
  for (int i=0; i<ROWS1; i++)
    selected_b[i] = sel_b;
  BShare* selected_a = malloc(ROWS1*sizeof(AShare));
  assert(selected_a!=NULL);
  for (int i=0; i<ROWS1; i++)
    selected_a[i] = sel_a;
  group_by_count(&t1, key_indices, 2, selected_b, selected_a, rand_b, rand_a);
  free(rand_a); free(rand_b);

  // Reuse ra to store remote arithmetic shares
  exchange_a_shares_array(selected_a, ra, ROWS1);

  // STEP 3: Convert arithmetic shares to binary shares
  #if DEBUG
    if (rank==0) {
      printf("Conversion.\n");
    }
  #endif
  // Reuse rb and selected_b to store binary shares of the counts
  convert_a_to_b_array(selected_a, ra, rb, selected_b, ROWS1);
  free(ra); free(selected_a);

  // Copy the binary counter to the last column of t1
  for (int i=0; i<ROWS1; i++) {
    t1.contents[i][4] = rb[i];          // Local share of count
    t1.contents[i][5] = selected_b[i];  // Remote share of count
  }
  free(rb); free(selected_b);

  // Step 4: Select tuples with count > 1
  #if DEBUG
    if (rank==0) {
      printf("Selection.\n");
    }
  #endif
  BShare sel_s = get_succ() % 2;
  Predicate_B p = {GC, NULL, NULL, 4, -1, sel_b, sel_s};
  BShare* sel_results = malloc(ROWS1*sizeof(BShare));
  select_b(t1, p, sel_results);
  // Step 5: Mask not selected tuples
  #if DEBUG
    if (rank==0) {
      printf("Masking.\n");
    }
  #endif
  mask(&t1, sel_results, BATCH_SIZE);
  free(sel_results);
  // Step 6: Shuffle before opening -- TODO: This can be done in O(n)
  #if DEBUG
    if (rank==0) {
      printf("Shuffling.\n");
    }
  #endif
  unsigned atts[1] = {0};
  bool asc2[1] = {1};
  bitonic_sort_batch(&t1, atts, 1, asc2, ROWS1/2);

  // Open first two attributes to the learner
  #if DEBUG
    if (rank==0) {
      printf("Open.\n");
    }
  #endif
  // Tuples are opened in batches of size BATCH_SIZE
  Data* result = malloc(BATCH_SIZE*2*sizeof(BShare));
  assert(result!=NULL);
  Data* opened = malloc(BATCH_SIZE*2*sizeof(BShare));
  assert(opened!=NULL);
  int start=0, limit=BATCH_SIZE, step;
  while (start<t1.numRows) {
    for (int i=start, k=0; i<limit; i++, k+=2) {
      result[k] = t1.contents[i][0]; // id
      result[k+1] = t1.contents[i][2]; // pwd
    }
    open_b_array(result, BATCH_SIZE*2, opened);
    // Next step
    start = limit;
    step = limit + BATCH_SIZE;
    limit = (step <= t1.numRows ? step : t1.numRows);
  }

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  // Print timing information
  if (rank == 0) {
    printf("%ld\t%.3f\n", ROWS1, elapsed);
  }

  free(t1.contents); free(result); free(opened);

  // tear down communication
  TCP_Finalize();
  return 0;
}
