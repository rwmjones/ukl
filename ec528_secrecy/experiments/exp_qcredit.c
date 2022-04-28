#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define DEBUG 0
#define COLS1 5

/**
 * Evaluates the correctness of Q4 in the Senate paper (credit score).
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

  // The input table t(uid, year-2019, 2019-year, score, score+threshold)
  BShareTable t1 = {-1, rank, ROWS1, 2*COLS1, 1};
  allocate_bool_shares_table(&t1);

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

  // STEP 1: SORT t1 on uid (attribute 1)
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  unsigned int att_index[1] = {0};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS1/2);

  // // stop timer
  // gettimeofday(&end, 0);
  // seconds = end.tv_sec - begin.tv_sec;
  // micro = end.tv_usec - begin.tv_usec;
  // elapsed = seconds + micro*1e-6;
  // // Print timing information
  // if (rank == 0) {
  //   printf("%ld\t%.3f\n", ROWS1, elapsed);
  // }

  // STEP 2: Select year=2019
  #if DEBUG
    if (rank==0) {
      printf("Selection.\n");
    }
  #endif
  Predicate_B p = {EQ, NULL, NULL, 2, 4};
  BShare* selected = malloc(ROWS1*sizeof(BShare));
  assert(selected!=NULL);
  select_b(t1, p, selected);

  // // stop timer
  // gettimeofday(&end, 0);
  // seconds = end.tv_sec - begin.tv_sec;
  // micro = end.tv_usec - begin.tv_usec;
  // elapsed = seconds + micro*1e-6;
  // // Print timing information
  // if (rank == 0) {
  //   printf("%ld\t%.3f\n", ROWS1, elapsed);
  // }

  // STEP 3: GROUP-BY-MIN-MAX on (uid)
  #if DEBUG
    if (rank==0) {
      printf("Group-by.\n");
    }
  #endif
  unsigned key_indices[1] = {0};
  group_by_min_max_sel(&t1, selected, 8, 6, key_indices, 1);
  free(selected);

  // // stop timer
  // gettimeofday(&end, 0);
  // seconds = end.tv_sec - begin.tv_sec;
  // micro = end.tv_usec - begin.tv_usec;
  // elapsed = seconds + micro*1e-6;
  // // Print timing information
  // if (rank == 0) {
  //   printf("%ld\t%.3f\n", ROWS1, elapsed);
  // }

  // STEP 4: Select tuples with max-min>threshold
  #if DEBUG
    if (rank==0) {
      printf("Selection.\n");
    }
  #endif
  Predicate_B p2 = {GR, NULL, NULL, 6, 8, -1, -1};
  BShare* sel_results = malloc(ROWS1*sizeof(BShare));
  assert(sel_results!=NULL);
  select_b(t1, p2, sel_results);


  // // stop timer
  // gettimeofday(&end, 0);
  // seconds = end.tv_sec - begin.tv_sec;
  // micro = end.tv_usec - begin.tv_usec;
  // elapsed = seconds + micro*1e-6;
  // // Print timing information
  // if (rank == 0) {
  //   printf("%ld\t%.3f\n", ROWS1, elapsed);
  // }

  // Step 5: Mask not selected tuples
  #if DEBUG
    if (rank==0) {
      printf("Masking.\n");
    }
  #endif
  mask(&t1, sel_results, ROWS1);
  free(sel_results);

  // // stop timer
  // gettimeofday(&end, 0);
  // seconds = end.tv_sec - begin.tv_sec;
  // micro = end.tv_usec - begin.tv_usec;
  // elapsed = seconds + micro*1e-6;
  // // Print timing information
  // if (rank == 0) {
  //   printf("%ld\t%.3f\n", ROWS1, elapsed);
  // }

  // Step 6: Shuffle before opening -- TODO: This can be done in O(n)
  #if DEBUG
    if (rank==0) {
      printf("Shuffling.\n");
    }
  #endif
  unsigned atts[1] = {0};
  bool asc2[1] = {1};
  bitonic_sort_batch(&t1, atts, 1, asc2, ROWS1/2);

  // // stop timer
  // gettimeofday(&end, 0);
  // seconds = end.tv_sec - begin.tv_sec;
  // micro = end.tv_usec - begin.tv_usec;
  // elapsed = seconds + micro*1e-6;
  // // Print timing information
  // if (rank == 0) {
  //   printf("%ld\t%.3f\n", ROWS1, elapsed);
  // }

  // There should be one selected line in the result with this order:
  // 0    (currently at index 0)
  // The rest of the lines in the result are garbage (due to multiplexing)
  #if DEBUG
    if (rank==0) {
      printf("Open.\n");
    }
  #endif
  // Tuples are opened in batches of size BATCH_SIZE
  Data* result = malloc(BATCH_SIZE*sizeof(BShare));
  assert(result!=NULL);
  Data* opened = malloc(BATCH_SIZE*sizeof(BShare));
  assert(opened!=NULL);
  int start=0, limit=BATCH_SIZE, step;
  while (start<t1.numRows) {
    for (int i=start, k=0; i<limit; i++, k++) {
      result[k] = t1.contents[i][0]; // id
    }
    open_b_array(result, BATCH_SIZE, opened);
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
