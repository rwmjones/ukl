#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define DEBUG 0
#define COLS 6  // Three 'original' columns plus two more columns for 'c-a' and
                // 'a-c' shares, and one more column for predicate evaluation

/**
 * Evaluates the performance of Q3 (aspirin count).
 **/

int main(int argc, char** argv) {

  if (argc < 4) {
    printf("\n\nUsage: %s <NUM_ROWS_1> <NUM_ROWS_2> <BATCH_SIZE>\n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS1 = atol(argv[argc - 1]); // input1 size
  const long ROWS2 = atol(argv[argc - 2]); // input2 size
  const int batch_size = atoi(argv[argc - 3]);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS1, 2*COLS, 1};
  allocate_bool_shares_table(&t1);
  BShareTable t2 = {-1, rank, ROWS2, 2*COLS, 2};
  allocate_bool_shares_table(&t2);

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1, r2;
    generate_random_table(&r1, ROWS1, COLS);
    generate_random_table(&r2, ROWS2, COLS);

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS1, 2*COLS, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS1, 2*COLS, 1};
    allocate_bool_shares_table(&t13);

    // t2 Bshare tables for P2, P3 (local to P1)
    BShareTable t22 = {-1, 1, ROWS2, 2*COLS, 2};
    allocate_bool_shares_table(&t22);
    BShareTable t23 = {-1, 2, ROWS2, 2*COLS, 2};
    allocate_bool_shares_table(&t23);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);
    // Generate boolean shares for r2
    generate_bool_share_tables(&r2, &t2, &t22, &t23);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS1*2*COLS, 1, sizeof(BShare));
    TCP_Send(&(t22.contents[0][0]), ROWS2*2*COLS, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS1*2*COLS, 2, sizeof(BShare));
    TCP_Send(&(t23.contents[0][0]), ROWS2*2*COLS, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
    free(r2.contents);
    free(t22.contents);
    free(t23.contents);

  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS1*2*COLS, 0, sizeof(BShare));
    TCP_Recv(&(t2.contents[0][0]), ROWS2*2*COLS, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  // OFFLINE PHASE: Generate dummy random numbers
  AShare* ra = malloc(batch_size*ROWS1*sizeof(AShare));
  assert(ra!=NULL);
  BShare* rb = malloc(batch_size*ROWS1*sizeof(BShare));
  assert(rb!=NULL);
  for (int i=0; i<batch_size*ROWS1; i++) {
    ra[i] = 0;
    rb[i] = 0;
  }

  // Start timer
  gettimeofday(&begin, 0);

  // STEP 1: Apply selections
  #if DEBUG
    if (rank==0) {
      printf("Applying selections.\n");
    }
  #endif
  BShare *sel1 = malloc(ROWS1*sizeof(BShare));
  assert(sel1!=NULL);
  BShare *rem_sel1 = malloc(ROWS1*sizeof(BShare));
  assert(rem_sel1!=NULL);
  BShare *sel2 = malloc(ROWS2*sizeof(BShare));
  assert(sel2!=NULL);
  BShare *rem_sel2 = malloc(ROWS2*sizeof(BShare));
  assert(rem_sel2!=NULL);
  // Diagnosis(pid, time, diag, diag-hd, hd-diag, sel)
  Predicate_B p = {EQ, NULL, NULL, 6, 8};
  // Apply 'd.diag=hd'
  select_b(t1, p, sel1);
  // Medication(pid, time, med, med-aspririn, aspririn-med, sel)
  // Apply 'm.med=aspirin'
  select_b(t2, p, sel2);

  exchange_shares_array(sel1, rem_sel1, ROWS1);
  exchange_shares_array(sel2, rem_sel2, ROWS2);

  // Copy shares to extra table columns
  for (int i=0; i<ROWS1; i++) {
    t1.contents[i][10] = sel1[i];
    t1.contents[i][11] = rem_sel1[i];
  }
  for (int i=0; i<ROWS2; i++) {
    t2.contents[i][10] = sel2[i];
    t2.contents[i][11] = rem_sel2[i];
  }
  free(sel1); free(sel2); free(rem_sel1); free(rem_sel2);

  // STEP 2: Sort relations
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  // Sort on s_i, pid, time (ASC)
  unsigned sort_att3[3] = {10,0,2};
  bool asc1[3] = {true,true,true};
  bitonic_sort_batch(&t1, sort_att3, 3, asc1, t1.numRows/2);
  // Sort on s_i, pid, time (DESC)
  bool asc2[3] = {true,true,false};
  bitonic_sort_batch(&t2, sort_att3, 3, asc2, t2.numRows/2);

  // STEP 3: Apply DISTINCT pid to each relation
  #if DEBUG
    if (rank==0) {
      printf("Applying DISTINCT.\n");
    }
  #endif
  BitShare* d1 = malloc(t1.numRows*sizeof(BitShare));
  assert(d1!=NULL);
  BitShare* rem_d1 = malloc(t1.numRows*sizeof(BitShare));
  assert(rem_d1!=NULL);
  BitShare* d2 = malloc(t2.numRows*sizeof(BitShare));
  assert(d2!=NULL);
  BitShare* rem_d2 = malloc(t2.numRows*sizeof(BitShare));
  assert(rem_d2!=NULL);
  distinct_batch(&t1, 0, d1, t1.numRows-1);
  distinct_batch(&t2, 0, d2, t2.numRows-1);

  // Get remote shares of distinct bits
  exchange_bit_shares_array(d1, rem_d1, t1.numRows);
  exchange_bit_shares_array(d2, rem_d2, t2.numRows);

  // STEP 4: Do the theta-join and, for each pair (i,j), evaluate the predicate
  #if DEBUG
    if (rank==0) {
      printf("Applying theta-join.\n");
    }
  #endif
  // 'sel2 AND d2 AND sel1 AND d1 AND m.time >= d.time'
  Predicate_B join_pred = {GEQ, NULL, NULL, 2, 2};          // m.time >= d.time
  Predicate_B eq_join_pred = {EQ, NULL, NULL, 0, 0};        // pid = pid
  int res_len = batch_size * ROWS1; // Number of elements in the join result
  BShare* result = malloc(res_len*sizeof(BShare)); // join result
  assert(result!=NULL);
  BShare* remote = malloc(res_len*sizeof(BShare)); // remote shares
  assert(remote!=NULL);
  // Used for counting rows
  AShare count = 0;
  BShare mask=1;
  // Join t2 with t1 on m.time >= d.time
  for (int i=0, bid=0; i<ROWS2; i+=batch_size, bid++) { // For each batch
    #if DEBUG
      if (rank==0) {
        printf("Inequality join.\n");
      }
    #endif
    join_b_batch(&t2, &t1, i, i+batch_size, 0, ROWS1, join_pred, remote,
                 result);

    // Get remote join results
    exchange_shares_array(result, remote, res_len);

    #if DEBUG
      if (rank==0) {
        printf("Equality join.\n");
      }
    #endif

    BShare* eq_result = malloc(res_len*sizeof(BShare)); // join result
    assert(eq_result!=NULL);
    BShare* eq_remote = malloc(res_len*sizeof(BShare)); // remote shares
    assert(eq_remote!=NULL);

    join_b_batch(&t2, &t1, i, i+batch_size, 0, ROWS1, eq_join_pred, eq_remote,
                 eq_result);

    // Get remote join results
    exchange_shares_array(eq_result, eq_remote, res_len);

    // Do the conjuction
    for (int j=0; j<res_len; j++) {
      result[j] = and_b(result[j], remote[j],
                        eq_result[j], eq_remote[j], get_next_rb())
                      & mask;
    }

    free(eq_remote); free(eq_result);

    // Get remote join results
    exchange_shares_array(result, remote, res_len);

    // Apply d_j AND d.time <= m.time in batch
    #if DEBUG
      if (rank==0) {
        printf("1st selection.\n");
      }
    #endif
    for (int j=0; j<res_len; j++) {
      result[j] = and_b(result[j], remote[j],
                        d1[j%ROWS1], rem_d1[j%ROWS1], get_next_rb())
                      & mask;
    }

    // Get remote shares
    exchange_shares_array(result, remote, res_len);

    // Apply s_j AND (d_j AND d.time <= m.time)
    #if DEBUG
      if (rank==0) {
        printf("2nd selection.\n");
      }
    #endif
    for (int j=0; j<res_len; j++) {
      result[j] = and_b(result[j], remote[j],
                          t1.contents[j%ROWS1][10],
                          t1.contents[j%ROWS1][11], get_next_rb())
                      & mask;
    }

    // Get remote shares
    exchange_shares_array(result, remote, res_len);

    // Apply d_i AND (s_j AND d_j AND d.time <= m.time)
    #if DEBUG
      if (rank==0) {
        printf("3rd selection.\n");
      }
    #endif
    int k=i;
    for (int j=0; j<res_len; j++) {
      result[j] = and_b(result[j], remote[j],
                          d2[k], rem_d2[k], get_next_rb())
                      & mask;
      if ((j+1)%ROWS1==0)
        k++;
    }

    // Get remote shares
    exchange_shares_array(result, remote, res_len);

    // Apply s_i (d_i AND s_j AND d_j AND d.time <= m.time)
    #if DEBUG
      if (rank==0) {
        printf("4th selection.\n");
      }
    #endif
    k=i;
    for (int j=0; j<res_len; j++) {
      result[j] = and_b(result[j], remote[j],
                        t2.contents[k][10], t2.contents[k][11], get_next_rb())
                      & mask;
      if ((j+1)%ROWS1==0)
        k++;
    }

    // Get remote shares
    exchange_shares_array(result, remote, res_len);

    // Convert result to arithmetic shares and count selected rows
    #if DEBUG
      if (rank==0) {
        printf("Conversion.\n");
      }
    #endif

    AShare* converted = malloc(res_len*sizeof(AShare));
    assert(converted!=NULL);
    convert_single_bit_array(result, ra, rb,
                              res_len, converted);

    // Update local counter
    #if DEBUG
      if (rank==0) {
        printf("Counting.\n");
      }
    #endif
    for (int k=0; k<res_len; k++) {
      count += converted[k];
    }
    free(converted);
  }

  // Free memory
  free(remote); free(result); free(d1); free(rem_d1);
  free(d2); free(rem_d2);
  free(ra); free(rb);

  // OPEN COUNT RESULT
  Data o_count = open_a(count);
  #if DEBUG
  if (rank == 0) {
    printf("COUNT: %lld\n", o_count);
  }
  #endif

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("%d\tQ3-BATCH\t%ld\t%ld\t%d\t%.3f\n",
            COLS, ROWS1, ROWS2, batch_size, elapsed);
  }

  free(t1.contents); free(t2.contents);

  // tear down communication
  TCP_Finalize();
  return 0;
}
