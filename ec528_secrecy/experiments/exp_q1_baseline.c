#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define DEBUG 0
#define COLS1 4
#define COLS2 1

static void group_by_count_rca(BShareTable* table, unsigned att_index);

/**
 * Evaluates the performance of Q1 (comorbidity).
 * This is the baseline implementation without optimizations.
 **/

int main(int argc, char** argv) {

  if (argc < 3) {
    printf("\n\nUsage: %s <NUM_ROWS_1> <NUM_ROWS_2>\n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS1 = atol(argv[argc - 2]); // input1 size
  const long ROWS2 = atol(argv[argc - 1]); // input2 size

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS1, 2*COLS1, 1}; // {pid, diag, cnt}
  allocate_bool_shares_table(&t1);
  BShareTable t2 = {-1, rank, ROWS2, 2*COLS2, 2};
  allocate_bool_shares_table(&t2);

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

  // start timer
  gettimeofday(&begin, 0);

  // STEP 1: IN
  #if DEBUG
    if (rank==0) {
      printf("IN.\n");
    }
  #endif
  BShare *in_res = malloc(ROWS1*sizeof(BShare));
  assert(in_res!=NULL);
  BShare *in_res_rem = malloc(ROWS1*sizeof(BShare));
  assert(in_res_rem!=NULL);
  in(&t1, &t2, 0, 0, in_res, ROWS1);

  // get remote bits of in
  exchange_shares_array(in_res, in_res_rem, ROWS1);

  // add selected bits to last 2 columns of in1
  for (int i=0; i<ROWS1; i++) {
    t1.contents[i][6] = in_res[i];
    t1.contents[i][7] = in_res_rem[i];
  }
  free(in_res); free(in_res_rem);

  // STEP 2: SORT t1 on diag (att=2)
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  unsigned int att_index[1] = {2};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS1/2);

  // STEP 3: GROUP-BY-COUNT on diag (att=2)
  #if DEBUG
    if (rank==0) {
      printf("Group-by.\n");
    }
  #endif
  group_by_count_rca(&t1, 2);

  // STEP 4: Sort by cnt
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  att_index[0] = 4;
  asc[0] = 0;
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS1/2);

  Data result[10][2];

  // Open first 10 elements
  for (int i=0; i<10; i++) {
    result[i][0] = open_b(t1.contents[i][2]); // diag
    result[i][1] = open_b(t1.contents[i][4]); // count
  }

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("\tQ1-BASELINE\t%ld\t%ld\t%.3f\n", ROWS1, ROWS2, elapsed);
  }

  #if DEBUG
    if (rank == 0) {
      for (int i=0; i<10; i++) {
        printf("[%d] (diag, cnt) = %lld, %lld\n", i, result[i][0], result[i][1]);
      }
    }
  #endif

  free(t1.contents); free(t2.contents);

  // tear down communication
  TCP_Finalize();
  return 0;
}

// Group-by-count that uses a Ripple-carry-adder
// The selected bits are in the last 2 columns of table (6, 7).
// The counters are in columns 4, and 5.
static void group_by_count_rca(BShareTable* table, unsigned att_index) {
  BShare** c = table->contents;
  BShare mask=1, max=0xFFFFFFFFFFFFFFFF;

  // Scan table and update counts by adding 'selected' bits
  int len = table->numCols/2 + 3;
  BShare local[len], remote[len];
  BShare bs1, bs2;
  for (int i=0, k=0; i<table->numRows-1; i++, k+=2) {
    // bs defines whether the adjacent rows c[i], c[i+1] are in the same group
    bs1 = eq_b(c[i][att_index], c[i][att_index+1],
                      c[i+1][att_index], c[i+1][att_index+1]);
    bs2 = exchange_shares(bs1);  // 1 round
    bs1 &= mask;                        // Keep LSB only
    bs2 &= mask;                        // Keep LSB only
    // Compute bn = (bs OR NOT selected_b[i]) = NOT(NOT bs AND selected_b[i])
    BShare bn1 = bs1 ^ mask,
           bn2 = bs2 ^ mask;
    bn1 = and_b(bn1, bn2, c[i][6] & mask, c[i][7] & mask,
                get_next_rb()) ^ mask;
    bn2 = exchange_shares(bn1);         // 1 round
    bn1 &= mask;                        // Keep LSB only
    bn2 &= mask;                        // Keep LSB only
    BShare b1 = -bn1;                   // Set all bits equal to LSB of bn1
    BShare b2 = -bn2;                   // Set all bits equal to LSB of bn2
    // Compute new_c[i] = b * dummy_row + (1-b) * row1
    for (int j=0; j<table->numCols-5; j+=2) {
      local[j/2] = and_b(b1, b2, max, max, get_next_rb());
      local[j/2] ^= and_b(~b1, ~b2, c[i][j], c[i][j+1], get_next_rb());
    }

    // Compute new_cnt = bs*(selected[i] + selected[i+1]) + (1-bs)*selected[i+1]
    BShare local_cnt, remote_cnt;
    BShare rnums[187];
    get_next_rb_array(rnums, 187);
    boolean_addition(c[i][4], c[i][5],
                     c[i+1][4], c[i+1][5],
                     &local_cnt, &remote_cnt, rnums);


    local[len-3] = and_b(bs1, bs2, local_cnt, remote_cnt,
                       get_next_r());

    local[len-3] ^= and_b(~bs1, ~bs2, c[i+1][4],
                        c[i+1][5], get_next_rb());

    // *** Masking *** //
    // Compute cond = bs AND selected_b[i] to propagate 'selected' bit
    BShare cond1 = and_b(bs1, bs2, c[i][6], c[i][7], get_next_rb());
    BShare cond2 = exchange_shares(cond1);  // 1 round
    cond1 = -cond1;                         // Set all bits equal to LSB
    cond2 = -cond2;                         // Set all bits equal to LSB
    // Compute cond * selected_b[i] + (1-cond)*selected_b[i+1]
    local[len-1] = and_b(cond1, cond2, c[i][6], c[i][7],
                         get_next_rb());
    local[len-1] ^= and_b(~cond1, ~cond2,
                          c[i+1][6], c[i+1][7],
                          get_next_rb());

    // Fetch remote boolean and arithmetic shares
    exchange_shares_array(local, remote, len);    // 1 round
    // Set c[i] = new_c[i]
    for (int j=0; j<table->numCols-5; j+=2) {
      c[i][j] = local[j/2];
      c[i][j+1] = remote[j/2];
    }
    // Propagate 'selected' bit
    c[i+1][6] = local[len-1];
    c[i+1][7] = remote[len-1];

    // Set new_cnt
    c[i+1][4] = local[len-3];
    c[i+1][5] = remote[len-3];
  }
  // Make sure we mask the last row if it's not selected
  // 1. Compute composite bit bl = NOT(bs) AND NOT(selected_b)
  BShare bl1 = and_b(bs1 ^ mask, bs2 ^ mask,
                     c[table->numRows-1][6] ^ mask,
                     c[table->numRows-1][7] ^ mask,
                     get_next_rb())
                   & mask;
  BShare bl2 = exchange_shares(bl1);            // 1 round
  BShare b1 = -bl1;
  BShare b2 = -bl2;
  // 2. Compute new_c[i] = bl * dummy_row + (1-bl) * last_row
  for (int j=0; j<table->numCols-1; j+=2) {
    local[j/2] = and_b(b1, b2, max, max, get_next_rb());
    local[j/2] ^= and_b(~b1, ~b2, c[table->numRows-1][j],
                        c[table->numRows-1][j+1], get_next_rb());
  }
  exchange_shares_array(local, remote, len);    // 1 round
  // 3. Multiplex
  for (int j=0; j<table->numCols-1; j+=2) {
    c[table->numRows-1][j] = local[j/2];
    c[table->numRows-1][j+1] = remote[j/2];
  }
}
