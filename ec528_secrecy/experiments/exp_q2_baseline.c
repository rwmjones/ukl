#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define DEBUG 0
#define COLS 6  // pid, time, time+15, time+56, diag-cdiff, cdiff-diag

static void materialized_join(BShareTable *input1, BShareTable *input2,
                        int leftcol, int rightcol, BShareTable* result);
static void materialized_join_geq(BShareTable *input1, BShareTable *input2,
                        int leftcol, int rightcol, BShare* result);
static unsigned long long geq_round_a(BShare, BShare, BShare, BShare, int);
static unsigned long long gr_round_b(BShare, BShare, BShare, BShare, int,
                                      char local[], char remote[]);
static unsigned long long gr_round_c_char(int, int, int, char local[], char remote[],
                                      char levels[], int *bit_count);

/**
 * Evaluates the performance of Q2 (rec. cdiff).
 **/

int main(int argc, char** argv) {

  if (argc < 2) {
    printf("\n\nUsage: %s <NUM_ROWS> \n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS = atol(argv[argc - 1]); // input1 size

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  // Diagnosis(pid, time, time+15, time+56, diag-cdif, cdif-diag)
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

  // start timer
  gettimeofday(&begin, 0);

  // STEP 1: Apply selection predicate 'diag=cdiff'
  #if DEBUG
    if (rank==0) {
      printf("1st selection.\n");
    }
  #endif
  BShare *sel = malloc(ROWS*sizeof(BShare));
  assert(sel!=NULL);
  BShare *rem_sel = malloc(ROWS*sizeof(BShare));
  assert(rem_sel!=NULL);
  // Diagnosis(pid, time, time+15, time+56, diag-cdiff, cdiff-diag)
  Predicate_B p = {EQ, NULL, NULL, 8, 10};
  // Apply 'diag=cdiff'
  select_b(t1, p, sel);

  exchange_shares_array(sel, rem_sel, ROWS);

  // Copy selection bits to 'diag-cdiff' column
  for (int i=0; i<ROWS; i++) {
    t1.contents[i][8] = sel[i];
    t1.contents[i][9] = rem_sel[i];
  }

  free(sel); free(rem_sel);

  // STEP 3: r1. Pid = r2. Pid
  BShareTable res_table = {-1, rank, ROWS*ROWS, 2*COLS+2, 1};
  allocate_bool_shares_table(&res_table);

  BShare *res_greater = malloc(ROWS*ROWS*sizeof(BShare));
  assert(res_greater!=NULL);
  BShare *rem_res_greater = malloc(ROWS*ROWS*sizeof(BShare));
  assert(rem_res_greater!=NULL);

  #if DEBUG
    if (rank==0) {
      printf("Equi-join.\n");
    }
  #endif

  // equality join
  materialized_join(&t1, &t1, 0, 0, &res_table);

  #if DEBUG
    if (rank==0) {
      printf("GEQ-join.\n");
    }
  #endif

  // greater join
  // we could do both conditions here so we only materialize one
  materialized_join_geq(&t1, &t1, 2, 2, res_greater);
  // get remote
  exchange_shares_array(res_greater, rem_res_greater, ROWS*ROWS);

  // Do the conjuction
  for (int j=0; j<ROWS*ROWS; j++) {
    res_greater[j] = and_b(res_greater[j], rem_res_greater[j],
                      res_table.contents[j][res_table.numCols-2],
                      res_table.contents[j][res_table.numCols-1], get_next_rb())
                    & 1;
  }

  free(t1.contents);

  // Get remote join results
  exchange_shares_array(res_greater, rem_res_greater, ROWS*ROWS);

    for (int j=0; j<ROWS*ROWS; j++) {
      res_table.contents[j][res_table.numCols-1] = rem_res_greater[j];
  }

  free(res_greater); free(rem_res_greater);

  // sort by pid asc, selected desc
  unsigned int att_index[2] = {0,res_table.numCols-2};
  bool asc[2] = {1,0};
  bitonic_sort_batch(&res_table, att_index, 2, asc, ROWS*ROWS/2);

  // distinct
  BitShare* d = malloc(ROWS*ROWS*sizeof(BitShare));
  assert(d!=NULL);
  BitShare* rem_d = malloc(ROWS*ROWS*sizeof(BitShare));
  assert(rem_d!=NULL);

  distinct_batch(&res_table, 0, d, ROWS*ROWS-1);

  exchange_bit_shares_array(d, rem_d, ROWS);
  // Evaluate s_i AND NOT(pid[i]==pid[i-1])
  for (int i=0; i<ROWS*ROWS; i++) {
    d[i] = and_b(res_table.contents[i][res_table.numCols-2],
                res_table.contents[i][res_table.numCols-1],
                 d[i] ^ 1, rem_d[i] ^ 1, get_next_rb())
                & 1;
  }
  exchange_bit_shares_array(d, rem_d, ROWS);

  BShare b1, b2;
  BShare max=0xFFFFFFFFFFFFFFFF;
  // We only need to multiplex the pid
  BShare *att = malloc(ROWS*ROWS*sizeof(BShare));
  assert(att!=NULL);
  for (int i=0; i<ROWS*ROWS; i++) {
    d[i] ^= 1;
    rem_d[i] ^= 1;
    b1 = - (BShare) d[i];
    b2 = - (BShare) rem_d[i];
    // Compute pid = b*max + (1-b)*pid
    att[i] = and_b(b1, b2, max, max, get_next_rb());
    att[i] ^= and_b(~b1, ~b2, res_table.contents[i][0], res_table.contents[i][1],
                    get_next_rb());
  }

  free(d); free(rem_d);

  // OPEN diagnosis
  Data *result = malloc(ROWS*ROWS*sizeof(Data));
  assert(result!=NULL);
  open_b_array(att, ROWS*ROWS, result);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("\tQ2-baseline\t%ld\t%.3f\n", ROWS, elapsed);
  }

  free(att);
  free(result); free(res_table.contents);

  // tear down communication
  TCP_Finalize();
  return 0;
}

// The result is stored in a new BShareTable whose first columns contain
// the matching pairs of the original tables and
// the last 2 columns contain the join result bits.
static void materialized_join(BShareTable *input1, BShareTable *input2,
                        int leftcol, int rightcol, BShareTable* result) {

  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);
  int res_index = result->numCols-2;
  BShare *temp_local = malloc((result->numRows)*sizeof(BShare));
  BShare *temp_remote = malloc((result->numRows)*sizeof(BShare));

  // compute bitwise x^y^1
  for (int i=0; i<input1->numRows; i++) {
    // copy outer input's join attribute to result table
    result->contents[i][0] = input1->contents[i][leftcol];
    result->contents[i][1] = input1->contents[i][leftcol+1];
    for (int j=0; j<input2->numRows; j++) {
      // initialize equality
      result->contents[i][res_index] = input1->contents[i][leftcol] ^ input2->contents[j][rightcol] ^ (~(BShare)0); // local share;
      result->contents[i][res_index+1] = input1->contents[i][leftcol+1] ^ input2->contents[j][rightcol+1] ^ (~(BShare)0); // remote share
    }
  }

  // The result is stored in the (numbits/2) rightmost bits of result, res2 elements
  for (int l=0; l<numlevels; l++) {
    for (int i=0; i<result->numRows; i++) {
      result->contents[i][res_index] = eq_b_level2(numbits >> l,
                                        result->contents[i][res_index],
                                        result->contents[i][res_index+1]);
    }

    // Exchange results of logical and, except for the final round
    // copy result column to temp_local and exchange it
    for (int i=0; i<result->numRows; i++) {
      temp_local[i] = result->contents[i][res_index];
    }
    exchange_shares_array(temp_local, temp_remote, result->numRows);
      // copy exchanged result back to remote column
    for (int i=0; i<result->numRows; i++) {
      result->contents[i][res_index+1] = temp_remote[i];
    }
  }
  free(temp_local); free(temp_remote);
}


// inequality join
static void materialized_join_geq(BShareTable *input1, BShareTable *input2,
                        int leftcol, int rightcol, BShare* result) {

  BShare** c1 = input1->contents;
  BShare** c2 = input2->contents;

  int share_length=sizeof(BShare)*8; // The length of BShare in number of bits
  long numElements = input1->numRows * input2->numRows;

  int len = (share_length-1)*sizeof(char) + sizeof(unsigned long long);
  // Local and remote bits per level
  unsigned long long *local_bits = malloc(numElements * sizeof(unsigned long long));
  assert(local_bits!=NULL);
  unsigned long long *remote_bits = malloc(numElements * sizeof(unsigned long long));
  assert(remote_bits!=NULL);

  // For each element, we reserve 1 byte for 63 levels + 1 BShare for last level
  char **local = allocate_2D_byte_array(numElements, len);
  char **remote = allocate_2D_byte_array(numElements, len);

  /** FIRST ROUND **/
  int index=0;
  for (int i=0; i<input1->numRows; i++) {
    for (int j=0; j<input2->numRows; j++) {
      local_bits[index++] = geq_round_a(c1[i][leftcol], c1[i][leftcol+1],
                                        c2[j][rightcol],
                                        c2[j][rightcol+1],
                                        share_length);
    }
  }

  // Get the second share of each bit as computed by the other party
  exchange_shares_array_u(local_bits, remote_bits, numElements);

  // Unpack bits and update levels
  for (long i=0; i<numElements; i++) {
    for (long j=0; j<share_length-1; j++) {
      local[i][j] = get_bit_u(local_bits[i], share_length-j-1);
      remote[i][j] = get_bit_u(remote_bits[i], share_length-j-1);
    }
    // Update last-level bits
    unsigned long long l_tmp = get_bit_u(local_bits[i], 0);
    unsigned long long r_tmp = get_bit_u(remote_bits[i], 0);
    memcpy(&local[i][share_length-1], &l_tmp, sizeof(unsigned long long));
    memcpy(&remote[i][share_length-1], &r_tmp, sizeof(unsigned long long));
  }

  /** SECOND ROUND **/
  index = 0;
  for (int i=0; i<input1->numRows; i++) {
    for (int j=0; j<input2->numRows; j++) {
      local_bits[index] = gr_round_b(c1[i][leftcol], c1[i][leftcol+1],
                                     c2[j][rightcol], c2[j][rightcol+1],
                                     share_length,
                                     &local[index][0], &remote[index][0]);
      index++;
    }
  }

  // Get the second share of each bit as computed by the other party
  exchange_shares_array_u(local_bits, remote_bits, numElements);

  for (int i=0; i<numElements; i++) {
    // Unpack the length/2 MSBs and store them at the last level
    unsigned long long tmp = ( remote_bits[i] >> (share_length/2) );
    memcpy(&remote[i][share_length-1], &tmp, sizeof(unsigned long long));
    // Unpack the rest and update odd levels
    for (int j=1; j<share_length-1; j+=2) {
      remote[i][j] = get_bit_u(remote_bits[i], j/2);
    }
  }

  /** REMAINING ROUNDS **/
  int rounds = (int) log2(share_length/2);
  char **levels = allocate_2D_byte_array(numElements, share_length/2);  // max 'length' levels per pair

  // Initialize level cache
  for (int i=0; i<numElements; i++) {
    for (int j=0; j<share_length/2; j++) {
      levels[i][j] = -1;
    }
  }

  int bits_left=share_length, bit_count;
  for (int r=1; r<=rounds; r++) {
    bits_left /= 2;
    for (int i=0; i<numElements; i++) {
      bit_count = 0;
      local_bits[i] = gr_round_c_char(r, bits_left, share_length,
                                 &local[i][0], &remote[i][0],
                                 &levels[i][0], &bit_count);
    }

    // Exchange all bits of the current round and unpack accordingly
    exchange_shares_array_u(local_bits, remote_bits, numElements);

    // Unpack bits of last level
    for (int i = 0; i < numElements; i++) {
      unsigned long long tmp = ( remote_bits[i] >> bit_count );
      memcpy(&remote[i][share_length-1], &tmp, sizeof(BShare));
      // Unpack the rest and reset level cache for next round
      int l=0;
      while ((levels[i][l] >= 0) & (l < share_length/2)) {
        remote[i][(int)levels[i][l]] = get_bit_u(remote_bits[i], l);
        levels[i][l++] = -1;  // Reset for next round
      }
    }
  }

  free(local_bits); free(remote_bits); free(levels);

  // One bitshare for each greater() comparison
  BShare mask = 1;
  for (int i=0; i<numElements; i++) {
    result[i] = 0;
    // Do a final XOR of all levels
    for (int j=0; j<share_length-1; j++) {
      result[i] ^= local[i][j];
    }
    // XOR with last level
    result[i] ^= *((unsigned long long*) &local[i][share_length-1]);
    result[i] &= mask;
  }

  free(local); free(remote);
}

static unsigned long long geq_round_a(BShare x1, BShare x2, BShare y1, BShare y2, int length) {
  // Compute (x_i ^ y_i)
  BShare xor1 = x1 ^ y1;
  BShare xor2 = x2 ^ y2;

  unsigned long long last_and = 0, last_ands = 0;
  const BShare mask=1;
  int index;  // The bit index (index=0 for the LSB)

  for (int i=0; i<length-1; i++) {
    // Compute ((x_{length-i-1} ^ y_{length-i-1}) AND x_{length-i-1})
    index = length-i-1;
    last_and = and_b(get_bit(xor1, index), get_bit(xor2, index),
                     get_bit(x1, index), get_bit(x2, index), get_next_rb())
                    & mask;
    // Pack result bit in last_ands to send all together
    last_ands |= ( last_and << index );
  }

  // Store ~(~x_0 AND y_0) at the last level (length-1)
  last_ands |= ( and_b(get_bit(x1, 0) ^ mask, get_bit(x2, 0) ^ mask,
                       get_bit(y1, 0), get_bit(y2, 0),
                       get_next_rb())
                      & mask ) ^ mask;

  // Set LSB to (x_0 AND ~y_0)
  // last_ands |= and_b(get_bit(x1, 0), get_bit(x2, 0),
  //                         get_bit(y1, 0) ^ mask, get_bit(y2, 0) ^ mask,
  //                         get_next_rb()) & mask;

  return last_ands;
}


// B. Compute next to last AND at odd levels as well as 1st round of pairwise
// ANDs at the last level. This step performs 'length' logical ANDs in total.
static unsigned long long gr_round_b(BShare x1, BShare x2, BShare y1, BShare y2,
                                      int length, char local[], char remote[]) {
  // Compute ~(x_i ^ y_i)
  BShare not_xor1 = ~(x1^y1);
  BShare not_xor2 = ~(x2^y2);
  int index;
  const BShare mask=1;

  unsigned long long local_bits = 0;
  for (int i=1, j=0; i<length-1; i+=2, j++) { // For all odd levels (length/2)
    index = length-i;
    // Set ~(x_{length-i} ^ y_{length-i}) next to the last bit
    local[i] |= ( get_bit(not_xor1, index) << 1 );
    // Set ~(x_{length-i} ^ y_{length-i}) next to the last remote bit
    remote[i] |= ( get_bit(not_xor2, index) << 1 );
    // Compute next to last logical AND for level i
    local[i] = ( and_b(get_bit_u8(local[i], 0), get_bit_u8(remote[i], 0),
                       get_bit_u8(local[i], 1), get_bit_u8(remote[i], 1),
                       get_next_rb()) & mask );
    // Pack result bit in local_bits to send all together
    local_bits |= (local[i] << j);
  }
  // Compute first round of pairwise logical ANDs at the last level
  unsigned long long tmp = eq_b_level2(length,
        unset_lsbs(not_xor1, 1) | *((unsigned long long*) &local[length-1]),
        unset_lsbs(not_xor2, 1) | *((unsigned long long*)  &remote[length-1]));
  memcpy(&local[length-1], &tmp, sizeof(unsigned long long));

  // Pack the length/2 result bits in the vacant MSBs of local_bits
  // local_bits |= ( ((unsigned long long) local[length-1]) << (length/2) );
  local_bits |= ( (*((unsigned long long*) &local[length-1])) << (length/2) );

  return local_bits;
}

static unsigned long long gr_round_c_char(int i, int bits_left, int length, char local[], char remote[],
                                      char levels[], int *bit_count) {

  int current_level, num_levels;
  const BShare mask=1;
  unsigned long long to_send = 0;
  num_levels = (1 << i);
  // Project common bits to avoid redundant computation (and communication)
  for (int p=bits_left-1; p>0; p-=2) {
    current_level = num_levels * (bits_left - p);
    for (int j=0; j<num_levels; j++) {
      // Project bits from last level
      BShare l_tmp = *((unsigned long long*) &local[length-1]);
      BShare r_tmp = *((unsigned long long*) &remote[length-1]);
      local[current_level] |= ( get_bit(l_tmp, p) << 1 );
      remote[current_level] |= ( get_bit(r_tmp, p) << 1 );
      // Do the logical AND
      local[current_level] = and_b(get_bit_u8(local[current_level], 0),
                                   get_bit_u8(remote[current_level], 0),
                                   get_bit_u8(local[current_level], 1),
                                   get_bit_u8(remote[current_level], 1),
                                   get_next_rb()) & mask;
      // Pack the result
      to_send |= ( local[current_level] << (*bit_count) );
      // Cache level to unpack remote bit later
      levels[*bit_count] = current_level;
      (*bit_count)++;
      current_level++;
      if ( current_level == (length-1) ) break;
    }
  }
  // Process last level
  BShare tmp = eq_b_level2(bits_left, *((unsigned long long*) &local[length-1]),
                                    *((unsigned long long*) &remote[length-1]));
  memcpy(&local[length-1], &tmp, sizeof(unsigned long long));
  // Pack bits of the last level
  to_send |= ( *((unsigned long long*) &local[length-1]) << (*bit_count) );
  return to_send;
}
