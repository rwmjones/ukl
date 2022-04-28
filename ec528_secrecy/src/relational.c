#include "relational.h"
#include "primitives.h"
#include "party.h"
#include <stdio.h>

// function declarations
static void select_eq(AShareTable input, int leftcol, AShare c1, AShare c2,
                       AShare result[]);
static void select_eq_b(BShareTable input, int leftcol, int rightcol,
                         BShare result[]);
static void select_greater_b(BShareTable input, int leftcol, BShare result[]);
static void select_greater_batch(BShareTable input, int leftcol, int rightcol,
                                  BShare result[]);
static void select_geq_b(BShareTable input, int leftcol, BShare result[]);
static void join_eq_b(BShareTable input1, BShareTable input2,
                        int leftcol, int rightcol, BShare result[]);
static void join_eq_b_batch(BShareTable *input1, BShareTable *input2,
                        int start1, int end1, int start2, int end2,
                        int leftcol, int rightcol, BShare *remote,
                        BShare *result);
static void join_geq_b_batch(BShareTable *input1, BShareTable *input2,
                        int start1, int end1, int start2, int end2,
                        int leftcol, int rightcol, BShare *remote,
                        BShare *result);
static void eq_bulk(int, int, BShare*, BShare*, int);
static void in_level(int, BShare*, BShare*);
static void distinct_batch_incr(BShareTable*, int, int, unsigned, BShare*);

// TODO: KEEP THOSE ONLY IN PRIMITIVES
static unsigned long long geq_round_a(BShare, BShare, BShare, BShare, int);
static unsigned long long gr_round_a(BShare, BShare, BShare, BShare, int);
static unsigned long long gr_round_b(BShare, BShare, BShare, BShare, int,
                                      char local[], char remote[]);
static unsigned long long gr_round_c_char(int, int, int, char local[], char remote[],
                                      char levels[], int *bit_count);

// Selection on arithmetic shares
void select_a(AShareTable input, Predicate p, AShare c1, AShare c2, AShare result[]) {
  switch (p.operation) {
      case EQ:
          //printf("Equality predicate\n");
          select_eq(input, p.leftcol, c1, c2, result);
          break;
      default:
          printf("Illegal operation. Only equality is supported for the moment.\n");
  }
}

// Internal equality select: right must be a pointer to a pair of constants
static void select_eq(AShareTable input, int leftcol, AShare c1, AShare c2, AShare result[]) {
  for (int i = 0; i < input.numRows ;i++) {

    // generate w and r
    WSharePair w = get_next_w();
    AShare r = get_next_r();

    // compute the eqality result
    result[i] = eq(input.contents[i][leftcol], // 1st share of left att
                    input.contents[i][leftcol+1], // 2nd share of left att
                    c1, // 1st share of right att (constant)
                    c2, // 2nd share left att (constant)
                    w.first, // 1st share of random w
                    w.second, // 2nd share of random w
                    r); // 1st share of random r
  }
}

void and_b_table(BShareTable input, int leftcol, int rightcol, int size, BShare result[]){
  for (int i = 0; i < size; i++){
    result[i] = and_b(input.contents[i][leftcol],
                      input.contents[i][leftcol + 1],
                      input.contents[i][rightcol],
                      input.contents[i][rightcol + 1], get_next_rb());
  }
}

// Selection on boolean shares
void select_b(const BShareTable input, Predicate_B p, BShare result[]) {
  switch (p.operation) {
    case EQ:
      select_eq_b(input, p.leftcol, p.rightcol, result);
      break;
    case GT:
      select_greater_b(input, p.leftcol, result);
      break;
    case GEQ:
      select_geq_b(input, p.leftcol, result);
      break;
    case GC:
      select_greater_batch_const(input, p.leftcol, p.cs1, p.cs2, result);
      break;
    case GR:
      select_greater_batch(input, p.leftcol, p.rightcol, result);
      break;
    case EQC:
      select_eq_batch_const(input, p.leftcol, p.cs1, p.cs2, result);
      break;
    default:
          printf("Illegal operation. Predicate %u not supported.\n", p.operation);
  }
}

/**
 *  Internal inequality select for boolean shares, i.e. att > c, where c is a public constant.
 *  This is copmputed as (c-att) < 0 and col points to the difference shares of the
 *  attribute to be tested:
 **/
static void select_greater_b(BShareTable input, int leftcol, BShare result[]) {
  for (int i = 0; i < input.numRows; i++) {
    result[i] = ltz_b(input.contents[i][leftcol]);
  }
}

/**
 *  Internal inequality select for boolean shares, i.e. att >= c, where c is a public constant.
 *  This is copmputed as ~((att - c) < 0) and col points to the difference shares of the
 *  attribute to be tested:
 **/
static void select_geq_b(BShareTable input, int leftcol, BShare result[]) {
  for (int i = 0; i < input.numRows; i++) {
    result[i] = (ltz_b(input.contents[i][leftcol])&1)^1;
  }
}

/**
 *  Internal equality select for boolean shares.
 *  leftcol and rightcol point to the difference shares of the attribute to be tested:
 *  leftcol: att-c, rightcol: c-att
 **/
static void select_eq_b(BShareTable input, int leftcol, int rightcol, BShare result[]) {
  // r = (c-att < 0) ^ (att-c < 0) ^ 1
  BShare z1, z2;
  for (int i = 0; i < input.numRows; i++) {
    z1 = ltz_b(input.contents[i][leftcol]);
    z2 = ltz_b(input.contents[i][rightcol]);
    result[i] = z1^z2^1;
  }
}

/**
 *  Internal inequality select for boolean shares.
 *  leftcol and rightcol point to two table columns
 **/
static void select_greater_batch(BShareTable input, int leftcol, int rightcol,
                                  BShare result[]) {
  int numElements = input.numRows;
  BShare** c = input.contents;
  int share_length=sizeof(BShare)*8; // The length of BShare in number of bits
  int len = (share_length-1)*sizeof(char) + sizeof(BShare);
  // Local and remote bits per level
  unsigned long long *local_bits = malloc(numElements * sizeof(BShare));
  assert(local_bits!=NULL);
  unsigned long long *remote_bits = malloc(numElements * sizeof(BShare));
  assert(remote_bits!=NULL);

  // For each element, we reserve 1 byte for 63 levels + 1 BShare for last level
  char **local = allocate_2D_byte_array(numElements, len);
  char **remote = allocate_2D_byte_array(numElements, len);

  /** FIRST ROUND **/
  for (int i=0; i<numElements; i++) {
    local_bits[i] = gr_round_a(c[i][leftcol], c[i][leftcol+1], c[i][rightcol],
                               c[i][rightcol+1], share_length);
  }

  // Get the second share of each bit as computed by the other party
  exchange_shares_array_u(local_bits, remote_bits, numElements);

  // Unpack bits and update levels
  for (int i=0; i<numElements; i++) {
    for (int j=0; j<share_length-1; j++) {
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
  for (int i=0; i<numElements; i++) {
    local_bits[i] = gr_round_b(c[i][leftcol], c[i][leftcol+1], c[i][rightcol],
                               c[i][rightcol+1], share_length,
                               &local[i][0], &remote[i][0]);
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
  // max 'length' levels per pair
  char **levels = allocate_2D_byte_array(numElements, share_length/2);

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
  BitShare mask = 1;
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

// equi-join
void join_b(BShareTable input1, BShareTable input2, Predicate_B p, BShare result[]) {
  switch (p.operation) {
      case EQ:
          //printf("Equality predicate\n");
          join_eq_b(input1, input2, p.leftcol, p.rightcol, result);
          break;
      default:
          printf("Illegal operation. Only equality is supported for the moment.\n");
  }
}

// internal nested-loop equality join for boolean shares
static void join_eq_b(BShareTable input1, BShareTable input2,
                        int leftcol, int rightcol, BShare result[]) {
  int i, j, k = 0;

  for (i = 0; i < input1.numRows; i++) {
    for (j = 0; j < input2.numRows; j++) {
      // generate rnums for the next equality
      result[k++] = eq_b(input1.contents[i][leftcol], input1.contents[i][leftcol+1],
                          input2.contents[j][rightcol], input2.contents[j][rightcol+1]);
    }
  }
}
 // batched join
void join_b_batch(BShareTable *input1, BShareTable *input2,
                    int start1, int end1, int start2, int end2,
                    Predicate_B p, BShare *remote, BShare *result) {

  switch (p.operation) {
      case EQ:
          //printf("Equality predicate\n");
          join_eq_b_batch(input1, input2,
                          start1, end1, start2, end2,
                          p.leftcol, p.rightcol, remote, result);
          break;
      case GEQ:
          //printf("Equality predicate\n");
          join_geq_b_batch(input1, input2,
                          start1, end1, start2, end2,
                          p.leftcol, p.rightcol, remote, result);
          break;
      default:
          printf("Illegal operation. Only equality is supported for the moment.\n");
  }

}

// batched version of internal nested-loop equality join for boolean shares
static void join_eq_b_batch(BShareTable *input1, BShareTable *input2,
                        int start1, int end1, int start2, int end2,
                        int leftcol, int rightcol, BShare* remote,
                        BShare* result) {

  assert( (end1<=input1->numRows) && (end2<=input2->numRows) );

  int len1 = end1-start1;
  int len2 = end2-start2;
  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);

  // compute bitwise x^y^1
  int k=0;
  for (int i=start1; i<end1; i++) {
    for (int j=start2; j<end2; j++) {
      result[k] = input1->contents[i][leftcol] ^ input2->contents[j][rightcol] ^ (~(BShare)0); // local share;
      remote[k] = input1->contents[i][leftcol+1] ^ input2->contents[j][rightcol+1] ^ (~(BShare)0); // remote share
      k++;
    }
  }

  // The result is stored in the (numbits/2) rightmost bits of result, res2 elements
  for (int l=0; l<numlevels; l++) {
    for (int i=0; i<len1*len2; i++) {
      result[i] = eq_b_level2(numbits >> l, result[i], remote[i]);
    }

    // Exchange results of logical and, except for the final round
    if (l != numlevels-1) {
      exchange_shares_array(result, remote, len1*len2);
    }
  }
}

// batched version of internal nested-loop equality join for boolean shares
static void join_geq_b_batch(BShareTable *input1, BShareTable *input2,
                        int start1, int end1, int start2, int end2,
                        int leftcol, int rightcol, BShare* _remote,
                        BShare* result) {
  assert( (end1<=input1->numRows) && (end2<=input2->numRows) );
  BShare** c1 = input1->contents;
  BShare** c2 = input2->contents;

  int len1 = end1-start1;
  int len2 = end2-start2;

  int share_length=sizeof(BShare)*8; // The length of BShare in number of bits
  long numElements = len1 * len2;

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
  for (int i=start1; i<end1; i++) {
    for (int j=start2; j<end2; j++) {
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
  for (int i=start1; i<end1; i++) {
    for (int j=start2; j<end2; j++) {
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

// Compares adjacent elements and populates 'distinct' array with secrets
// Expects an allocated 'distinct' array
void distinct(BShareTable* table, unsigned att_index, BitShare* distinct) {
  BShare** c = table->contents;
  BShare mask=1, max=0xFFFFFFFFFFFFFFFF;
  // First element of the sorted table is always in the set of distinct elements
  distinct[0] = eq_b(max, max, c[0][att_index], c[0][att_index+1])
                    ^ mask;
  // Compare elements in adjacent rows
  for (int i=0; i<table->numRows-1; i++) {
    distinct[i+1] = eq_b(c[i][att_index], c[i][att_index+1],
                         c[i+1][att_index], c[i+1][att_index+1])
                         ^ mask;
  }
}

// Same as distinct but works in batch mode
void distinct_batch(BShareTable* table, unsigned att_index, BitShare* distinct,
                    unsigned num_comparisons) {
  assert( (num_comparisons>0) && (num_comparisons<=table->numRows-1) );
  BShare **c = table->contents;
  BShare mask=1, max=0xFFFFFFFFFFFFFFFF;
  // Allocate arrays for local and remote shares of equality results
  BShare *local = malloc(num_comparisons*sizeof(BShare));
  assert(local!=NULL);
  BShare *remote = malloc(num_comparisons*sizeof(BShare));
  assert(remote!=NULL);
  // First element of the sorted table is always in the set of distinct elements
  distinct[0] = get_rank() / 2;
  int num_bits = sizeof(BShare)*8;
  int num_levels = log2(num_bits);
  int total_comparisons = table->numRows - 1;
  // For each 'sliding' batch
  int next_start, batch_comp;
  for (int i=0; i<total_comparisons; i+=num_comparisons) {
    // The start index of the next batch
    next_start = i + num_comparisons;
    // The number of comparisons in the current batch
    batch_comp = next_start <= total_comparisons ?
                               num_comparisons : total_comparisons-i;
    for (int j=0, k=i; j<batch_comp; j++, k++) {
      // x_i ^ y_i ^ 1
      local[j] = c[k][att_index] ^ c[k+1][att_index] ^ max;
      remote[j] = c[k][att_index+1] ^ c[k+1][att_index+1] ^ max;
    }
    // Apply equalities in bulk
    eq_bulk(num_levels, num_bits, local, remote, batch_comp);
    // Set results
    for (int j=0, k=i+1; j<batch_comp; j++, k++) {
      distinct[k] = local[j] ^ mask;
    }
  }
  free(local);
  free(remote);
}

// Same as distinct_batch but returns BShare
static void distinct_batch_incr(BShareTable* table, int start, int end,
                                    unsigned att_index, BShare* distinct) {
  int batch_size = end-start;
  int num_comparisons = start==0 ? batch_size-1 : batch_size;
  if (num_comparisons==0) { // We need one last comparison for the last element
    num_comparisons=1;
  }
  assert(num_comparisons>0);

  BShare **c = table->contents;
  BShare mask=1, max=0xFFFFFFFFFFFFFFFF;
  // Allocate arrays for local and remote shares of equality results
  BShare *local = malloc(num_comparisons*sizeof(BShare));
  assert(local!=NULL);
  BShare *remote = malloc(num_comparisons*sizeof(BShare));
  assert(remote!=NULL);

  int num_bits = sizeof(BShare)*8;
  int num_levels = log2(num_bits);
  int pos;

  if (start==0) {
    // First element of the sorted table is always in the set of distinct
    distinct[0] = get_rank() / 2;
    pos = 1;
  }
  else {
    // We need one more comparison with the last element of the previous batch
    start -= 1;
    pos = 0;
  }

  // For each 'sliding' batch
  for (int i=start; i<end-1; i+=num_comparisons) {
    for (int j=0, k=i; j<num_comparisons; j++, k++) {
      // x_i ^ y_i ^ 1
      local[j] = c[k][att_index] ^ c[k+1][att_index] ^ max;
      remote[j] = c[k][att_index+1] ^ c[k+1][att_index+1] ^ max;
    }
    // Apply equalities in bulk
    eq_bulk(num_levels, num_bits, local, remote, num_comparisons);
    // Set results
    for (int j=pos, k=0; j<batch_size; j++, k++) {
      distinct[j] = local[k] ^ mask;
    }
  }
  // Free memory
  free(local); free(remote);
}

// Applies geq() to adjacent rows in batch
void adjacent_geq(BShareTable* table, unsigned att_index1, unsigned att_index2,
                  BitShare* result, unsigned num_comparisons, int swap) {
  assert( (num_comparisons>0) && (num_comparisons<=table->numRows-1) );
  BShare **c = table->contents;

  int share_length=sizeof(BShare)*8; // The length of BShare in number of bits

  int len = (share_length-1)*sizeof(char) + sizeof(BShare);
  // Local and remote bits per level
  unsigned long long *local_bits = malloc(num_comparisons * sizeof(BShare));
  assert(local_bits!=NULL);
  unsigned long long *remote_bits = malloc(num_comparisons * sizeof(BShare));
  assert(remote_bits!=NULL);

  // For each element, we reserve 1 byte for 63 levels + 1 BShare for last level
  char **local = allocate_2D_byte_array(num_comparisons, len);
  char **remote = allocate_2D_byte_array(num_comparisons, len);

  char **levels = allocate_2D_byte_array(num_comparisons, share_length/2);  // max 'length' levels per pair

  // The max number of comparisons per batch
  int total_comparisons = table->numRows - 1;
  int next_start, batch_comp;
  // For each 'sliding' batch
  for (int p=0; p<total_comparisons; p+=num_comparisons) {
    // The start index of the next batch
    next_start = p + num_comparisons;
    // The number of comparisons in the current batch
    batch_comp = next_start <= total_comparisons ?
                               num_comparisons : total_comparisons-p;

    /** FIRST ROUND **/
    int index=0;
    for (int j=0, k=p; j<batch_comp; j++, k++) {
      local_bits[index++] = geq_round_a(c[k+swap][att_index1],
                                        c[k+swap][att_index1+1],
                                        c[k+1-swap][att_index2],
                                        c[k+1-swap][att_index2+1],
                                        share_length);
    }

    // Get the second share of each bit as computed by the other party
    exchange_shares_array_u(local_bits, remote_bits, batch_comp);

    // Unpack bits and update levels
    for (int i=0; i<batch_comp; i++) {
      for (int j=0; j<share_length-1; j++) {
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
    for (int j=0, k=p; j<batch_comp; j++, k++) {
      local_bits[index] = gr_round_b(c[k+swap][att_index1],
                                     c[k+swap][att_index1+1],
                                     c[k+1-swap][att_index2],
                                     c[k+1-swap][att_index2+1],
                                     share_length,
                                     &local[index][0], &remote[index][0]);
      index++;
    }

    // Get the second share of each bit as computed by the other party
    exchange_shares_array_u(local_bits, remote_bits, batch_comp);

    for (int i=0; i<batch_comp; i++) {
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

    // Initialize level cache
    for (int i=0; i<num_comparisons; i++) {
      for (int j=0; j<share_length/2; j++) {
        levels[i][j] = -1;
      }
    }

    int bits_left=share_length, bit_count;
    for (int r=1; r<=rounds; r++) {
      bits_left /= 2;
      for (int i=0; i<batch_comp; i++) {
        bit_count = 0;
        local_bits[i] = gr_round_c_char(r, bits_left, share_length,
                                   &local[i][0], &remote[i][0],
                                   &levels[i][0], &bit_count);
      }

      // Exchange all bits of the current round and unpack accordingly
      exchange_shares_array_u(local_bits, remote_bits, batch_comp);

      // Unpack bits of last level
      for (int i = 0; i < batch_comp; i++) {
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

    // One bitshare for each greater() comparison
    BShare mask = 1;
    for (int i=0, k=p; i<batch_comp; k++, i++) {
      result[k] = 0;
      // Do a final XOR of all levels
      for (int j=0; j<share_length-1; j++) {
        result[k] ^= local[i][j];
      }
      // XOR with last level
      result[k] ^= *((unsigned long long*) &local[i][share_length-1]);
      result[k] &= mask;
    }
  }
  // Free memory
  free(local_bits); free(remote_bits); free(levels);
  free(local); free(remote);
}

// Oblivious semi-join -- relies on join_eq_b_batch()
void in(BShareTable* left, BShareTable* right, unsigned left_index,
        unsigned right_index, BShare* in, unsigned num_rows_left) {
  // Right input must be a power of two
  assert( ceil(log2(right->numRows)) == floor(log2(right->numRows)));
  BShare mask=1;
  int num_levels = log2(right->numRows);
  // Total number of equalities per batch
  long num_comparisons = num_rows_left * right->numRows;
  // Allocate arrays for local and remote shares of equality results
  BShare *local = malloc(num_comparisons*sizeof(BShare));
  assert(local!=NULL);
  BShare *remote = malloc(num_comparisons*sizeof(BShare));
  assert(remote!=NULL);
  int next, left_end, num_comp_batch;
  // For all batches
  for (int i=0; i<left->numRows; i+=num_rows_left) {
    next = i + num_rows_left;
    left_end = next <= left->numRows ? next : left->numRows;
    num_comp_batch = (left_end - i) * right->numRows; // Comparisons in batch
    // 1. Do the first round of the semi-join for all 'num_rows_left' in bulk
    join_eq_b_batch(left, right, i, left_end, 0, right->numRows,
                    left_index, right_index, remote, local);
    exchange_shares_array(local, remote, num_comp_batch);
    // 2. Do the second round of the semi-join for all 'num_rows_left' in bulk
    for (int j=0; j<num_comp_batch; j++){
      // Compute eq_result ^ 1
      local[j] ^= mask;
      local[j] &= mask;
      remote[j] ^= mask;
      remote[j] &= mask;
    }
    // Compute equalities in bulk
    for (int l=0; l<num_levels; l++) {
      // For each row on the left
      for (int j=0; j<num_comp_batch; j+=right->numRows) {
        in_level(right->numRows >> l, &local[j], &remote[j]);
      }
      // Exchange results of logical AND, except for the final round
      if (l != num_levels-1) {
        exchange_shares_array(local, remote, num_comp_batch);
      }
    }
    // Set IN results
    for (int k=i, j=0; k<left_end; k++, j+=right->numRows) {
      in[k] = (local[j] ^ mask);
    }
  }
  free(local);
  free(remote);
}

// operates like in() but also takes into account a selected bit
// (sel_index) in the right table
void in_sel_right(BShareTable* left, BShareTable* right,
        unsigned left_index, unsigned right_index, unsigned sel_index,
        BShare* res_in, unsigned num_rows_left) {

  // Right input must be a power of two
  assert( ceil(log2(right->numRows)) == floor(log2(right->numRows)));
  BShare mask=1;
  int num_levels = log2(right->numRows);
  // Total number of equalities per batch
  long num_comparisons = num_rows_left * right->numRows;
  // Allocate arrays for local and remote shares of equality results
  BShare *local = malloc(num_comparisons*sizeof(BShare));
  assert(local!=NULL);
  BShare *remote = malloc(num_comparisons*sizeof(BShare));
  assert(remote!=NULL);
  int next, left_end, num_comp_batch;

  // For all batches
  for (int i=0; i<left->numRows; i+=num_rows_left) {
    next = i + num_rows_left;
    left_end = next <= left->numRows ? next : left->numRows;
    num_comp_batch = (left_end - i) * right->numRows; // Comparisons in batch

    // 1. Do the first round of the semi-join for all 'num_rows_left' in bulk
    join_eq_b_batch(left, right, i, left_end, 0, right->numRows,
                    left_index, right_index, remote, local);
    exchange_shares_array(local, remote, num_comp_batch);

    // Take into account the selected bit
    // We need to compute a logical and of local with selected
    for (long i=0; i<num_comp_batch; i++) {
      local[i] = and_b(local[i], remote[i],
                      right->contents[i%right->numRows][sel_index],
                      right->contents[i%right->numRows][sel_index+1],
                      get_next_rb());
    }
    exchange_shares_array(local, remote, num_comp_batch);

    // 2. Do the second round of the semi-join for all 'num_rows_left' in bulk
    for (int j=0; j<num_comp_batch; j++){
      // Compute eq_result ^ 1
      local[j] ^= mask;
      local[j] &= mask;
      remote[j] ^= mask;
      remote[j] &= mask;
    }
    // Compute equalities in bulk
    for (int l=0; l<num_levels; l++) {
      // For each row on the left
      for (int j=0; j<num_comp_batch; j+=right->numRows) {
        in_level(right->numRows >> l, &local[j], &remote[j]);
      }
      // Exchange results of logical AND, except for the final round
      if (l != num_levels-1) {
        exchange_shares_array(local, remote, num_comp_batch);
      }
    }
    // Set IN results
    for (int k=i, j=0; k<left_end; k++, j+=right->numRows) {
      res_in[k] = (local[j] ^ mask);
    }
  }
  free(local);
  free(remote);


}

// Used by in()
static void in_level(int num_elements, BShare* z1, BShare* z2) {
  const BShare mask = 1;
  for (int i=0, j=0; i<num_elements; i+=2, j++) {
    BShare bx1 = z1[i];
    BShare bx2 = z2[i];
    BShare by1 = z1[i+1];
    BShare by2 = z2[i+1];
    // store the result (and's LSB) in the result's jth bit
    BShare out = and_b(bx1, bx2, by1, by2, get_next_rb());
    z1[j] = (out & mask);
  }
}

// Used by in() and distinct_batch()
static void eq_bulk(int num_levels, int num_bits, BShare* local,
                     BShare* remote, int length) {
  // For all rounds of the equality check (6 in total)
  for (int l=0; l<num_levels; l++) {
    // Apply l-th round for all equalities in batch
    for (int k=0; k<length; k++) {
      local[k] = eq_b_level2(num_bits >> l, local[k], remote[k]);
    }
    // Exchange results of logical and, except for the final round
    if (l != num_levels-1) {
      exchange_shares_array(local, remote, length);
    }
  }
}

// Groups rows and counts the number of rows per group
// Takes into account the selected rows from previous operations
// The given BShareTable must be sorted on the group-by key(s)
void group_by_count(BShareTable* table, unsigned* key_indices, int num_keys,
                    BShare* selected_b, AShare* selected, BShare* rb,
                    AShare* ra) {
  BShare** c = table->contents;
  BShare mask=1, max=0xFFFFFFFFFFFFFFFF;
  // Fetch the second arithmetic share of each 'selected' bit -- 1 round
  AShare *remote_selected = malloc((table->numRows)*sizeof(AShare));
  assert(remote_selected!=NULL);
  exchange_a_shares_array(selected, remote_selected, table->numRows);
  // Fetch the second boolean share of each 'selected' bit -- 1 round
  BShare *remote_selected_b = malloc((table->numRows)*sizeof(BShare));
  assert(remote_selected_b!=NULL);
  exchange_shares_array(selected_b, remote_selected_b, table->numRows);
  // Scan table and update counts by adding 'selected' bits
  AShare ab1[2], ab2[2];
  BShare bb[2];
  int len = table->numCols/2 + 3;   // Number of attributes plus old and new
                                    // counts plus propagated 'selected' bit
  int rank = get_rank();
  int succ_rank = get_succ();
  BShare bs1, bs2;
  BShare local[len], remote[len];
  BShare local_bits[num_keys], remote_bits[num_keys];
  for (int i=0, k=0; i<table->numRows-1; i++, k+=2) {
    // Compute a single bit bs that denotes whether the adjacent rows c[i],
    // c[i+1] are in the same group
    for (int idx=0; idx<num_keys; idx++) {
      unsigned key_index = key_indices[idx];
      // Set local equality bit share
      local_bits[idx]  = eq_b(c[i][key_index], c[i][key_index+1],
                              c[i+1][key_index], c[i+1][key_index+1]);
    }
    // Fetch remote equality bit shares
    exchange_shares_array(local_bits, remote_bits, num_keys);   // 1 round
    // AND all equality bits to compute the final bit bs
    and_b_all(local_bits, remote_bits, num_keys);   // log(num_keys) rounds
    // bs denotes whether the adjacent rows c[i], c[i+1] are in the same group
    bs1 = local_bits[0];
    bs2 = remote_bits[0];
    // Compute bn = (bs OR NOT selected_b[i]) = NOT(NOT bs AND selected_b[i])
    BShare bn1 = bs1 ^ mask,
           bn2 = bs2 ^ mask;
    bn1 = and_b(bn1, bn2, selected_b[i] & mask, remote_selected_b[i] & mask,
                get_next_rb()) ^ mask;
    bn2 = exchange_shares(bn1);         // 1 round
    bn1 &= mask;                        // Keep LSB only
    bn2 &= mask;                        // Keep LSB only
    BShare b1 = -bn1;                   // Set all bits equal to LSB of bn1
    BShare b2 = -bn2;                   // Set all bits equal to LSB of bn2
    // Compute new_c[i] = b * dummy_row + (1-b) * row1
    for (int j=0; j<table->numCols; j+=2) {
      local[j/2] = and_b(b1, b2, max, max, get_next_rb());
      local[j/2] ^= and_b(~b1, ~b2, c[i][j], c[i][j+1], get_next_rb());
    }
    // Compute arithmetic shares from boolean shares
    bb[0] = bs1;
    bb[1] = bn1;
    convert_single_bit_array(bb, &ra[k], &rb[k], 2, ab1);   // 1 round
    exchange_shares_array(ab1, ab2, 2);                     // 1 round
    AShare a_bs1 = ab1[0], a_bs2 = ab2[0];
    AShare a_bn1 = ab1[1], a_bn2 = ab2[1];
    // Compute new_cnt = bs*(selected[i] + selected[i+1]) + (1-bs)*selected[i+1]
    AShare local_count = selected[i] + selected[i+1],
           remote_count = remote_selected[i] + remote_selected[i+1];
    local[len-3] = mul(a_bs1, a_bs2, local_count, remote_count,
                       get_next_r());
    local[len-3] += mul(rank%2 - a_bs1, succ_rank%2 - a_bs2, selected[i+1],
                        remote_selected[i+1], get_next_r());
    // Compute bn * max + (1-bn) * selected[i] to allow masking 'selected' bit
    local[len-2] = mul(a_bn1, a_bn2, max, max, get_next_r());
    local[len-2] += mul(rank%2 - a_bn1, succ_rank%2 - a_bn2,
                        selected[i], remote_selected[i], get_next_r());
    // Compute cond = bs AND selected_b[i] to propagate 'selected' bit
    BShare cond1 = and_b(bs1, bs2, selected_b[i], remote_selected_b[i],
                         get_next_rb());
    BShare cond2 = exchange_shares(cond1);  // 1 round
    cond1 = -cond1;                         // Set all bits equal to LSB
    cond2 = -cond2;                         // Set all bits equal to LSB
    // Compute cond * selected_b[i] + (1-cond)*selected_b[i+1]
    local[len-1] = and_b(cond1, cond2, selected_b[i], remote_selected_b[i],
                         get_next_rb());
    local[len-1] ^= and_b(~cond1, ~cond2,
                          selected_b[i+1], remote_selected_b[i+1],
                          get_next_rb());
    // Fetch remote boolean and arithmetic shares
    // NOTE: This works because BShare and AShare are both long long
    exchange_shares_array(local, remote, len);    // 1 round
    // Set c[i] = new_c[i]
    for (int j=0; j<table->numCols; j+=2) {
      c[i][j] = local[j/2];
      c[i][j+1] = remote[j/2];
    }
    // Propagate 'selected' bit
    selected_b[i+1] = local[len-1];
    remote_selected_b[i+1] = remote[len-1];
    // Update 'selected' bit
    selected[i] = local[len-2];
    remote_selected[i] = remote[len-2];
    // Set selected[i+1] = new_cnt
    selected[i+1] = local[len-3];
    remote_selected[i+1] = remote[len-3];
  }
  // Make sure we mask the last row if it's not selected
  // 1. Compute composite bit bl = NOT(bs) AND NOT(selected_b)
  BShare bl1 = and_b(bs1 ^ mask, bs2 ^ mask,
                     selected_b[table->numRows-1] ^ mask,
                     remote_selected_b[table->numRows-1] ^ mask,
                     get_next_rb())
                   & mask;
  BShare bl2 = exchange_shares(bl1);            // 1 round
  BShare b1 = -bl1;
  BShare b2 = -bl2;
  // 2. Compute new_c[i] = bl * dummy_row + (1-bl) * last_row
  for (int j=0; j<table->numCols; j+=2) {
    local[j/2] = and_b(b1, b2, max, max, get_next_rb());
    local[j/2] ^= and_b(~b1, ~b2, c[table->numRows-1][j],
                        c[table->numRows-1][j+1], get_next_rb());
  }
  exchange_shares_array(local, remote, len);    // 1 round
  // 3. Multiplex
  for (int j=0; j<table->numCols; j+=2) {
    c[table->numRows-1][j] = local[j/2];
    c[table->numRows-1][j+1] = remote[j/2];
  }
  // Final shuffle iff not followed by ORDER_BY
  free(remote_selected); free(remote_selected_b);
}

// Groups rows and computes min and max of the specified attributes per group
// Takes into account the selected rows from previous operations
// The given BShareTable must be sorted on the group-by key(s)
void group_by_min_max_sel(BShareTable* table, BShare* selected,
                          unsigned min_att, unsigned max_att,
                          unsigned* key_indices, int num_keys) {
  BShare** c = table->contents;
  BShare mask=1, max=0xFFFFFFFFFFFFFFFF;
  // Fetch the second arithmetic share of each 'selected' bit -- 1 round
  AShare *remote_selected = malloc((table->numRows)*sizeof(BShare));
  assert(remote_selected!=NULL);
  exchange_a_shares_array(selected, remote_selected, table->numRows);
  int len = table->numCols/2 + 4;   // Number of attributes plus new min and max
                                    // plus masked and propagated selected bits
  BShare bs1, bs2, bb1[2], bb2[2];  // Bit shares
  BShare local[len], remote[len];
  BShare local_bits[num_keys], remote_bits[num_keys];
  // Scan table and update min/max
  for (int i=0, k=0; i<table->numRows-1; i++, k+=2) {
    // Compute a single bit bs that denotes whether the adjacent rows c[i],
    // c[i+1] are in the same group
    for (int idx=0; idx<num_keys; idx++) {
      unsigned key_index = key_indices[idx];
      // Set local equality bit share
      local_bits[idx]  = eq_b(c[i][key_index], c[i][key_index+1],
                              c[i+1][key_index], c[i+1][key_index+1]);
    }
    // Fetch remote equality bit shares
    exchange_shares_array(local_bits, remote_bits, num_keys);   // 1 round
    // AND all equality bits to compute the final bit bs
    and_b_all(local_bits, remote_bits, num_keys);   // log(num_keys) rounds
    // bs denotes whether the adjacent rows c[i], c[i+1] are in the same group
    bs1 = local_bits[0];
    bs2 = remote_bits[0];
    // Compute bn = (bs OR NOT selected[i]) = NOT(NOT bs AND selected[i])
    // bn is used to mask the i-th row when "it is not in the same group with
    // the next row or it is not selected"
    BShare bn1 = bs1 ^ mask,
           bn2 = bs2 ^ mask;
    bn1 = and_b(bn1, bn2, selected[i] & mask, remote_selected[i] & mask,
                get_next_rb()) ^ mask;
    bn2 = exchange_shares(bn1);         // 1 round
    bn1 &= mask;                        // Keep LSB only
    bn2 &= mask;                        // Keep LSB only
    BShare b1 = -bn1;                   // Set all bits equal to LSB of bn1
    BShare b2 = -bn2;                   // Set all bits equal to LSB of bn2
    // Compute new_c[i] = b * dummy_row + (1-b) * row1
    for (int j=0; j<table->numCols; j+=2) {
      local[j/2] = and_b(b1, b2, max, max, get_next_rb());
      local[j/2] ^= and_b(~b1, ~b2, c[i][j], c[i][j+1], get_next_rb());
    }
    // Compute bmin = min_att[i] ?< min_att[i+1]
    bb1[0] = greater(c[i+1][min_att], c[i+1][min_att+1], c[i][min_att],
                     c[i][min_att+1]);
    // Compute bmax = max_att[i] ?> max_att[i+1]
    bb1[1] = greater(c[i][max_att], c[i][max_att+1], c[i+1][max_att],
                     c[i+1][max_att+1]);
    exchange_shares_array(bb1, bb2, 2);     // 1 round
    BShare bmin1 = bb1[0];
    BShare bmin2 = bb2[0];
    BShare bmax1 = bb1[1];
    BShare bmax2 = bb2[1];
    // Compute bl = selected[i] AND selected[i+1] ("both selected")
    bb1[0] = and_b(selected[i], remote_selected[i], selected[i+1],
                   remote_selected[i+1], get_next_rb())
                 & mask;
    // Compute bf = selected[i] AND NOT selected[i+1] ("first selected")
    bb1[1] = and_b(selected[i], remote_selected[i], selected[i+1] ^ mask,
                   remote_selected[i+1] ^ mask, get_next_rb())
                 & mask;
    exchange_shares_array(bb1, bb2, 2);         // 1 round
    BShare bl1 = bb1[0], bl2 = bb2[0], bf1 = bb1[1], bf2 = bb2[1];
    // Compute bc = bs AND bl ("same group and both selected")
    bb1[0] = and_b(bs1, bs2, bl1, bl2, get_next_rb()) & mask;
    // Compute br = bs AND bf ("same group and first selected")
    bb1[1] = and_b(bs1, bs2, bf1, bf2, get_next_rb()) & mask;
    exchange_shares_array(bb1, bb2, 2);       // 1 round
    BShare bc1 = bb1[0], bc2 = bb2[0], br1 = bb1[1], br2 = bb2[1];;
    // Compute composite bits:
    //  - bcx = bc AND bmax
    //  - bcn = bc AND bmin
    bb1[0] = and_b(bc1, bc2, bmax1, bmax2, get_next_rb()) & mask;
    bb1[1] = and_b(bc1, bc2, bmin1, bmin2, get_next_rb()) & mask;
    exchange_shares_array(bb1, bb2, 2); // 1 round
    BShare bcx1 = bb1[0], bcx2 = bb2[0], bcn1 = bb1[1], bcn2 = bb2[1];
    // Compute composite bits:
    //  - bu = bcx OR br = NOT (NOT bcx AND NOT br)
    //  - bp = bcn OR br = NOT (NOT bcn AND NOT br)
    bb1[0] = (and_b(bcx1 ^ mask, bcx2 ^ mask, br1 ^ mask, br2 ^ mask,
                    get_next_rb()) ^ mask)
                 & mask;
    bb1[1] = (and_b(bcn1 ^ mask, bcn2 ^ mask, br1 ^ mask, br2 ^ mask,
                    get_next_rb()) ^ mask)
                 & mask;
    exchange_shares_array(bb1, bb2, 2); // 1 round
    BShare bu1 = bb1[0], bu2 = bb2[0], bp1 = bb1[1], bp2 = bb2[1];
    // Compute new_max = bu*c[i] + (1-bu)*c[i+1]
    local[len-4] = and_b(-bu1, -bu2, c[i][max_att], c[i][max_att+1],
                         get_next_rb());
    local[len-4] ^= and_b(~(-bu1), ~(-bu2), c[i+1][max_att], c[i+1][max_att+1],
                         get_next_rb());
    // Compute new_min = bp*c[i] + (1-bp)*c[i+1]
    local[len-3] = and_b(-bp1, -bp2, c[i][min_att], c[i][min_att+1],
                         get_next_rb());
    local[len-3] ^= and_b(~(-bp1), ~(-bp2), c[i+1][min_att], c[i+1][min_att+1],
                         get_next_rb());
    // Compute bn * max + (1-bn) * selected[i] to mask the selected bit when
    // the "i-th row is in the same group with the next or it is not selected"
    local[len-2] = and_b(b1, b2, max, max, get_next_rb());
    local[len-2] ^= and_b(~b1, ~b2, selected[i], remote_selected[i],
                          get_next_rb());
    // Compute cond = bs AND selected[i] to propagate 'selected' bit when the
    // "i-th row is selected and belongs to the same group with the next row"
    BShare cond1 = and_b(bs1, bs2, selected[i], remote_selected[i],
                         get_next_rb());
    BShare cond2 = exchange_shares(cond1);  // 1 round
    cond1 = -cond1;                         // Set all bits equal to LSB
    cond2 = -cond2;                         // Set all bits equal to LSB
    // Compute cond * selected[i] + (1-cond)*selected[i+1]
    local[len-1] = and_b(cond1, cond2, selected[i], remote_selected[i],
                         get_next_rb());
    local[len-1] ^= and_b(~cond1, ~cond2, selected[i+1], remote_selected[i+1],
                          get_next_rb())
                        & mask;
    // Fetch remote shares
    exchange_shares_array(local, remote, len);    // 1 round
    // Set c[i] = new_c[i]
    for (int j=0; j<table->numCols; j+=2) {
      c[i][j] = local[j/2];
      c[i][j+1] = remote[j/2];
    }
    // Propagate 'selected' bit
    selected[i+1] = local[len-1];
    remote_selected[i+1] = remote[len-1];
    // Update previous 'selected' bit
    selected[i] = local[len-2];
    remote_selected[i] = remote[len-2];
    // Set new_min
    c[i+1][min_att] = local[len-3];
    c[i+1][min_att+1] = remote[len-3];
    // Set new_max
    c[i+1][max_att] = local[len-4];
    c[i+1][max_att+1] = remote[len-4];
  }
  // Make sure we mask the last row if it's not selected
  // 1. Compute composite bit bk = NOT(bs) AND NOT(selected)
  BShare bk1 = and_b(bs1 ^ mask, bs2 ^ mask,
                     selected[table->numRows-1] ^ mask,
                     remote_selected[table->numRows-1] ^ mask,
                     get_next_rb())
                   & mask;
  BShare bk2 = exchange_shares(bk1);            // 1 round
  BShare b1 = -bk1;
  BShare b2 = -bk2;
  // 2. Compute new_c[i] = bk * dummy_row + (1-bk) * last_row
  for (int j=0; j<table->numCols; j+=2) {
    local[j/2] = and_b(b1, b2, max, max, get_next_rb());
    local[j/2] ^= and_b(~b1, ~b2, c[table->numRows-1][j],
                        c[table->numRows-1][j+1], get_next_rb());
  }
  exchange_shares_array(local, remote, len);    // 1 round
  // 3. Multiplex
  for (int j=0; j<table->numCols; j+=2) {
    c[table->numRows-1][j] = local[j/2];
    c[table->numRows-1][j+1] = remote[j/2];
  }
  free(remote_selected);
  // Final shuffle iff not followed by ORDER_BY
}

// Groups rows and counts the number of rows per group
// The given BShareTable must be sorted on the group-by key(s)
void group_by_count_micro(BShareTable* table, unsigned* key_indices,
                          int num_keys, AShare* counters,
                          AShare* remote_counters, BShare* rb, AShare* ra) {
  BShare** c = table->contents;
  BShare max=0xFFFFFFFFFFFFFFFF;

  // Scan table and update counts by adding 'selected' bits
  AShare ab1, ab2;
  int len = table->numCols/2 + 2;   // Number of attributes plus new and old cnt
  int rank = get_rank();
  int succ_rank = get_succ();
  BShare bs1, bs2;
  BShare local[len], remote[len];
  BShare local_bits[num_keys], remote_bits[num_keys];
  for (int i=0, k=0; i<table->numRows-1; i++, k+=2) {
    // Compute a single bit bs that denotes whether the adjacent rows c[i],
    // c[i+1] are in the same group
    for (int idx=0; idx<num_keys; idx++) {
      unsigned key_index = key_indices[idx];
      // Set local equality bit share
      local_bits[idx]  = eq_b(c[i][key_index], c[i][key_index+1],
                              c[i+1][key_index], c[i+1][key_index+1]);
    }
    // Fetch remote equality bit shares
    exchange_shares_array(local_bits, remote_bits, num_keys);   // 1 round
    // AND all equality bits to compute the final bit bs
    and_b_all(local_bits, remote_bits, num_keys);   // log(num_keys) rounds
    // bs denotes whether the adjacent rows c[i], c[i+1] are in the same group
    bs1 = local_bits[0];
    bs2 = remote_bits[0];
    BShare b1 = -bs1;                   // Set all bits equal to LSB of bs1
    BShare b2 = -bs2;                   // Set all bits equal to LSB of bs2
    // Compute new_c[i] = b * dummy_row + (1-b) * row1
    for (int j=0; j<table->numCols; j+=2) {
      local[j/2] = and_b(b1, b2, max, max, get_next_rb());
      local[j/2] ^= and_b(~b1, ~b2, c[i][j], c[i][j+1], get_next_rb());
    }
    // Compute arithmetic shares from boolean shares
    ab1 = convert_single_bit(bs1, ra[k], rb[k]);    // 1 round
    ab2 = exchange_shares(ab1);                     // 1 round
    // Compute new_cnt = bs*(counters[i] + counters[i+1]) + (1-bs)*counters[i+1]
    AShare local_count = counters[i] + counters[i+1],
           remote_count = remote_counters[i] + remote_counters[i+1];
    local[len-2] = mul(ab1, ab2, local_count, remote_count,
                       get_next_r());
    local[len-2] += mul(rank%2 - ab1, succ_rank%2 - ab2, counters[i+1],
                        remote_counters[i+1], get_next_r());
    // Compute ab * max + (1-ab) * counters[i] to allow masking previous count
    local[len-1] = mul(ab1, ab2, max, max, get_next_r());
    local[len-1] += mul(rank%2 - ab1, succ_rank%2 - ab2,
                        counters[i], remote_counters[i], get_next_r());
    // Fetch remote boolean and arithmetic shares
    // NOTE: This works because BShare and AShare are both long long ints
    exchange_shares_array(local, remote, len);    // 1 round
    // Set c[i] = new_c[i]
    for (int j=0; j<table->numCols; j+=2) {
      c[i][j] = local[j/2];
      c[i][j+1] = remote[j/2];
    }
    // Update 'selected' bit
    counters[i] = local[len-1];
    remote_counters[i] = remote[len-1];
    // Set selected[i+1] = new_cnt
    counters[i+1] = local[len-2];
    remote_counters[i+1] = remote[len-2];
  }
  // Final shuffle iff not followed by ORDER_BY
}

// Fused group-by-join-aggregattion (sum) operation
// on two input tables, left and right.
void group_by_join(BShareTable* left, BShareTable* right, int start_left,
                   int end_left, int group_att_index, int left_join_index,
                   int right_join_index, int right_att_index, BShare* rb_left,
                   AShare* ra_left, BShare* rb_right, AShare* ra_right,
                   unsigned sum_res_index, unsigned count_res_index) {

  int rank = get_rank();
  int succ_rank = get_succ();

  int batch_size = end_left - start_left;

  // Equality bits
  BShare *b2 = malloc(batch_size*right->numRows*sizeof(BShare));
  assert(b2!=NULL);
  // Remote equality bits
  BShare *b2_remote = malloc(batch_size*right->numRows*sizeof(BShare));
  assert(b2_remote!=NULL);
  // Converted b2
  AShare *b2_a = malloc(batch_size*right->numRows*sizeof(AShare));
  assert(b2_a!=NULL);
  // Partial aggregate
  AShare *sum_right = calloc(batch_size, sizeof(AShare));
  assert(sum_right!=NULL);

  // Step 1: compute the equality predicate for one row of the left input
  // and all rows on the right input in a batch
  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);
  int index;
  // For each row in the batch
  for (int i=start_left, p=0; i<end_left; i++, p++) {
    // Compute bitwise x^y^1
    for (int j=0; j<right->numRows; j++) {
      index = p*right->numRows + j;
      b2[index] = left->contents[i][left_join_index] ^
                  right->contents[j][right_join_index] ^
                  (~(BShare)0); // local share;
      b2_remote[index] = left->contents[i][left_join_index+1] ^
                         right->contents[j][right_join_index+1] ^
                         (~(BShare)0); // remote share
    }
  }
  // The result is stored in the (numbits/2) rightmost bits of result
  for (int l=0; l<numlevels; l++) {
    // For each row in the batch
    for (int i=start_left, p=0; i<end_left; i++, p++) {
      // For each row on the right
      for (int k=0; k<right->numRows; k++) {
        index = p*right->numRows + k;
        b2[index] = eq_b_level2(numbits >> l, b2[index], b2_remote[index]);
      }
    }
    // Exchange results of logical and, except for the final round
    if (l != numlevels-1) {
      exchange_shares_array(b2, b2_remote, batch_size*right->numRows);
    }
  }

  // Step 2: Convert equality bits to arithmetic shares
  convert_single_bit_array(b2, ra_right, rb_right,
                            batch_size*right->numRows, b2_a);
  // exchange arithmetic bits
  exchange_shares_array(b2_a, b2_remote, batch_size*right->numRows);

  // Step 3: Compute the row sum as s+=b2[j] * score[j]
  // For each row in the batch
  for (int i=start_left, p=0; i<end_left; i++, p++) {
    for (int k=0; k<right->numRows; k++) {
      index = p*right->numRows + k;
      sum_right[p] += mul(b2_a[index], b2_remote[index],
                      right->contents[k][right_att_index],
                      right->contents[k][right_att_index+1],
                      get_next_r());
    }
  }
  // Free memory
  free(b2); free(b2_a); free(b2_remote);

  // Remote partial aggregate
  AShare *sum_right_remote = calloc(batch_size, sizeof(AShare));
  assert(sum_right_remote!=NULL);

  // Get remote partial sums
  exchange_shares_array(sum_right, sum_right_remote, batch_size);
  // Group-by bits
  BShare *b1 = malloc(batch_size*sizeof(BShare));
  assert(b1!=NULL);
  // Group-by remote bits
  BShare *b1_remote = malloc(batch_size*sizeof(BShare));
  assert(b1_remote!=NULL);
  // Converted group-by bits
  AShare *b1_a = malloc(batch_size*sizeof(AShare));
  assert(b1_a!=NULL);
  // Converted remote group-by bits
  AShare *b1_a_remote = malloc(batch_size*sizeof(AShare));
  assert(b1_a_remote!=NULL);

  // Step 4: Compute b1 (group-by), convert it to arithmetic and then
  distinct_batch_incr(left, start_left, end_left, group_att_index, b1);
  // Exchange boolean bits
  exchange_shares_array(b1, b1_remote, batch_size);
  // Convert equality bits to arithmetic shares
  convert_single_bit_array(b1, ra_left, rb_left, batch_size, b1_a);
  // Exchange arithmetic bits
  exchange_shares_array(b1_a, b1_a_remote, batch_size);

  // Aggregate and mask
  BShare max=0xFFFFFFFFFFFFFFFF;
  index = start_left;
  int pos = 0;
  if (index==0) { // If it's the very first row
    left->contents[index][sum_res_index] = sum_right[0];
    left->contents[index][sum_res_index+1] = sum_right_remote[0];
    index += 1; // Start aggregating from the second row
    pos += 1;
  }

  for (int i=index; i<end_left; i++) {
    // Compute res = (1-b1)*(prev + sum_right) + b1*sum_right
    left->contents[i][sum_res_index] = mul(rank%2 - b1_a[pos],
                     succ_rank%2 - b1_a_remote[pos],
                     left->contents[i-1][sum_res_index] + sum_right[pos],
                     left->contents[i-1][sum_res_index+1] + sum_right_remote[pos],
                     get_next_r());

    left->contents[i][sum_res_index] += mul(b1_a[pos], b1_a_remote[pos],
                                            sum_right[pos], sum_right_remote[pos],
                                            get_next_r());
    // Exchange the result aggregation to use it in the next iteration
    exchange_shares_array(&left->contents[i][sum_res_index],
                          &left->contents[i][sum_res_index+1], 1);
    BShare bb1 = -b1[pos];
    BShare bb2 = -b1_remote[pos];
    // Compute row_{i-1} = (1-b1)*max + b1*row_{i-1}
    for (int j=0; j<left->numCols-1; j+=2) {
      // NOTE: Some attributes are boolean shares some others arithmetic
      if ( (j==sum_res_index) ||
           (j==count_res_index) ) { // It's an arithmetic share
        AShare left_att = left->contents[i-1][j];
        AShare left_att_rem = left->contents[i-1][j+1];
        left->contents[i-1][j] = mul(rank%2 - b1_a[pos],
                                     succ_rank%2 - b1_a_remote[pos],
                                     max,
                                     max,
                                     get_next_r());
        left->contents[i-1][j] += mul(b1_a[pos], b1_a_remote[pos],
                                       left_att,
                                       left_att_rem,
                                       get_next_r());
      }
      else {  // It's a boolean share
        BShare left_att = left->contents[i-1][j];
        BShare left_att_rem = left->contents[i-1][j+1];
        left->contents[i-1][j] = and_b(~bb1, ~bb2,
                                       max,
                                       max,
                                       get_next_rb());
        left->contents[i-1][j] ^= and_b(bb1, bb2,
                                       left_att,
                                       left_att_rem,
                                       get_next_rb());
      }
    }
    pos++;
  }
  // Free memory
  free(b1); free(b1_a); free(b1_remote); free(b1_a_remote);
  free(sum_right); free(sum_right_remote);
}

static void bitonic_merge(BShare** contents, int low, int cnt,
                           unsigned index_1, unsigned index_2,
                           int num_elements, int asc) {
  if (cnt>1) {
    int k = cnt/2;
    for (int i=low; i<low+k; i++) {
      // Compare rows i, i+k and swap if necessary
      cmp_swap_g(contents[i], contents[i+k], index_1, index_2, num_elements,
                 asc);
    }
    bitonic_merge(contents, low, k, index_1, index_2, num_elements, asc);
    bitonic_merge(contents, low+k, k, index_1, index_2, num_elements, asc);
  }
}

// Sorts the given table of BShares in place
// Relies on cmp_swap(), hence, it works iff sort attributes have the same sign
void bitonic_sort(BShareTable* table, int low, int cnt, unsigned sort_attribute,
                  int asc) {
  if (cnt>1) {
    int k = cnt/2;
    // sort in ascending order since asc here is 1
    bitonic_sort(table, low, k, sort_attribute, 1);
    // sort in descending order since asc here is 0
    bitonic_sort(table, low+k, k, sort_attribute, 0);
    // Will merge whole sequence in ascending order
    // since asc=1
    bitonic_merge(table->contents, low, cnt, sort_attribute, sort_attribute,
                  table->numCols, asc);
  }
}

// Sorts the given table of BShares in place
// Same result as bitonic_sort() but works in batch mode
void bitonic_sort_batch(BShareTable* table, unsigned* sort_attributes,
                        int num_attributes, bool* asc, int batch_size) {
  // Batch size and table size must both be a power of two
  assert(ceil(log2(table->numRows)) == floor(log2(table->numRows)));
  assert(ceil(log2(batch_size)) == floor(log2(batch_size)));
  // Batch size must be less than or equal to n/2
  assert(batch_size <= (table->numRows / 2));
  int rounds = (int) log2(table->numRows);
  int num_batches = (table->numRows / 2) / batch_size;
  // printf("\nRounds: %d Num batches: %d\n", rounds, num_batches);
  // For each phase/level
  for (int i = 0; i < rounds; i++) {
    // For each column
    for(int j = 0; j <= i; j++) {
      int last_index = 0;
      // For each batch
      for (int r=0; r<num_batches; r++){
        last_index = cmp_swap_batch(last_index, table->contents,
                                      table->numRows, table->numCols,
                                      sort_attributes, num_attributes, i, j,
                                      asc, batch_size);
      }
    }
  }
}

// Masks the non-selected rows in the given table
void mask(BShareTable* table, BShare* selected, int batch_size) {
  BShare max=0xFFFFFFFFFFFFFFFF;
  BShare** c = table->contents;
  // Fetch the second arithmetic share of each 'selected' bit -- 1 round
  BShare* remote_selected = malloc((table->numRows)*sizeof(BShare));
  assert(remote_selected!=NULL);
  exchange_shares_array(selected, remote_selected, table->numRows);
  // Make sure batch size is not larger than the input table
  batch_size = (batch_size > table->numRows ? table->numRows : batch_size);
  // Allocate batches for local and remote shares
  int width = table->numCols/2, len = width * batch_size;
  BShare* local = malloc(len*sizeof(BShare));
  assert(local!=NULL);
  BShare* remote = malloc(len*sizeof(BShare));
  assert(remote!=NULL);
  int start=0, end=batch_size, step;
  BShare b1, b2;
  // For all rows in the input table
  while (start<table->numRows) {
    // For all rows within the given batch size
    for (int i=start, k=0; i<end; i++, k+=width) {
      b1 = selected[i] & 1;
      b2 = remote_selected[i] & 1;
      // Compute c_dummy = selected[i]*c[i] + (1-selected[i])*max
      for (int j=0; j<table->numCols; j+=2) {
        local[k+j/2] = and_b(-b1, -b2, c[i][j], c[i][j+1], get_next_rb());
        local[k+j/2] ^= and_b(~(-b1), ~(-b2), max, max, get_next_rb());
      }
    }
    // Fecth remote shares - 1 round
    exchange_shares_array(local, remote, len);
    for (int i=start, k=0; i<end; i++, k+=width) {
      // Set new row
      for (int j=0; j<table->numCols; j+=2) {
        c[i][j] = local[k+j/2];
        c[i][j+1] = remote[k+j/2];
      }
    }
    start = end;
    step = end + batch_size;
    end = (step <= table->numRows ? step : table->numRows);
  }
  free(remote_selected); free(local); free(remote);
}

// KEEP THOSE ONLY IN PRIMITIVES

// A. Compute the 'diagonal', i.e. the last unique AND at each level:
//     - ((x_i ^ y_i) AND x_i), 0 <= i < length-1
//     - (x_0 AND ~y_0), i = length-1
//
//     Levels are defined as follows:
//     x > y <=>
//      (x_l ^ y_l) & x_l  --->  (level 0)
//     ^ ~(x_l ^ y_l) & (x_{l−1} ^ y_{l−1}) & x_{l−1}  --->  (level 1)
//     ^ ~(x_l ^ y_l) & ~(x_{l−1} ^ y_{l−1}) & (x_{l−2} ^ y_{l−2}) & x_{l−2}  --->  (level 2)
//     ^ ...
//     ^ ~(x_l ^ y_l) & ~(x_{l−1} ^ y_{l−1}) &...& ~(x_2 ^ y_2) & ~(~x_1 & y_1)  --->  (level length-1)
//
//     This step evaluates 'length' logical ANDs in total.
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

// A. Compute the 'diagonal', i.e. the last unique AND at each level:
//     - ((x_i ^ y_i) AND x_i), 0 <= i < length-1
//     - (x_0 AND ~y_0), i = length-1
//
//     Levels are defined as follows:
//     x > y <=>
//      (x_l ^ y_l) & x_l  --->  (level 0)
//     ^ ~(x_l ^ y_l) & (x_{l−1} ^ y_{l−1}) & x_{l−1}  --->  (level 1)
//     ^ ~(x_l ^ y_l) & ~(x_{l−1} ^ y_{l−1}) & (x_{l−2} ^ y_{l−2}) & x_{l−2}  --->  (level 2)
//     ^ ...
//     ^ ~(x_l ^ y_l) & ~(x_{l−1} ^ y_{l−1}) &...& ~(x_2 ^ y_2) & (x_1 & ~y_1)  --->  (level length-1)
//
//     This step evaluates 'length' logical ANDs in total.
static unsigned long long gr_round_a(BShare x1, BShare x2, BShare y1, BShare y2, int length) {
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

  // Set LSB to (x_0 AND ~y_0)
  last_ands |= and_b(get_bit(x1, 0), get_bit(x2, 0),
                          get_bit(y1, 0) ^ mask, get_bit(y2, 0) ^ mask,
                          get_next_rb()) & mask;

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
