#include "primitives.h"
#include "utils.h"

#define XCHANGE_MSG_TAG 7

static BShare eq_b_level(int, BShare, BShare);
// static BShare eq_b_level2(int numbits, BShare z1, BShare z2);
static unsigned long long gr_round_a(BShare, BShare, BShare, BShare, int);
static unsigned long long geq_round_a(BShare, BShare, BShare, BShare, int);
static unsigned long long gr_round_b(BShare, BShare, BShare, BShare, int,
                                      char local[], char remote[]);
static unsigned long long gr_round_c(int, int, int, char local[], char remote[],
                                      int levels[], int *bit_count);
static unsigned long long gr_round_c_char(int, int, int, char local[], char remote[],
                                           char levels[], int *bit_count);
static int get_next_index(int, int, int);
static void compute_composite_2(BitShare **, BitShare **, BitShare **, BitShare **,
                                 int, int);
static void compute_composite_3(BitShare **, BitShare **, BitShare **, BitShare **,
                                 int, int);
static void update_b(BitShare **, BitShare **, bool *, int, int);

// Addition
AShare add(AShare x, AShare y)
{
  return x + y;
}

// Multiplication (assumes three parties)
AShare mul(AShare x1, AShare x2, AShare y1, AShare y2, AShare rnum)
{
  AShare z = (x1 * y1) + (x1 * y2) + (x2 * y1);
  // Add random number
  z += rnum;
  return z;
}

// Equality (assumes three parties)
AShare eq(AShare x1, AShare x2, AShare y1, AShare y2, AShare w1, AShare w2,
          AShare rnum)
{
  AShare s1 = x1 - y1;
  AShare s2 = x2 - y2;
  // Compute (x-y)w
  return mul(s1, s2, w1, w2, rnum);
}

// Logical OR (assumes three parties)
AShare or (AShare x1, AShare x2, AShare y1, AShare y2, AShare rnum)
{
  return mul(x1, x2, y1, y2, rnum);
}

// Logical AND (assumes three parties)
AShare and (AShare x1, AShare x2, AShare y1, AShare y2, AShare rnum1,
            AShare rnum2)
{
  AShare x_sq = mul(x1, x2, x1, x2, rnum1);
  AShare y_sq = mul(y1, y2, y1, y2, rnum2);
  return add(x_sq, y_sq);
}

// Complement
BShare not(BShare x)
{
  return ~x;
}

// Logical AND using boolean shares (assumes three parties)
inline BShare and_b(const BShare x1, const BShare x2, const BShare y1,
                    const BShare y2, const BShare rnum)
{
  BShare z = (x1 & y1) ^ (x1 & y2) ^ (x2 & y1);
  // XOR random number
  z ^= rnum;
  return z;
}

// Logical AND using boolean shares for arrays
inline void and_b_array(const BShare *x1, const BShare *x2,
                        const BShare *y1, const BShare *y2,
                        const BShare *rnum, int len, BShare *res)
{

  for (int i = 0; i < len; i++)
  {
    res[i] = (x1[i] & y1[i]) ^ (x1[i] & y2[i]) ^ (x2[i] & y1[i]);
    res[i] ^= rnum[i];
  }
}

// Logical AND of all elements in the given array
inline void and_b_all(BShare *x1, BShare *x2, int len)
{
  assert(len > 0);
  while (len > 1)
  {
    // For each pair of elements in the given array
    for (int i = 0; i < len - 1; i += 2)
    {
      x1[i / 2] = (x1[i] & x1[i + 1]) ^ (x1[i] & x2[i + 1]) ^ (x2[i] & x1[i + 1]);
      x1[i / 2] ^= get_next_rb();
    }
    // Get remote shares
    exchange_shares_array(x1, x2, len / 2);
    int m = len % 2;
    if (m != 0)
    { // Shift last odd element
      x1[len / 2] = x1[len - 1];
      x2[len / 2] = x2[len - 1];
    }
    // Number of remaining elements
    len = len / 2 + m;
  }
  // Keep LSB
  x1[0] &= 1;
  x2[0] &= 1;
}

// Logical AND using single-bit boolean shares (assumes three parties)
inline BitShare and_bit(BitShare x1, BitShare x2, BitShare y1, BitShare y2,
                        BitShare rnum)
{
  BitShare z = (x1 & y1) ^ (x1 & y2) ^ (x2 & y1);
  // XOR random bit
  z ^= rnum;
  return z;
}

// array-based and_bit
void and_bit_array(const BitShare *x1, const BitShare *x2,
                   const BitShare *y1, const BitShare *y2,
                   const BitShare *rnum, int len, BitShare *res)
{

  for (int i = 0; i < len; i++)
  {
    res[i] = (x1[i] & y1[i]) ^ (x1[i] & y2[i]) ^ (x2[i] & y1[i]);
    res[i] ^= rnum[i];
  }
}

// Logical XOR
BShare xor (BShare x, BShare y) {
  return x ^ y;
}

    // Boolean equality
    BShare eq_b(BShare x1, BShare x2, BShare y1, BShare y2)
{

  // compute bitwise x^y^1
  BShare res1 = x1 ^ y1 ^ (BShare)0xFFFFFFFFFFFFFFFF; // local share
  BShare res2 = x2 ^ y2 ^ (BShare)0xFFFFFFFFFFFFFFFF; // remote share

  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);

  // The result is stored in the (numbits/2) rightmost bits of res1, res2
  for (int l = 0; l < numlevels; l++)
  {
    res1 = eq_b_level2(numbits >> l, res1, res2);
    // Exchange results of logical and, except for the final round
    if (l != numlevels - 1)
    {
      res2 = exchange_shares(res1);
    }
  }

  // Return one share of the final result.
  // We need to call exchange again before using it in a subsequent operation.
  return res1;
}

// Blocking boolean equality (with eager rnum generation)
BShare eq_b_sync(BShare x1, BShare x2, BShare y1, BShare y2)
{

  // compute bitwise x^y^1
  BShare res1 = x1 ^ y1 ^ (~(BShare)0); // local share
  BShare res2 = x2 ^ y2 ^ (~(BShare)0); // remote share

  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);

  // The result is stored in the (numbits/2) rightmost bits of res1, res2
  for (int l = 0; l < numlevels; l++)
  {
    res1 = eq_b_level2(numbits >> l, res1, res2);

    // Exchange results of logical and, except for the final round
    if (l != numlevels - 1)
    {
      res2 = exchange_shares(res1);
    }
  }

  // Return one share of the final result.
  // We need to call exchange again before using it in a subsequent operation.
  return res1;
}

// Asynchronous boolean equality (with eager rnum generation)
BShare eq_b_async(BShare x1, BShare x2, BShare y1, BShare y2)
{

  // compute bitwise x^y^1
  BShare res1 = x1 ^ y1 ^ (~(BShare)0); // local share
  BShare res2 = x2 ^ y2 ^ (~(BShare)0); // remote share

  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);

  // The result is stored in the (numbits/2) rightmost bits of res1, res2
  for (int l = 0; l < numlevels; l++)
  {
    res1 = eq_b_level2(numbits >> l, res1, res2);

    // Exchange results of logical and, except for the final round
    if (l != numlevels - 1)
    {
      res2 = exchange_shares(res1);
    }
  }

  // Return one share of the final result.
  // We need to call exchange again before using it in a subsequent operation.
  return res1;
}

// Computes bitwise equality for one tree level
// The result is stored in the last numbits/2 bits of res
static BShare eq_b_level(int numbits, BShare z1, BShare z2)
{

  const BShare mask = 1;
  BShare res = 0;

  for (int i = 0, j = 0; i < numbits; i += 2, j++)
  {
    BShare bx1 = (z1 >> i) & mask;       // bit at position i
    BShare by1 = (z1 >> (i + 1)) & mask; // bit at position i+1
    BShare bx2 = (z2 >> i) & mask;       // bit at position i of 2nd share
    BShare by2 = (z2 >> (i + 1)) & mask; // bit at position i+1 of 2nd share

    // store the result (and's LSB) in the result's jth bit
    BShare out = and_b(bx1, bx2, by1, by2, get_next_rb());
    res |= (out & mask) << j;
  }
  return res;
}

// array-based boolean equality
void eq_b_array(BShare *x1, BShare *x2, BShare *y1, BShare *y2, long len,
                BShare *res)
{

  BShare *res2 = malloc(len * sizeof(BShare)); // remote shares
  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);

  // compute bitwise x^y^1
  for (long i = 0; i < len; i++)
  {
    res[i] = x1[i] ^ y1[i] ^ (~(BShare)0);  // local share;
    res2[i] = x2[i] ^ y2[i] ^ (~(BShare)0); // remote share
  }

  // The result is stored in the (numbits/2) rightmost bits of res, res2 elements
  for (int l = 0; l < numlevels; l++)
  {

    for (long i = 0; i < len; i++)
    {
      res[i] = eq_b_level2(numbits >> l, res[i], res2[i]);
    }

    // Exchange results of logical and, except for the final round
    if (l != numlevels - 1)
    {
      exchange_shares_array(res, res2, len);
    }
  }
  // The local share of the final result is stored in res.
  // We need to call exchange again before using it in a subsequent operation.
  free(res2);
}

// array-based boolean equality with interleave between computation and communication
// per element
void eq_b_array_inter(BShare *x1, BShare *x2, BShare *y1, BShare *y2, long len,
                      BShare *res)
{

  BShare *res2 = malloc(len * sizeof(BShare)); // remote shares
  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);
  int exchanges = 10;
  long batch_size = len / exchanges;

  // compute bitwise x^y^1
  for (long i = 0; i < len; i++)
  {
    res[i] = x1[i] ^ y1[i] ^ (~(BShare)0);  // local share;
    res2[i] = x2[i] ^ y2[i] ^ (~(BShare)0); // remote share
  }

  // // The result is stored in the (numbits/2) rightmost bits of res, res2 elements
  for (int l = 0; l < numlevels - 1; l++)
  {

    //   // exchange 10 times per level
    for (long i = 0; i < len; i++)
    {

      res[i] = eq_b_level2(numbits >> l, res[i], res2[i]);

      //     // first 9 exchanges
      if (((i + 1) % batch_size) == 0)
      {

        TCP_Recv(&res2[i - (batch_size - 1)], batch_size, get_succ(), sizeof(BShare));
        TCP_Send(&res[i - (batch_size - 1)], batch_size, get_pred(), sizeof(BShare));
      }

      //     // last exchange
      if (i == len - 1)
      {

        TCP_Recv(&res2[len - batch_size], batch_size, get_succ(), sizeof(BShare));
        TCP_Send(&res[len - batch_size], batch_size, get_pred(), sizeof(BShare));
      }
    }
  }

  // Last level (no exchange)
  for (long i = 0; i < len; i++)
  {
    res[i] = eq_b_level2(numbits >> (numlevels - 1), res[i], res2[i]);
  }

  // The local share of the final result is stored in res.
  // We need to call exchange again before using it in a subsequent operation.
  free(res2);
}

// array-based boolean equality with synchronous exchange
// and interleaving between batches of elements
void eq_b_array_inter_batch(BShare *x1, BShare *x2, BShare *y1, BShare *y2, long len,
                            BShare *res)
{

  BShare *res2 = malloc(len * sizeof(BShare)); // remote shares
  int numbits = sizeof(BShare) * 8;
  int numlevels = log2(numbits);
  // compute bitwise x^y^1
  for (long i = 0; i < len; i++)
  {
    res[i] = x1[i] ^ y1[i] ^ (~(BShare)0);  // local share;
    res2[i] = x2[i] ^ y2[i] ^ (~(BShare)0); // remote share
  }

  for (int l = 0; l < numlevels; l++)
  {
    for (long i = 0; i < len; i++)
    {
      res[i] = eq_b_level2(numbits >> l, res[i], res2[i]);
      //     // exchange result for element i, level l
      //     // except for the final round
      if (l != numlevels - 1)
      {
        TCP_Recv(&res2[i], 1, get_succ(), sizeof(BShare));
        TCP_Send(&res[i], 1, get_pred(), sizeof(BShare));
      }
    }
  }
  // The local share of the final result is stored in res.
  // We need to call exchange again before using it in a subsequent operation.
  free(res2);
}

// same as eq_b_level but without rnums argument
BShare eq_b_level2(int numbits, BShare z1, BShare z2)
{

  const BShare mask = 1;
  BShare res = 0;

  for (int i = 0, j = 0; i < numbits; i += 2, j++)
  {
    BShare bx1 = (z1 >> i) & mask;       // bit at position i
    BShare by1 = (z1 >> (i + 1)) & mask; // bit at position i+1
    BShare bx2 = (z2 >> i) & mask;       // bit at position i of 2nd share
    BShare by2 = (z2 >> (i + 1)) & mask; // bit at position i+1 of 2nd share

    // store the result (and's LSB) in the result's jth bit
    BShare out = and_b(bx1, bx2, by1, by2, get_next_rb());
    res |= (out & mask) << j;
  }
  return res;
}

// Computes x >? y using boolean shares in 1+logN communication rounds
// Avoids redundant computations by reusing results from previous rounds
BitShare greater(const BShare x1, const BShare x2,
                 const BShare y1, const BShare y2)
{

  const BShare mask = 1;
  int share_length = sizeof(BShare) * 8; // The length of BShare in number of bits
  int len = (share_length - 1) * sizeof(char) + sizeof(BShare);
  // Local and remote bits per level
  char local[len], remote[len];
  unsigned long long local_bits = 0, remote_bits = 0;

  /** FIRST ROUND **/
  local_bits = gr_round_a(x1, x2, y1, y2, share_length);

  // Get the second share of each bit as computed by the other party
  remote_bits = exchange_shares_u(local_bits);
  // Unpack bits and update levels
  for (int i = 0; i < share_length - 1; i++)
  {
    local[i] = (char)get_bit_u(local_bits, share_length - i - 1);
    remote[i] = (char)get_bit_u(remote_bits, share_length - i - 1);
  }
  // Update the last-level bits
  unsigned long long l_bit = get_bit_u(local_bits, 0);
  unsigned long long r_bit = get_bit_u(remote_bits, 0);
  memcpy(&local[share_length - 1], &l_bit, sizeof(unsigned long long));
  memcpy(&remote[share_length - 1], &r_bit, sizeof(unsigned long long));

  /** SECOND ROUND **/
  local_bits = gr_round_b(x1, x2, y1, y2, share_length, local, remote);

  // Get the second share of each bit as computed by the other party
  remote_bits = exchange_shares_u(local_bits);
  // Unpack the length/2 MSBs and store them at the last level
  unsigned long long tmp = (remote_bits >> (share_length / 2));
  memcpy(&remote[share_length - 1], &tmp, sizeof(unsigned long long));
  // Unpack the rest and update odd levels
  for (int i = 1; i < share_length - 1; i += 2)
  {
    remote[i] = get_bit_u(remote_bits, i / 2);
  }

  /** REMAINING ROUNDS **/
  int rounds = (int)log2(share_length / 2);

  int levels[share_length]; // Caches levels with projected bits at each round
  for (int i = 0; i < share_length; i++)
  { // Initialize level cache
    levels[i] = -1;
  }

  int bits_left = share_length, bit_count;
  ;
  for (int i = 1; i <= rounds; i++)
  {
    bit_count = 0;
    bits_left /= 2;
    local_bits = gr_round_c(i, bits_left, share_length,
                            local, remote, levels, &bit_count);

    // Exchange all bits of the current round and unpack accordingly
    remote_bits = exchange_shares_u(local_bits);
    // Unpack bits of last level
    BShare tmp = (remote_bits >> bit_count);
    memcpy(&remote[share_length - 1], &tmp, sizeof(BShare));
    // Unpack the rest and reset level cache
    int l = 0;
    while ((levels[l] >= 0) & (l < share_length))
    {
      remote[levels[l]] = get_bit_u(remote_bits, l);
      levels[l++] = -1; // Reset for next round
    }
  }

  /** Do a final XOR of all levels **/
  BShare res = 0;
  for (int i = 0; i < share_length - 1; i++)
  {
    res ^= local[i];
  }
  res ^= *((BShare *)&local[share_length - 1]);
  // Return local share
  return (BitShare)(res & mask);
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
static unsigned long long gr_round_a(BShare x1, BShare x2, BShare y1, BShare y2, int length)
{
  // Compute (x_i ^ y_i)
  BShare xor1 = x1 ^ y1;
  BShare xor2 = x2 ^ y2;

  unsigned long long last_and = 0, last_ands = 0;
  const BShare mask = 1;
  int index; // The bit index (index=0 for the LSB)

  for (int i = 0; i < length - 1; i++)
  {
    // Compute ((x_{length-i-1} ^ y_{length-i-1}) AND x_{length-i-1})
    index = length - i - 1;
    last_and = and_b(get_bit(xor1, index), get_bit(xor2, index),
                     get_bit(x1, index), get_bit(x2, index), get_next_rb()) &
               mask;
    // Pack result bit in last_ands to send all together
    last_ands |= (last_and << index);
  }

  // Set LSB to (x_0 AND ~y_0)
  last_ands |= and_b(get_bit(x1, 0), get_bit(x2, 0),
                     get_bit(y1, 0) ^ mask, get_bit(y2, 0) ^ mask,
                     get_next_rb()) &
               mask;

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
//     ^ ~(x_l ^ y_l) & ~(x_{l−1} ^ y_{l−1}) &...& ~(x_2 ^ y_2) & ~(~x_1 & y_1)  --->  (level length-1)
//
//     This step evaluates 'length' logical ANDs in total.
static unsigned long long geq_round_a(BShare x1, BShare x2, BShare y1, BShare y2, int length)
{
  // Compute (x_i ^ y_i)
  BShare xor1 = x1 ^ y1;
  BShare xor2 = x2 ^ y2;

  unsigned long long last_and = 0, last_ands = 0;
  const BShare mask = 1;
  int index; // The bit index (index=0 for the LSB)

  for (int i = 0; i < length - 1; i++)
  {
    // Compute ((x_{length-i-1} ^ y_{length-i-1}) AND x_{length-i-1})
    index = length - i - 1;
    last_and = and_b(get_bit(xor1, index), get_bit(xor2, index),
                     get_bit(x1, index), get_bit(x2, index), get_next_rb()) &
               mask;
    // Pack result bit in last_ands to send all together
    last_ands |= (last_and << index);
  }

  // Store ~(~x_0 AND y_0) at the last level (length-1)
  last_ands |= (and_b(get_bit(x1, 0) ^ mask, get_bit(x2, 0) ^ mask,
                      get_bit(y1, 0), get_bit(y2, 0),
                      get_next_rb()) &
                mask) ^
               mask;

  // Set LSB to (x_0 AND ~y_0)
  // last_ands |= and_b(get_bit(x1, 0), get_bit(x2, 0),
  //                         get_bit(y1, 0) ^ mask, get_bit(y2, 0) ^ mask,
  //                         get_next_rb()) & mask;

  return last_ands;
}

// B. Compute next to last AND at odd levels as well as 1st round of pairwise
// ANDs at the last level. This step performs 'length' logical ANDs in total.
static unsigned long long gr_round_b(BShare x1, BShare x2, BShare y1, BShare y2,
                                      int length, char local[], char remote[])
{
  // Compute ~(x_i ^ y_i)
  BShare not_xor1 = ~(x1 ^ y1);
  BShare not_xor2 = ~(x2 ^ y2);
  int index;
  const BShare mask = 1;

  unsigned long long local_bits = 0;
  for (int i = 1, j = 0; i < length - 1; i += 2, j++)
  { // For all odd levels (length/2)
    index = length - i;
    // Set ~(x_{length-i} ^ y_{length-i}) next to the last bit
    local[i] |= (get_bit(not_xor1, index) << 1);
    // Set ~(x_{length-i} ^ y_{length-i}) next to the last remote bit
    remote[i] |= (get_bit(not_xor2, index) << 1);
    // Compute next to last logical AND for level i
    local[i] = (and_b(get_bit_u8(local[i], 0), get_bit_u8(remote[i], 0),
                      get_bit_u8(local[i], 1), get_bit_u8(remote[i], 1),
                      get_next_rb()) &
                mask);
    // Pack result bit in local_bits to send all together
    local_bits |= (local[i] << j);
  }
  // Compute first round of pairwise logical ANDs at the last level
  unsigned long long tmp = eq_b_level2(length,
                                       unset_lsbs(not_xor1, 1) | *((unsigned long long *)&local[length - 1]),
                                       unset_lsbs(not_xor2, 1) | *((unsigned long long *)&remote[length - 1]));
  memcpy(&local[length - 1], &tmp, sizeof(unsigned long long));

  // Pack the length/2 result bits in the vacant MSBs of local_bits
  // local_bits |= ( ((unsigned long long) local[length-1]) << (length/2) );
  local_bits |= ((*((unsigned long long *)&local[length - 1])) << (length / 2));

  return local_bits;
}

// C. Continue in a loop. Each round breaks into the the following steps:
//    1. Project every other bit of the last level to 2^r levels,
//       starting at level 2^r * (bits_left - p), where p is the bit's index
//       (p=0 for the LSB). The bit is copied next to the LSB of the
//       corresponding level.
//    2. Evaluate a logical AND between the projected bit and the LSB at
//       the corresponding level.
//    3. Evaluate the next round of pairwise ANDs at the last level.
static unsigned long long gr_round_c(int i, int bits_left, int length, char local[], char remote[],
                                      int levels[], int *bit_count)
{

  int current_level, num_levels;
  const BShare mask = 1;
  unsigned long long to_send = 0;
  num_levels = (1 << i);
  // Project common bits to avoid redundant computation (and communication)
  for (int p = bits_left - 1; p > 0; p -= 2)
  {
    current_level = num_levels * (bits_left - p);
    for (int j = 0; j < num_levels; j++)
    {
      // Project bits from last level
      BShare l_tmp = *((unsigned long long *)&local[length - 1]);
      BShare r_tmp = *((unsigned long long *)&remote[length - 1]);
      local[current_level] |= (get_bit(l_tmp, p) << 1);
      remote[current_level] |= (get_bit(r_tmp, p) << 1);
      // Do the logical AND
      local[current_level] = and_b(get_bit_u8(local[current_level], 0),
                                   get_bit_u8(remote[current_level], 0),
                                   get_bit_u8(local[current_level], 1),
                                   get_bit_u8(remote[current_level], 1),
                                   get_next_rb()) &
                             mask;
      // Pack the result
      to_send |= (local[current_level] << (*bit_count));
      // Cache level to unpack remote bit later
      levels[*bit_count] = current_level;
      (*bit_count)++;
      current_level++;
      if (current_level == (length - 1))
        break;
    }
  }
  // Process last level
  BShare tmp = eq_b_level2(bits_left, *((unsigned long long *)&local[length - 1]),
                           *((unsigned long long *)&remote[length - 1]));
  memcpy(&local[length - 1], &tmp, sizeof(unsigned long long));
  // Pack bits of the last level
  to_send |= (*((unsigned long long *)&local[length - 1]) << (*bit_count));
  return to_send;
}

static unsigned long long gr_round_c_char(int i, int bits_left, int length, char local[], char remote[],
                                           char levels[], int *bit_count)
{

  int current_level, num_levels;
  const BShare mask = 1;
  unsigned long long to_send = 0;
  num_levels = (1 << i);
  // Project common bits to avoid redundant computation (and communication)
  for (int p = bits_left - 1; p > 0; p -= 2)
  {
    current_level = num_levels * (bits_left - p);
    for (int j = 0; j < num_levels; j++)
    {
      // Project bits from last level
      BShare l_tmp = *((unsigned long long *)&local[length - 1]);
      BShare r_tmp = *((unsigned long long *)&remote[length - 1]);
      local[current_level] |= (get_bit(l_tmp, p) << 1);
      remote[current_level] |= (get_bit(r_tmp, p) << 1);
      // Do the logical AND
      local[current_level] = and_b(get_bit_u8(local[current_level], 0),
                                   get_bit_u8(remote[current_level], 0),
                                   get_bit_u8(local[current_level], 1),
                                   get_bit_u8(remote[current_level], 1),
                                   get_next_rb()) &
                             mask;
      // Pack the result
      to_send |= (local[current_level] << (*bit_count));
      // Cache level to unpack remote bit later
      levels[*bit_count] = current_level;
      (*bit_count)++;
      current_level++;
      if (current_level == (length - 1))
        break;
    }
  }
  // Process last level
  BShare tmp = eq_b_level2(bits_left, *((unsigned long long *)&local[length - 1]),
                           *((unsigned long long *)&remote[length - 1]));
  memcpy(&local[length - 1], &tmp, sizeof(unsigned long long));
  // Pack bits of the last level
  to_send |= (*((unsigned long long *)&local[length - 1]) << (*bit_count));
  return to_send;
}

void greater_batch(const BShare *x1, const BShare *x2,
                   const BShare *y1, const BShare *y2,
                   int numElements, BitShare *res)
{

  int share_length = sizeof(BShare) * 8; // The length of BShare in number of bits
  int len = (share_length - 1) * sizeof(char) + sizeof(BShare);
  // Local and remote bits per level
  unsigned long long *local_bits = malloc(numElements * sizeof(BShare));
  assert(local_bits != NULL);
  unsigned long long *remote_bits = malloc(numElements * sizeof(BShare));
  assert(remote_bits != NULL);

  // For each element, we reserve 1 byte for 63 levels + 1 BShare for last level
  char **local = allocate_2D_byte_array(numElements, len);
  char **remote = allocate_2D_byte_array(numElements, len);

  /** FIRST ROUND **/
  for (int i = 0; i < numElements; i++)
  {
    local_bits[i] = gr_round_a(x1[i], x2[i], y1[i], y2[i], share_length);
  }

  // Get the second share of each bit as computed by the other party
  exchange_shares_array_u(local_bits, remote_bits, numElements);

  // Unpack bits and update levels
  for (int i = 0; i < numElements; i++)
  {
    for (int j = 0; j < share_length - 1; j++)
    {
      local[i][j] = get_bit_u(local_bits[i], share_length - j - 1);
      remote[i][j] = get_bit_u(remote_bits[i], share_length - j - 1);
    }
    // Update last-level bits
    unsigned long long l_tmp = get_bit_u(local_bits[i], 0);
    unsigned long long r_tmp = get_bit_u(remote_bits[i], 0);
    memcpy(&local[i][share_length - 1], &l_tmp, sizeof(unsigned long long));
    memcpy(&remote[i][share_length - 1], &r_tmp, sizeof(unsigned long long));
  }

  /** SECOND ROUND **/
  for (int i = 0; i < numElements; i++)
  {
    local_bits[i] = gr_round_b(x1[i], x2[i], y1[i], y2[i], share_length,
                               &local[i][0], &remote[i][0]);
  }

  // Get the second share of each bit as computed by the other party
  exchange_shares_array_u(local_bits, remote_bits, numElements);

  for (int i = 0; i < numElements; i++)
  {
    // Unpack the length/2 MSBs and store them at the last level
    unsigned long long tmp = (remote_bits[i] >> (share_length / 2));
    memcpy(&remote[i][share_length - 1], &tmp, sizeof(unsigned long long));
    // Unpack the rest and update odd levels
    for (int j = 1; j < share_length - 1; j += 2)
    {
      remote[i][j] = get_bit_u(remote_bits[i], j / 2);
    }
  }

  /** REMAINING ROUNDS **/
  int rounds = (int)log2(share_length / 2);
  char **levels = allocate_2D_byte_array(numElements, share_length / 2); // max 'length' levels per pair

  // Initialize level cache
  for (int i = 0; i < numElements; i++)
  {
    for (int j = 0; j < share_length / 2; j++)
    {
      levels[i][j] = -1;
    }
  }

  int bits_left = share_length, bit_count;
  for (int r = 1; r <= rounds; r++)
  {
    bits_left /= 2;
    for (int i = 0; i < numElements; i++)
    {
      bit_count = 0;
      local_bits[i] = gr_round_c_char(r, bits_left, share_length,
                                      &local[i][0], &remote[i][0],
                                      &levels[i][0], &bit_count);
    }

    // Exchange all bits of the current round and unpack accordingly
    exchange_shares_array_u(local_bits, remote_bits, numElements);

    // Unpack bits of last level
    for (int i = 0; i < numElements; i++)
    {
      unsigned long long tmp = (remote_bits[i] >> bit_count);
      memcpy(&remote[i][share_length - 1], &tmp, sizeof(BShare));
      // Unpack the rest and reset level cache for next round
      int l = 0;
      while ((levels[i][l] >= 0) & (l < share_length / 2))
      {
        remote[i][(int)levels[i][l]] = get_bit_u(remote_bits[i], l);
        levels[i][l++] = -1; // Reset for next round
      }
    }
  }

  free(local_bits);
  free(remote_bits);
  free(levels);

  // One bitshare for each greater() comparison
  BitShare mask = 1;
  for (int i = 0; i < numElements; i++)
  {
    res[i] = 0;
    // Do a final XOR of all levels
    for (int j = 0; j < share_length - 1; j++)
    {
      res[i] ^= local[i][j];
    }
    // XOR with last level
    res[i] ^= *((unsigned long long *)&local[i][share_length - 1]);
    res[i] &= mask;
  }

  free(local);
  free(remote);
}

void geq_batch(const BShare *x1, const BShare *x2,
               const BShare *y1, const BShare *y2,
               int numElements, BitShare *res)
{

  int share_length = sizeof(BShare) * 8; // The length of BShare in number of bits
  int len = (share_length - 1) * sizeof(char) + sizeof(BShare);
  // Local and remote bits per level
  unsigned long long *local_bits = malloc(numElements * sizeof(BShare));
  assert(local_bits != NULL);
  unsigned long long *remote_bits = malloc(numElements * sizeof(BShare));
  assert(remote_bits != NULL);

  // For each element, we reserve 1 byte for 63 levels + 1 BShare for last level
  char **local = allocate_2D_byte_array(numElements, len);
  char **remote = allocate_2D_byte_array(numElements, len);

  /** FIRST ROUND **/
  for (int i = 0; i < numElements; i++)
  {
    local_bits[i] = geq_round_a(x1[i], x2[i], y1[i], y2[i], share_length);
  }

  // Get the second share of each bit as computed by the other party
  exchange_shares_array_u(local_bits, remote_bits, numElements);

  // Unpack bits and update levels
  for (int i = 0; i < numElements; i++)
  {
    for (int j = 0; j < share_length - 1; j++)
    {
      local[i][j] = get_bit_u(local_bits[i], share_length - j - 1);
      remote[i][j] = get_bit_u(remote_bits[i], share_length - j - 1);
    }
    // Update last-level bits
    unsigned long long l_tmp = get_bit_u(local_bits[i], 0);
    unsigned long long r_tmp = get_bit_u(remote_bits[i], 0);
    memcpy(&local[i][share_length - 1], &l_tmp, sizeof(unsigned long long));
    memcpy(&remote[i][share_length - 1], &r_tmp, sizeof(unsigned long long));
  }

  /** SECOND ROUND **/
  for (int i = 0; i < numElements; i++)
  {
    local_bits[i] = gr_round_b(x1[i], x2[i], y1[i], y2[i], share_length,
                               &local[i][0], &remote[i][0]);
  }

  // Get the second share of each bit as computed by the other party
  exchange_shares_array_u(local_bits, remote_bits, numElements);

  for (int i = 0; i < numElements; i++)
  {
    // Unpack the length/2 MSBs and store them at the last level
    unsigned long long tmp = (remote_bits[i] >> (share_length / 2));
    memcpy(&remote[i][share_length - 1], &tmp, sizeof(unsigned long long));
    // Unpack the rest and update odd levels
    for (int j = 1; j < share_length - 1; j += 2)
    {
      remote[i][j] = get_bit_u(remote_bits[i], j / 2);
    }
  }

  /** REMAINING ROUNDS **/
  int rounds = (int)log2(share_length / 2);
  int **levels = allocate_int_2D_table(numElements, share_length); // max 'length' levels per pair

  // Initialize level cache
  for (int i = 0; i < numElements; i++)
  {
    for (int j = 0; j < share_length; j++)
    {
      levels[i][j] = -1;
    }
  }

  int bits_left = share_length, bit_count;
  for (int r = 1; r <= rounds; r++)
  {
    bits_left /= 2;
    for (int i = 0; i < numElements; i++)
    {
      bit_count = 0;
      local_bits[i] = gr_round_c(r, bits_left, share_length,
                                 &local[i][0], &remote[i][0],
                                 &levels[i][0], &bit_count);
    }

    // Exchange all bits of the current round and unpack accordingly
    exchange_shares_array_u(local_bits, remote_bits, numElements);

    // Unpack bits of last level
    for (int i = 0; i < numElements; i++)
    {
      unsigned long long tmp = (remote_bits[i] >> bit_count);
      memcpy(&remote[i][share_length - 1], &tmp, sizeof(BShare));
      // Unpack the rest and reset level cache for next round
      int l = 0;
      while ((levels[i][l] >= 0) & (l < share_length))
      {
        remote[i][levels[i][l]] = get_bit_u(remote_bits[i], l);
        levels[i][l++] = -1; // Reset for next round
      }
    }
  }

  free(local_bits);
  free(remote_bits);
  free(levels);

  // One bitshare for each greater() comparison
  BitShare mask = 1;
  for (int i = 0; i < numElements; i++)
  {
    res[i] = 0;
    // Do a final XOR of all levels
    for (int j = 0; j < share_length - 1; j++)
    {
      res[i] ^= local[i][j];
    }
    // XOR with last level
    res[i] ^= *((unsigned long long *)&local[i][share_length - 1]);
    res[i] &= mask;
  }

  free(local);
  free(remote);
}

// Computes x >=? y using boolean shares in 1+logN communication rounds
// Avoids redundant computations by reusing results from previous rounds
BitShare geq(const BShare x1, const BShare x2,
             const BShare y1, const BShare y2)
{

  BShare mask = 1;
  int rindex = 0,                  // The index of the next available rnum in rnums array
      length = sizeof(BShare) * 8; // The length of BShare in number of bits
  // Local and remote bits per level
  BShare local[length], remote[length];
  // Compute (x_i ^ y_i)
  BShare xor1 = x1 ^ y1;
  BShare xor2 = x2 ^ y2;
  // Compute ~(x_i ^ y_i)
  BShare not_xor1 = ~xor1;
  BShare not_xor2 = ~xor2;

  // A. Compute the 'diagonal', i.e. the last unique AND at each level:
  //     - ((x_i ^ y_i) AND x_i), 0 <= i < length-1
  //     - ~(~x_0 AND y_0), i = length-1
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
  unsigned long long last_and = 0, last_ands = 0;
  int index; // The bit index (index=0 for the LSB)
  for (int i = 0; i < length - 1; i++)
  {
    // Compute ((x_{length-i-1} ^ y_{length-i-1}) AND x_{length-i-1})
    index = length - i - 1;
    last_and = and_b(get_bit(xor1, index), get_bit(xor2, index),
                     get_bit(x1, index), get_bit(x2, index), get_next_rb()) &
               mask;
    // Pack result bit in last_ands to send all together
    last_ands |= (last_and << index);
  }
  // Store ~(~x_0 AND y_0) at the last level (length-1)
  local[length - 1] = (and_b(get_bit(x1, 0) ^ mask, get_bit(x2, 0) ^ mask,
                             get_bit(y1, 0), get_bit(y2, 0),
                             get_next_rb()) &
                       mask) ^
                      mask;
  // Set LSB to ~(~x_0 AND y_0)
  last_ands |= local[length - 1];
  // Get the second share of each bit as computed by the other party
  unsigned long long remote_last_ands = exchange_shares_u(last_ands);
  // Unpack bits and update levels
  for (int i = 0; i < length; i++)
  {
    local[i] = get_bit_u(last_ands, length - i - 1);
    remote[i] = get_bit_u(remote_last_ands, length - i - 1);
  }

  // B. Compute next to last AND at odd levels as well as 1st round of pairwise
  // ANDs at the last level. This step performs 'length' logical ANDs in total.
  unsigned long long local_bits = 0;
  for (int i = 1, j = 0; i < length - 1; i += 2, j++)
  { // For all odd levels (length/2)
    index = length - i;
    // Set ~(x_{length-i} ^ y_{length-i}) next to the last bit
    local[i] |= (get_bit(not_xor1, index) << 1);
    // Set ~(x_{length-i} ^ y_{length-i}) next to the last remote bit
    remote[i] |= (get_bit(not_xor2, index) << 1);
    // Compute next to last logical AND for level i
    local[i] = (and_b(get_bit(local[i], 0), get_bit(remote[i], 0),
                      get_bit(local[i], 1), get_bit(remote[i], 1),
                      get_next_rb()) &
                mask);
    // Pack result bit in local_bits to send all together
    local_bits |= (local[i] << j);
  }
  // Compute first round of pairwise logical ANDs at the last level
  local[length - 1] = eq_b_level(length, unset_lsbs(not_xor1, 1) | local[length - 1],
                                 unset_lsbs(not_xor2, 1) | remote[length - 1]);
  rindex += length / 2; // eq_b_level performed length/2 logical ANDs
  // Pack the length/2 result bits in the vacant MSBs of local_bits
  local_bits |= (((unsigned long long)local[length - 1]) << (length / 2));
  // Get the second share of each bit as computed by the other party
  unsigned long long remote_bits = exchange_shares_u(local_bits);
  // Unpack the length/2 MSBs and store them at the last level
  remote[length - 1] = (remote_bits >> (length / 2));
  // Unpack the rest and update odd levels
  for (int i = 1; i < length - 1; i += 2)
  {
    remote[i] = get_bit_u(remote_bits, i / 2);
  }

  // C. Continue in a loop. Each round breaks into the the following steps:
  //    1. Project every other bit of the last level to 2^r levels,
  //       starting at level 2^r * (bits_left - p), where p is the bit's index
  //       (p=0 for the LSB). The bit is copied next to the LSB of the
  //       corresponding level.
  //    2. Evaluate a logical AND between the projected bit and the LSB at
  //       the corresponding level.
  //    3. Evaluate the next round of pairwise ANDs at the last level.
  unsigned long long to_send, received;
  int rounds = (int)log2(length / 2);
  int bits_left = length, current_level, num_levels, bit_count;
  int levels[length]; // Caches levels with projected bits at each round
  for (int i = 0; i < length; i++)
  { // Initialize level cache
    levels[i] = -1;
  }
  for (int i = 1; i <= rounds; i++)
  {
    bits_left /= 2;
    to_send = 0;
    bit_count = 0;
    num_levels = (1 << i);
    // Project common bits to avoid redundant computation (and communication)
    for (int p = bits_left - 1; p > 0; p -= 2)
    {
      current_level = num_levels * (bits_left - p);
      for (int j = 0; j < num_levels; j++)
      {
        // Project bits from last level
        local[current_level] |= (get_bit(local[length - 1], p) << 1);
        remote[current_level] |= (get_bit(remote[length - 1], p) << 1);
        // Do the logical AND
        local[current_level] = and_b(get_bit(local[current_level], 0),
                                     get_bit(remote[current_level], 0),
                                     get_bit(local[current_level], 1),
                                     get_bit(remote[current_level], 1),
                                     get_next_rb()) &
                               mask;
        // Pack the result
        to_send |= (local[current_level] << bit_count);
        // Cache level to unpack remote bit later
        levels[bit_count] = current_level;
        bit_count++;
        current_level++;
        if (current_level == (length - 1))
          break;
      }
    }
    // Process last level
    local[length - 1] = eq_b_level(bits_left, local[length - 1],
                                   remote[length - 1]);
    rindex += bits_left / 2; // eq_b_level performed bits_left/2 logical ANDs
    // Pack bits of the last level
    to_send |= (local[length - 1] << bit_count);
    // Exchange all bits of the current round and unpack accordingly
    received = exchange_shares_u(to_send);
    // Unpack bits of last level
    remote[length - 1] = (received >> bit_count);
    // Unpack the rest and reset level cache
    int l = 0;
    while ((levels[l] >= 0) & (l < length))
    {
      remote[levels[l]] = get_bit_u(received, l);
      levels[l++] = -1; // Reset for next round
    }
  }
  // Do a final XOR of all levels
  BShare res = 0;
  for (int i = 0; i < length; i++)
  {
    res ^= local[i];
  }
  // Return local share
  return (BitShare)(res & mask);
}

// Compares and swaps two numbers x, y using their boolean shares
// Relies on greater() for number comparison, hence, it only works
// for numbers of the same sign
void cmp_swap(BShare *x1, BShare *x2, BShare *y1, BShare *y2,
              const BShare *rnums)
{
  // Compute x > y
  BitShare b = greater(*x1, *x2, *y1, *y2);
  BShare bs1 = to_bshare(b);
  BShare bs2 = exchange_shares(bs1);
  BShare b1 = -bs1; // Set all bits equal to LSB of bs1
  BShare b2 = -bs2; // Set all bits equal to LSB of bs2
  BShare local[2];
  // Compute min = b * y + (1-b) * x
  local[0] = and_b(b1, b2, *y1, *y2, rnums[0]);
  local[0] ^= and_b(~b1, ~b2, *x1, *x2, rnums[1]);
  // Compute max = b * x + (1-b) * y
  local[1] = and_b(b1, b2, *x1, *x2, rnums[2]);
  local[1] ^= and_b(~b1, ~b2, *y1, *y2, rnums[3]);
  // Get remote shares from the other party
  BShare remote[2];
  exchange_shares_array(local, remote, 2);
  // Swap
  *x1 = local[0];
  *x2 = remote[0];
  *y1 = local[1];
  *y2 = remote[1];
}

// Generalized compare and swap
void cmp_swap_g(BShare *r1, BShare *r2, unsigned att_index1,
                unsigned att_index2, int num_elements, int asc)
{
  // Compute x > y
  BitShare b = greater(r1[att_index1], r1[att_index1 + 1],
                       r2[att_index2], r2[att_index2 + 1]);
  BShare bs1 = to_bshare(b);
  BShare bs2 = exchange_shares(bs1);
  BShare b1 = -bs1; // Set all bits equal to LSB of bs1
  BShare b2 = -bs2; // Set all bits equal to LSB of bs2
  // Compute min, max for each pair of elements in the given arrays
  BShare local[num_elements];
  BShare r[2 * num_elements];
  get_next_rb_array(r, 2 * num_elements);
  for (int i = 0, j = 0; i < num_elements - 1; i += 2, j += 4)
  {
    // Compute min = b * y + (1-b) * x
    local[i] = and_b(b1, b2, r2[i], r2[i + 1], r[j]);
    local[i] ^= and_b(~b1, ~b2, r1[i], r1[i + 1], r[j + 1]);
    // Compute max = b * x + (1-b) * y
    local[i + 1] = and_b(b1, b2, r1[i], r1[i + 1], r[j + 2]);
    local[i + 1] ^= and_b(~b1, ~b2, r2[i], r2[i + 1], r[j + 3]);
  }
  // Get remote shares from the other party
  BShare remote[num_elements];
  exchange_shares_array(local, remote, num_elements);
  // Swap arrays
  int desc = !asc;
  for (int i = 0; i < num_elements - 1; i += 2)
  {
    r1[i] = local[i + desc];
    r1[i + 1] = remote[i + desc];
    r2[i] = local[i + 1 - desc];
    r2[i + 1] = remote[i + 1 - desc];
  }
}

// Adds to numbers using their boolean shares
// This function represents a single bit using
// a boolean share. As a result, it requires exchanging two BShares for each
// bitwise logical AND, whereas we should only exchange two bits
void boolean_addition(BShare x1, BShare x2, BShare y1, BShare y2,
                      BShare *z1, BShare *z2,
                      BShare *rnums)
{

  BShare c1[3], c2[3], xor = 0, carry1 = 0, carry2 = 0, mask = 1;
  // Add bits ingoring carries
  *z1 = x1 ^ y1;
  *z2 = x2 ^ y2;
  // Compute carry at position 0 (x_0 AND y_0)
  carry1 = and_b(get_bit(x1, 0), get_bit(x2, 0),
                 get_bit(y1, 0), get_bit(y2, 0), rnums[0]) &
           mask;
  carry2 = exchange_shares(carry1) & mask;
  // Compute carry at position i, ignoring the possible overflow carry
  for (int i = 1; i < (sizeof(BShare) * 8) - 1; i++)
  {
    // x_i AND y_i
    c1[0] = and_b(get_bit(x1, i), get_bit(x2, i),
                  get_bit(y1, i), get_bit(y2, i),
                  rnums[i]);
    // x_i AND c_{i-1}
    c1[1] = and_b(get_bit(x1, i), get_bit(x2, i),
                  get_bit(carry1, i - 1),
                  get_bit(carry2, i - 1),
                  rnums[i + 1]);
    // y_i AND c_{i-1}
    c1[2] = and_b(get_bit(y1, i), get_bit(y2, i),
                  get_bit(carry1, i - 1),
                  get_bit(carry2, i - 1),
                  rnums[i + 2]);
    // Get carry shares from the other party
    exchange_shares_array(c1, c2, 3);
    // Store locally computed bit in carry1
    xor = (c1[0] ^ c1[1] ^ c1[2]);
    carry1 |= ((xor&mask) << i);
    // Store other party's bit share in carry2
    xor = (c2[0] ^ c2[1] ^ c2[2]);
    carry2 |= ((xor&mask) << i);
  }
  // Add carries
  *z1 ^= (carry1 << 1);
  *z2 ^= (carry2 << 1);
}

void boolean_addition_batch(BShare *x1, BShare *x2, BShare *y1, BShare *y2,
                            BShare *res, int numElements)
{

  BShare xor = 0;
  BShare **c1 = allocate_2D_table(numElements, 3);
  BShare **c2 = allocate_2D_table(numElements, 3);

  BShare *carry1 = malloc(numElements * sizeof(BShare));
  BShare *carry2 = malloc(numElements * sizeof(BShare));
  BShare mask = 1;

  // Add bits ingoring carries
  for (int i = 0; i < numElements; i++)
  {
    res[i] = x1[i] ^ y1[i];
    // Compute carry at position 0 (x_0 AND y_0)
    carry1[i] = and_b(get_bit(x1[i], 0), get_bit(x2[i], 0),
                      get_bit(y1[i], 0), get_bit(y2[i], 0),
                      get_next_rb()) &
                mask;
  }

  exchange_shares_array(carry1, carry2, numElements);
  for (int i = 0; i < numElements; i++)
  {
    carry2[i] &= mask;
  }

  int bsize = sizeof(BShare) * 8;

  // Compute carry at position j, ignoring the possible overflow carry
  for (int j = 1; j < bsize - 1; j++)
  {
    for (int i = 0; i < numElements; i++)
    {
      // x_j AND y_j
      c1[i][0] = and_b(get_bit(x1[i], j), get_bit(x2[i], j),
                       get_bit(y1[i], j), get_bit(y2[i], j),
                       get_next_rb());
      // x_j AND c_{j-1}
      c1[i][1] = and_b(get_bit(x1[i], j), get_bit(x2[i], j),
                       get_bit(carry1[i], j - 1),
                       get_bit(carry2[i], j - 1),
                       get_next_rb());
      // y_i AND c_{i-1}
      c1[i][2] = and_b(get_bit(y1[i], j), get_bit(y2[i], j),
                       get_bit(carry1[i], j - 1),
                       get_bit(carry2[i], j - 1),
                       get_next_rb());
    }
    // Get carry shares from the other party
    // except for the last round
    if (j < bsize - 2)
    {
      exchange_shares_array(&c1[0][0], &c2[0][0], numElements * 3);
    }
    for (int i = 0; i < numElements; i++)
    {
      // Store locally computed bit in carry1
      xor = (c1[i][0] ^ c1[i][1] ^ c1[i][2]);
      carry1[i] |= ((xor&mask) << j);
      // Store other party's bit share in carry2
      xor = (c2[i][0] ^ c2[i][1] ^ c2[i][2]);
      carry2[i] |= ((xor&mask) << j);
    }
  }

  // Add carries
  for (int i = 0; i < numElements; i++)
  {
    res[i] ^= (carry1[i] << 1);
  }
  free(c1);
  free(c2);
  free(carry1);
  free(carry2);
}

// Less than 0
inline BShare ltz_b(BShare x)
{
  return (unsigned long long)x >> (sizeof(x) * 8 - 1);
}

// Less than 0 for an array of elements
void ltz_b_array(const BShare *x, int len, BShare *res)
{
  for (int i = 0; i < len; i++)
  {
    res[i] = ltz_b(x[i]);
  }
}

// Conversion of a single-bit BShare to an arithmetic share
AShare convert_single_bit(BShare bit, AShare ra, BShare rb)
{
  // reveal bit^rb
  Data z = reveal_b((bit & 1) ^ (rb & 1));
  // return [ra] * (1-2*z) + z
  // Only rank 2 needs to subtract 1 if z=1, so we multiply by rank mod 2
  ra = ra * (1 - 2 * z) + z * (get_rank() % 2);
  return ra;
}

// Conversion of a single-bit BShare array to an array of arithmetic shares
void convert_single_bit_array(BShare *bit, AShare *ra, BShare *rb, int len,
                              AShare *res)
{
  int i;
  BShare *z = malloc(len * sizeof(BShare));
  assert(z != NULL);
  for (i = 0; i < len; i++)
  {
    z[i] = (bit[i] & 1) ^ (rb[i] & 1);
  }
  reveal_b_array_async(z, len);
  // return [ra] * (1-2*z) + z
  // Only rank 2 needs to subtract 1 if z=1, so we multiply by rank mod 2
  for (i = 0; i < len; i++)
  {
    res[i] = ra[i] * (1 - 2 * z[i]) + z[i] * (get_rank() % 2);
  }
  free(z);
}

// Used by cmp_swap_batch() to get the next valid index
inline static int get_next_index(int pos, int area, int comp_per_box)
{
  int box_start = (pos / area) * area;
  if ((pos + 1) >= (box_start + comp_per_box))
  {
    return box_start + area;
  }
  return pos + 1;
}

// converts an arithmetic share to binary
void convert_a_to_b_array(AShare *xa1, AShare *xa2, BShare *xb1, BShare *xb2, int len)
{

  BShare *w1 = malloc(len * sizeof(BShare)); // local share of x3
  assert(w1 != NULL);
  BShare *w2 = malloc(len * sizeof(BShare)); // remote share of x3
  assert(w2 != NULL);
  BShare *r_temp = malloc(len * sizeof(BShare)); // random share
  assert(r_temp != NULL);
  BShare *r_temp2 = malloc(len * sizeof(BShare)); // random share
  assert(r_temp2 != NULL);

  if (get_rank() == 0)
  {
    // generate bool shares of x1+x2 (xa1 + xa2)
    BShare *z13 = malloc(len * sizeof(BShare));
    assert(z13 != NULL);
    BShare *z12 = malloc(len * sizeof(BShare));
    assert(z12 != NULL);
    for (int i = 0; i < len; i++)
    {
      xa2[i] += xa1[i]; // xa1 + xa2
    }
    for (int i = 0; i < len; i++)
    {
      generate_bool_share(xa2[i], &xa1[i], &z12[i], &z13[i]);
      // local share of xa1+xa2 now in xa1
    }
    // distribute shares to P2, P3

    TCP_Send(z12, len, 1, sizeof(BShare));
    TCP_Send(z13, len, 2, sizeof(BShare));

    free(z12);
    free(z13);

    //   // receive share of x3 from P3
    TCP_Recv(w1, len, 2, sizeof(BShare));
    //   // generate pairs of random binary shares (R1)
    get_next_rb_pair_array(xb2, xb1, len);
    //   // receive share from P2 and compute xb1
    TCP_Recv(r_temp, len, 1, sizeof(BShare));
    //   // compute xb1
    for (int i = 0; i < len; i++)
    {
      xb1[i] ^= r_temp[i];
      xb1[i] ^= xb2[i]; // R1
    }
    //   // xb2 contains the local share of R1
    //   // generate pairs of random binary shares (R2)
    get_next_rb_pair_array(r_temp, r_temp2, len);
    //   // r_temp contains the local share of R2
  }
  else if (get_rank() == 1)
  { // P2
    //   // receive share of x1+x2 frm P1
    TCP_Recv(xa1, len, 0, sizeof(AShare));
    //   // receive share of x3
    TCP_Recv(w1, len, 2, sizeof(BShare));

    //   // generate pairs of random binary shares (R1)
    get_next_rb_pair_array(xb2, xb1, len);
    //   // send local to P1
    TCP_Send(xb2, len, 0, sizeof(BShare));
    //   // xb2 contains the local share of R1
    //   // generate pairs of random binary shares (R2)
    get_next_rb_pair_array(r_temp, xb1, len);
    //   // receive share from P3 and compute xb1
    TCP_Recv(r_temp2, len, 2, sizeof(BShare));
    //   // compute xb1
    for (int i = 0; i < len; i++)
    {
      xb1[i] ^= r_temp2[i];
      xb1[i] ^= r_temp[i]; // R2
    }
    //   // r_temp contains the local share of R2
  }
  else
  { // P3
    //   // generate bool shares of x3 (xa1)
    BShare *w13 = malloc(len * sizeof(BShare));
    assert(w13 != NULL);

    for (int i = 0; i < len; i++)
    {
      generate_bool_share(xa1[i], &w13[i], &w2[i], &w1[i]);
    }

    //   // receive share of x1+x2 frm P1
    TCP_Recv(xa1, len, 0, sizeof(AShare));
    //   // distribute shares to P1, P2
    TCP_Send(w13, len, 0, sizeof(BShare));
    TCP_Send(w2, len, 1, sizeof(BShare));

    free(w13);

    //   // generate pairs of random binary shares (R1)
    get_next_rb_pair_array(xb2, xb1, len);
    //   // xb2 contains the local share of R1
    //   // generate pairs of random binary shares (R2)
    get_next_rb_pair_array(r_temp, xb1, len);
    //   // send local to P2
    TCP_Send(r_temp, len, 1, sizeof(BShare));
    //   // r_temp contains the local share of R2
  }

  /***** all parties *****/
  // get remote shares z2, w2
  exchange_shares_array(xa1, xa2, len);
  exchange_shares_array(w1, w2, len);
  // ripple-carry-adder
  boolean_addition_batch(xa1, xa2, w1, w2, r_temp2, len); // r_temp2: result of RCA
  free(w2);

  // share of third share
  for (int i = 0; i < len; i++)
  {
    w1[i] = r_temp2[i] ^ xb2[i] ^ r_temp[i];
  }
  free(r_temp);
  free(r_temp2);

  // reveal y to P3
  if (get_rank() == 0)
  {
    TCP_Send(w1, len, 2, sizeof(BShare));
  }
  else if (get_rank() == 1)
  { // P2
    TCP_Send(w1, len, 2, sizeof(BShare));
  }
  else
  { // P3
    TCP_Recv(xb1, len, 0, sizeof(BShare));
    TCP_Recv(xa1, len, 1, sizeof(AShare));

        for (int i=0; i<len; i++) {
          xb1[i] ^= xa1[i];
          xb1[i] ^= w1[i];
        }
      }
    /***** all parties *****/
    // exchange xb1, xb2
    exchange_shares_array(xb1, xb2, len);
    free(w1);
  }

  // Used by bitonic_sort_batch()
  // Applies each round of cmp_swap_g() for all 'num_comparisons'
  int cmp_swap_batch(int first_index, BShare **rows, int length, int row_length,
                     unsigned *att_indices, int num_attributes, int phase,
                     int column, bool *asc, int num_comparisons)
  {

    // Number of 'boxes' per column of bitonic sort
    long long boxes = length / (((long long)1) << (phase + 1 - column));
    // Number of comparisons per 'box'
    int comp_per_box = length / (2 * boxes);
    // The 'area' of the 'box'
    int area = 2 * comp_per_box;

    int numbits = sizeof(BShare) * 8;
    int numlevels = log2(numbits);

    // We keep 'num_attributes' bits per comparison
    BitShare **bs1 = allocate_2D_bit_table(num_comparisons, num_attributes);
    BitShare **bs2 = allocate_2D_bit_table(num_comparisons, num_attributes);

    // We keep 'num_attributes' bits per comparison
    BitShare **eq_bs1 = allocate_2D_bit_table(num_comparisons, num_attributes);
    BitShare **eq_bs2 = allocate_2D_bit_table(num_comparisons, num_attributes);

    int done, pos;

    // Apply greater_batch() and eq_b_array() for each sorting attribute
    for (int att = 0; att < num_attributes; att++)
    {
      // The sorting attribute
      int att_index = att_indices[att];
      // We perform 'num_comparisons' comparisons per 'column'
      unsigned long long *local = malloc(num_comparisons * sizeof(unsigned long long));
      assert(local != NULL);
      unsigned long long *remote = malloc(num_comparisons * sizeof(unsigned long long));
      assert(remote != NULL);
      // Level buffers for local and remote bits per comparison
      int num_bits = sizeof(BShare) * 8; // Each pair has num_bits (=64) levels
      int len = (num_bits - 1) * sizeof(char) + sizeof(BShare);
      char **local_bits = allocate_2D_byte_array(num_comparisons, len);
      char **remote_bits = allocate_2D_byte_array(num_comparisons, len);

      // Distance between two compared elements
      int d = 1 << (phase - column);

      // 1. Do 1st round of greater() for the whole batch
      done = 0;          // Number of comparisons done
      pos = first_index; // Index of the first element in comparison
      while ((pos < length) & (done != num_comparisons))
      {
        local[done] = gr_round_a(rows[pos][att_index], rows[pos][att_index + 1],
                                 rows[pos | d][att_index],
                                 rows[pos | d][att_index + 1],
                                 num_bits);
        pos = get_next_index(pos, area, comp_per_box);
        done++;
      }

      // Get remote bits and update levels
      exchange_shares_array_u(local, remote, done); // 1 round
      // Unpack bits and update levels
      for (int i = 0; i < done; i++)
      {
        for (int j = 0; j < num_bits - 1; j++)
        {
          local_bits[i][j] = get_bit_u(local[i], num_bits - j - 1);
          remote_bits[i][j] = get_bit_u(remote[i], num_bits - j - 1);
        }
        // Update last level bits
        unsigned long long l_tmp = get_bit_u(local[i], 0);
        unsigned long long r_tmp = get_bit_u(remote[i], 0);
        memcpy(&local_bits[i][num_bits - 1], &l_tmp, sizeof(unsigned long long));
        memcpy(&remote_bits[i][num_bits - 1], &r_tmp, sizeof(unsigned long long));
      }

      // 2. Do 2nd round of greater() for the whole batch
      done = 0;
      pos = first_index;
      while ((pos < length) & (done != num_comparisons))
      {
        local[done] = gr_round_b(rows[pos][att_index], rows[pos][att_index + 1],
                                 rows[pos | d][att_index],
                                 rows[pos | d][att_index + 1],
                                 num_bits, &local_bits[done][0],
                                 &remote_bits[done][0]);
        pos = get_next_index(pos, area, comp_per_box);
        done++;
      }
      // Get remote bits and update levels
      exchange_shares_array_u(local, remote, done); // 1 round
      for (int i = 0; i < done; i++)
      {
        // Unpack the num_bits/2 MSBs and store them at the last level
        unsigned long long tmp = (remote[i] >> (num_bits / 2));
        memcpy(&remote_bits[i][num_bits - 1], &tmp, sizeof(unsigned long long));
        // Unpack the rest and update odd levels
        for (int j = 1; j < num_bits - 1; j += 2)
        {
          remote_bits[i][j] = get_bit_u(remote[i], j / 2);
        }
      }

      // 3. Do remaining 5 rounds of greater()
      int rounds = (int)log2(num_bits / 2);
      int bits_left = num_bits, bit_count;
      // Caches levels with projected bits at each round
      char **levels = allocate_2D_byte_array(num_comparisons, num_bits / 2); // max 'num_bits' levels per pair

      // Initialize level cache
      for (int i = 0; i < num_comparisons; i++)
      {
        for (int j = 0; j < num_bits / 2; j++)
        {
          levels[i][j] = -1;
        }
      }
      // For each round of greater()
      for (int r = 1; r <= rounds; r++)
      {
        done = 0;
        bits_left /= 2;
        pos = first_index;
        while ((pos < length) & (done != num_comparisons))
        {
          bit_count = 0;
          local[done] = gr_round_c_char(r, bits_left, num_bits,
                                        &local_bits[done][0], &remote_bits[done][0],
                                        &levels[done][0], &bit_count);
          pos = get_next_index(pos, area, comp_per_box);
          done++;
        }
        // Exchange all bits of the current round and unpack accordingly
        exchange_shares_array_u(local, remote, done); // 1 round
        // Unpack bits of last level
        for (int i = 0; i < done; i++)
        {
          unsigned long long tmp = (remote[i] >> bit_count);
          memcpy(&remote_bits[i][num_bits - 1], &tmp, sizeof(unsigned long long));
          // Unpack the rest and reset level cache for next round
          int l = 0;
          while ((levels[i][l] >= 0) & (l < num_bits / 2))
          {
            remote_bits[i][(int)levels[i][l]] = get_bit_u(remote[i], l);
            levels[i][l++] = -1; // Reset for next round
          }
        }
      }
      // One bitshare for each greater() comparison
      BitShare mask = 1;
      for (int i = 0; i < done; i++)
      {
        bs1[i][att] = 0;
        // Do a final XOR of all levels
        for (int j = 0; j < num_bits - 1; j++)
        {
          bs1[i][att] ^= local_bits[i][j];
        }
        bs1[i][att] ^= *((unsigned long long *)&local_bits[i][num_bits - 1]);
        bs1[i][att] &= mask;
      }

      // Free memory
      free(local_bits);
      free(remote_bits);
      free(levels);

      // Now compute equality bits (another logL rounds)
      if ((num_attributes > 1) && (att < num_attributes - 1))
      {
        // Only if there are more than one sort attributes
        BShare *res = malloc(num_comparisons * sizeof(BShare));
        assert(res != NULL);
        BShare *res2 = malloc(num_comparisons * sizeof(BShare));
        assert(res2 != NULL);
        done = 0;          // Number of equality comparisons done
        pos = first_index; // Index of the first element in comparison
        while ((pos < length) & (done != num_comparisons))
        {
          BShare x1 = rows[pos][att_index];
          BShare x2 = rows[pos][att_index + 1];
          BShare y1 = rows[pos | d][att_index];
          BShare y2 = rows[pos | d][att_index + 1];
          res[done] = x1 ^ y1 ^ (~(BShare)0);  // local share;
          res2[done] = x2 ^ y2 ^ (~(BShare)0); // remote share

          pos = get_next_index(pos, area, comp_per_box);
          done++;
        }
        // The result is stored in the (numbits/2) rightmost bits of res, res2 elements
        for (int l = 0; l < numlevels; l++)
        {
          done = 0;          // Number of equality comparisons done
          pos = first_index; // Index of the first element in comparison
          while ((pos < length) & (done != num_comparisons))
          {
            res[done] = eq_b_level2(numbits >> l, res[done], res2[done]);

            pos = get_next_index(pos, area, comp_per_box);
            done++;
          }
          // Exchange results of logical and, except for the final round
          if (l != numlevels - 1)
          {
            exchange_shares_array(res, res2, done);
          }
        }
        // Copy equality bits for current attribute
        for (int c = 0; c < done; c++)
        {
          eq_bs1[c][att] = (res[c] & mask);
        }
        free(res);
        free(res2);
      }
    }
    // Get remote bit shares
    exchange_bit_shares_array(&bs1[0][0], &bs2[0][0],
                              num_comparisons * num_attributes); // 1 round
    exchange_bit_shares_array(&eq_bs1[0][0], &eq_bs2[0][0],
                              num_comparisons * num_attributes); // 1 round

    // Update greater() bits based on ASC/DESC direction
    update_b(bs1, bs2, asc, num_comparisons, num_attributes);

    // Compute composite bit for each comparison
    if (num_attributes == 2)
    {
      compute_composite_2(bs1, bs2, eq_bs1, eq_bs2, num_comparisons,
                          num_attributes);
    }
    else if (num_attributes == 3)
    { // num_attributes==3
      compute_composite_3(bs1, bs2, eq_bs1, eq_bs2, num_comparisons,
                          num_attributes);
    }

    // 4. Multiplexing
    BShare **local_rows = allocate_2D_table(num_comparisons, row_length);
    BShare **remote_rows = allocate_2D_table(num_comparisons, row_length);
    // Distance between two compared elements
    int d = 1 << (phase - column);
    done = 0;
    pos = first_index;
    while ((pos < length) & (done != num_comparisons))
    {
      BShare b1 = -(BShare)bs1[done][0]; // Set all bits equal to LSB
      BShare b2 = -(BShare)bs2[done][0]; // Set all bits equal to LSB
      // Compute min, max for each pair of elements in the given arrays
      BShare *r1 = rows[pos];
      BShare *r2 = rows[pos | d];
      BShare r[2 * row_length];
      // We need 4*row_length/2 = 2*row_length random numbers
      get_next_rb_array(r, 2 * row_length);
      // For each row element
      for (int j = 0, k = 0; j < row_length - 1; j += 2, k += 4)
      {
        // Compute min = b * y + (1-b) * x
        local_rows[done][j] = and_b(b1, b2, r2[j], r2[j + 1], r[k]);
        local_rows[done][j] ^= and_b(~b1, ~b2, r1[j], r1[j + 1], r[k + 1]);
        // Compute max = b * x + (1-b) * y
        local_rows[done][j + 1] = and_b(b1, b2, r1[j], r1[j + 1], r[k + 2]);
        local_rows[done][j + 1] ^= and_b(~b1, ~b2, r2[j], r2[j + 1], r[k + 3]);
      }
      pos = get_next_index(pos, area, comp_per_box);
      done++;
    }
    // Get remote shares from the other party  -- 1 round
    exchange_shares_array(&local_rows[0][0], &remote_rows[0][0],
                          num_comparisons * row_length);

    // 5. Update table rows
    done = 0;
    pos = first_index;
    while ((pos < length) & (done != num_comparisons))
    {
      // up=true means that max should be placed at the second slot,
      // otherwise at the first one
      int up = ((pos >> phase) & 2) == 0;
      int i = up ^ 1;
      // For each element in the row
      for (int j = 0; j < row_length - 1; j += 2)
      {
        rows[pos][j] = local_rows[done][j + i];
        rows[pos][j + 1] = remote_rows[done][j + i];
        rows[pos | d][j] = local_rows[done][j + 1 - i];
        rows[pos | d][j + 1] = remote_rows[done][j + 1 - i];
      }
      pos = get_next_index(pos, area, comp_per_box);
      done++;
    }

    // Free memory
    free(local_rows);
    free(remote_rows);
    free(bs1);
    free(bs2);
    free(eq_bs1);
    free(eq_bs2);

    // Return the last index checked for next call
    return pos;
  }

  // Updates computed bit shares according to ASC/DESC direction
  static void update_b(BitShare * *bg1, BitShare * *bg2, bool *asc,
                        int num_rows, int num_cols)
  {
    for (int i = 0; i < num_rows; i++)
    {
      for (int j = 0; j < num_cols; j++)
      {
        bg1[i][j] ^= !asc[j];
        bg2[i][j] ^= !asc[j];
      }
    }
  }

  // Compute composite b = b^1_g OR (b^1_e AND b^2_g) OR
  //                       (b^1_e AND b^2_e AND b^3_g) OR
  //                       ...
  //                       (b^1_e AND b^2_e AND ... AND b^(n-1)_e AND b^n_g)
  // This can be done in O(logn) rounds, where n is the number of sort attributes

  // Computes composite b = b^1_g OR (b^1_e AND b^2_g)
  // Composite bit shares are stored in bg1[0], bg2[0]
  // Requires 2 communication rounds in total (independent from 'num_rows')
  static void compute_composite_2(BitShare * *bg1, BitShare * *bg2,
                                   BitShare * *be1, BitShare * *be2,
                                   int num_rows, int num_cols)
  {
    assert(num_cols == 2);
    BShare mask = 1;
    // 1st round: for each row
    for (int i = 0; i < num_rows; i++)
    {
      // Compute b^1_e AND b^2_g and store result in be
      be1[i][0] = and_b(be1[i][0], be2[i][0],
                        bg1[i][1], bg2[i][1], get_next_rb()) &
                  mask;
    }
    // Get remote shares for all bits -- 1 round
    exchange_bit_shares_array(&be1[0][0], &be2[0][0], num_rows * num_cols);

    // 2nd round: for each row
    for (int i = 0; i < num_rows; i++)
    {
      // Compute NOT ( NOT(b^1_g) AND NOT(b^1_e AND b^2_g) )
      bg1[i][0] = and_b(bg1[i][0] ^ mask, bg2[i][0] ^ mask,
                        be1[i][0] ^ mask, be2[i][0] ^ mask, get_next_rb()) &
                  mask;
      bg1[i][0] ^= mask;
    }
    // Fetch remote shares -- 1 round
    exchange_bit_shares_array(&bg1[0][0], &bg2[0][0], num_rows * num_cols);
  }

  // Computes composite b = b^1_g OR (b^1_e AND b^2_g AND NOT(b^2_e)) OR
  //                        (b^1_e AND b^2_e AND b^3_g)
  // Composite bit shares are stored in bg1[0], bg2[0]
  // Requires 4 communication rounds in total (independent from 'num_rows')
  static void compute_composite_3(BitShare * *bg1, BitShare * *bg2,
                                   BitShare * *be1, BitShare * *be2,
                                   int num_rows, int num_cols)
  {
    assert(num_cols == 3);
    BShare mask = 1;
    BitShare **b1 = allocate_2D_bit_table(num_rows, 2);
    BitShare **b2 = allocate_2D_bit_table(num_rows, 2);

    // 1st round: for each row
    for (int i = 0; i < num_rows; i++)
    {
      // Compute b^1_e AND b^2_g
      b1[i][0] = and_b(be1[i][0], be2[i][0], bg1[i][1], bg2[i][1], get_next_rb()) & mask;
      // Compute b^1_e AND b^2_e
      b1[i][1] = and_b(be1[i][0], be2[i][0], be1[i][1], be2[i][1], get_next_rb()) & mask;
    }
    // Fetch remote shares for all rows
    exchange_bit_shares_array(&b1[0][0], &b2[0][0], num_rows * 2); // 1 round

    // 2nd round: for each row
    for (int i = 0; i < num_rows; i++)
    {
      // Compute (b^1_e AND b^2_g) AND NOT(b^2_e)
      b1[i][0] = and_b(b1[i][0], b2[i][0],
                       be1[i][1] ^ mask, be2[i][1] ^ mask, get_next_rb()) &
                 mask;
      // Compute (b^1_e AND b^2_e) AND b^3_g
      b1[i][1] = and_b(b1[i][1], b2[i][1], bg1[i][2], bg2[i][2], get_next_rb()) & mask;
    }
    // Fetch remote shares for all rows
    exchange_bit_shares_array(&b1[0][0], &b2[0][0], num_rows * 2); // 1 round

    // 3rd round: for each row
    for (int i = 0; i < num_rows; i++)
    {
      // Compute ( NOT(b^1_g) AND NOT(b^1_e AND NOT(b^2_e) AND b^2_g) )
      b1[i][0] = and_b(bg1[i][0] ^ mask, bg2[i][0] ^ mask,
                       b1[i][0] ^ mask, b2[i][0] ^ mask, get_next_rb()) &
                 mask;
    }
    // Fetch remote shares for all rows
    exchange_bit_shares_array(&b1[0][0], &b2[0][0], num_rows * 2); // 1 round

    // 4th round: for each row
    for (int i = 0; i < num_rows; i++)
    {
      // Compute ( NOT(b^1_g) AND NOT(b^1_e AND NOT(b^2_e) AND b^2_g) )
      //           AND NOT(b^1_e AND b^2_e AND b^3_g)
      b1[i][0] = and_b(b1[i][0], b2[i][0],
                       b1[i][1] ^ mask, b2[i][1] ^ mask, get_next_rb()) &
                 mask;
    }
    // Fetch remote shares for all rows
    exchange_bit_shares_array(&b1[0][0], &b2[0][0], num_rows * 2); // 1 round
    // Store back composite bit shares
    for (int i = 0; i < num_rows; i++)
    {
      bg1[i][0] = b1[i][0] ^ mask;
      bg2[i][0] = b2[i][0] ^ mask;
    }
    free(b1);
    free(b2);
  }
