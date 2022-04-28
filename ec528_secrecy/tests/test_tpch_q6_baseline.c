#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "test-utils.h"

#define DEBUG 0
#define COLS_L 5
#define D1 1994
#define D2 1996
#define DISC_1 1
#define DISC_2 6
#define QUANT 24

/**
 * Tests the correctness of TPC-H Q6 (baseline).
 *
 * SELECT SUM(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE
 * FROM LINEITEM
 * WHERE L_SHIPDATE >= '1994-01-01'
 * AND L_SHIPDATE < dateadd(yy, 1, cast('1994-01-01' as date))
 * AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
 * AND L_QUANTITY < 24
 **/

int main(int argc, char** argv) {

  const long ROWS_L = 16; // LINEITEM input size

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // 0: L_EXTENDEDPRICE, 2: L_DISCOUNT, 4: L_SHIPDATE,
  // 6: L_QUANTITY
  // Cols 8 and 9 store the result of the selection
  AShareTable t1 = {-1, rank, ROWS_L, 2*COLS_L, 2};
  allocate_a_shares_table(&t1);

  // shares of constants (dates)
  AShare sd1, sd2, disc1, disc2, q;

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // STEP 1: Selection L_SHIPDATE >= D1 (col 4)
  #if DEBUG
    if (rank==0) {
      printf("1st selection on LINEITEM.\n");
    }
  #endif

  BShare *a_ldate = malloc(ROWS_L*sizeof(BShare));
  assert(a_ldate!=NULL);
  BShare *a_rem_ldate = malloc(ROWS_L*sizeof(BShare));
  assert(a_rem_ldate!=NULL);

  BShare *lrec = malloc(ROWS_L*sizeof(BShare));
  assert(lrec!=NULL);
  BShare *rem_lrec = malloc(ROWS_L*sizeof(BShare));
  assert(rem_lrec!=NULL);

  BitShare *sel = malloc(ROWS_L*sizeof(BShare));
  assert(sel!=NULL);
  BitShare *rem_sel = malloc(ROWS_L*sizeof(BShare));
  assert(rem_sel!=NULL);

  // populate vector
  for(int i=0; i<ROWS_L; i++) {
    a_ldate[i] = t1.contents[i][4];
    a_rem_ldate[i] = t1.contents[i][5];
  }

  // convert L_SHIPDATE to boolean shares
  convert_a_to_b_array(a_ldate, a_rem_ldate, lrec, rem_lrec, ROWS_L);

  free(a_ldate); free(a_rem_ldate);

  /** convert constant to boolean **/
  AShare rem_sd1 = exchange_shares(sd1);
  BShare sd1_b, rem_sd1_b;
  convert_a_to_b_array(&sd1, &rem_sd1, &sd1_b, &rem_sd1_b, 1);

  geq_batch_const(lrec, rem_lrec, sd1_b, rem_sd1_b, ROWS_L, sel);
  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // Copy selection bits to columns 8, 9
  for (int i=0; i<ROWS_L; i++) {
    t1.contents[i][8] = (BShare)sel[i];
    t1.contents[i][9] = (BShare)rem_sel[i];
  }

 // STEP 2: Apply selection L_SHIPDATE < D2 (col 4)
 // computed as ~ (L_SHIPDATE >= D2)
  #if DEBUG
    if (rank==0) {
      printf("2nd selection on LINEITEM.\n");
    }
  #endif

  /** convert constant to boolean **/
  AShare rem_sd2 = exchange_shares(sd2);
  BShare sd2_b, rem_sd2_b;
  convert_a_to_b_array(&sd2, &rem_sd2, &sd2_b, &rem_sd2_b, 1);
  geq_batch_const(lrec, rem_lrec, sd2_b, rem_sd2_b, ROWS_L, sel);
  // compute not selected
  for (int i=0; i<ROWS_L; i++) {
      sel[i] ^= 1;
  }
  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // and with previous selection
   for (int i=0; i<ROWS_L; i++) {
     sel[i] = and_b((BShare)sel[i], (BShare)rem_sel[i],
     t1.contents[i][8], t1.contents[i][9], get_next_rb()) & 1;
  }
  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // Copy selection bits to columns 8, 9
  for (int i=0; i<ROWS_L; i++) {
    t1.contents[i][8] = (BShare)sel[i];
    t1.contents[i][9] = (BShare)rem_sel[i];
  }

  // STEP 3: Apply selection L_DISCOUNT > DISC_1 (col 2)
  #if DEBUG
    if (rank==0) {
      printf("3rd selection on LINEITEM.\n");
    }
  #endif

  BShare *a_ldisc = malloc(ROWS_L*sizeof(BShare));
  assert(a_ldisc!=NULL);
  BShare *a_rem_ldisc = malloc(ROWS_L*sizeof(BShare));
  assert(a_rem_ldisc!=NULL);

  // populate vector
  for(int i=0; i<ROWS_L; i++) {
    a_ldisc[i] = t1.contents[i][2];
    a_rem_ldisc[i] = t1.contents[i][3];
  }

  // convert L_DISC to boolean shares
  convert_a_to_b_array(a_ldisc, a_rem_ldisc, lrec, rem_lrec, ROWS_L);

  /** convert constant to boolean **/
  AShare rem_disc1 = exchange_shares(disc1);
  BShare disc1_b, rem_disc1_b;
  convert_a_to_b_array(&disc1, &rem_disc1, &disc1_b, &rem_disc1_b, 1);

  greater_batch_const(lrec, rem_lrec, disc1_b, rem_disc1_b, ROWS_L, sel);
  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // and with previous selection
   for (int i=0; i<ROWS_L; i++) {
     sel[i] = and_b((BShare)sel[i], (BShare)rem_sel[i],
     t1.contents[i][8], t1.contents[i][9], get_next_rb()) & 1;
  }
  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // Copy selection bits to columns 8, 9
  for (int i=0; i<ROWS_L; i++) {
    t1.contents[i][8] = (BShare)sel[i];
    t1.contents[i][9] = (BShare)rem_sel[i];
  }

 // STEP 4: Apply selection L_DISCOUNT < DISC_2 (col 2)
 // computed as ~ (L_DISCOUNT >= D2)
  #if DEBUG
    if (rank==0) {
      printf("4th selection on LINEITEM.\n");
    }
  #endif

  /** convert constant to boolean **/
  AShare rem_disc2 = exchange_shares(disc2);
  BShare disc2_b, rem_disc2_b;
  convert_a_to_b_array(&disc2, &rem_disc2, &disc2_b, &rem_disc2_b, 1);

  geq_batch_const(lrec, rem_lrec, disc2_b, rem_disc2_b, ROWS_L, sel);
  // compute not selected
  for (int i=0; i<ROWS_L; i++) {
      sel[i] ^= 1;
  }
  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // and with previous selection
   for (int i=0; i<ROWS_L; i++) {
     sel[i] = and_b((BShare)sel[i], (BShare)rem_sel[i],
     t1.contents[i][8], t1.contents[i][9], get_next_rb()) & 1;
  }

  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // Copy selection bits to columns 8, 9
  for (int i=0; i<ROWS_L; i++) {
    t1.contents[i][8] = (BShare)sel[i];
    t1.contents[i][9] = (BShare)rem_sel[i];
  }

 // STEP 5: Apply selection L_QUANTITY < Q (col 6)
 // computed as ~ (L_QUANTITY >= Q)
  #if DEBUG
    if (rank==0) {
      printf("5th selection on LINEITEM.\n");
    }
  #endif

  // populate vector
  for(int i=0; i<ROWS_L; i++) {
    a_ldisc[i] = t1.contents[i][6];
    a_rem_ldisc[i] = t1.contents[i][7];
  }

  // convert L_DISC to boolean shares
  convert_a_to_b_array(a_ldisc, a_rem_ldisc, lrec, rem_lrec, ROWS_L);

   /** convert constant to boolean **/
  AShare rem_q = exchange_shares(q);
  BShare q_b, rem_q_b;
  convert_a_to_b_array(&q, &rem_q, &q_b, &rem_q_b, 1);

  geq_batch_const(lrec, rem_lrec, q_b, rem_q_b, ROWS_L, sel);
  // compute not selected
  for (int i=0; i<ROWS_L; i++) {
      sel[i] ^= 1;
  }
  exchange_bit_shares_array(sel, rem_sel, ROWS_L);

  // and with previous selection
  for (int i=0; i<ROWS_L; i++) {
     sel[i] = and_b((BShare)sel[i], (BShare)rem_sel[i],
     t1.contents[i][8], t1.contents[i][9], get_next_rb()) & 1;
  }

  exchange_bit_shares_array(sel, rem_sel, ROWS_L);
  free(a_ldisc); free(a_rem_ldisc);

  // STEP 6: Convert selected from boolean to arithmetic
  #if DEBUG
    if (rank==0) {
      printf("Conversion.\n");
    }
  #endif

  AShare *a_sel = malloc(ROWS_L*sizeof(AShare));
  assert(a_sel!=NULL);
  AShare *rem_a_sel = malloc(ROWS_L*sizeof(AShare));
  assert(rem_a_sel!=NULL);

  AShare *ra = calloc(ROWS_L, sizeof(AShare));
  assert(ra!=NULL);
  BShare *rb = calloc(ROWS_L, sizeof(BShare));
  assert(rb!=NULL);

  BShare *bshare_sel = malloc(ROWS_L*sizeof(BShare));
  assert(bshare_sel!=NULL);

  for (int i=0; i<ROWS_L; i++) {
      bshare_sel[i] = sel[i];
  }

  convert_single_bit_array(bshare_sel, ra, rb, ROWS_L, a_sel);
  exchange_a_shares_array(a_sel, rem_a_sel, ROWS_L);

  free(sel); free(bshare_sel); free(rem_sel);
  free(ra); free(rb);

  // STEP 7: Multiplication L_EXTENDEDPRICE*L_DISCOUNT (0, 2) and sum
  AShare final_sum = 0;
  AShare *sum = calloc(ROWS_L, sizeof(AShare));
  assert(sum!=NULL);
  AShare *rem_sum = calloc(ROWS_L, sizeof(AShare));
  assert(rem_sum!=NULL);

  for (int i=0; i<ROWS_L; i++) {
    sum[i] = mul(t1.contents[i][0], t1.contents[i][1],
                    t1.contents[i][2], t1.contents[i][3], get_next_r());
  }

  exchange_a_shares_array(sum, rem_sum, ROWS_L);

  for (int i=0; i<ROWS_L; i++) {
    final_sum += mul(a_sel[i], rem_a_sel[i],
                    sum[i], rem_sum[i], get_next_r());
  }

  free(a_sel); free(rem_a_sel);

  // Open sum
  Data result = open_a(final_sum);

  if (rank == 0) {
    assert(result==10);
    #if DEBUG
      printf("sum = %lld\n", result);
    #endif
    printf("TEST TPC-H Q6 Baseline: OK.\n");
  }

  free(t1.contents);

  // tear down communication
  TCP_Finalize();
  return 0;
}
