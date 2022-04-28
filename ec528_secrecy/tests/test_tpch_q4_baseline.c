#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "test-utils.h"

#define DEBUG 0
#define COLS_O 4
#define COLS_L 4
#define D1 1993
#define D2 1995

/**
 * Tests the correctness of TPC-H Q4 (baseline).
 *
 * SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT FROM ORDERS
 * WHERE O_ORDERDATE >= '1993-07-01'
 * AND O_ORDERDATE < dateadd(mm,3, cast('1993-07-01' as date))
 * AND EXISTS (
 *    SELECT * FROM LINEITEM WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE)
 * GROUP BY O_ORDERPRIORITY
 * ORDER BY O_ORDERPRIORITY
 **/

int main(int argc, char** argv) {

  const long ROWS_O = 4; // ORDERS input size
  const long ROWS_L = 16; // LINEITEM input size

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // 0: O_ORDERKEY, 2: O_ORDERDATE, 4: O_ORDERPRIORITY
  // Cols 6 and 7 store the result of the selection
  BShareTable t1 = {-1, rank, ROWS_O, 2*COLS_O, 1};
  allocate_bool_shares_table(&t1);
  // 0: L_ORDERKEY, 2: L_COMMITDATE, 4: L_RECEIPTDATE
  // Cols 6 and 7 store the result of the selection
  BShareTable t2 = {-1, rank, ROWS_L, 2*COLS_L, 2};
  allocate_bool_shares_table(&t2);

  // shares of constants (dates)
  BShare sd1, sd2;

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // STEP 1: Selection L_COMMITDATE < L_RECEIPTDATE
  #if DEBUG
    if (rank==0) {
      printf("1st selection on LINEITEM.\n");
    }
  #endif

  BShare *cdate = malloc(ROWS_L*sizeof(BShare));
  assert(cdate!=NULL);
  BShare *rem_cdate = malloc(ROWS_L*sizeof(BShare));
  assert(rem_cdate!=NULL);

  BShare *rdate = malloc(ROWS_L*sizeof(BShare));
  assert(rdate!=NULL);
  BShare *rem_rdate = malloc(ROWS_L*sizeof(BShare));
  assert(rem_rdate!=NULL);

  // populate vectors
  for(int i=0; i<ROWS_L; i++) {
    cdate[i] = t2.contents[i][2];
    rem_cdate[i] = t2.contents[i][3];
    rdate[i] = t2.contents[i][4];
    rem_rdate[i] = t2.contents[i][5];
  }

  BitShare *sel_l = malloc(ROWS_L*sizeof(BitShare));
  assert(sel_l!=NULL);
  BitShare *rem_sel_l = malloc(ROWS_L*sizeof(BitShare));
  assert(rem_sel_l!=NULL);

  greater_batch(rdate, rem_rdate, cdate, rem_cdate, ROWS_L, sel_l);
  exchange_bit_shares_array(sel_l, rem_sel_l, ROWS_L);

  // Copy selection bits to columns 6, 7
  for (int i=0; i<ROWS_L; i++) {
    t2.contents[i][6] = (BShare)sel_l[i];
    t2.contents[i][7] = (BShare)rem_sel_l[i];
  }

  free(cdate); free(rem_cdate); free(rdate); free(rem_rdate);
  free(sel_l); free(rem_sel_l);

 // STEP 2: Apply selection O_ORDERDATE >= D1
  #if DEBUG
    if (rank==0) {
      printf("1st selection on ORDERS.\n");
    }
  #endif

  BShare *odate = malloc(ROWS_O*sizeof(BShare));
  assert(odate!=NULL);
  BShare *rem_odate = malloc(ROWS_O*sizeof(BShare));
  assert(rem_odate!=NULL);

  // populate vector
  for(int i=0; i<ROWS_O; i++) {
    odate[i] = t1.contents[i][2];
    rem_odate[i] = t1.contents[i][3];
  }

  BitShare *sel = malloc(ROWS_O*sizeof(BitShare));
  assert(sel!=NULL);
  BitShare *rem_sel = malloc(ROWS_O*sizeof(BitShare));
  assert(rem_sel!=NULL);

  BShare rem_sd1 = exchange_shares(sd1);
  geq_batch_const(odate, rem_odate, sd1, rem_sd1, ROWS_O, sel);
  exchange_bit_shares_array(sel, rem_sel, ROWS_O);

  // STEP 3: Apply selection O_ORDERDATE < D2
  #if DEBUG
    if (rank==0) {
      printf("2nd selection on ORDERS.\n");
    }
  #endif

  BitShare *sel2 = malloc(ROWS_O*sizeof(BitShare));
  assert(sel2!=NULL);
  BitShare *rem_sel2 = malloc(ROWS_O*sizeof(BitShare));
  assert(rem_sel2!=NULL);

  BShare rem_sd2 = exchange_shares(sd2);
  geq_batch_const(odate, rem_odate, sd2, rem_sd2, ROWS_O, sel2);
  for(int i=0; i<ROWS_O; i++) {
    sel2[i] ^= (BitShare)1;
  }
  exchange_bit_shares_array(sel2, rem_sel2, ROWS_O);
  free(odate); free(rem_odate);

  // STEP 4: Compute AND of selections
  BShare mask = 1;
  #if DEBUG
    if (rank==0) {
      printf("AND of selections on ORDERS.\n");
    }
  #endif

  BShare *res_sel = malloc(ROWS_O*sizeof(BShare));
  assert(res_sel!=NULL);
  BShare *rem_res_sel = malloc(ROWS_O*sizeof(BShare));
  assert(rem_res_sel!=NULL);

  for (int j=0; j<ROWS_O; j++) {
    res_sel[j] = and_b((BShare)sel[j], (BShare)rem_sel[j],
                    (BShare)sel2[j], (BShare)rem_sel2[j],
                    get_next_rb()) & mask;
  }
  exchange_shares_array(res_sel, rem_res_sel, ROWS_O);
  free(sel); free(rem_sel); free(sel2); free(rem_sel2);

  // STEP 5: Fused Selection-IN
  #if DEBUG
    if (rank==0) {
      printf("(fused) IN.\n");
    }
  #endif

  BShare *in_res = malloc(ROWS_O*sizeof(BShare));
  assert(in_res!=NULL);
  BShare *rem_in_res = malloc(ROWS_O*sizeof(BShare));
  assert(rem_in_res!=NULL);
  in_sel_right(&t1, &t2, 0, 0, 6, in_res, ROWS_O);
  exchange_shares_array(in_res, rem_in_res, ROWS_O);

  #if DEBUG
    if (rank==0) {
      printf("AND with result of IN.\n");
    }
  #endif

  for (int j=0; j<ROWS_O; j++) {
    res_sel[j] = and_b(in_res[j], rem_in_res[j],
                      res_sel[j], rem_res_sel[j], get_next_rb())
                      & mask;
  }
  exchange_shares_array(res_sel, rem_res_sel, ROWS_O);

  free(in_res); free(rem_in_res);

  // Copy selected bit to the ORDERS table
  for (int i=0; i<ROWS_O; i++) {
    t1.contents[i][6] = res_sel[i];
    t1.contents[i][7] = rem_res_sel[i];
  }
  free(res_sel); free(rem_res_sel);

  // STEP 6: Order by O_ORDERPRIORITY (att=4)
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  unsigned int att_index[1] = {4};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS_O/2);

  // STEP 7: GROUP-BY-COUNT on O_ORDERPRIORITY (att=4)
  #if DEBUG
    if (rank==0) {
      printf("Group-by.\n");
    }
  #endif

  unsigned int gr_index[1] = {4};
  group_by_sum_rca(&t1, gr_index, 1);

  // There should be 2 valid lines in the result with this order:
  // (O_ORDERPRIORITY, COUNT) = 100, 2
  // (O_ORDERPRIORITY, COUNT) = 200, 1
  // The rest of the lines in the result are garbage (due to masking)
  Data result[ROWS_O][2];
  // Open all elements
  for (int i=0; i<ROWS_O; i++) {
    result[i][0] = open_b(t1.contents[i][4]); // O_ORDERPRIORITY
    result[i][1] = open_b(t1.contents[i][6]); // COUNT
  }

  if (rank == 0) {
    assert(result[1][0]==100);
    assert(result[1][1]==2);
    assert(result[2][0]==200);
    assert(result[2][1]==1);
    #if DEBUG
      for (int i=0; i<ROWS_O; i++) {
        printf("[%d] (O_ORDERPRIORITY, COUNT) = %lld, %lld\n",
                 i, result[i][0], result[i][1]);
      }
    #endif
    printf("TEST TPC-H Q4 Baseline: OK.\n");
  }

  free(t1.contents); free(t2.contents);

  // tear down communication
  TCP_Finalize();
  return 0;
}
