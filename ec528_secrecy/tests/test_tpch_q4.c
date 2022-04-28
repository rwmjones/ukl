#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "test-utils.h"

#define DEBUG 0
#define COLS_O 5
#define COLS_L 4
#define D1 1993
#define D2 1995
// ORDERS input size
#define ROWS_O 4
// LINEITEM input size
#define ROWS_L 16

/**
 * Tests the correctness of TPC-H Q4.
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

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // 0: O_ORDERKEY, 2: O_ORDERDATE, 4: O_ORDERPRIORITY,
  // 6: O_ORDERDATE - D1, 8: O_ORDERDATE - D2
  BShareTable t1 = {-1, rank, ROWS_O, 2*COLS_O, 1};
  allocate_bool_shares_table(&t1);
  // 0: L_ORDERKEY, 2: L_COMMITDATE, 4: L_RECEIPTDATE,
  // 6: L_COMMITDATE - L_RECEIPTDATE
  BShareTable t2 = {-1, rank, ROWS_L, 2*COLS_L, 2};
  allocate_bool_shares_table(&t2);

  // initialize rand bits for conversion (all equal to 0)
  BShare *rb = calloc(ROWS_O, sizeof(BShare));
  AShare *ra = calloc(ROWS_O, sizeof(AShare));
  // initialize rand bits for group-by (all equal to 0)
  BShare *rand_b = calloc(2*(ROWS_O-1), sizeof(BShare));
  AShare *rand_a = calloc(2*(ROWS_O-1), sizeof(AShare));

  if (rank == 0) { //P1
    // Initialize input data and shares
    Data orders[ROWS_O][COLS_O] = {{2, 1994, 200, 1, -1},
                                  {1, 1993, 100, 0, -2},
                                  {4, 1994, 100, 1, -1},
                                  {7, 1991, 300, -2, -6}};

    Data lineitem[ROWS_L][COLS_L] = {{1, 1991, 1993, -2},
                                    {2, 1992, 1994, -2},
                                    {3, 1993, 1993, 0},
                                    {4, 1992, 1994, -2},
                                    {5, 1995, 1994, 1},
                                    {6, 1997, 1998, -1},
                                    {1, 1995, 1994, 1},
                                    {2, 1995, 1994, 1},
                                    {3, 1990, 1997, -7},
                                    {4, 2000, 1999, 1},
                                    {5, 1998, 1994, 4},
                                    {6, 1996, 1993, 3},
                                    {6, 1993, 1994, -1},
                                    {6, 1995, 1992, 3},
                                    {5, 1999, 1995, 4},
                                    {3, 1993, 1994, -1}};

    Data ** c1 = allocate_2D_data_table(ROWS_O, COLS_O);
    for (int i=0;i<ROWS_O;i++){
      for(int j=0;j<COLS_O;j++){
        c1[i][j] = orders[i][j];
      }
    }

    Data ** c2 = allocate_2D_data_table(ROWS_L, COLS_L);
    for (int i=0;i<ROWS_L;i++){
      for(int j=0;j<COLS_L;j++){
        c2[i][j] = lineitem[i][j];
      }
    }

    Table r_orders = {-1, ROWS_O, COLS_O, c1};
    Table r_lineitem = {-1, ROWS_L, COLS_L, c2};

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS_O, 2*COLS_O, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS_O, 2*COLS_O, 1};
    allocate_bool_shares_table(&t13);

    // t2 Bshare tables for P2, P3 (local to P1)
    BShareTable t22 = {-1, 1, ROWS_L, 2*COLS_L, 2};
    allocate_bool_shares_table(&t22);
    BShareTable t23 = {-1, 2, ROWS_L, 2*COLS_L, 2};
    allocate_bool_shares_table(&t23);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r_orders, &t1, &t12, &t13);
    // Generate boolean shares for r2
    generate_bool_share_tables(&r_lineitem, &t2, &t22, &t23);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS_O*2*COLS_O, 1, sizeof(BShare));
    TCP_Send(&(t22.contents[0][0]), ROWS_L*2*COLS_L, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS_O*2*COLS_O, 2, sizeof(BShare));
    TCP_Send(&(t23.contents[0][0]), ROWS_L*2*COLS_L, 2, sizeof(BShare));

    // free temp tables
    free(c1);
    free(t12.contents);
    free(t13.contents);
    free(c2);
    free(t22.contents);
    free(t23.contents);

  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS_O*2*COLS_O, 0, sizeof(BShare));
    TCP_Recv(&(t2.contents[0][0]), ROWS_L*2*COLS_L, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // STEP 1: SORT ORDERS on priority (att=4)
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  unsigned int att_index[1] = {4};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS_O/2);

  // STEP 2: Selection L_COMMITDATE < L_RECEIPTDATE
  #if DEBUG
    if (rank==0) {
      printf("1st selection on LINEITEM.\n");
    }
  #endif

  BShare *sel_l = malloc(ROWS_L*sizeof(BShare));
  assert(sel_l!=NULL);
  BShare *rem_sel_l = malloc(ROWS_L*sizeof(BShare));
  assert(rem_sel_l!=NULL);

  Predicate_B p_l = {GT, NULL, NULL, 6, -1};
  select_b(t2, p_l, sel_l);
  exchange_shares_array(sel_l, rem_sel_l, ROWS_L);

  // Copy selection bits to columns 6, 7
  for (int i=0; i<ROWS_L; i++) {
    t2.contents[i][6] = sel_l[i];
    t2.contents[i][7] = rem_sel_l[i];
  }

  free(sel_l); free(rem_sel_l);

  // STEP 3: Fused Selection-IN
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

  // STEP 4: Apply selection O_ORDERDATE >= D1
  #if DEBUG
    if (rank==0) {
      printf("1st selection on ORDERS.\n");
    }
  #endif

  BShare *sel = malloc(ROWS_O*sizeof(BShare));
  assert(sel!=NULL);
  BShare *rem_sel = malloc(ROWS_O*sizeof(BShare));
  assert(rem_sel!=NULL);

  Predicate_B p = {GEQ, NULL, NULL, 6, -1};
  select_b(t1, p, sel);
  exchange_shares_array(sel, rem_sel, ROWS_O);

  // Copy selection bits to O_ORDERDATE - D1 column
  for (int i=0; i<ROWS_O; i++) {
    t1.contents[i][6] = sel[i];
    t1.contents[i][7] = rem_sel[i];
  }

  // STEP 5: Apply selection O_ORDERDATE < D2
  #if DEBUG
    if (rank==0) {
      printf("2nd selection on ORDERS.\n");
    }
  #endif

  Predicate_B p2 = {GT, NULL, NULL, 8, -1};
  select_b(t1, p2, sel);
  exchange_shares_array(sel, rem_sel, ROWS_O);

  // Copy selection bits to O_ORDERDATE - D2 column
  for (int i=0; i<ROWS_O; i++) {
    t1.contents[i][8] = sel[i];
    t1.contents[i][9] = rem_sel[i];
  }

  // STEP 6: Compute AND of selections
  BShare mask = 1;
  #if DEBUG
    if (rank==0) {
      printf("AND of selections.\n");
    }
  #endif

  BShare *res_sel = malloc(ROWS_O*sizeof(BShare));
  assert(res_sel!=NULL);
  BShare *rem_res_sel = malloc(ROWS_O*sizeof(BShare));
  assert(rem_res_sel!=NULL);

  for (int j=0; j<ROWS_O; j++) {
    res_sel[j] = and_b(t1.contents[j][6], t1.contents[j][7],
                      t1.contents[j][8], t1.contents[j][9], get_next_rb())
                      & mask;
  }
  exchange_shares_array(res_sel, rem_res_sel, ROWS_O);

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

  free(in_res); free(rem_in_res);

  // STEP 7: GROUP-BY-COUNT on O_ORDERPRIORITY (att=4)
  #if DEBUG
    if (rank==0) {
      printf("Conversion.\n");
    }
  #endif
  // a. get arithmetic shares of selected bits
  AShare *sel_a = malloc(ROWS_O*sizeof(AShare));
  assert(sel_a!=NULL);
  convert_single_bit_array(res_sel, ra, rb, ROWS_O, sel_a);
  #if DEBUG
    if (rank==0) {
      printf("Group-by.\n");
    }
  #endif
  unsigned key_indices[1] = {4};
  group_by_count(&t1, key_indices, 1, res_sel, sel_a, rand_b, rand_a);

  free(ra); free(rb); free(sel); free(rem_sel); free(res_sel);
  free(rand_a); free(rand_b);

  // There should be 2 valid lines in the result with this order:
  // (O_ORDERPRIORITY, COUNT) = 100, 2
  // (O_ORDERPRIORITY, COUNT) = 200, 1
  // The rest of the lines in the result are garbage (due to masking)
  Data result[ROWS_O][2];
  // Open all elements
  for (int i=0; i<ROWS_O; i++) {
    result[i][0] = open_b(t1.contents[i][4]); // O_ORDERPRIORITY
    result[i][1] = open_a(sel_a[i]); // COUNT
  }

  if (rank == 0) {
    assert(result[1][0]==100);
    assert(result[1][1]==2);
    assert(result[2][0]==200);
    assert(result[2][1]==1);
    #if DEBUG
      for (int i=0; i<ROWS_O; i++) {
        printf("[%d] (O_ORDERPRIORITY, COUNT) = %lld, %lld\n", i, result[i][0], result[i][1]);
      }
    #endif
  }

  if (rank == 0) {
    printf("TEST TPC-H Q4: OK.\n");
  }

  free(t1.contents); free(t2.contents); free(sel_a);

  // tear down communication
  TCP_Finalize();
  return 0;
}
