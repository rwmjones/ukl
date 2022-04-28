#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "test-utils.h"

#define DEBUG 0

// Table one: boolean
// Needed COLS1:            0:SHIPDATE, 1:QUANTITY, 2:DISCOUNT, 3:EXTENDEDPRICE
// In addition to :         4:99-SHIPDATE, 5:SHIPDATE-200, 6:QUANTITY-24, 7:DISCOUNT-0.05, 8:0.07-DISCOUNT
// In addition to SEL:      9:SEL_START, 10:SEL_END, 11:DISCOUNT_MIN, 12:DISCOUNT_MAX, 13:SEL_QUANTITY
#define ROWS1 8
#define COLS1 14

// Table two: arithmetic
// Needed COLS1:            0:DISCOUNT, 1:EXTENDEDPRICE
#define COLS2 2

// SELECT SUM(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE
// FROM LINEITEM
// WHERE L_SHIPDATE >= '1994-01-01'
// AND L_SHIPDATE < dateadd(yy, 1, cast('1994-01-01' as date))
// AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
// AND L_QUANTITY < 24

void allocate_int_shares_table_row(AShareTable *table){
  int length = sizeof(AShare *) * table->numRows +
               sizeof(AShare) * table->numRows * table->numCols;
  table->contents = (AShare **)malloc(length);
  assert(table->contents != NULL);
  AShare *ptr = (AShare *)(table->contents + table->numRows);
  for (int i = 0; i < table->numRows; i++)
    table->contents[i] = (ptr + table->numCols * i);
}

int main(int argc, char **argv){
  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable tb = {-1, rank, ROWS1, 2 * COLS1, 1};
  allocate_bool_shares_table(&tb);
  AShareTable ta = {-1, rank, ROWS1, 2 * COLS2, 1};
  allocate_int_shares_table_row(&ta);

  BShare rb[ROWS1];
  AShare ra[ROWS1];

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  // start timer
  gettimeofday(&begin, 0);

// STEP 1: Check Selection Conditions
#if DEBUG
  if (rank == 0){
    printf("Evaluating Selection Conditions.\n");
  }
#endif

  // Selection Conditions
  // What operators I have: =, < 0
  // 0:SHIPDATE >= 100  or  4:99-SHIPDATE < 0
  // 0:SHIPDATE <  200  or  5:SHIPDATE-200 < 0
  // 2:QUANTITY < 24    or  6:QUANTITY-24 < 0
  // 1:DISCOUNT >= 0.05 or  Not(7:DISCOUNT-0.05 < 0)
  // 1:DISCOUNT <= 0.07 or  Not(8:0.07-DISCOUNT < 0)
  Predicate_B CONDS[5] = {{GT, NULL, NULL, 8, 8},
                          {GT, NULL, NULL, 10, 10},
                          {GT, NULL, NULL, 12, 12},
                          {GT, NULL, NULL, 14, 14},
                          {GT, NULL, NULL, 16, 16}};

  BShare *sel = malloc(ROWS1 * sizeof(BShare));
  assert(sel != NULL);
  BShare *rem_sel = malloc(ROWS1 * sizeof(BShare));
  assert(rem_sel != NULL);

  int ind = 18;
  for (int k = 0; k < 5; k++){
    select_b(tb, CONDS[k], sel);
    exchange_shares_array(sel, rem_sel, ROWS1);

    if (k < 3){
      for (int i = 0; i < ROWS1; i++){
        tb.contents[i][ind] = sel[i];
        tb.contents[i][ind + 1] = rem_sel[i];
      }
    }
    else{
      for (int i = 0; i < ROWS1; i++){
        // we have odd number of parties so each party can just
        // xor their share with 1 to do the negation
        tb.contents[i][ind] = sel[i] ^ 1;
        tb.contents[i][ind + 1] = rem_sel[i] ^ 1;
      }
    }

    ind += 2;
  }

  // STEP 2: Select rows
  // Needed COLS1:             0:SHIPDATE, 1:QUANTITY, 2:DISCOUNT, 3:EXTENDEDPRICE
  // In addition to :         4:99-SHIPDATE, 5:SHIPDATE-200, 6:QUANTITY-24, 7:DISCOUNT-0.05, 8:0.07-DISCOUNT
  // In addition to SEL:      9:SEL_START, 10:SEL_END, 11:DISCOUNT_MIN, 12:DISCOUNT_MAX, 13:SEL_QUANTITY
  // Selection order:
  // AND(AND(AND(1, 2), AND(3,4))), 5)
#if DEBUG
  if (rank == 0){
    printf("Applying Selection.\n");
  }
#endif

  // 1, 2 >> 2
  and_b_table(tb, 18, 20, ROWS1, sel);
  exchange_shares_array(sel, rem_sel, ROWS1);
  for (int i = 0; i < ROWS1; i++){
    tb.contents[i][20] = sel[i];
    tb.contents[i][21] = rem_sel[i];
  }

  // 3, 4 >> 4
  and_b_table(tb, 22, 24, ROWS1, sel);
  exchange_shares_array(sel, rem_sel, ROWS1);
  for (int i = 0; i < ROWS1; i++){
    tb.contents[i][24] = sel[i];
    tb.contents[i][25] = rem_sel[i];
  }

  // 2, 5 >> 5
  and_b_table(tb, 18, 26, ROWS1, sel);
  exchange_shares_array(sel, rem_sel, ROWS1);
  for (int i = 0; i < ROWS1; i++){
    tb.contents[i][26] = sel[i];
    tb.contents[i][27] = rem_sel[i];
  }

  // 4, 5 >> 5
  and_b_table(tb, 24, 26, ROWS1, sel);
  exchange_shares_array(sel, rem_sel, ROWS1);
  for (int i = 0; i < ROWS1; i++){
    tb.contents[i][26] = sel[i];
    tb.contents[i][27] = rem_sel[i];
  }

  // STEP 3: Change boolean to arth
#if DEBUG
  if (rank == 0){
    printf("Changing Selection boolean share to arithmetic.\n");
  }
#endif
  AShare *res = malloc(ROWS1 * sizeof(AShare));
  assert(res != NULL);
  convert_single_bit_array(sel, ra, rb, ROWS1, res);
  free(sel);
  free(rem_sel);

  AShare *res_remote = malloc(ROWS1 * sizeof(AShare));
  assert(res_remote != NULL);
  exchange_shares_array(res, res_remote, ROWS1);

  // STEP 4: Multiplication
#if DEBUG
  if (rank == 0){
    printf("Changing Selection boolean share to arithmetic.\n");
  }
#endif
  AShare *mulRes = malloc(ROWS1 * sizeof(AShare));
  assert(mulRes != NULL);

  AShare *mulResRemote = malloc(ROWS1 * sizeof(AShare));
  assert(mulResRemote != NULL);

  // 2:DISCOUNT * 3:EXTENDEDPRICE
  for (int i = 0; i < ROWS1; i++){
    mulRes[i] = mul(ta.contents[i][0], ta.contents[i][1], ta.contents[i][2], ta.contents[i][3], get_next_r());
  }
  exchange_shares_array(mulRes, mulResRemote, ROWS1);

  long long summation = 0;
  // choosing those with selection line of 1
  for (int i = 0; i < ROWS1; i++){
    mulRes[i] = mul(mulRes[i], mulResRemote[i], res[i], res_remote[i], get_next_r());
    summation += mulRes[i];
  }

  // Step Four: getting arthmetic shares
  AShare summation_share = summation;
  long long summationRes = open_a(summation_share);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro * 1e-6;

  Data result[ROWS1][6];
  for (int i = 0; i < ROWS1; i++){
#if DEBUG
    result[i][0] = open_b(tb.contents[i][18]);
    result[i][1] = open_b(tb.contents[i][20]);
    result[i][2] = open_b(tb.contents[i][22]);
    result[i][3] = open_b(tb.contents[i][24]);
    result[i][4] = open_b(tb.contents[i][26]);
#endif
    result[i][5] = open_a(mulRes[i]);
  }

  if (rank == 0){
    assert(result[0][5] == 0);
    assert(result[1][5] == 0);
    assert(result[2][5] == 0);
    assert(result[3][5] == 0);
    assert(result[4][5] == 0);
    assert(result[5][5] == 760);
    assert(result[6][5] == 1064);
    assert(result[7][5] == 912);
    assert(summationRes == 2736);
#if DEBUG
    for (int i = 0; i < ROWS1; i++){
      printf("%lld, %lld, %lld, %lld, %lld, %lld\n", result[i][0], result[i][1], result[i][2], result[i][3], result[i][4], result[i][5]);
    }
    printf("RES=\t%lld\n", summationRes);
#endif
  }

  if (rank == 0){
    printf("TEST Q6: OK.\n");
  }

  free(tb.contents);
  free(ta.contents);

  // tear down communication
  TCP_Finalize();
  return 0;
}
