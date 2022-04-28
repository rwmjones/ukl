#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define DEBUG 0
#define COLS_O 4
#define COLS_L 4
#define D1 1993
#define D2 1995

/**
 * Evaluates the performance of TPC-H Q4.
 **/

int main(int argc, char** argv) {

  if (argc < 3) {
    printf("\n\nUsage: %s <NUM_ROWS_ORDERS> <NUM_ROWS_LINEITEM>\n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS_O = atol(argv[argc - 2]); // input1 size
  const long ROWS_L = atol(argv[argc - 1]); // input2 size

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS_O, 2*COLS_O, 1};
  allocate_bool_shares_table(&t1);
  BShareTable t2 = {-1, rank, ROWS_L, 2*COLS_L, 2};
  allocate_bool_shares_table(&t2);

  // shares of constants (dates)
  BShare sd1, sd2;

  BShare *rb = calloc(ROWS_O, sizeof(BShare));
  AShare *ra = calloc(ROWS_O, sizeof(AShare));
  BShare *rand_b = calloc(2*(ROWS_O-1), sizeof(BShare));
  AShare *rand_a = calloc(2*(ROWS_O-1), sizeof(AShare));

  // initialize rand bits for conversion (all equal to 0)
  for (int i=0; i<ROWS_O; i++) {
    ra[i] = (unsigned int) 0;
    rb[i] = (unsigned int) 0;
  }

  // initialize rand bits for group-by (all equal to 0)
  for (int i=0; i<2*(ROWS_O-1); i++) {
    rand_a[i] = (unsigned int) 0;
    rand_b[i] = (unsigned int) 0;
  }

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1, r2;
    generate_random_table(&r1, ROWS_O, COLS_O);
    generate_random_table(&r2, ROWS_L, COLS_L);

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

    BShare d12, d22, d13, d23;

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);
    // Generate boolean shares for r2
    generate_bool_share_tables(&r2, &t2, &t22, &t23);
    // generate shares for constants
    generate_bool_share(D1, &sd1, &d12, &d13);
    generate_bool_share(D2, &sd2, &d22, &d23);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS_O*2*COLS_O, 1, sizeof(BShare));
    TCP_Send(&(t22.contents[0][0]), ROWS_L*2*COLS_L, 1, sizeof(BShare));
    TCP_Send(&d12, 1, 1, sizeof(BShare));
    TCP_Send(&d22, 1, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS_O*2*COLS_O, 2, sizeof(BShare));
    TCP_Send(&(t23.contents[0][0]), ROWS_L*2*COLS_L, 2, sizeof(BShare));
    TCP_Send(&d13, 1, 2, sizeof(BShare));
    TCP_Send(&d23, 1, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
    free(r2.contents);
    free(t22.contents);
    free(t23.contents);

  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS_O*2*COLS_O, 0, sizeof(BShare));
    TCP_Recv(&(t2.contents[0][0]), ROWS_L*2*COLS_L, 0, sizeof(BShare));
    TCP_Recv(&sd1, 1, 0, sizeof(BShare));
    TCP_Recv(&sd2, 1, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  // start timer
  gettimeofday(&begin, 0);

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

  // Open results
  BShare *s_result = malloc(2*ROWS_O*sizeof(BShare));
  Data *result = malloc(2*ROWS_O*sizeof(Data));
  for (int i=0; i<ROWS_O; i+=2) {
    s_result[i] = t1.contents[i][4]; // O_ORDERPRIORITY
    s_result[i+1] = t1.contents[i][6]; // COUNT
  }
  open_b_array(s_result, 2*ROWS_O, s_result);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("\tTPCH-Q4-BASELINE\t%ld\t%ld\t%.3f\n", ROWS_O, ROWS_L, elapsed);
  }

  free(t1.contents); free(t2.contents); free(s_result); free(result);

  // tear down communication
  TCP_Finalize();
  return 0;
}
