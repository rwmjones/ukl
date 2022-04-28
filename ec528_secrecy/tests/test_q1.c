#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "test-utils.h"

#define DEBUG 0
#define COLS1 3
#define COLS2 1

/**
 * Evaluates the performance of Q1 (comorbidity).
 **/

int main(int argc, char** argv) {

  const long ROWS1 = 8; // input1 size
  const long ROWS2 = 8; // input2 size

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS1, 2*COLS1, 1}; // {pid, diag, cnt}
  allocate_bool_shares_table(&t1);
  BShareTable t2 = {-1, rank, ROWS2, 2*COLS2, 2};
  allocate_bool_shares_table(&t2);

  BShare *rb = malloc(ROWS1*sizeof(BShare));
  AShare *ra = malloc(ROWS1*sizeof(AShare));
  BShare *rand_b = malloc(2*(ROWS1-1)*sizeof(BShare));
  AShare *rand_a = malloc(2*(ROWS1-1)*sizeof(AShare));

  // initialize rand bits for conversion (all equal to 0)
  for (int i=0; i<ROWS1; i++) {
    ra[i] = (unsigned int) 0;
    rb[i] = (unsigned int) 0;
  }

  // initialize rand bits for group-by (all equal to 0)
  for (int i=0; i<2*(ROWS1-1); i++) {
    rand_a[i] = (unsigned int) 0;
    rand_b[i] = (unsigned int) 0;
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  // start timer
  gettimeofday(&begin, 0);

  // STEP 1: SORT t1 on diag (att=2)
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  unsigned int att_index[1] = {2};
  bool asc[1] = {1};
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS1/2);

  // STEP 2: IN
  #if DEBUG
    if (rank==0) {
      printf("IN.\n");
    }
  #endif
  BShare *in_res = malloc(ROWS1*sizeof(BShare));
  assert(in_res!=NULL);
  in(&t1, &t2, 0, 0, in_res, ROWS1);

  // STEP 3: GROUP-BY-COUNT on diag (att=2)
  #if DEBUG
    if (rank==0) {
      printf("Conversion.\n");
    }
  #endif
  // a. get arithmetic shares of selected bits
  AShare *in_res_a = malloc(ROWS1*sizeof(AShare));
  assert(in_res_a!=NULL);
  convert_single_bit_array(in_res, ra, rb, ROWS1, in_res_a);
  #if DEBUG
    if (rank==0) {
      printf("Group-by.\n");
    }
  #endif
  unsigned key_indices[1] = {2};
  group_by_count(&t1, key_indices, 1, in_res, in_res_a, rand_b, rand_a);
  free(rand_a); free(rand_b);

  // reuse ra, rb arrays for exchange of arithmetic counts
  exchange_a_shares_array(in_res_a, ra, ROWS1);

  // STEP 4: sort group's output on count
  // reuse rb, in_res for result of conversion to binary
  #if DEBUG
    if (rank==0) {
      printf("Conversion.\n");
    }
  #endif
  convert_a_to_b_array(in_res_a, ra, rb, in_res, ROWS1);
  free(in_res_a); free(ra);

  // order by count
  // first copy the binary counter to the last column of t1
  for (int i=0; i<ROWS1; i++) {
    t1.contents[i][4] = rb[i]; // local share of count
    t1.contents[i][5] = in_res[i]; // remote share of count
  }
  free(rb); free(in_res);

  // STEP 5: Sort by cnt
  #if DEBUG
    if (rank==0) {
      printf("Sorting.\n");
    }
  #endif
  att_index[0] = 4;
  asc[0] = 0;
  bitonic_sort_batch(&t1, att_index, 1, asc, ROWS1/2);

  // There should be three lines in the result with this order:
  // 8,3    (currently at index 4)
  // 10, 2  (currently at index 5)
  // 9, 1   (currently at index 6)
  // The rest of the lines in the result are garbage (due to multiplexing)
  Data result[ROWS1][2];
  // Open first 8 elements
  for (int i=0; i<ROWS1; i++) {
    result[i][0] = open_b(t1.contents[i][2]); // diag
    result[i][1] = open_b(t1.contents[i][4]); // count
  }

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    assert(result[4][0]==8);
    assert(result[4][1]==3);
    assert(result[5][0]==10);
    assert(result[5][1]==2);
    assert(result[6][0]==9);
    assert(result[6][1]==1);
    #if DEBUG
      for (int i=0; i<ROWS1; i++) {
        printf("[%d] (diag, cnt) = %lld, %lld\n", i, result[i][0], result[i][1]);
      }
    #endif
  }

  if (rank == 0) {
    printf("TEST Q1: OK.\n");
  }

  free(t1.contents); free(t2.contents);

  // tear down communication
  TCP_Finalize();
  return 0;
}
