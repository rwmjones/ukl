#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

#define COLS 2

static void materialized_join(BShareTable *input1, BShareTable *input2,
                        int leftcol, int rightcol, BShareTable* result);

/**
 * Evaluates the performance of materialized join-group-by-count.
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
  BShareTable t1 = {-1, rank, ROWS1, 2*COLS, 1};
  allocate_bool_shares_table(&t1);
  BShareTable t2 = {-1, rank, ROWS2, 2*COLS, 2};
  allocate_bool_shares_table(&t2);

  // random bits required by group-by
  AShare *rand_a = malloc(2*(ROWS1*ROWS2-1)*sizeof(AShare));
  BShare *rand_b = malloc(2*(ROWS1*ROWS2-1)*sizeof(BShare));
  AShare *ra = malloc(ROWS1*ROWS2*sizeof(AShare));
  BShare *rb = malloc(ROWS1*ROWS2*sizeof(BShare));

  // initialize rand bits (all equal to 1)
  for (int i=0; i<2*(ROWS1*ROWS2-1); i++) {
    rand_a[i] = (unsigned int) 1;
    rand_b[i] = (unsigned int) 1;
  }

  for (int i=0; i<ROWS1*ROWS2; i++) {
    ra[i] = (unsigned int) 1;
    rb[i] = (unsigned int) 1;
  }

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1, r2;
    generate_random_table(&r1, ROWS1, COLS);
    generate_random_table(&r2, ROWS2, COLS);

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS1, 2*COLS, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS1, 2*COLS, 1};
    allocate_bool_shares_table(&t13);

    // t2 Bshare tables for P2, P3 (local to P1)
    BShareTable t22 = {-1, 1, ROWS2, 2*COLS, 2};
    allocate_bool_shares_table(&t22);
    BShareTable t23 = {-1, 2, ROWS2, 2*COLS, 2};
    allocate_bool_shares_table(&t23);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);
    // Generate boolean shares for r2
    generate_bool_share_tables(&r2, &t2, &t22, &t23);

    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS1*2*COLS, 1, sizeof(BShare));
    TCP_Send(&(t22.contents[0][0]), ROWS2*2*COLS, 1, sizeof(BShare));

    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS1*2*COLS, 2, sizeof(BShare));
    TCP_Send(&(t23.contents[0][0]), ROWS2*2*COLS, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
    free(r2.contents);
    free(t22.contents);
    free(t23.contents);

  }
  else { //P2 or P3
    TCP_Recv(&(t1.contents[0][0]), ROWS1*2*COLS, 0, sizeof(BShare));
    TCP_Recv(&(t2.contents[0][0]), ROWS2*2*COLS, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);
  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  /* =======================================================
     Measure naive join group-by count
  ======================================================== */
  /**
   * SELECT a, COUNT(a)
   * FROM t1, t2
   * WHERE t1.a=t2.a
   * GROUP BY a
  **/
  // start timer
  gettimeofday(&begin, 0);

  // allocate join result table
  BShareTable res_table = {-1, rank, ROWS1*ROWS2, 2*COLS+2, 1};
  allocate_bool_shares_table(&res_table);

  // Apply join first and materialize result
  materialized_join(&t1, &t2, 0, 0, &res_table);

  // we don't need original tables anymore
  free(t1.contents); free(t2.contents);

  // sort on group-by predicate
  unsigned int sort_att[1] = {0};
  bool asc[1] = {1};
  bitonic_sort_batch(&res_table, sort_att, 1, asc, res_table.numRows/2);

  // copy the join bits into an array to provide as an argument for
  // group-by-count
  BShare *join_selected = malloc(ROWS1*ROWS2*sizeof(BShare));
  assert(join_selected !=NULL);

  // convert selected bits to arithmetic
  AShare *join_selected_a = malloc(ROWS1*ROWS2*sizeof(BShare));
  assert(join_selected_a !=NULL);

  convert_single_bit_array(join_selected, ra, rb, ROWS1*ROWS2,
                            join_selected_a);

  // apply group-by-count on join output
  // the results are in join_selected_a
  unsigned key_indices[1] = {0};
  group_by_count(&res_table, key_indices, 1, join_selected, join_selected_a, rb, ra);

  free(join_selected);

  // open result
  Data *open_res = malloc(ROWS1*ROWS2*sizeof(Data));
  assert(open_res !=NULL);
  open_array(join_selected_a, ROWS1*ROWS2, open_res);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("%ld\tNAIVE GROUP-BY-JOIN\t%.3f\n", ROWS1, elapsed);
  }

  free(res_table.contents); free(open_res);

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
    // last exchange not required by this query
    if (l < numlevels-1) {
      for (int i=0; i<result->numRows; i++) {
        temp_local[i] = result->contents[i][res_index];
      }
      exchange_shares_array(temp_local, temp_remote, result->numRows);
        // copy exchanged result back to remote column
      for (int i=0; i<result->numRows; i++) {
        result->contents[i][res_index+1] = temp_remote[i];
      }
    }
  }
  free(temp_local); free(temp_remote);
}
