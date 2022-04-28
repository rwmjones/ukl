#define _GNU_SOURCE

#include <stdio.h>
#include <assert.h>
#include <sys/time.h>
#include <string.h>

#include "exp-utils.h"

#define COLS 1

/**
 * Evaluates the performance of the batched group-by-count operator.
 **/

int main(int argc, char** argv) {

  char *args[9] = { NULL };
  int i = 0;
  ssize_t read = 0;
  size_t n = 0;
  FILE *fp;
  char *nl;
  argc = 8;


  if (argc < 2) {
    printf("\n\nUsage: %s <NUM_ROWS>\n\n", argv[0]);
    return -1;
  }

  fp = fopen("/config", "r");

  while((read = getline(&args[i], &n, fp)) > 0) {
    nl = strchr(args[i], '\n');
    if (nl)
      *nl = 0;
    i++;
    n = 0;
  }
  fclose(fp);

  printf("Initializing comms\n");
  // initialize communication
  init(argc, args);

  const long ROWS = strtol(args[7], NULL, 10);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  // The input tables per party
  BShareTable t1 = {-1, rank, ROWS, 2*COLS, 1};
  allocate_bool_shares_table(&t1);
  printf("Starting\n");

  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1;
    generate_random_table(&r1, ROWS, COLS);

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS, 2*COLS, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS, 2*COLS, 1};
    allocate_bool_shares_table(&t13);

printf("Sharing\n");
    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, &t1, &t12, &t13);

printf("Sending to 2\n");
    //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS*2*COLS, 1, sizeof(BShare));

printf("Sending to 3\n");
    //Send shares to P3
    TCP_Send(&(t13.contents[0][0]), ROWS*2*COLS, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
  }
  else if (rank == 1) { //P2
    TCP_Recv(&(t1.contents[0][0]), ROWS*2*COLS, 0, sizeof(BShare));
  }
  else { //P3
    TCP_Recv(&(t1.contents[0][0]), ROWS*2*COLS, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  AShare *counters = malloc(ROWS*sizeof(AShare));
  AShare *remote_counters = malloc(ROWS*sizeof(AShare));

  BShare *rand_a = malloc(2*(ROWS-1)*sizeof(BShare));
  BShare *rand_b = malloc(2*(ROWS-1)*sizeof(BShare));

  // initialize counters
  for (int i=0; i<ROWS; i++) {
    counters[i] = rank % 2;
    remote_counters[i] = succ % 2;
  }

  // initialize rand bits (all equal to 1)
  for (int i=0; i<2*(ROWS-1); i++) {
    rand_a[i] = (unsigned int) 1;
    rand_b[i] = (unsigned int) 1;
  }

  /* =======================================================
     1. Measure group-by
  ======================================================== */
  // start timer
  gettimeofday(&begin, 0);

  unsigned key_indices[1] = {0};
  group_by_count_micro(&t1, key_indices, 1, counters, remote_counters, rand_b,
                                                                       rand_a);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    fp = fopen("/output", "w");
    fprintf(fp, "%ld\tGROUP-BY\t%.3f\n", ROWS, elapsed);
    fclose(fp);
    printf("%ld\tGROUP-BY\t%.3f\n", ROWS, elapsed);

  }

  free(t1.contents); free(counters); free(remote_counters);
  free(rand_a); free(rand_b);

  // tear down communication
  TCP_Finalize();
  return 0;
}
