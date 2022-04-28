#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "exp-utils.h"

/**
 * Evaluates the performance of the ripple-carry-adder.
 **/

int main(int argc, char** argv) {

  if (argc < 2) {
    printf("\n\nUsage: %s [INPUT_SIZE]\n\n", argv[0]);
    return -1;
  }

  // initialize communication
  init(argc, argv);

  const long ROWS = atol(argv[argc - 1]); // input size

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  BShare *r1s1, *r1s2, *r2s1, *r2s2;
  r1s1 = malloc(ROWS*sizeof(BShare));
  r1s2 = malloc(ROWS*sizeof(BShare));
  r2s1 = malloc(ROWS*sizeof(BShare));
  r2s2 = malloc(ROWS*sizeof(BShare));

  if (rank == 0) { //P1
    // Initialize input data and shares
    Data *r1, *r2;
    r1 = malloc(ROWS*sizeof(Data));
    r2 = malloc(ROWS*sizeof(Data));
    BShare *r1s3, *r2s3;
    r1s3 = malloc(ROWS*sizeof(BShare));
    r2s3 = malloc(ROWS*sizeof(BShare));

    // generate random data for r1 and r2
    for (long i=0; i<ROWS; i++) {
      r1[i] = random();
      r2[i] = random();
    }

    init_sharing();

    // generate r1 and r2 shares
    for (long i=0; i<ROWS; i++) {
      generate_bool_share(r1[i], &r1s1[i], &r1s2[i], &r1s3[i]);
      generate_bool_share(r2[i], &r2s1[i], &r2s2[i], &r2s3[i]);
    }

    //Send shares to P2
    TCP_Send(r1s2, ROWS, 1, sizeof(BShare));
    TCP_Send(r1s3, ROWS, 1, sizeof(BShare));
    TCP_Send(r2s2, ROWS, 1, sizeof(BShare));
    TCP_Send(r2s3, ROWS, 1, sizeof(BShare));
    //Send shares to P3
    TCP_Send(r1s3, ROWS, 2, sizeof(BShare));
    TCP_Send(r1s1, ROWS, 2, sizeof(BShare));
    TCP_Send(r2s3, ROWS, 2, sizeof(BShare));
    TCP_Send(r2s1, ROWS, 2, sizeof(BShare));

    // free temp tables
    free(r1);
    free(r2);
    free(r1s3);
    free(r2s3);
  }
  else { //P2 or P3
    TCP_Recv(r1s1, ROWS, 0, sizeof(BShare));
    TCP_Recv(r1s2, ROWS, 0, sizeof(BShare));
    TCP_Recv(r2s1, ROWS, 0, sizeof(BShare));
    TCP_Recv(r2s2, ROWS, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  /* =======================================================
     1. Measure array-based rca
  ======================================================== */
  BShare *res_array = malloc(ROWS*sizeof(BShare));

  // start timer
  gettimeofday(&begin, 0);

  boolean_addition_batch(r1s1, r1s2, r2s1, r2s2, res_array, ROWS);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("%ld\t%.3f\n", ROWS, elapsed);
  }

  free(r1s1); free(r1s2); free(r2s1); free(r2s2); free(res_array);

  // tear down communication
  TCP_Finalize();
  return 0;
}
