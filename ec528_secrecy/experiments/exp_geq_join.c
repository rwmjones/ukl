#include <stdio.h>
#include <assert.h>
#include <sys/random.h>
#include <sys/time.h>

#include "exp-utils.h"

#define COLS1 2
#define COLS2 2


int main(int argc, char** argv) {

  // initialize communication
  init(argc, argv);

  if (argc < 3) {
    printf("\n\nUsage: %s <NUM_ROWS_1> <NUM_ROWS_2>\n\n", argv[0]);
    return -1;
  }

  const int ROWS1 = atoi(argv[argc - 2]);
  const int ROWS2 = atoi(argv[argc - 1]);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  printf("[%d] ROWS1: %d\t ROWS2: %d\n", rank, ROWS1, ROWS2);

  // r1: first relation, r2: second relation
  BShare *r1s1, *r1s2, *r2s1, *r2s2;

  r1s1 = malloc(ROWS1*COLS1*sizeof(BShare));
  assert(r1s1!=NULL);
  r1s2 = malloc(ROWS1*COLS1*sizeof(BShare));
  assert(r1s2!=NULL);
  r2s1 = malloc(ROWS1*COLS1*sizeof(BShare));
  assert(r2s1!=NULL);
  r2s2 = malloc(ROWS1*COLS1*sizeof(BShare));
  assert(r2s2!=NULL);

  if (rank == 0) { //P1
    // Initialize input data and shares
    Data r1[ROWS1][COLS1];
    Data r2[ROWS2][COLS2];
    BShare r1s3[ROWS1][COLS1], r2s3[ROWS2][COLS2];

    // generate random data for r1
    for (int i=0; i<ROWS1; i++) {
      for (int j=0; j<COLS1; j++) {
        r1[i][j] = random();
      }
    }

    // generate random data for r2
    for (int i=0; i<ROWS2; i++) {
      for (int j=0; j<COLS2; j++) {
        r2[i][j] = random();
      }
    }

    printf("Done with initialization.\n");

    init_sharing();

    // generate r1 shares
    for (int i=0; i<ROWS1; i++) {
        for (int j=0; j<COLS1; j++) {
            generate_bool_share(r1[i][j], &r1s1[i*COLS1+j], &r1s2[i*COLS1+j], &r1s3[i][j]);
        }
    }

    // generate r2 shares
    for (int i=0; i<ROWS2; i++) {
        for (int j=0; j<COLS2; j++) {
            generate_bool_share(r2[i][j], &r2s1[i*COLS2+j], &r2s2[i*COLS2+j], &r2s3[i][j]);
        }
    }

    printf("Done with share generation.\n");

    //Send shares to P2
    TCP_Send(r1s2, ROWS1*COLS1, 1, sizeof(BShare));
    TCP_Send(r2s2, ROWS2*COLS2, 1, sizeof(BShare));
    TCP_Send(&r1s3[0][0], ROWS1*COLS1, 1, sizeof(BShare));
    TCP_Send(&r2s3[0][0], ROWS2*COLS2, 1, sizeof(BShare));
    //Send shares to P3
    TCP_Send(&r1s3[0][0], ROWS1*COLS1, 2, sizeof(BShare));
    TCP_Send(&r2s3[0][0], ROWS2*COLS2, 2, sizeof(BShare));
    TCP_Send(r1s1, ROWS1*COLS1, 2, sizeof(BShare));
    TCP_Send(r2s1, ROWS2*COLS2, 2, sizeof(BShare));
  }
  else { //P2 or P3
    TCP_Recv(r1s1, ROWS1*COLS1, 0, sizeof(BShare));
    TCP_Recv(r2s1, ROWS2*COLS2, 0, sizeof(BShare));
    TCP_Recv(r1s2, ROWS1*COLS1, 0, sizeof(BShare));
    TCP_Recv(r2s2, ROWS2*COLS2, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  struct timeval begin, end;
  long seconds, micro;
  double elapsed;

  // Start timer
  gettimeofday(&begin, 0);

  printf("Rank [%d]: seeds exchnaged.\n", rank);

  // allocate BShare tables
  BShareTable in1 = {-1, rank, ROWS1, 2*COLS1, 1},
              in2 = {-1, rank, ROWS2, 2*COLS2, 2};
  allocate_bool_shares_table(&in1);
  allocate_bool_shares_table(&in2);

  // copy shares into the BShareTables
  for (int i=0; i<ROWS1; i++) {
    for (int j=0; j<COLS1; j++) {
        in1.contents[i][2*j] = r1s1[i*COLS1+j];
        in1.contents[i][2*j+1] = r1s2[i*COLS1+j];
    }
  }
  // relation 2
  for (int i=0; i<ROWS2; i++) {
    for (int j=0; j<COLS2; j++) {
        in2.contents[i][2*j] = r2s1[i*COLS2+j];
        in2.contents[i][2*j+1] = r2s2[i*COLS2+j];
    }
  }

  // free temp share tables
  free(r1s1);
  free(r1s2);
  free(r2s1);
  free(r2s2);

  printf("Rank [%d]: tables allocated... Starting computation.\n", rank);
  Predicate_B p = {GEQ, NULL, NULL, 0, 0};

  // test batch join
  int batch_size1 = ROWS1;
  int batch_size2 = ROWS2;
  BShare *res = malloc(batch_size1*batch_size2*sizeof(BShare)); // batched join result
  BShare *rem = malloc(batch_size1*batch_size2*sizeof(BShare)); // this is not really needed for geq
  assert(res!=NULL);
  join_b_batch(&in1, &in2, 0, batch_size1,
                0, batch_size2, p, rem, res);

  Data *out = malloc(batch_size1*batch_size2*sizeof(BShare)); // open result
  assert(out!=NULL);
  open_b_array(res, batch_size1*batch_size2, out);

  printf("Rank [%d]: Done.\n", rank);

  free(res);
  free(rem);
  free(out);

  // stop timer
  gettimeofday(&end, 0);
  seconds = end.tv_sec - begin.tv_sec;
  micro = end.tv_usec - begin.tv_usec;
  elapsed = seconds + micro*1e-6;

  if (rank == 0) {
    printf("EXP-GEQ-JOIN\t%d\t%d\t%.3f\n",
            ROWS1, ROWS2, elapsed);
  }

  // tear down communication
  TCP_Finalize();
  return 0;
}
