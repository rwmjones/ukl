#include <stdio.h>
#include <assert.h>

#include "test-utils.h"

#define DEBUG 0

int main(int argc, char** argv) {

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  BShare x1s1[7], x1s2[7], x1s3[7], x2s1[7], x2s2[7], x2s3[7],
         x3s1[7], x3s2[7], x3s3[7], x4s1[6], x4s2[6], x4s3[6],
         x5s1[6], x5s2[6], x5s3[6], x6s1[6], x6s2[6], x6s3[6];

  if (rank == 0) { //P1
    // Initialize input data and shares
    Data x1[7] = {1, 0, 0, 0, 1, 1, 0};
    Data x2[7] = {1, 1, 1, 1, 1, 1, 1};
    Data x3[7] = {0, 0, 0, 0, 0, 0, 0};
    Data x4[6] = {0, 0, 0, 0, 0, 0};
    Data x5[6] = {1, 1, 1, 1, 1, 1};
    Data x6[6] = {1, 0, 0, 0, 1, 1};

    init_sharing();
    for (int i=0; i<7; i++) {
        generate_bool_share(x1[i], &x1s1[i], &x1s2[i], &x1s3[i]);
        generate_bool_share(x2[i], &x2s1[i], &x2s2[i], &x2s3[i]);
        generate_bool_share(x3[i], &x3s1[i], &x3s2[i], &x3s3[i]);
    }
    for (int i=0; i<6; i++) {
        generate_bool_share(x4[i], &x4s1[i], &x4s2[i], &x4s3[i]);
        generate_bool_share(x5[i], &x5s1[i], &x5s2[i], &x5s3[i]);
        generate_bool_share(x6[i], &x6s1[i], &x6s2[i], &x6s3[i]);
    }
    //Send shares to P2
    TCP_Send(&x1s2, 7, 1, sizeof(BShare));
    TCP_Send(&x2s2, 7, 1, sizeof(BShare));
    TCP_Send(&x3s2, 7, 1, sizeof(BShare));
    TCP_Send(&x4s2, 6, 1, sizeof(BShare));
    TCP_Send(&x5s2, 6, 1, sizeof(BShare));
    TCP_Send(&x6s2, 6, 1, sizeof(BShare));
    TCP_Send(&x1s3, 7, 1, sizeof(BShare));
    TCP_Send(&x2s3, 7, 1, sizeof(BShare));
    TCP_Send(&x3s3, 7, 1, sizeof(BShare));
    TCP_Send(&x4s3, 6, 1, sizeof(BShare));
    TCP_Send(&x5s3, 6, 1, sizeof(BShare));
    TCP_Send(&x6s3, 6, 1, sizeof(BShare));
    //Send shares to P3
    TCP_Send(&x1s3, 7, 2, sizeof(BShare));
    TCP_Send(&x2s3, 7, 2, sizeof(BShare));
    TCP_Send(&x3s3, 7, 2, sizeof(BShare));
    TCP_Send(&x4s3, 6, 2, sizeof(BShare));
    TCP_Send(&x5s3, 6, 2, sizeof(BShare));
    TCP_Send(&x6s3, 6, 2, sizeof(BShare));
    TCP_Send(&x1s1, 7, 2, sizeof(BShare));
    TCP_Send(&x2s1, 7, 2, sizeof(BShare));
    TCP_Send(&x3s1, 7, 2, sizeof(BShare));
    TCP_Send(&x4s1, 6, 2, sizeof(BShare));
    TCP_Send(&x5s1, 6, 2, sizeof(BShare));
    TCP_Send(&x6s1, 6, 2, sizeof(BShare));
  }
  else { //P2 and P3
    TCP_Recv(&x1s1, 7, 0, sizeof(BShare));
    TCP_Recv(&x2s1, 7, 0, sizeof(BShare));
    TCP_Recv(&x3s1, 7, 0, sizeof(BShare));
    TCP_Recv(&x4s1, 6, 0, sizeof(BShare));
    TCP_Recv(&x5s1, 6, 0, sizeof(BShare));
    TCP_Recv(&x6s1, 6, 0, sizeof(BShare));
    TCP_Recv(&x1s2, 7, 0, sizeof(BShare));
    TCP_Recv(&x2s2, 7, 0, sizeof(BShare));
    TCP_Recv(&x3s2, 7, 0, sizeof(BShare));
    TCP_Recv(&x4s2, 6, 0, sizeof(BShare));
    TCP_Recv(&x5s2, 6, 0, sizeof(BShare));
    TCP_Recv(&x6s2, 6, 0, sizeof(BShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  Data res;
  // Test in-bulk ANDs
  and_b_all(x1s1, x1s2, 7);
  res = open_b(x1s1[0]);
  #if DEBUG
  if (rank==0) {
    printf("[%d] Result 1 (open): %lld\n", rank, res);
  }
  #endif
  if (rank==0) {
    assert(res==0);
  }
  and_b_all(x2s1, x2s2, 7);
  res = open_b(x2s1[0]);
  #if DEBUG
  if (rank==0) {
    printf("[%d] Result 2 (open): %lld\n", rank, res);
  }
  #endif
  if (rank==0) {
    assert(res==1);
  }
  and_b_all(x3s1, x3s2, 7);
  res = open_b(x3s1[0]);
  #if DEBUG
  if (rank==0) {
  printf("[%d] Result 3 (open): %lld\n", rank, res);
  }
  #endif
  if (rank==0) {
    assert(res==0);
  }
  and_b_all(x4s1, x4s2, 6);
  res = open_b(x4s1[0]);
  #if DEBUG
  if (rank==0) {
  printf("[%d] Result 4 (open): %lld\n", rank, res);
  }
  #endif
  if (rank==0) {
    assert(res==0);
  }
  and_b_all(x5s1, x5s2, 6);
  res = open_b(x5s1[0]);
  #if DEBUG
  if (rank==0) {
  printf("[%d] Result 5 (open): %lld\n", rank, res);
  }
  #endif
  if (rank==0) {
    assert(res==1);
  }
  and_b_all(x6s1, x6s2, 6);
  res = open_b(x6s1[0]);
  #if DEBUG
  if (rank==0) {
  printf("[%d] Result 6 (open): %lld\n", rank, res);
  }
  #endif
  if (rank==0) {
    assert(res==0);
  }

  if (rank==0) {
    printf("TEST AND_B_ALL(): OK.\n");
  }

  // tear down communication
  // MPI_Finalize();
  return 0;
}
