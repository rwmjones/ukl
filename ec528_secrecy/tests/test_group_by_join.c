#include <stdio.h>
#include <assert.h>

#include "test-utils.h"

#define DEBUG 0
#define ROWS1 7
#define COLS1 4
#define ROWS2 6
#define COLS2 2

int main(int argc, char** argv) {

  // initialize communication
  init(argc, argv);

  // Number elements from the left input per batch
  int batch_size = 3;

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  BShare rb_left[ROWS1], rb_right[ROWS2*batch_size];
  AShare ra_left[ROWS1], ra_right[ROWS2*batch_size];

  // r1: left (SSN, PostCode), r2: right (SSN, score)
  BShare r1s1[ROWS1][COLS1], r1s2[ROWS1][COLS1], r1s3[ROWS1][COLS1],
         r2s1[ROWS2][COLS2], r2s2[ROWS2][COLS2], r2s3[ROWS2][COLS2];

  if (rank == 0) { //P1

    // Initialize input data and shares
    Data r1[ROWS1][COLS1] = {{1, 10, 0, 1}, {2, 10, 0, 1}, {4, 10, 0, 1},
                             {3, 11, 0, 1}, {5, 12, 0, 1},
                             {6, 12, 0, 1}, {2, 12, 0, 1}};
    Data r2[ROWS2][COLS2] = {{1, 10}, {2, 20}, {3, 10}, {4, -5}, {5, 5},
                              {6, 0}};

    init_sharing();

    // generate r1 shares
    for (int i=0; i<ROWS1; i++) {
      for (int j=0; j<COLS1-2; j++) {
        generate_bool_share(r1[i][j], &r1s1[i][j], &r1s2[i][j], &r1s3[i][j]);
      }
    }
    // Last two columns (avg_score, count) are arithmetic shares
    for (int i=0; i<ROWS1; i++) {
      for (int j=2; j<COLS1; j++) {
        generate_int_share(r1[i][j], &r1s1[i][j], &r1s2[i][j], &r1s3[i][j]);
      }
    }

    // generate r2 shares
    for (int i=0; i<ROWS2; i++) {
      for (int j=0; j<COLS2-1; j++) {
        generate_bool_share(r2[i][j], &r2s1[i][j], &r2s2[i][j], &r2s3[i][j]);
      }
    }

    // the score attribute is an arithmetic share AShare
    for (int i=0; i<ROWS2; i++) {
      for (int j=1; j<COLS2; j++) {
        generate_int_share(r2[i][j], &r2s1[i][j], &r2s2[i][j], &r2s3[i][j]);
      }
    }

    //Send shares to P2
    TCP_Send(&r1s2[0][0], ROWS1 * COLS1, 1, sizeof(BShare));
    TCP_Send(&r2s2[0][0], ROWS2 * COLS2, 1, sizeof(BShare));
    TCP_Send(&r1s3[0][0], ROWS1 * COLS1, 1, sizeof(BShare));
    TCP_Send(&r2s3[0][0], ROWS2 * COLS2, 1, sizeof(BShare));
    //Send shares to P3
    TCP_Send(&r1s3, ROWS1 * COLS1, 2, sizeof(BShare));
    TCP_Send(&r2s3, ROWS2 * COLS2, 2, sizeof(BShare));
    TCP_Send(&r1s1, ROWS1 * COLS1, 2, sizeof(BShare));
    TCP_Send(&r2s1, ROWS2 * COLS2, 2, sizeof(BShare));


    BShare rb2_left[ROWS1], rb3_left[ROWS1], rb2_right[ROWS2*batch_size], rb3_right[ROWS2*batch_size];
    AShare ra2_left[ROWS1], ra3_left[ROWS1], ra2_right[ROWS2*batch_size], ra3_right[ROWS2*batch_size];

    // Generate random bits and corresponding shares
    generate_rand_bit_shares(rb_left, ra_left, rb2_left, ra2_left, rb3_left, ra3_left, ROWS1);
    generate_rand_bit_shares(rb_right, ra_right, rb2_right, ra2_right, rb3_right, ra3_right, ROWS2*batch_size);

    // Send random bit shares
    // Send shares to P2
    TCP_Send(&rb2_left, ROWS1, 1, sizeof(BShare));
    TCP_Send(&ra2_left, ROWS1, 1, sizeof(AShare));
    TCP_Send(&rb2_right, ROWS2 * batch_size, 1, sizeof(BShare));
    TCP_Send(&ra2_right, ROWS2 * batch_size, 1, sizeof(AShare));

    // Send shares to P3
    TCP_Send(&rb3_left, ROWS1, 2, sizeof(BShare));
    TCP_Send(&ra3_left, ROWS1, 2, sizeof(AShare));
    TCP_Send(&rb3_right, ROWS2 * batch_size, 2, sizeof(BShare));
    TCP_Send(&ra3_right, ROWS2 * batch_size, 2, sizeof(AShare));
  }
  else { //P2 or P3
    TCP_Recv(&r1s1[0][0], ROWS1 * COLS1, 0, sizeof(BShare));
    TCP_Recv(&r2s1[0][0], ROWS2 * COLS2, 0, sizeof(BShare));
    TCP_Recv(&r1s2[0][0], ROWS1 * COLS1, 0, sizeof(BShare));
    TCP_Recv(&r2s2[0][0], ROWS2 * COLS2, 0, sizeof(BShare));

    TCP_Recv(&rb_left, ROWS1, 0, sizeof(BShare));
    TCP_Recv(&ra_left, ROWS1, 0, sizeof(AShare));
    TCP_Recv(&rb_right, ROWS2 * batch_size, 0, sizeof(BShare));
    TCP_Recv(&ra_right, ROWS2 * batch_size, 0, sizeof(AShare));
  }

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // test group_by_join
  #if DEBUG
    if (rank == 0) {
      printf("\nTesting group_by_join\n");
    }
  #endif

  // allocate BShare tables
  BShareTable in1 = {-1, rank, ROWS1, 2*COLS1, 1}, in2 = {-1, rank, ROWS2, 2*COLS2, 2};
  allocate_bool_shares_table(&in1);
  allocate_bool_shares_table(&in2);

  // copy shares into the BShareTables
  for (int i=0; i<ROWS1; i++) {
      in1.contents[i][0] = r1s1[i][0];
      in1.contents[i][1] = r1s2[i][0];
      in1.contents[i][2] = r1s1[i][1];
      in1.contents[i][3] = r1s2[i][1];
      in1.contents[i][4] = r1s1[i][2];
      in1.contents[i][5] = r1s2[i][2];
      in1.contents[i][6] = r1s1[i][3];
      in1.contents[i][7] = r1s2[i][3];
  }
  // relation 2
  for (int i=0; i<ROWS2; i++) {
      in2.contents[i][0] = r2s1[i][0];
      in2.contents[i][1] = r2s2[i][0];
      in2.contents[i][2] = r2s1[i][1];
      in2.contents[i][3] = r2s2[i][1];
  }

  // in1 (SSN, PostCode, sum, count), in2 (SSN, score)
  int next, end;
  for (int start=0; start<ROWS1; start+=batch_size) {
    next = start+batch_size;
    end = next <= ROWS1 ? next : ROWS1;
    group_by_join(&in1, &in2, start, end, 2, 0, 0, 2,
                  rb_left, ra_left, rb_right, ra_right,
                  4, 6);
  }

  // reveal the group_by attribute and the aggregation (sum)
  Data out[ROWS1*2], rem_out[ROWS1*2];
  for (int i=0, k=0; i<in1.numRows; i++, k+=2) {
    out[k] = in1.contents[i][2];
    out[k+1] = in1.contents[i][4];
  }
  unsigned a[1] = {1};
  unsigned b[1] = {0};
  open_mixed_array(out, ROWS1, 2, rem_out, a, 1, b, 1);

  // assert and print result
  if (rank == 0) {
    #if DEBUG
    printf("Left input (open): \n");
    #endif
    for (int i=0; i<ROWS1*2; i+=2) {
        #if DEBUG
        printf("%lld %lld\n", rem_out[i], rem_out[i+1]);
        #endif
        if (i==4) {
          assert(rem_out[i] == 10);
          assert(rem_out[i+1] == 25);
        }
        if (i==6) {
          assert(rem_out[i] == 11);
          assert(rem_out[i+1] == 10);
        }
        if (i==12) {
          assert(rem_out[i] == 12);
          assert(rem_out[i+1] == 25);
        }
    }
    printf("TEST GROUP-BY-JOIN: OK.\n");
  }

  // tear down communication
  TCP_Finalize();
  return 0;
}
