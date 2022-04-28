#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "test-utils.h"

#define DEBUG 1

struct shares {
  AShare s1;
  AShare s2;
  AShare s3;
};

void test_simple_query(struct shares, struct shares, struct shares);

int main(void) {

  struct shares r = {4, 4, -8}; // r = 0
  struct shares r2 = {-1, 6, -5}; // r2 = 0
  struct shares w = {7, 9, -14}; // w = 2

  // test simple SQL query
  test_simple_query(w, r, r2);

  return 0;
}

/*******************************************************************
 * Evaluates simple SQL query:
 *
 *  SELECT id
 *  FROM r1, r2 on r1.id = r2.id
 *  WHERE r1.att = 1 AND r2.att = 2
 *
 *     r1            r2
 *  id | att      id | att
 *  1     1       1     2
 *  2     1       2     2
 *  3     7       3     2
 *  4     1       5     3
 *
 *  Query result:
 *      id
 *      1
 *      2
 *
 *            r1 shares
 *      id        |       att
 *  1 = -2 +1 +2      1 = -2 +1 +2
 *  2 = 2 +0 +0       1 = 15 -13 -1
 *  3 = 7 -2 -1       7 = 9 -8 +6
 *  4 = 14 -10 +0     1 = 1 -1 +1
 *
 *            r2 shares
 *      id        |       att
 *  1 = -2 +1 +2      2 = 2 +1 -1
 *  2 = 2 +0 +0       2 = 15 -13 +0
 *  3 = 7 -3 -1       2 = 9 -8 +1
 *  5 = 14 -9 +0      3 = 1 +1 +1
 * *****************************************************************/

void test_simple_query(struct shares w, struct shares r, struct shares r2) {

  // Arithmetic shares
  struct shares r1id1 = {-2, 1, 2};
  struct shares r1id2 = {2, 0, 0};
  struct shares r1id3 = {7, -2, -1};
  struct shares r1id4 = {14, -10, 0};

  struct shares r1att1 = {-2, 1, 2};
  struct shares r1att2 = {15, -13, -1};
  struct shares r1att3 = {9, -8, 6};
  struct shares r1att4 = {1, -1, 1};

  struct shares r2id1 = {-2, 1, 2};
  struct shares r2id2 = {2, 0, 0};
  struct shares r2id3 = {7, -3, -1};
  struct shares r2id4 = {14, -9, 0};

  struct shares r2att1 = {2, 1, -1};
  struct shares r2att2 = {15, -13, 0};
  struct shares r2att3 = {9, -8, 1};
  struct shares r2att4 = {1, 1, 1};

  AShare r1_all[4][6] = {
    {r1id1.s1, r1id1.s2, r1id1.s3, r1att1.s1, r1att1.s2, r1att1.s3},
    {r1id2.s1, r1id2.s2, r1id2.s3, r1att2.s1, r1att2.s2, r1att2.s3},
    {r1id3.s1, r1id3.s2, r1id3.s3, r1att3.s1, r1att3.s2, r1att3.s3},
    {r1id4.s1, r1id4.s2, r1id4.s3, r1att4.s1, r1att4.s2, r1att4.s3}
  };

  // Assign shares to parties
  AShare **p1_shares = allocate_shares_table(4,4);
  AShareTable p1_r1 = {0,1,4,4,0,p1_shares};
  AShare **p2_shares = allocate_shares_table(4,4);
  AShareTable p2_r1 = {0,2,4,4,0,p2_shares};
  AShare **p3_shares = allocate_shares_table(4,4);
  AShareTable p3_r1 = {0,3,4,4,0,p3_shares};
  // Column indices in r1_all
  int p1_indices[4] = {0,1,3,4};  // 1st and 2nd share of each attribute
  int p2_indices[4] = {1,2,4,5};  // 2nd and 3rd share of each attribute
  int p3_indices[4] = {2,0,5,3};  // 3rd and 1st share of each attribute
  // Populate tables
  populate_shares_table(&p1_r1, (AShare*) r1_all, 6, p1_indices);
  populate_shares_table(&p2_r1, (AShare*) r1_all, 6, p2_indices);
  populate_shares_table(&p3_r1, (AShare*) r1_all, 6, p3_indices);

  #if DEBUG
  printf("Priting share tables for r1...\n");

  print_shares_table(&p1_r1);
  print_shares_table(&p2_r1);
  print_shares_table(&p3_r1);

  #endif

  // Repeat for r2_all
  AShare r2_all[4][6] = {
    {r1id1.s1, r2id1.s2, r2id1.s3, r2att1.s1, r2att1.s2, r2att1.s3},
    {r2id2.s1, r2id2.s2, r2id2.s3, r2att2.s1, r2att2.s2, r2att2.s3},
    {r2id3.s1, r2id3.s2, r2id3.s3, r2att3.s1, r2att3.s2, r2att3.s3},
    {r2id4.s1, r2id4.s2, r2id4.s3, r2att4.s1, r2att4.s2, r2att4.s3}
  };

  // Assign shares to parties
  AShare **p1_shares2 = allocate_shares_table(4,4);
  AShareTable p1_r2 = {0,1,4,4,1,p1_shares2};
  AShare **p2_shares2 = allocate_shares_table(4,4);
  AShareTable p2_r2 = {0,2,4,4,1,p2_shares2};
  AShare **p3_shares2 = allocate_shares_table(4,4);
  AShareTable p3_r2 = {0,3,4,4,1,p3_shares2};
  // Populate tables
  populate_shares_table(&p1_r2, (AShare*) r2_all, 6, p1_indices);
  populate_shares_table(&p2_r2, (AShare*) r2_all, 6, p2_indices);
  populate_shares_table(&p3_r2, (AShare*) r2_all, 6, p3_indices);

  #if DEBUG
  printf("Priting share tables for r2...\n");

  print_shares_table(&p1_r2);
  print_shares_table(&p2_r2);
  print_shares_table(&p3_r2);

  #endif

  // P1: Apply selection on r1 (att = 1)
  AShare selection_p1_r1[4];
  for (int i=0;i<p1_r1.numRows;i++) {
    selection_p1_r1[i] = eq(p1_r1.contents[i][2], // 1st share of att
                            p1_r1.contents[i][3], // 2nd share of att
                            1, // 1st share of constant '1'
                            0, // 2nd share of constant '1'
                            w.s1, // 1st share of random w
                            w.s2, // 2nd share of random w
                            r.s1); // 1st share of random r
  }

  #if DEBUG
  printf("Selection_p1_r1:\n");
  for (int i=0;i<p1_r1.numRows;i++)  {
    printf("%lld ",selection_p1_r1[i]);
  }
  printf("\n");
  #endif

  // P2: Apply selection on r1 (att = 1)
  AShare selection_p2_r1[4];
  for (int i=0;i<p2_r1.numRows;i++) {
    selection_p2_r1[i] = eq(p2_r1.contents[i][2], // 1st share of att
                            p2_r1.contents[i][3], // 2nd share of att
                            0, // 2nd share of constant '1'
                            0, // 3rd share of constant '1'
                            w.s2, // 2nd share of random w
                            w.s3, // 3rd share of random w
                            r.s2); // 2nd share of random r
  }

  #if DEBUG
  printf("Selection_p2_r1:\n");
  for (int i=0;i<p2_r1.numRows;i++)  {
    printf("%lld ",selection_p2_r1[i]);
  }
  printf("\n");
  #endif

  // P3: Apply selection on r1 (att = 1)
  AShare selection_p3_r1[4];
  for (int i=0;i<p3_r1.numRows;i++) {
    selection_p3_r1[i] = eq(p3_r1.contents[i][2], // 1st share of att
                            p3_r1.contents[i][3], // 2nd share of att
                            0, // 3rd share of constant '1'
                            1, // 1st share of constant '1'
                            w.s3, // 3rd share of random w
                            w.s1, // 1st share of random w
                            r.s3); // 3rd share of random r
  }

  #if DEBUG
  printf("Selection_p3_r1:\n");
  for (int i=0;i<p3_r1.numRows;i++)  {
    printf("%lld ",selection_p3_r1[i]);
  }
  printf("\n");

  printf("Rows selected from r1: \n");
  for (int i=0;i<4;i++) {
    AShare res = selection_p1_r1[i] + selection_p2_r1[i] + selection_p3_r1[i];
    printf("%d\n", res == 0);
  }
  #endif

  // P1: Apply selection on r2 (att = 2)
  AShare selection_p1_r2[4];
  for (int i=0;i<p1_r2.numRows;i++) {
    selection_p1_r2[i] = eq(p1_r2.contents[i][2], // 1st share of att
                            p1_r2.contents[i][3], // 2nd share of att
                            2, // 1st share of constant '1'
                            0, // 2nd share of constant '1'
                            w.s1, // 1st share of random w
                            w.s2, // 2nd share of random w
                            r.s1); // 1st share of random r
  }

  #if DEBUG
  printf("Selection_p1_r2:\n");
  for (int i=0;i<p1_r2.numRows;i++)  {
    printf("%lld ",selection_p1_r2[i]);
  }
  printf("\n");
  #endif

  // P2: Apply selection on r2 (att = 2)
  AShare selection_p2_r2[4];
  for (int i=0;i<p2_r2.numRows;i++) {
    selection_p2_r2[i] = eq(p2_r2.contents[i][2], // 1st share of att
                            p2_r2.contents[i][3], // 2nd share of att
                            0, // 2nd share of constant '1'
                            0, // 3rd share of constant '1'
                            w.s2, // 2nd share of random w
                            w.s3, // 3rd share of random w
                            r.s2); // 2nd share of random r
  }

  #if DEBUG
  printf("Selection_p2_r2:\n");
  for (int i=0;i<p2_r2.numRows;i++)  {
    printf("%lld ",selection_p2_r2[i]);
  }
  printf("\n");
  #endif

  // P3: Apply selection on r2 (att = 2)
  AShare selection_p3_r2[4];
  for (int i=0;i<p3_r2.numRows;i++) {
    selection_p3_r2[i] = eq(p3_r2.contents[i][2], // 1st share of att
                            p3_r2.contents[i][3], // 2nd share of att
                            0, // 3rd share of constant '1'
                            2, // 1st share of constant '1'
                            w.s3, // 3rd share of random w
                            w.s1, // 1st share of random w
                            r.s3); // 3rd share of random r
  }

  #if DEBUG
  printf("Selection_p3_r2:\n");
  for (int i=0;i<p3_r2.numRows;i++)  {
    printf("%lld ",selection_p3_r2[i]);
  }
  printf("\n");

  printf("Rows selected from r2: \n");
  for (int i=0;i<4;i++) {
    AShare res = selection_p1_r2[i] + selection_p2_r2[i] + selection_p3_r2[i];
    printf("%d\n", res == 0);
  }
  #endif

  /** 1st phase: Each party computes its join predicate share
   * and the conjuction of the selection predicates **/

  // join equality results
  AShare z1[p1_r1.numRows*p1_r2.numRows];
  AShare z2[p2_r1.numRows*p2_r2.numRows];
  AShare z3[p3_r1.numRows*p3_r2.numRows];

  // selection results
  AShare selection_p1[p1_r1.numRows*p1_r2.numRows];
  AShare selection_p2[p2_r1.numRows*p2_r2.numRows];
  AShare selection_p3[p3_r1.numRows*p3_r2.numRows];
  int k = 0;

  // P1: apply the join
  for (int i=0;i<p1_r1.numRows;i++) {
    AShare left_id_1 = p1_r1.contents[i][0];
    AShare left_id_2 = p1_r1.contents[i][1];
    for (int j=0;j<p1_r2.numRows;j++) {
      AShare right_id_1 = p1_r2.contents[j][0];
      AShare right_id_2 = p1_r2.contents[j][1];
      z1[k] = eq(left_id_1, left_id_2, right_id_1, right_id_2,
                   w.s1, w.s2,r.s1);
      // Break selections
      selection_p1[k++] = and(selection_p1_r1[i], selection_p2_r1[i],
                         selection_p1_r2[j], selection_p2_r2[j],
                         r.s1, r2.s1);
    }
  }

  // P2: apply the join
  k = 0;
  for (int i=0;i<p2_r1.numRows;i++) {
    AShare left_id_1 = p2_r1.contents[i][0];
    AShare left_id_2 = p2_r1.contents[i][1];
    for (int j=0;j<p2_r2.numRows;j++) {
      AShare right_id_1 = p2_r2.contents[j][0];
      AShare right_id_2 = p2_r2.contents[j][1];
      z2[k] = eq(left_id_1, left_id_2, right_id_1, right_id_2,
                   w.s2, w.s3,r.s2);
      // Break selections
      selection_p2[k++] = and(selection_p2_r1[i], selection_p3_r1[i],
                         selection_p2_r2[j], selection_p3_r2[j],
                         r.s2, r2.s2);
    }
  }

  // P2: apply the join
  k = 0;
  for (int i=0;i<p3_r1.numRows;i++) {
    AShare left_id_1 = p3_r1.contents[i][0];
    AShare left_id_2 = p3_r1.contents[i][1];
    for (int j=0;j<p3_r2.numRows;j++) {
      AShare right_id_1 = p3_r2.contents[j][0];
      AShare right_id_2 = p3_r2.contents[j][1];
      z3[k] = eq(left_id_1, left_id_2, right_id_1, right_id_2,
                   w.s3, w.s1,r.s3);
      // Break selections
      selection_p3[k++] = and(selection_p3_r1[i], selection_p1_r1[i],
                         selection_p3_r2[j], selection_p1_r2[j],
                         r.s3, r2.s3);
    }
  }

  /** 2nd phase: after receiving the selection results from the predecessor party,
   * each party now computes the final join result as the conjunction of the selection
   * and join predicates **/

  // results
  AShare out1[p1_r1.numRows*p1_r2.numRows];
  AShare out2[p2_r1.numRows*p2_r2.numRows];
  AShare out3[p3_r1.numRows*p3_r2.numRows];

  // P1: compute the output result
  for (int i=0; i<p1_r1.numRows*p1_r2.numRows; i++) {
    out1[i] = and(z1[i], z2[i], selection_p1[i], selection_p2[i], r.s1, r2.s1);
  }

  // P2: compute the output result
  for (int i=0; i<p2_r1.numRows*p2_r2.numRows; i++) {
    out2[i] = and(z2[i], z3[i], selection_p2[i], selection_p3[i], r.s2, r2.s2);
  }

  // P3: compute the output result
  for (int i=0; i<p3_r1.numRows*p3_r2.numRows; i++) {
    out3[i] = and(z3[i], z1[i], selection_p3[i], selection_p1[i], r.s3, r2.s3);
  }

  #if DEBUG
  printf("Rows selected from join: \n");
  for (int i=0;i<16;i++) {
    AShare res = out1[i] + out2[i] + out3[i];
    if (res == 0) {
    printf("Row selected: %d\n", i);
    }
  }
  #endif

  TCP_Finalize();
}
