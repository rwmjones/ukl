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

  BShare xs1[10], xs2[10], xs3[10], ys1[10], ys2[10], ys3[10],
         xs11[10], xs21[10], xs31[10], ys11[10], ys21[10], ys31[10],
         zs1[8][2], zs2[8][2], zs3[8][2],
         zs31[8][3], zs32[8][3], zs33[8][3];

  // Initialize input data and shares
  Data x[10] = {111, -4, -17, 2345, 999, 0, -28922, 1231241, 0, 23437};
  Data y[10] = {0, -4, -5, 123556, 999, 70, -243242, 12421421413421, 0, 78};
  Data z[8][2] = {{2, 43}, {1, 42}, {3, 42}, {15, 42}, {2, 42}, {17, 43},
                  {0, 44}, {255, 1}};
  Data z3[8][3] = {{2, 42, 3}, {1, 42, 12}, {3, 42, 133}, {15, 142, 11},
                   {2, 42, 1}, {17, 43, 43}, {1, 44, 0}, {255, 1}};

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // test cmp_swap
  #if DEBUG
    if (rank == 0) {
      printf("\n[%d] Testing cmp_swap\n", rank);
    }
  #endif
  int len = 323;    // We need 323 logical ANDs in total for each inequality
  BShare rnums[len];
  for (int i=0; i<10; i++) {
    // Populate rnums array
    // TODO: We can instead use get_next_rb() in eq_b()
    for (int j=0; j<len; j++) {
      rnums[j] = get_next_rb();
    }

    // test cmp_swap
    cmp_swap(&xs1[i], &xs2[i], &ys1[i], &ys2[i], rnums);
    // reveal the result
    Data min = open_b(xs1[i]);
    Data max = open_b(ys1[i]);

    // assert and print result
    if (rank == 0) {
      #if DEBUG
        printf("[%d] Min (open): %lld\n", rank, min);
        printf("[%d] Max (open): %lld\n", rank, max);
      #endif
      assert(min<=max);
    }
  }

  if (rank==0) {
    printf("TEST CMP_SWAP(): OK.\n");
  }

  // test cmp_swap_g
  #if DEBUG
    if (rank == 0) {
      printf("\n[%d] Testing generalized cmp_swap\n", rank);
    }
  #endif
  BShare r1[20], r2[20];
  // populate arrays
  for (int i=0, j=0; i<19; i+=2, j++) {
    r1[i] = xs11[j];
    r1[i+1] = xs21[j];
    r2[i] = ys11[j];
    r2[i+1] = ys21[j];
  }

  cmp_swap_g(r1, r2, 12, 12, 20, 1);

  // update share arrays
  BShare xs1_new[10], xs2_new[10], ys1_new[10], ys2_new[10];
  for (int i=0, j=0; i<19; i+=2, j++) {
    xs1_new[j] = r1[i];
    xs2_new[j] = r1[i+1];
    ys1_new[j] = r2[i];
    ys2_new[j] = r2[i+1];
  }

  // reveal the result
  Data res1[10], res2[10];
  open_b_array(xs1_new, 10, res1);
  open_b_array(ys1_new, 10, res2);

  // assert and print result
  if (rank == 0) {
    #if DEBUG
      printf("\n [%d] Original x:\n", rank);
      for (int i=0;i<10;i++) {
        printf("%lld ", x[i]);
      }
      printf("\n [%d] Original y:\n", rank);
      for (int i=0;i<10;i++) {
        printf("%lld ", y[i]);
      }
      printf("\n[%d] Generalized Min (open):\n", rank);
    #endif
    for (int i=0;i<10;i++) {
      #if DEBUG
        printf("%lld ", res1[i]);
      #endif
      assert(res1[i]==y[i]);
    }
    #if DEBUG
      printf("\n[%d] Generalized Max (open):\n", rank);
    #endif
    for (int i=0;i<10;i++) {
      #if DEBUG
        printf("%lld ", res2[i]);
      #endif
      assert(res2[i]==x[i]);
    }
    printf("TEST CMP_SWAP_G(): OK.\n");
  }

  // test sort
  #if DEBUG
    if (rank == 0) {
      printf("\n[%d] Testing bitonic sort\n", rank);
    }
  #endif

  BShareTable t = {-1, rank, 8, 2*2, 1};
  allocate_bool_shares_table(&t);
  // copy shares into the BShareTables
  for (int i=0; i<8; i++) {
      t.contents[i][0] = zs1[i][0];
      t.contents[i][1] = zs2[i][0];
      t.contents[i][2] = zs1[i][1];
      t.contents[i][3] = zs2[i][1];
  }
  // sort in place
  bitonic_sort(&t,0,8,0,1);

  // update share arrays
  BShare zs[16];
  for (int i=0; i<8; i++) {
    zs[2*i] = t.contents[i][0];
    zs[2*i+1] = t.contents[i][2];
  }

  // reveal the result
  Data out[16];
  open_b_array(zs, 16, out);

  if (rank==0) {
    Data srt_z[8][2] = {{0, 44}, {1, 42}, {2, 42}, {2, 43}, {3, 42}, {15, 42},
                        {17, 43}, {255, 1}};
    #if DEBUG
      printf("[%d] Sorted array (open):\n", rank);
    #endif
    for (int i=0; i<16; i+=2) {
      #if DEBUG
        printf("%lld %lld ", out[i], out[i+1]);
      #endif
      assert(out[i]==srt_z[i/2][0]);
      assert(out[i+1]==srt_z[i/2][1]);
    }
    printf("TEST SORT: OK.\n");
  }

  // test sort
  #if DEBUG
    if (rank == 0) {
      printf("\n[%d] Testing batched bitonic sort\n", rank);
    }
  #endif

  BShareTable t_b = {-1, rank, 8, 2*2, 1};
  allocate_bool_shares_table(&t_b);
  // copy shares into the BShareTables
  for (int i=0; i<8; i++) {
      t_b.contents[i][0] = zs1[i][0];
      t_b.contents[i][1] = zs2[i][0];
      t_b.contents[i][2] = zs1[i][1];
      t_b.contents[i][3] = zs2[i][1];
  }

  // sort in place
  unsigned sort_att[1] = {0};
  bool asc[1] = {1};
  bitonic_sort_batch(&t_b,sort_att,1,asc, 2);

  // update share arrays
  for (int i=0; i<8; i++) {
    zs[2*i] = t_b.contents[i][0];
    zs[2*i+1] = t_b.contents[i][2];
  }

  // reveal the result
  Data out_b[16];
  open_b_array(zs, 16, out_b);

  if (rank==0) {
    Data srt_z[8][2] = {{0, 44}, {1, 42}, {2, 42}, {2, 43}, {3, 42}, {15, 42},
                        {17, 43}, {255, 1}};
    #if DEBUG
      printf("[%d] Sorted array (open):\n", rank);
    #endif
    for (int i=0; i<16; i+=2) {
      #if DEBUG
        printf("%lld %lld ", out_b[i], out_b[i+1]);
      #endif
      assert(out_b[i]==srt_z[i/2][0]);
      assert(out_b[i+1]==srt_z[i/2][1]);
    }
    printf("TEST BATCH SORT: OK.\n");
  }

  // test sort
  #if DEBUG
    if (rank == 0) {
      printf("\n[%d] Testing batched bitonic sort with 2 sort attributes\n",
             rank);
    }
  #endif

  BShareTable t_b2 = {-1, rank, 8, 2*2, 1};
  allocate_bool_shares_table(&t_b2);
  // copy shares into the BShareTables
  for (int i=0; i<8; i++) {
      t_b2.contents[i][0] = zs1[i][0];
      t_b2.contents[i][1] = zs2[i][0];
      t_b2.contents[i][2] = zs1[i][1];
      t_b2.contents[i][3] = zs2[i][1];
  }

  // sort in place
  unsigned sort_att2[2] = {0,2};
  bool asc2[2] = {true,false};
  bitonic_sort_batch(&t_b2, sort_att2, 2, asc2, 4);

  // update share arrays
  for (int i=0; i<8; i++) {
    zs[2*i] = t_b2.contents[i][0];
    zs[2*i+1] = t_b2.contents[i][2];
  }

  // reveal the result
  Data out_b2[16];
  open_b_array(zs, 16, out_b2);

  if (rank==0) {
    Data srt_z[8][2] = {{0, 44}, {1, 42}, {2, 43}, {2, 42}, {3, 42}, {15, 42},
                        {17, 43}, {255, 1}};
    #if DEBUG
      printf("[%d] Sorted array (open):\n", rank);
    #endif
    for (int i=0; i<16; i+=2) {
      #if DEBUG
        printf("%lld %lld ", out_b2[i], out_b2[i+1]);
      #endif
      assert(out_b2[i]==srt_z[i/2][0]);
      assert(out_b2[i+1]==srt_z[i/2][1]);
    }
    printf("TEST BATCH SORT (2 ATTRIBUTES): OK.\n");
  }

  // test sort
  #if DEBUG
    if (rank == 0) {
      printf("\n[%d] Testing batched bitonic sort with 3 sort attributes\n",
             rank);
    }
  #endif

  BShareTable t_b3 = {-1, rank, 8, 3*2, 1};
  allocate_bool_shares_table(&t_b3);
  // copy shares into the BShareTables
  for (int i=0; i<8; i++) {
      t_b3.contents[i][0] = zs31[i][0];
      t_b3.contents[i][1] = zs32[i][0];
      t_b3.contents[i][2] = zs31[i][1];
      t_b3.contents[i][3] = zs32[i][1];
      t_b3.contents[i][4] = zs31[i][2];
      t_b3.contents[i][5] = zs32[i][2];
  }

  // sort in place
  unsigned sort_att3[3] = {0,2,4};
  bool asc3[3] = {true,false,true};
  bitonic_sort_batch(&t_b3, sort_att3, 3, asc3, 4);

  // update share arrays
  BShare zz3[24];
  for (int i=0; i<8; i++) {
    zz3[3*i] = t_b3.contents[i][0];
    zz3[3*i+1] = t_b3.contents[i][2];
    zz3[3*i+2] = t_b3.contents[i][4];
  }

  // reveal the result
  Data out_b3[24];
  open_b_array(zz3, 24, out_b3);

  if (rank==0) {
    // Data z3[8][3] = {{2, 42, 3}, {1, 42, 12}, {3, 42, 133}, {15, 142, 11},
    //                  {2, 42, 1}, {17, 43, 43}, {1, 44, 0}, {255, 1}};
    Data srt_z3[8][3] = {{1, 44, 0}, {1, 42, 12}, {2, 42, 1}, {2, 42, 3},
                         {3, 42, 133}, {15, 142, 11}, {17, 43, 43}, {255, 1}};
    #if DEBUG
      printf("[%d] Sorted array (open):\n", rank);
    #endif
    for (int i=0; i<24; i+=3) {
      #if DEBUG
        printf("%lld %lld %lld  ", out_b3[i], out_b3[i+1], out_b3[i+2]);
      #endif
      assert(out_b3[i]==srt_z3[i/3][0]);
      assert(out_b3[i+1]==srt_z3[i/3][1]);
      assert(out_b3[i+2]==srt_z3[i/3][2]);
    }
    printf("TEST BATCH SORT (3 ATTRIBUTES): OK.\n");
  }
  // tear down communication
  TCP_Finalize();
  return 0;
}
