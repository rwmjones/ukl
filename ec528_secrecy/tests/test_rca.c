#include <stdio.h>
#include <assert.h>

#include "test-utils.h"
#include "limits.h"

#define DEBUG 0

int main(int argc, char** argv) {

  // initialize communication
  init(argc, argv);

  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();

  BShare xs1[10], xs2[10], xs3[10], ys1[10], ys2[10], ys3[10];

  //exchange seeds
  exchange_rsz_seeds(succ, pred);

  // test x[i] == y[i]
  Data res[10] = {35, -3, 0, -240, 693, 0, 0, 0, LLONG_MAX+1,
                    LLONG_MIN-1};
  int len = 187;  // Each boolean addition requires 187 logical ANDs
  BShare rnums[len];
  for (int i=0; i<10; i++) {
    // populate rnums array
    // TODO: We can instead use get_next_rb() in eq_b()
    for (int j=0; j<len; j++) {
      rnums[j] = get_next_rb();
    }
    // test x==y
    BShare first;
    BShare second;
    boolean_addition(xs1[i], xs2[i], ys1[i], ys2[i], &first, &second, rnums);
    // reveal the result
    Data out = open_b(first);

    // assert and print result
    if (rank == 0) {
      #if DEBUG
      printf("[%d] %d Result (open): %lld == %lld\n", rank, i, out, res[i]);
      #endif
      assert(out == res[i]);
    }
  }

  // test boolean_addition_batch
  BShare res_batched[10];
  Data res_out[10];
  boolean_addition_batch(xs1, xs2, ys1, ys2, res_batched, 10);
  open_b_array(res_batched, 10, res_out);
  if (rank == 0) {
    for (int i=0; i<10; i++) {
      #if DEBUG
      printf("[%d] %d Result-batched (open): %lld == %lld\n", rank, i, res_out[i], res[i]);
      #endif
      assert(res_out[i] == res[i]);
    }
  }


  if (rank == 0) {
    printf("TEST RIPPLE CARRY ADDER: OK.\n");
  }
  // tear down communication
  TCP_Finalize();
  return 0;
}
