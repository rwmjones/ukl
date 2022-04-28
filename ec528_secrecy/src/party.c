#include "party.h"

#include <sodium.h>
#include "mpctypes.h"

#define RAND_STATE_SIZE 128
#define MSG_TAG 42

Seed seed_local = 0, seed_remote = 0;
char local_state[RAND_STATE_SIZE], remote_state[RAND_STATE_SIZE];

static void check_init_seeds(const char*);

int exchange_rsz_seeds(int succ_rank, int pred_rank) {

  // initialize random number generator
  if (sodium_init() == -1) {
        return 1;
  }

  // generate local seed
  randombytes_buf(&seed_local, sizeof(seed_local));

  // // send seed to successor
  TCP_Send(&seed_local, 1, succ_rank, sizeof(Seed));
  // // receive remote seed
  TCP_Recv(&seed_remote, 1, pred_rank, sizeof(Seed));
  // init generator states
  initstate(seed_local, local_state, RAND_STATE_SIZE);
  initstate(seed_remote, remote_state, RAND_STATE_SIZE);

  return 0;
}

WSharePair get_next_w() {
    printf("NOT IMPLEMENTED: %s\n", __func__);
    exit(EXIT_FAILURE);
  /*  WSharePair wpair;
    wpair.first = 0;
    wpair.second = 0;
    return wpair; */
}

// Generate one random arithmetic share
AShare get_next_r() {
  check_init_seeds(__func__);
  setstate(local_state);
  long long r_local = random();
  setstate(remote_state);
  return r_local - random();
}

// Generate one random binary share
BShare get_next_rb() {
  check_init_seeds(__func__);

  setstate(local_state);
  long long r_local = random();
  setstate(remote_state);
  return r_local ^ random();
}

// Generate pairs of random shares
// one local and one remote
void get_next_rb_pair_array(BShare *r1, BShare *r2, int len) {
  check_init_seeds(__func__);

  // Generate len random shares using the local seed
  setstate(local_state);
  for (int i=0; i<len; i++) {
    r1[i] = random();
  }

  // Generate len random shares using the remote seed
  setstate(remote_state);
  for (int i=0; i<len; i++) {
    r2[i] = random();
  }
}

// Generate an array of random binary shares
void get_next_rb_array(BShare *rnum, int len) {
  check_init_seeds(__func__);

  // Generate len random shares using the local seed
  setstate(local_state);
  for (int i=0; i<len; i++) {
    rnum[i] = random();
  }

  // Generate len random shares using the remote seed
  // and xor them with the corresponding local ones
  setstate(remote_state);
  for (int i=0; i<len; i++) {
    rnum[i] ^= random();
  }
}

// check if seeds have been initialized
static void check_init_seeds(const char* f) {
    if (seed_local == 0 || seed_remote == 0) {
        fprintf(stderr, "ERROR: exchange_rsz_seeds() must be called before %s\n", f);
        exit(EXIT_FAILURE);
    }
}
