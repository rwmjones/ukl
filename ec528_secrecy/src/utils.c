#include "utils.h"

#define SHARE_TAG 193

// Initializes the BShare tables and distributed random shares across parties
void init_tables(BShareTable *t1, BShareTable *t2) {
  // Get ranks
  const int rank = get_rank();
  const int pred = get_pred();
  const int succ = get_succ();
  // Table dimensions
  int ROWS1 = t1->numRows;
  int COLS1 = t1->numCols/2;
  int ROWS2 = t2->numRows;
  int COLS2 = t2->numCols/2;
  // Initialize
  if (rank == 0) { //P1
    // Initialize input data and shares
    Table r1, r2;
    generate_random_table(&r1, ROWS1, COLS1);
    generate_random_table(&r2, ROWS2, COLS2);

    // t1 Bshare tables for P2, P3 (local to P1)
    BShareTable t12 = {-1, 1, ROWS1, 2*COLS1, 1};
    allocate_bool_shares_table(&t12);
    BShareTable t13 = {-1, 2, ROWS1, 2*COLS1, 1};
    allocate_bool_shares_table(&t13);

    // t2 Bshare tables for P2, P3 (local to P1)
    BShareTable t22 = {-1, 1, ROWS2, 2*COLS2, 2};
    allocate_bool_shares_table(&t22);
    BShareTable t23 = {-1, 2, ROWS2, 2*COLS2, 2};
    allocate_bool_shares_table(&t23);

    init_sharing();

    // Generate boolean shares for r1
    generate_bool_share_tables(&r1, t1, &t12, &t13);
    // Generate boolean shares for r2
    generate_bool_share_tables(&r2, t2, &t22, &t23);

    // //Send shares to P2
    TCP_Send(&(t12.contents[0][0]), ROWS1*2*COLS1, 1, sizeof(BShare));
    TCP_Send(&(t22.contents[0][0]), ROWS2*2*COLS2, 1, sizeof(BShare));

    //Send shares to P3

    TCP_Send(&(t13.contents[0][0]), ROWS1*2*COLS1, 2, sizeof(BShare));
    TCP_Send(&(t23.contents[0][0]), ROWS2*2*COLS2, 2, sizeof(BShare));

    // free temp tables
    free(r1.contents);
    free(t12.contents);
    free(t13.contents);
    free(r2.contents);
    free(t22.contents);
    free(t23.contents);

  }
  else if (rank == 1) { //P2

    TCP_Recv(&(t1->contents[0][0]), ROWS1*2*COLS1, 0, sizeof(BShare));
    TCP_Recv(&(t2->contents[0][0]), ROWS2*2*COLS2, 0, sizeof(BShare));

  }
  else { //P3

    TCP_Recv(&(t1->contents[0][0]), ROWS1*2*COLS1, 0, sizeof(BShare));
    TCP_Recv(&(t2->contents[0][0]), ROWS2*2*COLS2, 0, sizeof(BShare));
  }
  //exchange seeds
  exchange_rsz_seeds(succ, pred);
}

// Allocate 2D array
void allocate_int_shares_table(AShareTable *table) {
  AShare **contents;
  contents = malloc(table->numRows * sizeof(AShare*));
  for (int i=0;i<table->numRows;i++) {
    contents[i] = malloc(table->numCols * sizeof(AShare));
  }
  table->contents = contents;
}

// Allocate 2D array
Data** allocate_table(long rows, long columns) {
  Data **contents;
  contents = malloc(rows * sizeof(Data*));
  for (int i=0;i<rows;i++) {
    contents[i] = malloc(columns * sizeof(Data));
  }
  return contents;
}

// Populate 2D array
void populate_shares_table(AShareTable* party_shares, AShare* all_shares,
                           int columns, int* share_indices) {
  for (int i=0;i<party_shares->numRows;i++) {
      for (int j=0;j<party_shares->numCols;j++) {
          party_shares->contents[i][j] = *(
            all_shares + (i * columns) + share_indices[j]
          );
      }
  }
}

// Generates random table
void generate_random_table(Table *table, int rows, int columns) {
  Data** contents = allocate_table(rows, columns);
  for (int i=0; i<rows; i++) {
    for (int j=0;j<columns; j++) {
      randombytes_buf(&contents[i][j], sizeof(Data));
    }
  }
  table->numRows = rows;
  table->numCols = columns;
  table->contents = contents;
}

void allocate_bool_shares_table(BShareTable *table) {
  int length = sizeof(BShare*) * table->numRows +
               sizeof(BShare) * table->numRows * table->numCols;
  table->contents = (BShare**) malloc(length);
  assert(table->contents != NULL);
  BShare* ptr = (BShare *)(table->contents + table->numRows);
  for(int i = 0; i < table->numRows; i++)
    table->contents[i] = (ptr + table->numCols * i);
}

void allocate_a_shares_table(AShareTable *table) {
  int length = sizeof(AShare*) * table->numRows +
               sizeof(AShare) * table->numRows * table->numCols;
  table->contents = (AShare**) malloc(length);
  assert(table->contents != NULL);
  AShare* ptr = (AShare *)(table->contents + table->numRows);
  for(int i = 0; i < table->numRows; i++)
    table->contents[i] = (ptr + table->numCols * i);
}

// Print shares table to standard output
void print_shares_table(AShareTable* party_shares) {
  printf("Data owner: %d\n", party_shares->ownerId);
  printf("Party: %d\n", party_shares->partyId);
  printf("Rows: %d\n", party_shares->numRows);
  printf("Columns: %d\n", party_shares->numCols);
  printf("Relation Id: %d\n", party_shares->relationId);
  printf("Contents: \n");
  for (int i=0; i<party_shares->numRows; i++) {
      for (int j=0; j<party_shares->numCols; j++) {
          printf("%lld ", party_shares->contents[i][j]);
      }
      printf("\n");
  }
}

// Print shares table to standard output
void print_bool_shares_table(BShareTable* party_shares) {
  printf("Data owner: %d\n", party_shares->ownerId);
  printf("Party: %d\n", party_shares->partyId);
  printf("Rows: %d\n", party_shares->numRows);
  printf("Columns: %d\n", party_shares->numCols);
  printf("Relation Id: %d\n", party_shares->relationId);
  printf("Contents: \n");
  for (int i=0; i<party_shares->numRows; i++) {
      for (int j=0; j<party_shares->numCols; j++) {
          printf("%lld ", party_shares->contents[i][j]);
      }
      printf("\n");
  }
}

// Returns the i-th bit of the input boolean share as a boolean share itself
inline BShare get_bit(BShare s, int i) {
  return ( (s >> i) & (BShare) 1 );
}

// Returns the i-th bit of the input boolean share as a boolean share itself
inline BShare get_bit_u8(char s, char i) {
  return ( (s >> i) & (BShare) 1 );
}

inline BShare get_bit_u(unsigned long long s, int i) {
  return ( (s >> i) & 1 );
}

// Returns a boolean share equivalent to the given single-bit boolean
inline BShare to_bshare(const BitShare s) {
  return ((BShare) s) & (~(BShare) 0);
}

// Returns a new boolean share with the i LSBs set to zero
inline BShare unset_lsbs(const BShare s, int i) {
  return ((s >> i) << i);
}

void print_binary(const BShare s) {
  int length = sizeof(BShare)*8;
  char bits[length+1];
  for (int i=0; i<length; i++) {
    if (get_bit(s, i) == 1) {
      bits[length-i-1] = '1';
    }
    else {
      bits[length-i-1] = '0';
    }
  }
  bits[length] = '\0';
  printf("Share %lld in binary format: %s\n", s, bits);
}

BShare** allocate_2D_table(long numRows, long numCols) {
  long length = sizeof(BShare*) * numRows + sizeof(BShare) * numRows * numCols;
  BShare **contents = (BShare**) malloc(length);
  BShare* ptr = (BShare *)(contents + numRows);
  for(long i = 0; i < numRows; i++)
    contents[i] = (ptr + numCols * i);
  assert(contents != NULL);
  return contents;
}

BitShare** allocate_2D_bit_table(long numRows, long numCols) {
  long length = sizeof(BitShare*) * numRows +
                  sizeof(BitShare) * numRows * numCols;
  BitShare **contents = (BitShare**) malloc(length);
  BitShare* ptr = (BitShare *)(contents + numRows);
  for(long i = 0; i < numRows; i++)
    contents[i] = (ptr + numCols * i);
  assert(contents != NULL);
  return contents;
}

// Used by greater() and related methods
char** allocate_2D_byte_array(long numRows, long numCols) {
  long length = sizeof(char*) * numRows + sizeof(char) * numRows * numCols;
  char **contents = (char**) malloc(length);
  char* ptr = (char *)(contents + numRows);
  for(long i = 0; i < numRows; i++)
    contents[i] = (ptr + numCols * i);
  assert(contents != NULL);
  return contents;
}

// Used by greater() and related methods
Data** allocate_2D_data_table(long numRows, long numCols) {
  long length = sizeof(Data*) * numRows + sizeof(Data) * numRows * numCols;
  Data **contents = (Data**) malloc(length);
  Data* ptr = (Data *)(contents + numRows);
  for(long i = 0; i < numRows; i++)
    contents[i] = (ptr + numCols * i);
  assert(contents != NULL);
  return contents;
}

int** allocate_int_2D_table(long numRows, long numCols) {
  long length = sizeof(int*) * numRows + sizeof(int) * numRows * numCols;
  int **contents = (int**) malloc(length);
  int* ptr = (int *)(contents + numRows);
  for(long i = 0; i < numRows; i++)
    contents[i] = (ptr + numCols * i);
  assert(contents != NULL);
  return contents;
}

void generate_and_share_random_data(int rank, BShare *r1s1, BShare *r1s2, long ROWS) {
  if (rank == 0) { //P1
    // Initialize input data and shares
    Data *r1;
    r1 = malloc(ROWS*sizeof(Data));
    BShare *r1s3;
    r1s3 = malloc(ROWS*sizeof(BShare));

    // generate random data
    for (long i=0; i<ROWS; i++) {
      r1[i] = random();
    }

    init_sharing();

    // generate r1  shares
    for (long i=0; i<ROWS; i++) {
      generate_bool_share(r1[i], &r1s1[i], &r1s2[i], &r1s3[i]);
    }

    //Send shares to P2

    TCP_Send(r1s2, ROWS, 1, sizeof(BShare));
    TCP_Send(r1s3, ROWS, 1, sizeof(BShare));
    //Send shares to P3

    TCP_Send(r1s3, ROWS, 2, sizeof(BShare));
    TCP_Send(r1s1, ROWS, 2, sizeof(BShare));

    // free temp tables
    free(r1);
    free(r1s3);
  }
  else if (rank == 1) { //P2

    TCP_Recv(r1s1, ROWS, 0, sizeof(BShare));
    TCP_Recv(r1s2, ROWS, 0, sizeof(BShare));  
  }
  else { //P3

    TCP_Recv(r1s1, ROWS, 0, sizeof(BShare));
    TCP_Recv(r1s2, ROWS, 0, sizeof(BShare));
  }
}
