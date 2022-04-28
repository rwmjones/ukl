#include "comm.h"
#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <getopt.h>

#define NUM_PARTIES 3
#define XCHANGE_MSG_TAG 7
#define OPEN_MSG_TAG 777
#define DEFAULT_PORT 8000

extern char *optarg;
extern int optind, opterr, optopt;

struct secrecy_config config;

static void check_init(const char *f);

const static char *opt_str = "r:c:p:i:h";

const static struct option opts[] = {
    { "rank",    required_argument, NULL, 'r' },
    { "count",   required_argument, NULL, 'c' },
    { "port",    required_argument, NULL, 'p' },
    { "ips",     required_argument, NULL, 'i' },
    { "help",    no_argument,       NULL, 'h' },
    { NULL,      0,                 NULL, 0 }
};

static void print_usage(const char *name)
{
    printf("Usage: %s <opts>\n", name);
    printf("<opts>:\n");
    printf("    -r|--rank     The rank of this node (from 0 to parties - 1)\n");
    printf("    -c|--count    The count of parties participating\n");
    printf("    -p|--port     The to use for internode communication, defaults to 8000\n");
    printf("    -i|--ips      Comma delimited list of ip addresses in rank order\n");
    printf("    -h|--help     Print this message\n");
};

static int parse_opts(int argc, char **argv)
{
    int c;
    char *haystack = NULL;
    unsigned int i;

    // Set the port here, if the user specified a value this will be overwritten
    config.port = DEFAULT_PORT;
    int opt_index = 0;
    while (1)
    {
        c = getopt_long(argc, argv, opt_str, opts, &opt_index);
        if (c == -1)
            break;

        switch (c)
        {
        case 'r':
            config.rank = atoi(optarg);
            break;

        case 'c':
            config.num_parties = atoi(optarg);
            break;

        case 'p':
            config.port = atoi(optarg);
            break;

        case 'i':
            if (optarg == NULL)
            {
                printf("Missing argument to --ips switch\n");
                print_usage(argv[0]);
                return -1;
            }

            haystack = optarg;
            break;

        case 'h':
        case '?':
            print_usage(argv[0]);
            return -1;

        default:
            printf("Unknown option -%o\n", c);
            print_usage(argv[0]);
            return -1;
        }
    }

    if (haystack != NULL)
    {
        config.ip_list = calloc(config.num_parties, sizeof(char*));
        if (config.ip_list == NULL)
        {
            printf("Failed to allocate memory for ip list\n");
            return -1;
        }
        char *next = NULL;
        i = 0;
        do
        {
            config.ip_list[i] = haystack;
            i++;
            if (i >= config.num_parties)
                break;
            next = strchr(haystack, ',');
            if (next != NULL)
            {
                *next = '\0';
                haystack = next + 1;
            }
        } while(next != NULL);
    }

    if (config.ip_list == NULL || config.num_parties == 0)
    {
        printf("Invalid configuration, you must specify node rank, count of parties, and the ip list\n");
        print_usage(argv[0]);
        return -1;
    }

    config.initialized = 1;

    return 0;
}

// initialize communication, config.rank, num_parties
void init(int argc, char **argv)
{
    if (parse_opts(argc, argv))
    {
        printf("Failed to parse input options\n");
        exit(1);
    }

    TCP_Init();
    // this protocol works with 3 parties only
    if (config.rank == 0 && config.num_parties != NUM_PARTIES)
    {
        fprintf(stderr, "ERROR: The number of processes must be %d for %s\n", NUM_PARTIES, argv[0]);
    }
}

// exchange boolean shares: this is blocking
BShare exchange_shares(BShare s1)
{
  BShare s2;
  // send s1 to predecessor
  TCP_Send(&s1, 1, get_pred(), sizeof(BShare));
  TCP_Recv(&s2, 1, get_succ(), sizeof(BShare));
  // // receive remote seed from successor
  return s2;
}

// exchange boolean shares: this is blocking
unsigned long long exchange_shares_u(unsigned long long s1)
{
  unsigned long long s2;
  // send s1 to predecessor

  TCP_Send(&s1, 1, get_pred(), sizeof(unsigned long long));
  // // receive remote seed from successor

  TCP_Recv(&s2, 1, get_succ(), sizeof(unsigned long long));
  return s2;
}

// Exchanges single-bit boolean shares: this is blocking
BitShare exchange_bit_shares(BitShare s1)
{
  BitShare s2;
  // send s1 to predecessor
  TCP_Send(&s1, 1, get_pred(), sizeof(BitShare));
  // // receive remote seed from successor

  TCP_Recv(&s2, 1, get_succ(), sizeof(BitShare));
  return s2;
}

void exchange_shares_array(const BShare *shares1, BShare *shares2, long length)
{
  TCP_Send(shares1, length, get_pred(), sizeof(BShare));
  TCP_Recv(shares2, length, get_succ(), sizeof(BShare));
}

void exchange_shares_array_u(const unsigned long long *shares1,
                             unsigned long long *shares2, int length)
{
  TCP_Send(shares1, length, get_pred(), sizeof(unsigned long long));
  TCP_Recv(shares2, length, get_succ(), sizeof(unsigned long long));
}

void exchange_a_shares_array(const AShare *shares1, AShare *shares2, int length)
{
  TCP_Send(shares1, length, get_pred(), sizeof(AShare));
  TCP_Recv(shares2, length, get_succ(), sizeof(AShare));
}

void exchange_bit_shares_array(const BitShare *shares1, BitShare *shares2,
                               int length)
{
  TCP_Send(shares1, length, get_pred(), sizeof(BitShare));
  TCP_Recv(shares2, length, get_succ(), sizeof(BitShare));
}

int get_rank()
{
  check_init(__func__);
  return config.rank;
}

int get_succ()
{
  check_init(__func__);
  return (get_rank() + 1) % NUM_PARTIES;
}

int get_pred()
{
  check_init(__func__);
  return ((get_rank() + NUM_PARTIES) - 1) % NUM_PARTIES;
}

// [boolean share] open a value in P1 (rank=0)
Data open_b(BShare s)
{
  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(&s, 1, 0, sizeof(BShare));
    return s;
  }
  else if (config.rank == 0)
  {
    Data msg, res = s;
    // P1 receives shares from P2, P3
    TCP_Recv(&msg, 1, 1, sizeof(Data));
    res ^= msg;
    TCP_Recv(&msg, 1, 2, sizeof(Data));
    res ^= msg;

    return res;
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
    return 1;
  }
}

// [boolean share] open a value in P1 (rank=0)
Data open_bit(BitShare s)
{
  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(&s, 1, 0, sizeof(BitShare));
    return s;
  }
  else if (config.rank == 0)
  {
    bool msg, res = s;
    //   // P1 receives shares from P2, P3

    TCP_Recv(&msg, 1, 1, sizeof(bool));
    res ^= msg;
    TCP_Recv(&msg, 1, 2, sizeof(bool));
    res ^= msg;
    return res;
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
    return 1;
  }
}

// [boolean share] open an array of values in P1 (rank=0)
// MUST BE CALLED ONLY ONCE AS IT MUTATES THE GIVEN TABLE s
void open_b_array(BShare *s, int len, Data res[])
{
  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(s, len, 0, sizeof(BShare));
  }
  else if (config.rank == 0)
  {
    BShare *msg = malloc(len * sizeof(BShare));
    assert(msg != NULL);
    //   // P1 receives shares from P2, P3
    TCP_Recv(msg, len, 1, sizeof(BShare));

    for (int i = 0; i < len; i++)
    {
      res[i] = s[i] ^ msg[i];
    }

    TCP_Recv(msg, len, 2, sizeof(BShare));

    for (int i = 0; i < len; i++)
    {
      res[i] = res[i] ^ msg[i];
    }
    free(msg);
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
  }
}

// [boolean share] open an array of values in P1 (rank=0)
// MUST BE CALLED ONLY ONCE AS IT MUTATES THE GIVEN TABLE s
void open_byte_array(char *s, int len, char res[])
{
  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {

    TCP_Send(s, len, 0, sizeof(char));
  }
  else if (config.rank == 0)
  {
    char *msg = malloc(len * sizeof(char));
    assert(msg != NULL);
    // P1 receives shares from P2, P3
    TCP_Recv(msg, len, 1, sizeof(char));

    for (int i = 0; i < len; i++)
    {
      res[i] = s[i] ^ msg[i];
    }

    TCP_Recv(msg, len, 2, sizeof(char));

    for (int i = 0; i < len; i++)
    {
      res[i] = res[i] ^ msg[i];
    }
    free(msg);
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
  }
}

// [arithmetic share] open a value in P1 (rank=0)
Data open_a(AShare s)
{
  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(&s, 1, 0, sizeof(AShare));

    return s;
  }
  else if (config.rank == 0)
  {
    Data msg, res = s;
    // P1 receives shares from P2, P3
    TCP_Recv(&msg, 1, 1, sizeof(Data));

    res += msg;
    TCP_Recv(&msg, 1, 2, sizeof(Data));

    res += msg;
    return res;
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
    return 1;
  }
}

// [arithmetic share] open an array of arithmetic shares in P1 (rank=0)
void open_array(AShare *s, int len, Data res[])
{

  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(s, len, 0, sizeof(AShare));
  }
  else if (config.rank == 0)
  {
    AShare *msg = malloc(len * sizeof(AShare));
    assert(msg != NULL);
    // P1 receives shares from P2, P3
    TCP_Recv(msg, len, 1, sizeof(AShare));

    for (int i = 0; i < len; i++)
    {
      res[i] = s[i] + msg[i];
    }

    TCP_Recv(msg, len, 2, sizeof(AShare));

    for (int i = 0; i < len; i++)
    {
      res[i] = res[i] + msg[i];
    }
    free(msg);
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
  }
}

// [arithmetic share] open an array of arithmetic shares in P1 (rank=0)
void open_mixed_array(BShare *s, int rows, int cols, Data res[],
                      unsigned *a, int al, unsigned *b, int bl)
{

  assert((al + bl) == cols);
  int len = rows * cols;
  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(s, len, 0, sizeof(BShare));
  }
  else if (config.rank == 0)
  {
    BShare *msg = malloc(rows * cols * sizeof(BShare)); // BShare and AShare have equal size
    assert(msg != NULL);
    // P1 receives shares from P2, P3
    TCP_Recv(msg, len, 1, sizeof(BShare));

    for (int i = 0; i < len; i += cols)
    {
      // If aritmetic
      for (int j = 0; j < al; j++)
      {
        res[i + a[j]] = s[i + a[j]] + msg[i + a[j]];
      }
      // If boolean
      for (int j = 0; j < bl; j++)
      {
        res[i + b[j]] = s[i + b[j]] ^ msg[i + b[j]];
      }
    }

    TCP_Recv(msg, len, 2, sizeof(BShare));

    for (int i = 0; i < len; i += cols)
    {
      // If aritmetic
      for (int j = 0; j < al; j++)
      {
        res[i + a[j]] = res[i + a[j]] + msg[i + a[j]];
      }
      // If boolean
      for (int j = 0; j < bl; j++)
      {
        res[i + b[j]] = res[i + b[j]] ^ msg[i + b[j]];
      }
    }
    free(msg);
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
  }
}

void open_bit_array(BitShare *s, int len, bool res[])
{
  // P2, P3 send their shares to P1
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(s, len, 0, sizeof(BitShare));
  }
  else if (config.rank == 0)
  {
    BitShare *msg = malloc(len * sizeof(BitShare));
    assert(msg != NULL);

    // P1 receives shares from P2, P3
    TCP_Recv(msg, len, 1, sizeof(BitShare));
    for (int i = 0; i < len; i++)
    {
      res[i] = s[i] ^ msg[i];
    }

    TCP_Recv(msg, len, 2, sizeof(BitShare));

    for (int i = 0; i < len; i++)
    {
      res[i] = res[i] ^ msg[i];
    }
    free(msg);
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
  }
}

// [boolean share] open a value in all parties
Data reveal_b(BShare s)
{
  Data res = s;

  // P2, P3 send their shares to P1 and receive the result
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(&s, 1, 0, sizeof(BShare));
    TCP_Recv(&res, 1, 0, sizeof(Data));
  }
  else if (config.rank == 0)
  {
    Data msg;
    // P1 receives shares from P2, P3
    TCP_Recv(&msg, 1, 1, sizeof(Data));
    res ^= msg;
    TCP_Recv(&msg, 1, 2, sizeof(Data));
    res ^= msg;

    // P1 sends result to P2, P3
    TCP_Send(&res, 1, 1, sizeof(Data));
    TCP_Send(&res, 1, 2, sizeof(Data));
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
    return 1;
  }
  return res;
}

// [boolean share] open an array of values in all parties
void reveal_b_array(BShare *s, int len)
{
  // P2, P3 send their shares to P1 and receive the result
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(s, len, 0, sizeof(BShare));
    TCP_Recv(s, len, 0, sizeof(BShare));
  }
  else if (config.rank == 0)
  {
    Data *msg = malloc(len * sizeof(Data));
    assert(msg != NULL);
    // P1 receives shares from P2, P3
    TCP_Recv(&msg[0], len, 1, sizeof(Data));
    for (int i = 0; i < len; i++)
    {
      s[i] ^= msg[i];
    }
    TCP_Recv(&msg[0], len, 2, sizeof(Data));
    for (int i = 0; i < len; i++)
    {
      s[i] ^= msg[i];
    }
    // P1 sends result to P2, P3
    TCP_Send(s, len, 1, sizeof(BShare));
    TCP_Send(s, len, 2, sizeof(BShare));
    free(msg);
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
  }
}

void reveal_b_array_async(BShare *s, int len)
{
  // // P2, P3 send their shares to P1 and receive the result
  if (config.rank == 1 || config.rank == 2)
  {
    TCP_Send(s, len, 0, sizeof(BShare));
    TCP_Recv(s, len, 0, sizeof(BShare));
  }
  else if (config.rank == 0)
  {
    Data *msg = malloc(len * sizeof(Data));
    assert(msg != NULL);
    Data *msg2 = malloc(len * sizeof(Data));
    assert(msg2 != NULL);
    //   // P1 receives shares from P2, P3
    TCP_Recv(&msg[0], len, 1, sizeof(Data));
    TCP_Recv(&msg2[0], len, 2, sizeof(Data));

    for (int i = 0; i < len; i++)
    {
      s[i] ^= msg[i];
      s[i] ^= msg2[i];
    }

    // P1 sends result to P2, P3
    TCP_Send(s, len, 1, sizeof(BShare));
    TCP_Send(s, len, 2, sizeof(BShare));

    free(msg);
    free(msg2);
  }
  else
  {
    fprintf(stderr, "ERROR: Invalid config.rank %d.\n", config.rank);
  }
}

// check if communication has been initialized
static void check_init(const char *f)
{
  if (!config.initialized)
  {
    fprintf(stderr, "ERROR: init() must be called before %s\n", f);
  }
}
