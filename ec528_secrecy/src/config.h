#ifndef SECRECY_CONFIG_H_
#define SECRECY_CONFIG_H_

struct secrecy_config {
    unsigned int rank;
    unsigned int num_parties;
    int initialized;
    unsigned short port;
    char **ip_list;
};

#endif