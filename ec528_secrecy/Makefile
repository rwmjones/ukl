.PHONY: all clean

export CFLAGS = -std=c99 -ggdb -O3 -Wall -I../src
export LDFLAGS = -lsodium -lm
export CC = gcc
export AR = ar

all:
	$(MAKE) -C src/ all
	$(MAKE) -C tests/ all
	$(MAKE) -C experiments/ all
	$(MAKE) -C examples/ all

clean:
	$(MAKE) -C src/ clean
	$(MAKE) -C tests/ clean
	$(MAKE) -C experiments/ clean
	$(MAKE) -C examples/ clean
