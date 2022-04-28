#!/bin/bash

### ### ###                ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###                ### ### ###

### paths configuration ###
TEST="./exp-equality-tcp"
RANK=1

### LOCAL RUN (3 MPI Processes on the same node)
for INPUT_SIZE in 1000 10000 100000 1000000 10000000 #100000000
do
  echo "Running experiment with INPUT_SIZE=$INPUT_SIZE"
  $TEST $RANK $INPUT_SIZE
done
