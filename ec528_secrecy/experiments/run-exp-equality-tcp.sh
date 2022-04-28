#!/bin/bash

TEST="./exp-equality"
RANK="1"
FILE="tcp_timing.txt"
CAT="/bin/cat"

for INPUT_SIZE in 1024 4096 16384 65536 262144 1048576
do
	echo "Running exp-equality for input size = $INPUT_SIZE"
	$TEST $RANK $INPUT_SIZE
done

$CAT $FILE
