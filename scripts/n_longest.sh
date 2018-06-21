#!/bin/bash

# Extract the N longest reads in a memory friendly way

export LD_LIBRARY_PATH=/crex/proj/uppstore2017145/V3/software/lib
EXE=/proj/uppstore2017145/V3/git/src/stream_longest/longest_n

NREADS=$1
FASTA_SRC=$2
FASTA_TGT=$3

$EXE $NREADS $FASTA_SRC > $FASTA_TGT 
