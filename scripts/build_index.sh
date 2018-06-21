#!/bin/bash

# Build the fasta index for large FA files

export LD_LIBRARY_PATH=/crex/proj/uppstore2017145/V3/software/lib
EXE=/proj/uppstore2017145/V3/git/src/faidx_ops/build_index

module load boost/1.63.0_gcc6.3.0 gcc/6.3.0

INDEX_TGT=$1
FASTA_SRC=$2

$EXE $INDEX_TGT $FASTA_SRC
