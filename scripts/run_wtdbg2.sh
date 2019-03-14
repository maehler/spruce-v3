#!/bin/bash -l

#SBATCH -A snic2018-3-317
#SBATCH -C mem1TB
#SBATCH -p node
#SBATCH -n 20
#SBATCH -t 2-00:00:00
#SBATCH -J spruce_wtdbg2
#SBATCH -o /proj/uppstore2017145/V3/wtdbg2/wtdbg2_assembly_%j.log

set -eu

# Assembly or consensus?
if [[ $# -ne 1 ]]; then
    echo >&2 "usage: $0 assembly|consensus"
    exit 1
fi

if [[ $1 != "assembly" ]] && [[ $1 != "consensus" ]]; then
    echo >&2 "error: invalid method '$1'"
    exit 1
fi

set +u
conda activate wtdbg2
set -u

proj_dir="/proj/uppstore2017145/V3"
output_dir="$proj_dir/wtdbg2"

if [[ ! -d $output_dir ]]; then
    mkdir $output_dir
fi

if [[ $1 == "assembly" ]]; then
    wtdbg2 \
        -i "$proj_dir/data/genome/ps_021/fasta/30x_longest_clean.fasta" \
        -fo "$output_dir/spruce" \
        -t 20 \
        -x sq \
        -g 20g
elif [[ $1 == "consensus" ]]; then
    echo >&2 "error: not implemented yet"
    exit 1
fi
