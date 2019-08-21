#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=core
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
#SBATCH --time=1:00:00
#SBATCH --job-name=spruce_og
#SBATCH --mail-type=ALL
#SBATCH --mail-user=niklas.mahler@umu.se

set -eu

if [[ $# -ne 2 ]]; then
    echo >&2 "usage: $0 <database> <input las>"
    exit 1
fi

module load gnuparallel/20180822
source /proj/uppstore2017145/V3/software/activate.sh

database=$1
input_las=$2

if [[ ! -f ${input_las} ]]; then
    echo "error: file or directory not found: ${input_las}" >&2
fi

# TODO: This trim track should ideally be based on the
# alignments after fixing gaps, but for some reason
# OGbuild does not like this file.
trim_track=stitch_trim
n_edges_to_dead_ends=1
output_dir=$(dirname ${input_las})/components

if [[ ! -d ${output_dir} ]]; then
    mkdir ${output_dir}
fi

OGbuild \
    -s \
    -t ${trim_track} \
    -c ${n_edges_to_dead_ends} \
    ${database} \
    ${input_las} \
    ${output_dir}/${database}
