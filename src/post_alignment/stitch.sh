#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --nodes=1
#SBATCH --ntasks=20
#SBATCH --cpus-per-task=1
#SBATCH --time=4-00:00:00
#SBATCH --job-name=spruce_stitching
#SBATCH --mail-type=ALL
#SBATCH --mail-user=niklas.mahler@umu.se

set -eu

if [[ $# -ne 2 ]]; then
    echo >&2 "usage: $0 <database> <input directory>"
    exit 1
fi

module load gnuparallel/20180822
source /proj/uppstore2017145/V3/software/activate.sh

database=$1
input_directory=$2
output_suffix="stitch"

if [[ ! -d ${input_directory} ]]; then
    echo >&2 "error: input directory not found: ${input_directory}"
    exit 1
fi

# LAstitch parameters
max_distance=40

find ${input_directory} -maxdepth 1 -type f -regex ".+${database}\.[0-9]+\.las" | \
    xargs -n1 basename | \
    cut -f2 -d. | \
    sort -n | \
    parallel \
        --jobs 20 \
        --results ${input_directory}/${output_suffix}_logs/${output_suffix}_{} \
        LAstitch \
        -f ${max_distance} \
        ${database} \
        ${database}.{}.las \
        ${database}.{}.${output_suffix}.las
