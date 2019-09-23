#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --nodes=1
#SBATCH --ntasks=20
#SBATCH --cpus-per-task=1
#SBATCH --time=1-00:00:00
#SBATCH --job-name=spruce_trim
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

if [[ ! -d ${input_directory} ]]; then
    echo >&2 "error: input directory not found: ${input_directory}"
    exit 1
fi

cd ${input_directory}

# LAq parameters
quality_th=30
min_n_segments=5
output_q_track=stitch_q${quality_th}_q
output_trim_track=stitch_q${quality_th}_trim

find . -maxdepth 1 -type f -regex ".+${database}\.[0-9]+\.stitch\.las" | \
    xargs -n1 basename | \
    cut -f2 -d. | \
    sort -n | \
    parallel \
        --jobs 20 \
        --results post_annotate_logs/post_annotate_{} \
        LAq \
        -b {} \
        -d ${quality_th} \
        -s ${min_n_segments} \
        -T ${output_trim_track} \
        -Q ${output_q_track} \
        ${database} \
        ${database}.{}.stitch.las

parallel TKmerge -d ${database} ::: ${output_q_track} ${output_trim_track}
