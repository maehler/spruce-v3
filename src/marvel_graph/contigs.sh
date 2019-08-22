#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --ntasks=20
#SBATCH --cpus-per-task=1
#SBATCH --time=2-00:00:00
#SBATCH --job-name=spruce_tour2fasta
#SBATCH --mail-type=ALL
#SBATCH --mail-user=niklas.mahler@umu.se

set -eu

if [[ $# -ne 2 ]]; then
    echo >&2 "usage: $0 <database> <components directory>"
    exit 1
fi

module load gnuparallel/20180822
source /proj/uppstore2017145/V3/software/activate.sh

database=$1
input_dir=$2

if [[ ! -d ${input_dir} ]]; then
    echo "error: file or directory not found: ${input_dir}" >&2
fi

# Exclude paths that have already been extracted
fastas=$(find ${input_dir} -type f -name "*.tour.fasta" -size +0 | sed 's/\.fasta$//' | sort)
paths=$(find ${input_dir} -type f -name "*tour.paths" | sed 's/\.paths$//' | sort)
paths_to_extract=$(comm -13 <(echo "${fastas}") <(echo "${paths}") | sort -V)

# TODO: The trim track should ideally be based on the
# alignments after fixing gaps, but this did not play
# well with OGbuild for some reason.
trim_track=stitch_trim

echo "${paths_to_extract}" | \
    xargs -n10 |
    parallel \
        --colsep ' ' \
        --jobs 10 \
        --results tour_to_fasta_logs/job_{#} \
        tour2fasta.py \
        -t ${trim_track} \
        ${database} \
        {1}.graphml {1}.paths \
        {2}.graphml {2}.paths \
        {3}.graphml {3}.paths \
        {4}.graphml {4}.paths \
        {5}.graphml {5}.paths \
        {6}.graphml {6}.paths \
        {7}.graphml {7}.paths \
        {8}.graphml {8}.paths \
        {9}.graphml {9}.paths \
        {10}.graphml {10}.paths
