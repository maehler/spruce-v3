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

echo "${paths_to_extract}" |
    parallel \
        --jobs 10 \
        --results tour_to_fasta_logs/{} \
        tour2fasta.py \
        -t ${trim_track} \
        ${database} \
        {}.graphml {}.paths
