#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --nodes=1
#SBATCH --cpus-per-task=20
#SBATCH --time=2-00:00:00
#SBATCH --job-name=spruce_filtering
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

# LAfilter parameters
stitch_distance=100
min_non_repeat_bases=300
trim_track=gap_trim
repeat_track=stitch_repeats
min_align_length=1000
max_leftovers=0

# Avoid rerunning stuff that has already (potentially) finished
outfiles=$(find . -type f -name "${database}.*.filtered.las" -size +0)
if [[ ! -z ${outfiles} ]]; then
    outfiles=$(echo "${outfiles}" | xargs -n1 basename | sort)
fi

finished_blocks=$(echo "${outfiles}" | grep -o '[0-9]\+' | sort)
existing_blocks=$(find . -maxdepth 1 -type f -name "${database}.*.gap.las" | grep -o '[0-9]\+' | sort)
blocks_to_run=$(comm -13 <(echo "${finished_blocks}") <(echo "${existing_blocks}") | sort -n)

parallel \
    --jobs 20 \
    --results post_filtering_logs/post_filtering_{} \
    LAfilter \
    -p \
    -s ${stitch_distance} \
    -n ${min_non_repeat_bases} \
    -t ${trim_track} \
    -r ${repeat_track} \
    -o ${min_align_length} \
    -u ${max_leftovers} \
    ${database} \
    ${database}.{}.gap.las \
    ${database}.{}.filtered.las ::: ${blocks_to_run}

LAmerge -n 16 -S filtered ${database} ${database}.filtered.las
