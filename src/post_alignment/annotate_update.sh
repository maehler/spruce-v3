#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --nodes=1
#SBATCH --cpus-per-task=20
#SBATCH --time=2-00:00:00
#SBATCH --job-name=spruce_annotate
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
trim_quality_threshold=30
min_n_segments=5
input_trim_track=stitch_q${trim_quality_threshold}_trim
input_q_track=stitch_q${trim_quality_threshold}_q
output_trim_track=gap_q${trim_quality_threshold}_trim
output_q_track=gap_q${trim_quality_threshold}_q

# Avoid rerunning stuff that has already (potentially) finished
afiles=$(find . -type f -name ".${database}.*.${output_trim_track}.a2" -size +0 | \
    xargs -n1 basename | sed 's/\.a2$//' | sort)
dfiles=$(find . -type f -name ".${database}.*.${output_trim_track}.d2" -size +0 | \
    xargs -n1 basename | sed 's/\.d2$//' | sort)

finished_blocks=$(comm -12 <(echo "${afiles}") <(echo "${dfiles}") | grep -o '[0-9]\+' | sort)
existing_blocks=$(find . -maxdepth 1 -type f -name "${database}.*.gap_q${trim_quality_threshold}_trim.las" | grep -o '[0-9]\+' | sort)
blocks_to_run=$(comm -13 <(echo "${finished_blocks}") <(echo "${existing_blocks}"))

echo "Updating trim track for $(echo "${blocks_to_run}" | wc -l) blocks"

echo "${blocks_to_run}" | \
    sort -n | \
    parallel \
        --jobs 10 \
        --results post_annotation_update/post_annoupdate_{} \
        LAq \
        -u \
        -b {} \
        -d ${trim_quality_threshold} \
        -t ${input_trim_track} \
        -T ${output_trim_track} \
        -q ${input_q_track} \
        -Q ${output_q_track} \
        ${database} \
        ${database}.{}.gap_${trim_quality_threshold}_trim.las > /dev/null

TKmerge -d ${database} ${output_trim_track}
TKmerge -d ${database} ${output_q_track}
