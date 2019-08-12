#!/bin/bash -l

set -eu

if [[ $# -ne 3 ]]; then
    echo >&2 "usage: $0 <database> <input directory> <max blocks>"
    exit 1
fi

database=$1
input_directory=$2
max_blocks=$3

if ! [[ ${max_blocks} =~ ^[0-9]+$ ]]; then
    echo >&2 "error: max blocks has to be an integer"
    exit 1
fi

if [[ ${max_blocks} -lt 1 ]]; then
    echo >&2 "error: max blocks has to be a positive non-zero integer"
    exit 1
fi

export script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

if [[ ! -d ${input_directory} ]]; then
    echo >&2 "error: input directory not found: ${input_directory}"
    exit 1
fi

cd ${input_directory}

if [[ ! -d gap_tasks ]]; then
    mkdir gap_tasks
fi

start_block=$(find . -maxdepth 1 -type f -regex ".+${database}\.[0-9]+\.gap\.las" | \
    xargs -n1 basename | \
    cut -f2 -d. | \
    sort -nr | \
    head -n1)

task_id=$(date +%s)
task_file_prefix="gap_${task_id}_"

find . -maxdepth 1 -type f -regex ".+${database}\.[0-9]+\.stitch\.las" | \
    xargs -n1 basename | \
    cut -f2 -d. | \
    sort -n | \
    awk -v database=${database} -v start_block=${start_block} '{if ($1 > start_block) {printf("%s.%d.stitch.las\n", database, $1)}}' | \
    head -n ${max_blocks} | \
    split -l 20 - gap_tasks/${task_file_prefix}

echo "Submitting $(cat gap_tasks/${task_file_prefix}* | wc -l) blocks"

find gap_tasks -type f -name "${task_file_prefix}*" \
    -exec sh -c 'fn={}; sbatch --output=post_gap_logs/$(basename ${fn})_%j.log ${script_directory}/gaps.sh ${fn}' \;
