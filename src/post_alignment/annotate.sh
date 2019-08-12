#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --nodes=1
#SBATCH --ntasks=20
#SBATCH --cpus-per-task=1
#SBATCH --time=2-00:00:00
#SBATCH --job-name=spruce_annotate
#SBATCH --mail-type=ALL
#SBATCH --mail-user=niklas.mahler@umu.se

set -eu

if [[ $# -lt 3 ]] || [[ $# -gt 4 ]]; then
    echo >&2 "usage: $0 <database> <input directory> <coverage> [<repeat track>]
    
    NOTES

        The supplied repeat track should be an existing repeat track based on
        the current database. This repeat track will then be merged with the
        repeats that are found by LArepeat and be stored in the track 'combined_repeats'."
    exit 1
fi

module load gnuparallel/20180822
source /proj/uppstore2017145/V3/software/activate.sh

database=$1
input_directory=$2
coverage=$3
repeats=
if [[ $# -eq 4 ]]; then
    repeats=$4
fi

if [[ ! -d ${input_directory} ]]; then
    echo >&2 "error: input directory not found: ${input_directory}"
    exit 1
fi

cd ${input_directory}

# LAq parameters
min_n_segments=5
output_q_track=stitch_q
output_trim_track=stitch_trim

find . -maxdepth 1 -type f -regex ".+${database}\.[0-9]+\.stitch\.las" | \
    xargs -n1 basename | \
    cut -f2 -d. | \
    sort -n | \
    parallel \
        --jobs 20 \
        --results post_annotate_logs/post_annotate_{} \
        LAq \
        -b {} \
        -s ${min_n_segments} \
        -T ${output_trim_track} \
        -Q ${output_q_track} \
        ${database} \
        ${database}.{}.stitch.las

# LArepeat parameters
output_repeat_track=stitch_repeats
region_start=2.0
region_end=1.7

find . -maxdepth 1 -type f -regex ".+${database}\.[0-9]+\.stitch\.las" | \
    xargs -n1 basename | \
    cut -f2 -d. | \
    sort -n | \
    parallel \
        --jobs 20 \
        --results post_annotate_logs/post_repeat_annotate_{} \
        LArepeat \
        -b {} \
        -c ${coverage} \
        -h ${region_start} \
        -l ${region_end} \
        -t ${output_repeat_track} \
        ${database} ${database}.{}.stitch.las

parallel TKmerge -d ${database} ::: ${output_q_track} ${output_trim_track} ${output_repeat_track}
if [[ ! -z ${repeats} ]]; then
    TKcombine ${database} combined_repeats ${output_repeat_track} ${repeats}
fi
