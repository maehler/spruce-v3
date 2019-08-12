#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --ntasks=20
#SBATCH --cpus-per-task=1
#SBATCH --time=2-00:00:00
#SBATCH --job-name=spruce_gaps

set -eu

module load gnuparallel/20180822

if [[ $# -ne 1 ]]; then
    echo >&2 "usage: $0 <block file>"
    exit 1
fi

source /proj/uppstore2017145/V3/software/activate.sh

block_file=$1

database=$(head -n1 ${block_file} | cut -f1 -d.)
blocks=$(cut -f2 -d. ${block_file})

# LAgap parameters
min_distance=100
trim_track=stitch_trim

echo "# Working on blocks" ${blocks} "(${block_file})"

# Copy database
cp ${database}.db .${database}.bps .${database}.idx ${SNIC_TMP}
# Copy trim track 
cp .${database}.${trim_track}.* ${SNIC_TMP}

parallel -j20 \
    LAgap \
    -s ${min_distance} \
    -t ${trim_track} \
    ${SNIC_TMP}/{1} {3} {1}.{2}.gap.las ::: ${database} ::: ${blocks} ::::+ ${block_file}
