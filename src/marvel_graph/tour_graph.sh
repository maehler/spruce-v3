#!/bin/bash -l

#SBATCH --account=snic2018-3-317
#SBATCH --partition=node
#SBATCH --ntasks=20
#SBATCH --cpus-per-task=1
#SBATCH --time=2-00:00:00
#SBATCH --job-name=spruce_og_tour
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

# Exclude components that have already been toured
paths=$(find ${input_dir} -type f -name "*.tour.paths" | sed 's/\.tour\.paths$//' | sort)
graphs=$(find ${input_dir} -type f -name "*.graphml" ! -name "*.tour.graphml" | sed 's/\.graphml$//' | sort)
graphs_to_tour=$(comm -13 <(echo "${paths}") <(echo "${graphs}") | sort -V)

echo "${graphs_to_tour}" | \
    parallel \
    --jobs 20 \
    --results graph_tour_logs/graph_tour_{/} \
    OGtour.py --circular ${database} {}.graphml
