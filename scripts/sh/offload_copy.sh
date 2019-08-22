#!/bin/bash -l

#SBATCH -A snic2018-3-317
#SBATCH -p core
#SBATCH -n 12
#SBATCH -c 1
#SBATCH -J offload_copy
#SBATCH -o offload_copy_%j.log
#SBATCH -t 10-00:00:00
#SBATCH --mail-type END,FAIL
#SBATCH --mail-user niklas.mahler@umu.se
#SBATCH --profile all

set -eu

n_from=$(($#-1))
sources=(${@:1:${n_from}})
destination=${@: -1}

if [[ $# -lt 2 ]]; then
    echo "error: too few arguments"
    exit 1
fi

if [[ ${#sources[@]} -gt 1 ]] && [[ ! -d $destination ]]; then
    echo "error: destination must be a directory in this case" >&2
    exit 1
fi

printf '%s\n' "${sources[@]}" | \
    sort -R | \
    xargs -n1 -P 12 -I {} \
    rsync -avPu {} "${destination}"
