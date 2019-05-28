#!/bin/bash -l

#SBATCH -A snic2018-3-317
#SBATCH -n 1
#SBATCH -p core
#SBATCH -J axolotl_data
#SBATCH -o /proj/uppstore2017145/V3/data/axolotl_data_download_%j.log
#SBATCH -t 2-00:00:00

set -u

module load bioinfo-tools sratools/2.9.1-1

projdir=/proj/uppstore2017145/V3/data
accessions=${projdir}/axolotl_pacbio_accessions.txt

while read acc; do
    prefetch \
        --max-size 100G \
        --output-directory ${projdir}/axolotl_pacbio \
        ${acc}
done < ${accessions}
