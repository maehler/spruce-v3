#!/bin/bash -l

#SBATCH -A snic2018-3-317
#SBATCH -n 1
#SBATCH -p core
#SBATCH -J axolotl_fasta
#SBATCH -o /proj/uppstore2017145/V3/data/axolotl_fasta_%j.log
#SBATCH -t 1-00:00:00

set -u

module load bioinfo-tools sratools/2.9.1-1

projdir=/proj/uppstore2017145/V3/data
indir=${projdir}/axolotl_pacbio
outdir=${indir}/fasta

fastq-dump --outdir ${outdir} --fasta 70 $(find ${indir} -maxdepth 1 -type f -name "*.sra")
tmp_fasta=$(find ${outdir} -maxdepth 1 -name "*.fasta")

cat ${tmp_fasta} > ${outdir}/axolotl_pacbio.fasta
rm ${tmp_fasta}
