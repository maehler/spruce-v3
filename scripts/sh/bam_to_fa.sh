#!/bin/bash

# Take all BAM files from the sequel and drop them into a FASTA for MARVEL

module load bioinfo-tools samtools

INDIR="/proj/uppstore2017145/V3/data/ps_021/rawdata/ps_021_001"
OUTFILE="/proj/uppstore2017145/V3/data/ps_021/fasta/full_data.fasta"

>$OUTFILE
find $INDIR -name "*.bam" | while read l; do echo $l; samtools view $l | awk '{printf(">%s\n%s\n", $1, $10)}' >> $OUTFILE; done
