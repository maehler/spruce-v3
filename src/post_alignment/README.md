# MARVEL post-alignment processing

After having done the two rounds of pairwise read alignments, it is time for some cleaning up and filtering of the data before building the assembly graph.
The steps that were performed on the axolotl data were

1. Repair alignments (`LAstitch`).
2. Create annotation tracks based on the stitched alignments (`LAq`, `LArepeat`, and `TKmerge`).
3. Merge repeat annotations with repeat annotations from the first round (`TKcombine`).
4. Remove gaps (`LAgap`).
5. Recalculate trim tracks based on cleaned up gaps (`LAq`, `TKmerge`).
6. Filter repeat induced alignments (`LAfilter`).
7. Finally merge all alignments into a single file (`LAmerge`).

In this folder, the scripts for performing these steps are kept.

## `stitch.sh`

Perform step 1.

## `annotate.sh`

Perform steps 2 and 3.

## `gap_submit.sh`

Perform step 4.
Due to time and I/O constraints, this will submit multiple sbatch jobs, and it is up to the user to make sure that things are not overwritten.

## `annotate_update.sh`

Perform step 5.

## `filter.sh`

Perform steps 6 and 7.
