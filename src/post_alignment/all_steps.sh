#!/bin/bash -l

set -eu

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
proj_dir=/proj/uppstore2017-145/V3/marvel_runs_round2_repeats

# Step 1
stitch_id=$(sbatch --parsable -o ${proj_dir}/logs/post_stitch.log ${script_dir}/stitch.sh spruce_patched ${proj_dir})

# Steps 2 and 3
annotate_id=$(sbatch \
    --parsable \
    --output=${proj_dir}/logs/post_annotate.log \
    --dependency=afterok:${stitch_id} \
    ${script_dir}/annotate.sh spruce_patched ${proj_dir} 30)

# Step 4

