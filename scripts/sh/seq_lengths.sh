#!/bin/bash

set -eu

if [[ $# -ne 1 ]]; then
    echo >&2 "usage: $0 FASTA"
    exit 1
fi

if [[ ! -f $1 ]]; then
    echo >&2 "error: file or directory not found: $1"
    exit 1
fi

awk '/^>/ { printf("%s%s\t", (NR > 1 ? "\n" : ""), $0); next;  } { printf("%s", $0);  } END { printf("\n"); }' $1 | \
    awk 'BEGIN { FS="\t"; } { printf("%d\n", length($2)); }'
