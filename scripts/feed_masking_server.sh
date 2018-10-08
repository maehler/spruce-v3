#!/bin/bash -l

#SBATCH -A snic2018-3-317
#SBATCH -p core
#SBATCH -n 8
#SBATCH -t 1-00:00:00

# Feed the masking server with finished alingments

set -eu

if [[ $# -ne 3 ]]; then
    echo >&2 "error: incorrect number of arguments"
    echo >&2 "usage: $0 <host> <port> <directory>"
    echo >&2
    echo >&2 "Find all compressed las files in <directory>,"
    echo >&2 "decompress them and feed them to the masking"
    echo >&2 "server on <host>:<port>."
    exit 1
fi

host=$1
port=$2
dir=$3

if ! type pigz >/dev/null 2>&1; then
    echo >&2 "error: this script requires pigz: not found in path"
    exit 1
fi

# The masking server wants uncompressed files
find $dir -type f -name "*.las.gz" | xargs -n 500 pigz -d -p 8

# Find the directories where the files are located and
# feed these to the masking server
find $dir -type f -name "*.las" -exec dirname {} \; | \
    sort -u | \
    xargs -n1 DMctl -h $host -p $port done

# Compress alignments again
find $dir -type f -name "*.las" | xargs -n 500 pigz -p 8
