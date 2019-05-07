#!/usr/bin/local python

import argparse
from collections import defaultdict
import gzip
import nose
import os
import re
import subprocess
import sys

import tests

header_regex = re.compile('^>([^/]+)/(\d+)/(\d+)_(\d+)')
verbose = False

class seq_range:

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def distance(self, other):
        if self.start < other.start:
            return max(0, other.start - self.end)
        return max(0, self.start - other.end)

    def overlaps(self, other):
        return self.end >= other.start or other.end <= self.start

    def __repr__(self):
        return '({},{})'.format(self.start, self.end)

    def __str__(self):
        return '{}_{}'.format(self.start, self.end)

class header:

    def __init__(self, cell, hole, range):
        self.cell = cell
        self.hole = hole
        self.range = range

    @classmethod
    def parse(cls, h):
        m = header_regex.match(str(h))
        if m is None:
            raise ValueError('invalid Pacbio header: {}'.format(h))
        g = m.groups()
        return cls(g[0], int(g[1]), seq_range(int(g[2]), int(g[3])))

    def overlaps(self, other):
        if self.cell == other.cell and self.hole == other.hole:
            return self.range.overlaps(other.range)
        return False

    def distance(self, other):
        if self.cell == other.cell and self.hole == other.hole:
            return self.range.distance(other.range)
        raise TypeError('sequences are from different cells/holes')

    def __repr__(self):
        return str(self)

    def __str__(self):
        return '{}/{}/{}'.format(self.cell, self.hole, self.range)

def generate_headers(fasta):
    global verbose

    if fasta is not None and fasta.endswith('gz'):
        f = gzip.open(fasta, mode='rt')
    elif fasta is None:
        f = sys.stdin
    else:
        f = open(fasta)

    for line in f:
        if line.startswith('>'):
            try:
                h = header.parse(line.strip())
            except ValueError as ve:
                if verbose:
                    print('error: {}'.format(ve), file=sys.stderr)
            else:
                yield h
        else:
            continue

    f.close()

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('fasta', help='Pacbio FASTA file (or just the headers) '
                        'sorted according to moviename, hole number, and qstart '
                        '(optionally gzipped, default: read from stdin)',
                        nargs='?')
    parser.add_argument('--bin-width', '-b', help='Bin width for gap histogram '
                        '(default: 100)', default=100, type=int)
    parser.add_argument('-v', '--verbose', help='Verbose output',
                        action='store_true')

    parser.add_argument('--test', help='Run the test suite',
                        action='store_true')

    args = parser.parse_args()

    if args.fasta is not None and not os.path.exists(args.fasta):
        parser.error('error: file or directory not found: {}'.format(args.fasta))

    return args

def main():
    global verbose
    args = parse_args()

    verbose = args.verbose

    if args.test:
        tests_ok = nose.run(argv=['nosetests', '-v'], module=tests, exit=True)
        if tests_ok:
            sys.exit(0)
        sys.exit(1)

    ranges = []
    current_hole = None
    current_cell = None

    hist = defaultdict(int)
    gaps = defaultdict(int)
    n_multiple = 0
    n_overlapping = 0

    for h in generate_headers(args.fasta):
        if current_cell is not None \
           and (h.cell != current_cell or h.hole != current_hole) \
           and len(ranges) == 1:
            # Only a single sequence from the ZMW, nothing to compare
            hist[1] += 1
            ranges = []
        elif (h.cell != current_cell or h.hole != current_hole) \
           and len(ranges) > 1:
            n_multiple += 1
            hist[len(ranges)] += 1
            overlaps = False
            for i in range(len(ranges)-1):
                if ranges[i].overlaps(ranges[i+1]):
                   overlaps = True
                dist = ranges[i].distance(ranges[i+1])
                gaps[dist // args.bin_width] += 1
            if any(ranges[i].overlaps(ranges[i+1]) \
                   for i in range(len(ranges)-1)):
                n_overlapping += 1
            ranges = []
        current_cell = h.cell
        current_hole = h.hole
        ranges.append(h)

    if len(ranges) > 1:
        n_multiple += 1
        if any(ranges[i].overlaps(ranges[i+1]) \
               for i in range(len(ranges)-1)):
            n_overlapping += 1

    hist[len(ranges)] += 1

    print('n_multiple: {}\nn_overlapping: {}' \
          .format(n_multiple, n_overlapping))

    print('number of reads per hole:')
    for k in sorted(hist.keys()):
        print('{}\t{}'.format(k, hist[k]))

    print('\ngap histogram (bin width: {}):'.format(args.bin_width))
    for k in sorted(gaps.keys()):
        print('{}\t{}'.format(k*args.bin_width, gaps[k]))

if __name__ == '__main__':
    main()
