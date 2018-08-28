#!/usr/bin/env python

import argparse
import os
import sys

import marvelous_jobs as mj

def is_project(directory='.'):
    db_name = os.path.abspath(os.path.join(directory, 'marveldb'))
    return os.path.exists(db_name)

def init(directory='.', force=False):
    if is_project() and not force:
        print('error: project is already initialised, delete '
              '`marveldb` or use --force if you want to start over',
             file=sys.stderr)
        exit(1)
    db_name = os.path.join(directory, 'marveldb')
    mj.marvel_db(db_name)

# Helper functions for the argument parsing
def directory_exists(s):
    return os.path.exists(s) and os.path.isdir(s)

def posititive_integer(i):
    return type(i) is int and i > 0

def parse_args():
    parser = argparse.ArgumentParser(description='Job manager for MARVEL')
    
    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='subcommand')

    # Initialisation
    init_parser = subparsers.add_parser('init', help='Initialise a new project')
    init_parser.add_argument('directory', help='directory where to '
                             'initialise the run (default: .)',
                             default='.', nargs='?')
    init_parser.add_argument('-f', '--force', help='Force overwrite of '
                             'existing database', action='store_true')

    # DBprepare
    prep_parser = subparsers.add_parser('prepare', help='Prepare data files')
    prep_parser.add_argument('wd', help='working directory')
    prep_parser.add_argument('db',
                             help='Name of the resulting database')
    prep_parser.add_argument('fasta',
                             help='Input reads in FASTA format')
    prep_parser.add_argument('-s', '--blocksize',
                             help='Database block size in megabases (default: 200)',
                             default=200, type=int, metavar='N',
                             dest='blocksize')

    args = parser.parse_args()

    if args.subcommand == 'init':
        args.directory = os.path.abspath(args.directory)
        if not directory_exists(args.directory):
            parser.error('directory not found: {0}'.format(args.directory))

    if args.subcommand == 'prepare':
        if not positive_integer(args.blocksize):
            parser.error('blocksize must be a positive non-zero integer')

    if args.subcommand is None:
        parser.parse_args(['-h'])

    return(args)

def main():
    args = parse_args()

    if args.subcommand == 'init':
        init(args.directory, force=args.force)

if __name__ == '__main__':
    main()
