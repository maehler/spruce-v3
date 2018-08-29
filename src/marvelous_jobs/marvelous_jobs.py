#!/usr/bin/env python

import argparse
import os
from subprocess import Popen, PIPE
import sys

import marvelous_jobs as mj

import marvel

def is_project(directory='.'):
    db_name = os.path.abspath(os.path.join(directory, 'marveldb'))
    return os.path.exists(db_name)

def init(name, coverage, directory='.', force=False):
    if is_project(directory) and not force:
        print('error: project is already initialised, delete '
              '`marveldb` or use --force if you want to start over',
             file=sys.stderr)
        exit(1)
    db_name = os.path.join(directory, 'marveldb')
    mj.marvel_db(filename=db_name, name=name,
                 coverage=coverage, force=force)

def prepare(fasta, blocksize, force=False):
    db_name = os.path.join('.', 'marveldb')
    db = mj.marvel_db.from_file(db_name)
    if db.is_prepared() and not force:
        print('error: database is already prepared', file=sys.stderr)
        exit(1)
    projname = db.info('name')
    args = [
        os.path.join(marvel.config.PATH_SCRIPTS, 'DBprepare.py'),
        '--blocksize', str(blocksize),
        projname,
        fasta
    ]
    p = Popen(args, shell=False)
    p.wait()

    if p.returncode != 0:
        print('an error has occured in DBprepare.py', file=sys.stderr)
        sys.exit(p.returncode)

    prepare_db_filename = '{0}.db'.format(projname)
    with open(prepare_db_filename) as f:
        for line in f:
            if line.strip().startswith('blocks'):
                n_blocks = int(line.strip().split()[-1])

    if force:
        db.remove_blocks()
        db.remove_jobs()

    for i in range(1, n_blocks + 1):
        db.add_block(i, '{0}.{1}'.format(projname, i))
        db.add_job(i, i)

    for i in range(1, n_blocks + 1):
        for j in range(i + 1, n_blocks + 1):
            db.add_job(i, j)

    db.prepare(force=force)

def info():
    db_name = os.path.join('.', 'marveldb')
    db = mj.marvel_db.from_file(db_name)
    project_info = db.info()
    print('MARVEL project started on {0}'.format(project_info['started on']))
    widest = max(map(len, db.info().keys()))
    for k, v in db.info().items():
        print('{0:>{widest}}: {1}'.format(k, v, widest=widest))

# Helper functions for the argument parsing
def directory_exists(s):
    return os.path.exists(s) and os.path.isdir(s)

def positive_integer(i):
    return type(i) is int and i > 0

def parse_args():
    parser = argparse.ArgumentParser(description='Job manager for MARVEL')
    
    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='subcommand')

    # Initialisation
    init_parser = subparsers.add_parser('init', help='Initialise a new project')
    init_parser.add_argument('name', help='name of the project')
    init_parser.add_argument('-x', '--coverage', help='sequencing coverage of'
                             ' the project (default: 30)', default=30,
                             type=int)
    init_parser.add_argument('directory', help='directory where to '
                             'initialise the run (default: .)',
                             default='.', nargs='?')
    init_parser.add_argument('-f', '--force', help='force overwrite of '
                             'existing database', action='store_true')

    # Info
    info_parser = subparsers.add_parser('info', help='Show project info')

    # DBprepare
    prep_parser = subparsers.add_parser('prepare', help='Prepare data files')
    prep_parser.add_argument('fasta', help='input reads in FASTA format')
    prep_parser.add_argument('-s', '--blocksize',
                             help='database block size in megabases (default: 200)',
                             default=200, type=int, metavar='N',
                             dest='blocksize')
    prep_parser.add_argument('-f', '--force', help='force prepare',
                             action='store_true')

    args = parser.parse_args()

    if args.subcommand == 'init':
        args.directory = os.path.abspath(args.directory)
        if not directory_exists(args.directory):
            parser.error('directory not found: {0}'.format(args.directory))
        if not positive_integer(args.coverage):
            parser.error('coverage must be a positive non-zero integer')

    if args.subcommand == 'prepare':
        if not positive_integer(args.blocksize):
            parser.error('blocksize must be a positive non-zero integer')

    if args.subcommand is None:
        parser.parse_args(['-h'])

    return(args)

def main():
    args = parse_args()

    if args.subcommand == 'init':
        init(directory=args.directory, name=args.name,
             coverage=args.coverage, force=args.force)
    if args.subcommand == 'prepare':
        prepare(fasta=args.fasta, blocksize=args.blocksize, force=args.force)
    if args.subcommand == 'info':
        info()

if __name__ == '__main__':
    main()
