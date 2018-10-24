#!/usr/bin/env python

import argparse
import os
import queue
from subprocess import Popen, PIPE
import sys
import time

import marvelous_jobs as mj
from marvelous_jobs import __version__
from marvelous_jobs import marvelous_config as mc
from marvelous_jobs import daligner_job_array, masking_server_job, prepare_job
from marvelous_jobs import slurm_utils

import marvel

def is_project(directory='.'):
    db_name = os.path.abspath(os.path.join(directory, 'marveldb'))
    return os.path.exists(db_name)

def get_database():
    db_name = os.path.join('.', 'marveldb')
    db = mj.marvel_db.from_file(db_name)
    return db

def init(name, coverage, account=None, directory='.', force=False,
         n_jobs=3000):
    if is_project(directory) and not force:
        print('error: project is already initialised, delete '
              '`marveldb` or use --force if you want to start over',
             file=sys.stderr)
        exit(1)
    db_name = os.path.join(directory, 'marveldb')
    mj.marvel_db(filename=db_name, name=name,
                 coverage=coverage, force=force)

    config = mc(cdict={
        'general': {
            'account': account,
            'database': db_name,
            'directory': os.path.abspath(directory),
            'max_number_of_jobs': n_jobs
        },
        'daligner': {
            'verbose': True,
            'identity': True,
            'tuple_suppression_frequency': 20,
            'correlation_rate': 0.7,
            'threads': 4,
            'timelimit': '1-00:00:00',
            'jobs_per_task': 100
        },
        'DMserver': {
            'threads': 4,
            'port': 12345,
            'timelimit': '10-00:00:00'
        }
    })

def prepare(fasta, blocksize, script_directory, log_directory, force=False):
    if not is_project():
        print('error: no project found in current directory, '
              'did you run init?', file=sys.stderr)
        sys.exit(1)

    db = get_database()
    update_statuses()

    if db.is_prepared() and not force:
        print('error: database is already prepared', file=sys.stderr)
        exit(1)
    elif not db.is_prepared() \
            and db.prepare_status() in (slurm_utils.status.pending,
                                        slurm_utils.status.completing):
        print('error: database preparation already in progress',
              file=sys.stderr)
        exit(1)

    projname = db.info('name')

    if not os.path.exists(script_directory):
        os.mkdir(script_directory)
    if not os.path.exists(log_directory):
        os.mkdir(log_directory)

    config = mc()
    config.set('general', 'blocksize', blocksize)
    config.set('general', 'script_directory',
               os.path.abspath(script_directory))
    config.set('general', 'log_directory', os.path.abspath(log_directory))
    config.set('general', 'fasta', os.path.abspath(fasta))

    db.add_prepare_job()
    job = prepare_job(projname, fasta, blocksize,
                      script_directory=config.get('general',
                                                  'script_directory'),
                      log_directory=config.get('general', 'log_directory'),
                      account=config.get('general', 'account'))
    jobid = job.start()
    db.update_prepare_job_id(jobid)

def start_daligner(jobs_per_task=100, force=False, no_masking=False):
    config = mc()
    db = get_database()
    update_statuses()

    if jobs_per_task > config.getint('general', 'max_number_of_jobs'):
        print('error: number of jobs per task ({0}) cannot be more than the '
              'maximum number of jobs ({1})' \
              .format(jobs_per_task, config.get('general', 'max_number_of_jobs')))
        sys.exit(1)

    if not no_masking and \
           (not db.has_masking_job() or \
            db.masking_status()[1] not in (slurm_utils.status.running,
                                           slurm_utils.status.pending)):
        print('error: no masking server job has been started, run mask start',
              file=sys.stderr)
        sys.exit(1)

    if not db.is_prepared():
        prepare_status = db.prepare_status()
        if prepare_status is None:
            print('error: database preparation has not been started, run '
                  'prepare', file=sys.stderr)
        else:
            print('error: database preparation is not done yet, status is {0}'\
                  .format(db.prepare_status()), file=sys.stderr)
        exit(1)

    config.set('daligner', 'jobs_per_task', jobs_per_task)

    projname = db.get_project_name()

    prepare_db_filename = '{0}.db'.format(projname)
    with open(prepare_db_filename) as f:
        for line in f:
            if line.strip().startswith('blocks'):
                n_blocks = int(line.strip().split()[-1])

    mask_jobid = db.get_masking_jobid()

    if force:
        db.remove_blocks()
        stop_daligner()
        db.remove_daligner_jobs()
        daligner_script = os.path.join(config.get('general', 'script_directory'),
                                       daligner_job_array.filename)
        if os.path.exists(daligner_script):
            os.remove(daligner_script)

    print('Adding daligner jobs to database: ', end='')

    total_n_jobs = int(n_blocks + ((n_blocks - 1) * n_blocks) / 2)
    current_job = 0

    n_jobs = 5000
    jobchunk = []

    for i in range(1, n_blocks + 1):
        block_name = '{0}.{1}'.format(projname, i)
        try:
            db.add_block(i, block_name)
        except RuntimeError as rte:
            print('error: {0}, use --force to override'.format(rte), file=sys.stderr)
            exit(1)
        current_job += 1
        jobchunk.append((current_job, i, i, 1, not no_masking))
        if current_job % n_jobs == 0:
            db.add_daligner_jobs(jobchunk)
            jobchunk = []
            print('\rAdding daligner jobs to database: {0}/{1}'.format(current_job, total_n_jobs), end='')

    for i in range(1, n_blocks + 1):
        for j in range(i + 1, n_blocks + 1):
            current_job += 1
            jobchunk.append((current_job, i, j, i + 1, not no_masking))
            if current_job % n_jobs == 0:
                db.add_daligner_jobs(jobchunk)
                jobchunk = []
                print('\rAdding daligner jobs to database: {0}/{1}'.format(current_job, total_n_jobs), end='')

    if len(jobchunk) > 0:
        db.add_daligner_jobs(jobchunk)
        print('\rAdding daligner jobs to database: {0}/{1}'.format(current_job, total_n_jobs), end='')

    print()

def get_daligner_array(ntasks, config, masking_jobid=None):
    job_array = daligner_job_array(ntasks,
                                   config.get('general', 'database'),
                                   script_directory=config.get('general',
                                                               'script_directory'),
                                   log_directory=config.get('general',
                                                            'log_directory'),
                                   jobs_per_task=config.get('daligner',
                                                            'jobs_per_task'),
                                   masking_jobid=masking_jobid,
                                   masking_port=config.getint('DMserver',
                                                              'port'),
                                   account=config.get('general', 'account'),
                                   timelimit=config.get('daligner',
                                                        'timelimit'),
                                   verbose=config.getboolean('daligner',
                                                             'verbose'),
                                   identity=config.getboolean('daligner',
                                                              'identity'),
                                   tuple_suppression_frequency=config.getint('daligner',
                                                                             'tuple_suppression_frequency'),
                                   correlation_rate=config.getfloat('daligner',
                                                                    'correlation_rate'),
                                   threads=config.getint('daligner', 'threads'))

    return job_array

def submit_daligner_jobs(ntasks, config, masking_jobid=None):
    if ntasks == 0:
        return
    job_array = get_daligner_array(ntasks, config, masking_jobid)
    return job_array.start()

def update_daligner_queue():
    config = mc()
    db = get_database()
    update_statuses()

    jobs_not_started = db.n_daligner_jobs(slurm_utils.status.notstarted)
    if jobs_not_started == 0:
        print('error: no jobs left to queue', file=sys.stderr)
        sys.exit(1)

    max_jobs = min(jobs_not_started,
                   config.getint('general', 'max_number_of_jobs'))
    jobs_per_task = config.getint('daligner', 'jobs_per_task')
    queued_jobs = db.n_daligner_jobs(slurm_utils.status.reserved,
                                     slurm_utils.status.running,
                                     slurm_utils.status.pending,
                                     slurm_utils.status.completing,
                                     slurm_utils.status.configuring)

    if db.any_using_masking():
        queued_jobs += 1

    if max_jobs < jobs_per_task:
        tasks_to_queue = 1
        print('Queuing {0} jobs...'.format(max_jobs - queued_jobs))
    else:
        tasks_to_queue = (max_jobs - queued_jobs) // jobs_per_task
        print('Queuing {0} jobs...'.format(tasks_to_queue * jobs_per_task))

    if tasks_to_queue == 0:
        return

    try:
        jobid = submit_daligner_jobs(tasks_to_queue, config, db.get_masking_jobid())
    except RuntimeError as rte:
        print('error: job submission failed\n{0}'.format(rte), file=sys.stderr)
        sys.exit(1)

def stop_daligner(status=(slurm_utils.status.running,
                          slurm_utils.status.pending)):
    db = get_database()
    update_statuses()

    jobs = db.get_daligner_jobs(status=tuple(status))
    jobids = db.get_daligner_jobids(jobs)

    # For now stop whole arrays, so if only running jobs
    # are supposed to be stopped, also pending jobs in the
    # that are in the same array as the running jobs will
    # be stopped.
    array_ids = set()
    for ji in jobids.values():
        array_id = ji.split('_')[0]
        array_ids.add(array_id)

    print('Stopping daligner jobs...')

    if len(array_ids) > 0:
        slurm_utils.cancel_jobs(array_ids)
    # Also cancel any reservations. If jobs have been reserved,
    # then their corresponding array has been started and will
    # be cancelled regardless.
    db.cancel_daligner_reservation()

    update_statuses()

def reserve_daligner(n_jobs, cancel=False):
    config = mc()
    db = get_database()

    if cancel:
        db.cancel_daligner_reservation()
        return

    jobs = db.reserve_daligner_jobs(n_jobs)

    for j in jobs:
        print('{rowid}\t{block_id1}\t{block_id2}'.format(**j))

def start_mask(threads=4, port=12345, constraint=None, cluster=None):
    config = mc()
    db = get_database()

    db.update_masking_job_status()
    masking_status = db.masking_status()

    config.set('DMserver', 'threads', threads)
    config.set('DMserver', 'port', port)
    config.set('DMserver', 'constraint', constraint)
    config.set('DMserver', 'cluster', cluster)
    config.set('DMserver', 'checkpoint_file',
               os.path.join(config.get('general', 'directory'),
                            'masking_checkpoint'))

    if masking_status is not None \
       and masking_status[1] in (slurm_utils.status.running,
                                 slurm_utils.status.pending,
                                 slurm_utils.status.completing):
        print('error: masking server already queued/running', file=sys.stderr)
        sys.exit(1)

    job = masking_server_job(db.get_project_name(),
                             db.get_coverage(),
                             config.get('DMserver', 'checkpoint_file'),
                             script_directory=config.get('general',
                                                         'script_directory'),
                             log_directory=config.get('general',
                                                      'log_directory'),
                             port=port,
                             threads=threads,
                             constraint=constraint,
                             cluster=cluster,
                             account=config.get('general', 'account'),
                             timelimit=config.get('DMserver', 'timelimit'))
    db.add_masking_job(job)
    job.save_script()

    try:
        jobid = job.start()
    except RuntimeError as rte:
        print('error: failed to start masking server', file=sys.stderr)
        print(rte, file=sys.stderr)
        sys.exit(1)

    db.update_masking_job_id(jobid=jobid)

def stop_mask():
    config = mc()
    db = get_database()

    masking_status = db.masking_status()

    if masking_status is None:
        print('error: no masking server initialised', file=sys.stderr)
        sys.exit(1)

    if masking_status[1] not in (slurm_utils.status.running,
                                 slurm_utils.status.pending):
        print('error: masking server already stopped', file=sys.stderr)
        sys.exit(1)

    stop_daligner()

    job = masking_server_job(db.get_project_name(),
                             db.get_coverage(),
                             jobid=masking_status[0],
                             script_directory=config.get('general',
                                                         'script_directory'),
                             log_directory=config.get('general',
                                                      'log_directory'),
                             port=config.getint('DMserver', 'port'),
                             threads=config.getint('DMserver', 'threads'),
                             constraint=config.get('DMserver', 'constraint'),
                             cluster=config.get('DMserver', 'cluster'),
                             account=config.get('general', 'account'),
                             timelimit=config.get('DMserver', 'timelimit'))

    print('Stopping masking server...')

    if masking_status[1] == slurm_utils.status.running:
        job.stop()

    try:
        job.cancel()
    except RuntimeError as rte:
        print('error: {0}'.format(rte), file=sys.stderr)
        sys.exit(1)

    try:
        db.update_masking_job_status()
    except ValueError as ve:
        print('error: {0}'.format(ve), file=sys.stderr)
        sys.exit(1)

def mask_status():
    db_name = os.path.join('.', 'marveldb')
    db = mj.marvel_db.from_file(db_name)
    db.update_masking_job_status()
    masking_status = db.masking_status()
    if masking_status is None:
        print('No masking server initialised, did you run mask start?')
    else:
        print('Job {0}: {1}, last update on {2}'.format(*masking_status))

def info():
    if not is_project():
        print('error: no project found in current directory, '
              'did you run init?', file=sys.stderr)
        sys.exit(1)
    db = get_database()
    update_statuses()
    project_info = db.info()
    print('MARVEL project started on {0}'.format(project_info['started on']))
    widest = max(map(len, db.info().keys()))
    for k, v in db.info().items():
        print('{0:>{widest}}: {1}'.format(k, v, widest=widest))

def update_and_restart():
    config = mc()
    db = get_database()
    update_statuses()

    old_masking_ip = db.get_masking_ip()
    masking_status = db.masking_status()

    if masking_status is not None \
            and masking_status[1] not in \
            (slurm_utils.status.notstarted,
             slurm_utils.status.running,
             slurm_utils.status.pending) \
            and db.any_using_masking():
        print('Masking server is down, restarting...')
        stop_daligner(status=(slurm_utils.status.running,))
        start_mask(threads=config.get('DMserver', 'threads'),
                   port=config.get('DMserver', 'port'),
                   constraint=config.get('DMserver', 'constraint'),
                   cluster=config.get('DMserver', 'cluster'))

    update_statuses()

    masking_ip_changed = old_masking_ip != db.get_masking_ip()

    print('Restarting failed or cancelled jobs...')
    if masking_ip_changed:
        print('Masking server IP changed, also restarting pending jobs...')

    failed_jobs = db.get_daligner_jobs(status=(slurm_utils.status.failed,
                                               slurm_utils.status.cancelled,
                                               slurm_utils.status.timeout))

    if len(failed_jobs) > 0:
        jobid = submit_daligner_jobs(failed_jobs, config, db.get_masking_jobid)
        db.update_daligner_jobs(failed_jobs, jobid)

    if masking_ip_changed:
        pending_jobs = db.get_daligner_jobs(status=(slurm_utils.status.pending))
        if len(pending_jobs) > 0:
            slurm_utils.cancel_jobs(pending_jobs)
            jobid = submit_daligner_jobs(pending_jobs, config,
                                         db.get_masking_jobid)
            db.update_daligner_jobs(pending_jobs, jobid)

    update_statuses()

def update_statuses():
    db = get_database()

    try:
        db.update_masking_job_status()
    except ValueError as ve:
        print('warning: {0}, job has most likely expired'.format(ve),
              file=sys.stderr)
    if not db.is_prepared():
        db.update_prepare_job_status()

    prepare_status = db.prepare_status()
    if not db.is_prepared() and prepare_status == slurm_utils.status.completed:
        db.prepare()

    # Update status of jobs that have been submitted but not failed or
    # completed, i.e. their job state is one of CONFIGURING, RUNNING,
    # PENDING, or COMPLETING
    start = time.time()
    djs = db.get_daligner_jobs(status=(slurm_utils.status.pending,
                                       slurm_utils.status.configuring,
                                       slurm_utils.status.running,
                                       slurm_utils.status.completing))
    print('fetched jobs in {0}'.format(time.time() - start))
    db.update_daligner_jobs(djs)

# Helper functions for the argument parsing
def directory_exists(s):
    return os.path.exists(s) and os.path.isdir(s)

def positive_integer(i):
    return type(i) is int and i > 0

def parse_args():
    parser = argparse.ArgumentParser(description='Job manager for MARVEL')

    parser.add_argument('--version', action='version',
                        version='%(prog)s v{0}'.format(__version__))

    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='subcommand')

    # Initialisation
    init_parser = subparsers.add_parser('init', help='Initialise a new project')
    init_parser.add_argument('name', help='name of the project')
    init_parser.add_argument('-A', '--account', help='SLURM account where '
                             'jobs should be run')
    init_parser.add_argument('-x', '--coverage', help='sequencing coverage of'
                             ' the project (default: 30)', default=30,
                             type=int)
    init_parser.add_argument('-n', '--max-jobs', help='maximum number of '
                             'jobs allowed to be submitted at any point '
                             '(default: 3000)',
                             type=int, default=3000)
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
    prep_parser.add_argument('-d', '--script-directory', help='directory where to '
                             'store scripts (default: scripts)',
                             default=os.path.join('.', 'scripts'))
    prep_parser.add_argument('-l', '--log-directory', help='directory where to '
                             'store log files (default: logs)',
                             default=os.path.join('.', 'logs'))

    # Masking server
    mask_parser = subparsers.add_parser('mask', help='Masking server')
    mask_subparsers = mask_parser.add_subparsers(title='masking command',
                                                 dest='subsubcommand')

    # Masking server status
    mask_status = mask_subparsers.add_parser('status', help='masking server '
                                             'status')

    # Start masking server
    mask_start = mask_subparsers.add_parser('start', help='start masking server')
    mask_start.add_argument('-C', '--constraint', help='node constraint')
    mask_start.add_argument('-t', '--threads', help='number of worker threads '
                            '(default: 4)',
                            type=int, default=4)
    mask_start.add_argument('-p', '--port', help='port to listen to (default: '
                            '12345)', type=int, default=12345)
    mask_start.add_argument('-M', '--cluster', help='cluster to run on')

    # Stop masking server
    mask_stop = mask_subparsers.add_parser('stop', help='stop masking server')

    # daligner
    dalign_parser = subparsers.add_parser('daligner', help='Run daligner')
    dalign_subparsers = dalign_parser.add_subparsers(title='daligner command',
                                                     dest='subsubcommand')

    # daligner start
    dalign_start = dalign_subparsers.add_parser('start', help='initialise '
                                                'daligner jobs')
    dalign_start.add_argument('-n', '--jobs-per-task', help='number of jobs '
                              'that each task in a job array will run',
                              type=int, default=100)
    dalign_start.add_argument('-f', '--force', help='forcefully add daligner '
                              'jobs, removing any existing jobs',
                              action='store_true')
    dalign_start.add_argument('--no-masking', help='do not use the masking '
                              'server', action='store_true')

    # daligner update
    dalign_update = dalign_subparsers.add_parser('update', help='submit '
                                                 'daligner jobs')

    # daligner stop
    dalign_stop = dalign_subparsers.add_parser('stop', help='stop daligner jobs')

    # daligner list
    dalign_reserve = dalign_subparsers.add_parser('reserve', help='reserve '
                                                  'daligner jobs')
    dalign_reserve.add_argument('-n', help='maximum number of jobs to reserve '
                                '(default: 1)', default=1, type=int)
    dalign_reserve.add_argument('--cancel', help='cancel all active '
                                'reservations', action='store_true')

    # Update status and restart jobs if necessary
    fix_parser = subparsers.add_parser('fix', help='Update and restart jobs')

    args = parser.parse_args()

    # Argument validation
    if args.subcommand == 'init':
        args.directory = os.path.abspath(args.directory)
        if not directory_exists(args.directory):
            parser.error('directory not found: {0}'.format(args.directory))
        if not positive_integer(args.coverage):
            parser.error('coverage must be a positive non-zero integer')
        if not positive_integer(args.max_jobs):
            parser.error('number of jobs must be a positive non-zer integer')

    if args.subcommand == 'prepare':
        if not positive_integer(args.blocksize):
            parser.error('blocksize must be a positive non-zero integer')

    if args.subcommand == 'mask' and args.subsubcommand == 'start':
        if not positive_integer(args.threads):
            parser.error('number of threads must be a positive non-zero integer')
        if not positive_integer(args.port):
            parser.error('port must be a positive non-zero integer')
            parser.error('{0} is not a valid node'.format(args.node))

    if args.subcommand == 'daligner' and args.subsubcommand == 'start':
        if not positive_integer(args.jobs_per_task):
            parser.error('jobs per task must be a positive non-zero integer')
        if args.jobs_per_task > 1000:
            parser.error('jobs per task cannot exceed 1000')
    if args.subcommand == 'daligner' and args.subsubcommand == 'reserve':
        if not positive_integer(args.n):
            parser.error('number of jobs must be a positive non-zero integer')

    if args.subcommand is None:
        parser.parse_args(['-h'])

    return(args)

def main():
    args = parse_args()

    if args.subcommand == 'init':
        init(directory=args.directory, name=args.name,
             account=args.account,
             coverage=args.coverage, force=args.force,
             n_jobs=args.max_jobs)
    elif not is_project():
        print('error: no project found, have you run init?', file=sys.stderr)
        sys.exit(1)
    if args.subcommand == 'prepare':
        prepare(fasta=args.fasta,
                blocksize=args.blocksize,
                script_directory=args.script_directory,
                log_directory=args.log_directory,
                force=args.force)
    if args.subcommand == 'mask' and args.subsubcommand == 'start':
        start_mask(args.threads, args.port, args.constraint, args.cluster)
    if args.subcommand == 'mask' and args.subsubcommand == 'status':
        mask_status()
    if args.subcommand == 'mask' and args.subsubcommand == 'stop':
        stop_mask()
    if args.subcommand == 'daligner' and args.subsubcommand == 'start':
        start_daligner(args.jobs_per_task, args.force, args.no_masking)
    if args.subcommand == 'daligner' and args.subsubcommand == 'update':
        update_daligner_queue()
    if args.subcommand == 'daligner' and args.subsubcommand == 'stop':
        stop_daligner()
    if args.subcommand == 'daligner' and args.subsubcommand == 'reserve':
        reserve_daligner(n_jobs=args.n, cancel=args.cancel)
    if args.subcommand == 'fix':
        update_and_restart()
    if args.subcommand == 'info':
        info()

if __name__ == '__main__':
    main()
