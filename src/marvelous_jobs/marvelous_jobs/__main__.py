#!/usr/bin/env python

import argparse
import hashlib
import os
import queue
import re
import sqlite3
from subprocess import Popen, PIPE
import sys
import time

import marvelous_jobs as mj
from marvelous_jobs import __version__
from marvelous_jobs import marvelous_config as mc
from marvelous_jobs import daligner_job_array
from marvelous_jobs import masking_server_job
from marvelous_jobs import prepare_job
from marvelous_jobs import merge_job_array
from marvelous_jobs import annotate_job_array
from marvelous_jobs import patch_job_array
from marvelous_jobs import stats_job_array
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
         n_jobs=1000):
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

def start_daligner(jobs_per_task=100, max_simultaneous_tasks=None,
                   force=False, no_masking=False, comparisons_per_job=1):
    config = mc()
    db = get_database()
    update_statuses()

    run_directory = os.path.join(config.get('general', 'directory'),
                                 'daligner_runs')
    config.set('daligner', 'run_directory', run_directory)
    if run_directory is not None and not os.path.isdir(run_directory):
        os.mkdir(run_directory)

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
    config.set('daligner', 'max_simultaneous_tasks', max_simultaneous_tasks)
    config.set('daligner', 'comparisons_per_job', comparisons_per_job)

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

def get_daligner_array(ntasks, config, db, masking_jobid=None):
    # Reserve jobs
    reservation_token = hashlib.md5(str(time.time()).encode('utf-8')).hexdigest()
    n_jobs = 0
    rowids = []
    for i in range(1, ntasks + 1):
        reservation = db.reserve_daligner_jobs(token='{0}_{1}'.format(reservation_token, i),
                                               max_jobs=config.getint('daligner', 'jobs_per_task'),
                                               comparisons_per_job=config.getint('daligner',
                                                                                 'comparisons_per_job'))
        reservation_filename = os.path.join(
            config.get('daligner', 'run_directory'),
            'daligner_task_{0}_{1}.txt'.format(reservation_token, i))
        with open(reservation_filename, 'w') as f:
            for d in reservation:
                rowids += d['rowids']
                n_jobs += len(d['rowids'])
                f.write('\t'.join(map(str, [d['source_block']] + \
                                            d['rowids'] + \
                                            d['target_blocks'])) + '\n')
    job_array = daligner_job_array(ntasks,
                                   config.get('general', 'database'),
                                   reservation_token = reservation_token,
                                   run_directory = config.get('daligner',
                                                              'run_directory'),
                                   script_directory=config.get('general',
                                                               'script_directory'),
                                   log_directory=config.get('general',
                                                            'log_directory'),
                                   jobs_per_task=config.get('daligner',
                                                            'jobs_per_task'),
                                   max_simultaneous_tasks=config.getint('daligner',
                                                                        'max_simultaneous_tasks'),
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

    return job_array, n_jobs, rowids

def submit_daligner_jobs(ntasks, config, db, masking_jobid=None):
    if ntasks == 0:
        return
    job_array, n_jobs, rowids = get_daligner_array(ntasks, config, db, masking_jobid)
    return job_array.start(), n_jobs, rowids

def backup_database():
    config = mc()
    db = get_database()
    print('Checking database integrity...')
    if not db.integrity_check():
        print('error: database corrupted, manual cleanup required',
              file=sys.stderr)
        sys.exit(1)
    print('Backing up database...')
    db.backup('{0}.backup'.format(config.get('general', 'database')))

def update_daligner_queue(n_tasks):
    config = mc()
    db = get_database()
    print('Checking database integrity...')
    if not db.integrity_check():
        print('error: database corrupted, manual cleanup required',
              file=sys.stderr)
        sys.exit(1)
    print('Backing up database...')
    db.backup('{0}.backup'.format(config.get('general', 'database')))
    update_statuses()

    jobs_not_started = db.n_daligner_jobs(slurm_utils.status.notstarted)
    if jobs_not_started == 0:
        print('error: no jobs left to queue', file=sys.stderr)
        sys.exit(1)

    max_slurm_jobs = config.getint('general', 'max_number_of_jobs')
    if n_tasks is None:
        n_tasks = max_slurm_jobs

    jobs_per_task = config.getint('daligner', 'jobs_per_task') * \
        config.getint('daligner', 'comparisons_per_job')
    max_tasks = min(jobs_not_started // jobs_per_task,
                    n_tasks)
    queued_tasks = db.get_n_running_tasks()

    if db.any_using_masking():
        queued_tasks += 1

    if max_tasks == 0:
        tasks_to_queue = 1
    elif max_slurm_jobs - queued_tasks < max_tasks:
        tasks_to_queue = max_slurm_jobs - queued_tasks
    else:
        tasks_to_queue = max_tasks

    if tasks_to_queue == 0:
        print('Queueing no jobs...')
        return

    print('Queueing jobs...')

    try:
        jobid, n_jobs, rowids = submit_daligner_jobs(tasks_to_queue, config, db, db.get_masking_jobid())
        db.set_daligner_jobids(rowids, jobid)
    except RuntimeError as rte:
        print('error: job submission failed\n{0}'.format(rte), file=sys.stderr)
        sys.exit(1)

    print('Queued {0} jobs'.format(n_jobs))

def stop_daligner(status=(slurm_utils.status.running,
                          slurm_utils.status.pending)):
    db = get_database()
    update_statuses()

    jobs = db.get_daligner_jobs(status=tuple(status))
    jobids = db.get_daligner_jobids(jobs)

    unique_tasks = set()
    for ji in jobids.values():
        unique_tasks.add(ji)

    print('Stopping daligner jobs...')

    if len(unique_tasks) > 0:
        slurm_utils.cancel_jobs(unique_tasks)
    # Also cancel any reservations. If jobs have been reserved,
    # then their corresponding array has been started and will
    # be cancelled regardless.
    db.cancel_daligner_reservation()

    update_statuses()

def get_reservation_token():
    return hashlib.md5(str(time.time()).encode('utf-8')).hexdigest()

def cancel_daligner_reservation():
    config = mc()
    db = get_database()

    reserved_ids = db.get_daligner_jobs(status=slurm_utils.status.reserved)
    reserved_jobids = set(db.get_daligner_jobids(reserved_ids).values())
    db.cancel_daligner_reservation()
    slurm_utils.cancel_jobs(reserved_jobids)

def list_blocks():
    config = mc()
    db = get_database()

    blocks = db.get_blocks()

    print('\n'.join(map(str, blocks)))

def list_completed_blocks():
    config = mc()
    db = get_database()

    blocks = db.get_completed_blocks()

    print('\n'.join(map(str, blocks)))

def merge_blocks(n, n_files, max_simultaneous_tasks=None):
    config = mc()
    db = get_database()

    project = db.get_project_name()
    print('Fetching completed blocks...')
    blocks = db.get_completed_blocks()

    merged_filename = os.path.join(config.get('general', 'directory'),
                                   '{}.{{}}.las'.format(project))

    run_directory = os.path.join(config.get('general', 'directory'),
                                 'merge_runs')
    if not os.path.isdir(run_directory):
        os.mkdir(run_directory)

    reservation_token = get_reservation_token()
    reservation_file = os.path.join(run_directory, 'merge_task_{}_{{}}.txt' \
                                    .format(reservation_token))

    blocks_to_merge = []
    task_id = 0
    print('Reserving blocks...')
    for b in blocks:
        if len(blocks_to_merge) == n:
            break

        fname = merged_filename.format(b)
        if os.path.exists(fname):
            # print('{} already exists'.format(os.path.basename(fname)))
            continue

        open(fname, 'a').close()
        blocks_to_merge.append(b)
        task_id += 1
        with open(reservation_file.format(task_id), 'w') as f:
            f.write('{}\n'.format(b))

    print('Reserved {} blocks'.format(len(blocks_to_merge)))

    merge_job = merge_job_array(blocks_to_merge,
                                project,
                                n_files=n_files,
                                max_simultaneous_tasks=max_simultaneous_tasks,
                                script_directory=config.get('general',
                                                            'script_directory'),
                                log_directory=config.get('general',
                                                         'log_directory'),
                                reservation_token=reservation_token,
                                run_directory=run_directory,
                                account=config.get('general', 'account'),
                                verbose=False)

    jobid = merge_job.start()

    print('Jobs submitted in job array {}'.format(jobid))

def get_merged_blocks():
    """Get a list of merged blocks.

    A block is considered merged if the merged file exists,
    has a size larger than 0, and the latest modification of
    it happened at least five minutes ago.

    Returns
    -------
    list
        A list of integers corresponding to the IDs of
        merged blocks.
    """
    config = mc()
    db = get_database()

    directory = config.get('general', 'directory')
    project = db.get_project_name()

    las_regex = re.compile(r'^{}\.(\d+)\.las$'.format(project))

    merged_blocks = []
    for fname in os.listdir(directory):
        if las_regex.match(fname):
            fs = os.stat(os.path.join(directory, fname))
            if fs.st_size > 0 and \
                    time.time() - fs.st_mtime > 5*60:
                merged_blocks.append(int(las_regex.match(fname).group(1)))

    return sorted(merged_blocks)

def annotate_blocks(n, max_simultaneous_tasks, force=False):
    config = mc()
    db = get_database()

    project = db.get_project_name()
    directory = config.get('general', 'directory')
    print('Fetching merged blocks...')
    blocks = get_merged_blocks()
    print('{} merged blocks found'.format(len(blocks)))

    run_directory = os.path.join(directory, 'annotate_runs')
    if not os.path.exists(run_directory):
        os.mkdir(run_directory)
    reservation_token = get_reservation_token()
    reservation_file = os.path.join(run_directory,
                                    'annotate_task_{}_{{}}.txt' \
                                        .format(reservation_token))

    q_file = os.path.join(directory, '.{}.{{}}.q.a2'.format(project))
    trim_file = os.path.join(directory, '.{}.{{}}.trim.a2'.format(project))

    print('Reserving maximum {} blocks...'.format(n))
    blocks_to_annotate = []
    task_id = 0
    for b in blocks:
        if not force \
           and os.path.exists(q_file.format(b)) \
           and os.path.exists(trim_file.format(b)):
            continue

        if len(blocks_to_annotate) == n:
            break

        open(trim_file.format(b), 'a').close()
        open(q_file.format(b), 'a').close()
        blocks_to_annotate.append(b)

        task_id += 1
        with open(reservation_file.format(task_id), 'w') as f:
            f.write('{}\n'.format(b))
    print('Reserved {} blocks'.format(len(blocks_to_annotate)))
    if len(blocks_to_annotate) == 0:
        print('No blocks to annotate')
        return

    job = annotate_job_array(blocks_to_annotate,
                             project,
                             max_simultaneous_tasks,
                             script_directory=config.get('general',
                                                         'script_directory'),
                             log_directory=config.get('general',
                                                      'log_directory'),
                             reservation_token=reservation_token,
                             run_directory=run_directory,
                             account=config.get('general', 'account'))

    jobid = job.start()

    print('Jobs submitted in job array {}'.format(jobid))

def patch_blocks(n, max_simultaneous_tasks, force=False):
    config = mc()
    db = get_database()

    project = db.get_project_name()
    directory = config.get('general', 'directory')

    # Check if the merged annotation files are available
    q_files = [os.path.join(directory, '{}.q.{}2'.format(project, x)) \
              for x in ['a', 'd']]
    trim_files = [os.path.join(directory, '{}.trim.{}2'.format(project, x)) \
                  for x in ['a', 'd']]
    block_annot_file = os.path.join(directory, '.{}.{{}}.{{}}.{{}}2'.format(project))
    if not all(os.path.exists(x) for x in q_files) \
       or not all(os.path.exists(y) for y in trim_files):
        print('Merged annotation files not found, '
              'looking for block annotations.')
        # Are all block-specific files present?
        missing_annot = False
        for b in range(1, db.get_n_blocks()+1):
            if not os.path.exists(block_annot_file.format(b, 'q', 'a')) \
               or not os.path.exists(block_annot_file.format(b, 'q', 'd')) \
               or not os.path.exists(block_annot_file.format(b, 'trim', 'a')) \
               or not os.path.exists(block_annot_file.format(b, 'trim', 'd')):
                missing_annot = True
                break
        if missing_annot:
            print('Some block annotations are missing.\n'
                  'Run marvelous_jobs blocks annotate to generate these.',
                  file=sys.stderr)
        else:
            print('All block annotations are ready, '
                  'but TKmerge has to be run before '
                  'patching can start')
        exit(1)

    print('Fetching merged blocks...')
    blocks = get_merged_blocks()
    print('{} merged blocks found'.format(len(blocks)))

    run_directory = os.path.join(directory, 'patch_runs')
    if not os.path.exists(run_directory):
        os.mkdir(run_directory)
    reservation_token = get_reservation_token()
    reservation_file = os.path.join(run_directory,
                                    'patch_task_{}_{{}}.txt' \
                                    .format(reservation_token))

    #fasta_file = os.path.join(directory, '{}.{{}}.fixed.fasta'.format(project))
    q_file = os.path.join(directory, '.{}.{{}}.q.a2'.format(project))

    print('Reserving maximum {} blocks...'.format(n))
    blocks_to_patch = []
    task_id = 0
    for b in blocks:
        if not os.path.exists(q_file.format(b)):
            continue

        fasta_file = os.path.join(directory,
                                  patch_job_array.out_filename \
                                    .format(db=project, block=b))
        if not force \
           and os.path.exists(fasta_file):
            continue

        if len(blocks_to_patch) == n:
            break

        open(fasta_file, 'a').close()
        blocks_to_patch.append(b)

        task_id += 1
        with open(reservation_file.format(task_id), 'w') as f:
            f.write('{}\n'.format(b))
    print('Reserved {} blocks'.format(len(blocks_to_patch)))
    if len(blocks_to_patch) == 0:
        print('No blocks available to patch')
        return

    job = patch_job_array(blocks_to_patch,
                          project,
                          max_simultaneous_tasks,
                          script_directory=config.get('general',
                                                      'script_directory'),
                          log_directory=config.get('general', 'log_directory'),
                          reservation_token=reservation_token,
                          run_directory=run_directory,
                          account=config.get('general', 'account'))

    jobid = job.start()

    print('Jobs submitted in job array {}'.format(jobid))

def block_stats(n, max_simultaneous_tasks=None, force=False):
    config = mc()
    db = get_database()

    project = db.get_project_name()
    directory = config.get('general', 'directory')

    get_merged_blocks()

    print('Fetching merged blocks...')
    blocks = get_merged_blocks()
    print('{} merged blocks found'.format(len(blocks)))

    run_directory = os.path.join(directory, 'stats_runs')
    if not os.path.exists(run_directory):
        os.mkdir(run_directory)
    reservation_token = get_reservation_token()
    reservation_file = os.path.join(run_directory,
                                    'stats_task_{}_{{}}.txt' \
                                        .format(reservation_token))

    output_dir = os.path.join(directory, 'stats')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    stats_file_template = os.path.join(output_dir, '{}.{}.stats.txt')

    print('Reserving maximum {} blocks...'.format(n))
    blocks_to_do = []
    task_id = 0
    for b in blocks:
        if len(blocks_to_do) == n:
            break

        stats_file = stats_file_template.format(project, b)
        if not force and os.path.exists(stats_file):
            continue

        open(stats_file, 'a').close()

        blocks_to_do.append(b)
        task_id += 1
        with open(reservation_file.format(task_id), 'w') as f:
            f.write('{}\n'.format(b))
    print('Reserved {} blocks'.format(len(blocks_to_do)))
    if len(blocks_to_do) == 0:
        print('No blocks to process')
        return

    job = stats_job_array(blocks_to_do,
                          project,
                          stats_file_template,
                          max_simultaneous_tasks=max_simultaneous_tasks,
                          script_directory=config.get('general',
                                                      'script_directory'),
                          log_directory=config.get('general', 'log_directory'),
                          reservation_token=reservation_token,
                          run_directory=run_directory,
                          account=config.get('general', 'account'))

    jobid = job.start()

    print('Jobs submitted in job array {}'.format(jobid))

def list_reservations():
    config = mc()
    db = get_database()

    update_statuses()

    rowids = db.get_daligner_jobs(status = slurm_utils.status.reserved)
    blocks = db.get_daligner_blocks(rowids, with_jobid=True)

    for b1, b2, jobid in blocks:
        print('{0:4}\t{1:4}\t{2}'.format(b1, b2, jobid))

def start_mask(threads=None, port=None, constraint=None, cluster=None):
    config = mc()
    db = get_database()

    db.update_masking_job_status()
    masking_status = db.masking_status()

    if threads is None and config.get('DMserver', 'threads') is None:
        config.set('DMserver', 'threads', 4)
    elif threads is not None:
        config.set('DMserver', 'threads', threads)

    if port is None and config.get('DMserver', 'port') is None:
        config.set('DMserver', 'port', 12345)
    elif port is not None:
        config.set('DMserver', 'port', port)

    if constraint is None and config.get('DMserver', 'constraint') is None:
        config.set('DMserver', 'constraint', None)
    elif constraint is not None:
        config.set('DMserver', 'constraint', constraint)

    if cluster is None and config.get('DMserver', 'cluster') is None:
        config.set('DMserver', 'cluster', None)
    elif cluster is not None:
        config.set('DMserver', 'cluster', cluster)

    if config.get('DMserver', 'checkpoint_file') is None:
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
                             port=config.getint('DMserver', 'port'),
                             threads=config.getint('DMserver', 'threads'),
                             constraint=config.get('DMserver', 'constraint'),
                             cluster=config.get('DMserver', 'cluster'),
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
                             config.get('DMserver', 'checkpoint_file'),
                             script_directory=config.get('general',
                                                         'script_directory'),
                             log_directory=config.get('general',
                                                      'log_directory'),
                             jobid=masking_status[0],
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

    print('Resetting failed or cancelled jobs...')
    if masking_ip_changed:
        print('Masking server IP changed, also resetting pending jobs...')

    failed_jobs = db.get_daligner_jobs(status=(slurm_utils.status.failed,
                                               slurm_utils.status.cancelled,
                                               slurm_utils.status.timeout))

    if len(failed_jobs) > 0:
        db.reset_daligner_jobs(failed_jobs)

    # Remove the daligner script to avoid having old dependencies lying around
    daligner_script = os.path.join(config.get('general', 'script_directory'),
                                   daligner_job_array.filename)
    if os.path.exists(daligner_script):
        os.remove(daligner_script)

    update_statuses()

def update_statuses():
    config = mc()
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

    # Update status of jobs that have a status of RESERVED or RUNNING.
    # At the moment these are the only two statuses that a daligner job
    # can have.
    start = time.time()
    djs = db.get_daligner_jobs(status=(slurm_utils.status.running,
                                       slurm_utils.status.reserved))
    print('fetched jobs in {0}'.format(time.time() - start))
    db.update_daligner_jobs(djs, log_directory=config.get('general',
                                                          'log_directory'))

# Helper functions for the argument parsing
def directory_exists(s):
    return os.path.exists(s) and os.path.isdir(s)

def positive_integer(i):
    return type(i) is int and i > 0

def parse_args():
    parser = argparse.ArgumentParser(description='Job manager for MARVEL.')

    parser.add_argument('--version', action='version',
                        version='%(prog)s v{0}'.format(__version__))

    subparsers = parser.add_subparsers(dest='subcommand',
                                       metavar='sub-command')
    subparsers.required = True

    # Initialisation
    init_parser = subparsers.add_parser('init', help='Initialise a new project',
        description='Initialise a new MARVEL project.')
    init_parser.add_argument('name', help='name of the project')
    init_parser.add_argument('-A', '--account', help='SLURM account where '
                             'jobs should be run')
    init_parser.add_argument('-x', '--coverage', help='sequencing coverage of'
                             ' the project (default: 30)', default=30,
                             type=int)
    init_parser.add_argument('-n', '--max-jobs', help='maximum number of '
                             'jobs allowed to be submitted in a single array '
                             '(default: 1000)',
                             type=int, default=1000)
    init_parser.add_argument('directory', help='directory where to '
                             'initialise the run (default: .)',
                             default='.', nargs='?')
    init_parser.add_argument('-f', '--force', help='force overwrite of '
                             'existing database', action='store_true')

    # Info
    info_parser = subparsers.add_parser('info', help='Show project info',
        description='Show project information.')

    # Backup
    backup_parser = subparsers.add_parser('backup', help='Backup the database',
        description='Make a backup of the marvelous_jobs database.')

    # DBprepare
    prep_parser = subparsers.add_parser('prepare', help='Prepare data files',
        description='Prepare sequence data for MARVEL by splitting it into '
                    'blocks and converting it to an appropriate format.')
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
    mask_parser = subparsers.add_parser('mask', help='Masking server',
        description='Manage the masking server.')
    mask_subparsers = mask_parser.add_subparsers(dest='subsubcommand',
                                                 metavar='masking-command')
    mask_subparsers.required = True

    # Masking server status
    mask_status = mask_subparsers.add_parser('status', help='masking server '
                                             'status',
        description='Show the status of the masking server.')

    # Start masking server
    mask_start = mask_subparsers.add_parser('start', help='start masking server',
                                            description='Start the masking '
                                            'server.')
    mask_start.add_argument('-C', '--constraint', help='node constraint')
    mask_start.add_argument('-t', '--threads', help='number of worker threads '
                            '(default: 4)',
                            type=int)
    mask_start.add_argument('-p', '--port', help='port to listen to (default: '
                            '12345)', type=int)

    # Stop masking server
    mask_stop = mask_subparsers.add_parser('stop', help='stop masking server',
                                           description='Stop the masking server.')

    # blocks
    block_parser = subparsers.add_parser('blocks', help='List block '
                                         'information')
    block_parser.add_argument('--complete', help='list completed blocks',
                              action='store_true')

    block_subparsers = block_parser.add_subparsers(dest='subsubcommand',
                                                   metavar='block-command')

    # blocks merge
    block_merge = block_subparsers.add_parser('merge', help='merge blocks',
        description='Validate and merge LAS files from a block into a single '
                    'LAS file.')
    block_merge.add_argument('-n', help='maximum number of blocks to process '
                             '(default: 1)', type=int, default=1)
    block_merge.add_argument('--max-simultaneous-tasks', help='maximum number '
                             'of tasks allowed to run simultaneously '
                             '(default: N)', type=int)
    block_merge.add_argument('-m', help='number of files to process '
                             'simultaneously (default: 32)', type=int,
                             default=32)

    # blocks annotate
    block_annotate = block_subparsers.add_parser('annotate',
                                                 help='annotate blocks',
                                                 description='Create quality '
                                                 'and trim annotation tracks '
                                                 'for a LAS file.')
    block_annotate.add_argument('-n', help='maximum number of blocks to process '
                                '(default: 1)', type=int, default=1)
    block_annotate.add_argument('--max-simultaneous-tasks', help='maximum number '
                                'of tasks allowed to run simultaneously '
                                '(default: N)', type=int)
    block_annotate.add_argument('-f', '--force', help='start annotation even if '
                                'annotation files already exist',
                                action='store_true')

    # blocks patch
    block_patch = block_subparsers.add_parser('patch',
                                              help='patch blocks',
                                              description='Run LAfix on a '
                                              'merged and annotated block.')
    block_patch.add_argument('-n', help='maximum number of blocks to process '
                             '(default: 1)', type=int, default=1)
    block_patch.add_argument('--max-simultaneous-tasks', help='maximum number '
                             'of tasks allowed to run simultaneously '
                             '(default: N)', type=int)
    block_patch.add_argument('-f', '--force', help='start patching even if '
                             'the corresponding fasta file already exists',
                             action='store_true')

    # blocks stats
    block_stats = block_subparsers.add_parser('stats',
                                              help='calculate summary '
                                              'statistics',
                                              description='Run LAstats on '
                                              'a merged block.')
    block_stats.add_argument('-n', help='maximum number of blocks to process '
                             '(default: 1)', type=int, default=1)
    block_stats.add_argument('--max-simultaneous-tasks', help='maximum number '
                             'of tasks allowed to run simultaneously '
                             '(default: N)', type=int)
    block_stats.add_argument('-f', '--force', help='start patching even if '
                             'the corresponding fasta file already exists',
                             action='store_true')

    # daligner
    dalign_parser = subparsers.add_parser('daligner', help='Run daligner',
        description='Manage daligner jobs.')
    dalign_subparsers = dalign_parser.add_subparsers(dest='subsubcommand',
                                                     metavar='daligner-command')
    dalign_subparsers.required = True

    # daligner start
    dalign_start = dalign_subparsers.add_parser('start', help='initialise '
                                                'daligner jobs',
        description='Initialise daligner jobs by populating the database '
                    'with the blocks and the individual jobs that will be '
                    'run.')
    dalign_start.add_argument('-n', '--jobs-per-task', help='number of jobs '
                              'that each task in a job array will run',
                              type=int, default=100)
    dalign_start.add_argument('-m', '--max-simultaneous-tasks', help='maximum '
                              'number of tasks allowed to run simultaneously',
                              type=int)
    dalign_start.add_argument('-c', '--comparisons-per-job', help='the number '
                              'of block comparisons that should be run for '
                              'each daligner job (default: 1)', type=int,
                              default=1)
    dalign_start.add_argument('-f', '--force', help='forcefully add daligner '
                              'jobs, removing any existing jobs',
                              action='store_true')
    dalign_start.add_argument('--no-masking', help='do not use the masking '
                              'server', action='store_true')

    # daligner update
    dalign_update = dalign_subparsers.add_parser('update', help='submit '
                                                 'daligner jobs',
        description='Queue a new set of daligner jobs. The number of jobs '
                    'that actually will be queued depends on the maximum number of '
                    'jobs that are allowed according to the config.ini file')
    dalign_update.add_argument('-n', help='size of job array to queue',
                               type=int)

    # daligner stop
    dalign_stop = dalign_subparsers.add_parser('stop', help='stop daligner jobs',
        description='Stop all currently queued, reserved, and running daligner '
                    'jobs.')

    # daligner list
    dalign_reserve = dalign_subparsers.add_parser('reservation',
                                                  help='manage '
                                                  'daligner job reservations',
        description='List all current daligner reservations. '
                    'Prints the IDs of block 1, block 2, and the SLURM job id.')
    dalign_reserve.add_argument('--cancel', help='cancel all active '
                                'reservations', action='store_true')

    # Update status and restart jobs if necessary
    fix_parser = subparsers.add_parser('fix', help='Update and reset jobs',
                                       description='If jobs have failed or '
                                       'been cancelled, this function resets '
                                       'them to have a status of NOSTARTED '
                                       'so that they will be eligible for '
                                       'running when running `marvelous_jobs '
                                       'update`. Also, if for some reason '
                                       'the masking server has stopped, it will '
                                       'be restarted.')

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
        if args.threads is not None and not positive_integer(args.threads):
            parser.error('number of threads must be a positive non-zero integer')
        if args.port is not None and not positive_integer(args.port):
            parser.error('port must be a positive non-zero integer')
            parser.error('{0} is not a valid node'.format(args.node))

    if args.subcommand == 'daligner' and args.subsubcommand == 'start':
        if not positive_integer(args.jobs_per_task):
            parser.error('jobs per task must be a positive non-zero integer')
        elif args.jobs_per_task > 1000:
            parser.error('jobs per task cannot exceed 1000')
        if args.max_simultaneous_tasks is not None \
           and not positive_integer(args.max_simultaneous_tasks):
            parser.error('maximum number of simultaneous tasks must be a '
                         'positive non-zero integer')
        if not positive_integer(args.comparisons_per_job):
            parser.error('comparisons per job must be a '
                         'positive non-zero integer')
    if args.subcommand == 'daligner' and args.subsubcommand == 'update':
        if args.n is not None and not positive_integer(args.n):
            parser.error('n must be a non-zero integer')

    if args.subcommand == 'blocks' and (args.subsubcommand == 'merge' \
                                        or args.subsubcommand == 'annotate' \
                                        or args.subsubcommand == 'patch' \
                                        or args.subsubcommand == 'stats'):
        if not positive_integer(args.n):
            parser.error('n must be a non-zero integer')
        if args.max_simultaneous_tasks is not None and \
                not positive_integer(args.max_simultaneous_tasks):
            parser.error('max-simultaneous-tasks must be a non-zero integer')
    if args.subcommand == 'blocks' and args.subsubcommand == 'merge':
        if not positive_integer(args.m):
            parser.error('m must be a non-zero integer')

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
    if args.subcommand == 'backup':
        backup_database()
    if args.subcommand == 'prepare':
        prepare(fasta=args.fasta,
                blocksize=args.blocksize,
                script_directory=args.script_directory,
                log_directory=args.log_directory,
                force=args.force)
    if args.subcommand == 'mask' and args.subsubcommand == 'start':
        start_mask(args.threads, args.port, args.constraint, cluster=None)
    if args.subcommand == 'mask' and args.subsubcommand == 'status':
        mask_status()
    if args.subcommand == 'mask' and args.subsubcommand == 'stop':
        stop_mask()
    if args.subcommand == 'daligner' and args.subsubcommand == 'start':
        start_daligner(jobs_per_task=args.jobs_per_task, force=args.force,
                       no_masking=args.no_masking,
                       max_simultaneous_tasks=args.max_simultaneous_tasks,
                       comparisons_per_job=args.comparisons_per_job)
    if args.subcommand == 'blocks' and args.subsubcommand is None:
        if args.complete:
            list_completed_blocks()
        else:
            list_blocks()
    if args.subcommand == 'blocks' and args.subsubcommand == 'merge':
        merge_blocks(n=args.n, n_files=args.m,
                     max_simultaneous_tasks=args.max_simultaneous_tasks)
    if args.subcommand == 'blocks' and args.subsubcommand == 'annotate':
        annotate_blocks(n=args.n,
                        max_simultaneous_tasks=args.max_simultaneous_tasks,
                        force=args.force)
    if args.subcommand == 'blocks' and args.subsubcommand == 'patch':
        patch_blocks(n=args.n,
                     max_simultaneous_tasks=args.max_simultaneous_tasks,
                     force=args.force)
    if args.subcommand == 'blocks' and args.subsubcommand == 'stats':
        block_stats(n=args.n,
                    max_simultaneous_tasks=args.max_simultaneous_tasks,
                    force=args.force)

    if args.subcommand == 'daligner' and args.subsubcommand == 'update':
        update_daligner_queue(n_tasks=args.n)
    if args.subcommand == 'daligner' and args.subsubcommand == 'stop':
        stop_daligner()
    if args.subcommand == 'daligner' and args.subsubcommand == 'reservation':
        if not args.cancel:
            list_reservations()
        else:
            cancel_daligner_reservation()
    if args.subcommand == 'fix':
        update_and_restart()
    if args.subcommand == 'info':
        info()

if __name__ == '__main__':
    main()
