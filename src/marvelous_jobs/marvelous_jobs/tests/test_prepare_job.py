from nose.tools import assert_equals
from nose.tools import assert_not_equals
from nose.tools import assert_true
from nose.tools import assert_false
from nose.tools import assert_count_equal
import os
import time

import marvelous_jobs as mj
from marvelous_jobs.tests import db, n_blocks, config

def test_prepare_script():
    job = mj.prepare_job('test',
                         config.get('general', 'fasta'),
                         config.getint('general', 'blocksize'),
                         script_directory=config.get('general', 'script_directory'),
                         log_directory=config.get('general', 'log_directory'))
    assert_false(os.path.exists(job.filename))
    job.start(dryrun=True)
    assert_true(os.path.exists(job.filename))
    modtime = os.path.getmtime(job.filename)

    job = mj.prepare_job('test',
                         config.get('general', 'fasta'),
                         config.getint('general', 'blocksize'),
                         script_directory=config.get('general', 'script_directory'),
                         log_directory=config.get('general', 'log_directory'))

    time.sleep(0.1)
    job.start(dryrun=True)
    assert_equals(modtime, os.path.getmtime(job.filename))

    job = mj.prepare_job('test',
                         config.get('general', 'fasta'),
                         config.getint('general', 'blocksize') + 10,
                         script_directory=config.get('general', 'script_directory'),
                         log_directory=config.get('general', 'log_directory'))
    time.sleep(0.1)
    job.start(dryrun=True)
    assert_not_equals(modtime, os.path.getmtime(job.filename))

def test_prepare_script_sbatch_args():
    job = mj.prepare_job('test',
                         config.get('general', 'fasta'),
                         config.getint('general', 'blocksize'),
                         script_directory=config.get('general', 'script_directory'),
                         log_directory=config.get('general', 'log_directory'))
    cmd = job.start(dryrun=True)
    assert_true('-o {0}/marvel_prepare.log' \
                .format(config.get('general', 'log_directory')) in cmd)
