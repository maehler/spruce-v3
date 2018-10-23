from nose.tools import assert_equals
from nose.tools import assert_true
from nose.tools import assert_false
from nose.tools import assert_count_equal
import os
import time

import marvelous_jobs as mj
from marvelous_jobs.tests import db, n_blocks, config

def test_array_script():
    job = mj.daligner_job_array(10,
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(job.filename,
                  os.path.join(config.get('general', 'script_directory'),
                               'daligner_array.sh'))
    assert_false(os.path.isfile(job.filename))
    job.save_script()
    assert_true(os.path.isfile(job.filename))
    os.remove(job.filename)

def test_array_indices():
    job = mj.daligner_job_array(1,
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(job._array_index_str(), '1')
    job = mj.daligner_job_array(100,
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(job._array_index_str(), '1-100')
    job = mj.daligner_job_array(200,
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(job._array_index_str(), '1-200')
    job = mj.daligner_job_array(201,
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(job._array_index_str(), '1-201')

def test_array_submit():
    job = mj.daligner_job_array(10,
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'),
                                log_directory=config.get('general',
                                                         'log_directory'))
    assert_false(os.path.isfile(job.filename))
    cmd = job.start(dryrun=True)
    assert_true(os.path.isfile(job.filename))
    assert_true('-o {0}' \
                .format(os.path.join(config.get('general',
                                                'log_directory'),
                                     'daligner_array_%A_%a.log')) \
                in cmd)

    job = mj.daligner_job_array(10,
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    cmd = job.start(dryrun=True)
    assert_true('-o daligner_array_%A_%a.log' in cmd)
    os.remove(job.filename)

def test_file_creation():
    job1 = mj.daligner_job_array(10,
                                 config.get('general', 'database'),
                                 config.get('general', 'script_directory'),
                                 log_directory=config.get('general',
                                                          'log_directory'),
                                 jobs_per_task=10)
    cmd1 = job1.start(dryrun=True)
    modtime = os.path.getmtime(job1.filename)
    assert_true(cmd1.endswith(' 10'))

    job2 = mj.daligner_job_array(100,
                                 config.get('general', 'database'),
                                 config.get('general', 'script_directory'),
                                 jobs_per_task=100)
    cmd2 = job2.start(dryrun=True)
    time.sleep(0.1)
    assert_equals(modtime, os.path.getmtime(job2.filename))
    assert_true(cmd2.endswith(' 100'))

    os.remove(job2.filename)
    assert_false(os.path.exists(job1.filename))
