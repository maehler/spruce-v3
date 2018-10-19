from nose.tools import assert_equals
from nose.tools import assert_true
from nose.tools import assert_false
from nose.tools import assert_count_equal
import os

import marvelous_jobs as mj
from marvelous_jobs.tests import db, n_blocks, config

def test_array_script():
    job = mj.daligner_job_array(range(1, 101),
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(job.filename,
                  os.path.join(config.get('general', 'script_directory'),
                               'daligner_array.sh'))
    assert_false(os.path.isfile(job.filename))
    job.save_script()
    assert_true(os.path.isfile(job.filename))
    os.remove(job.filename)

def test_array_simple_taskid():
    job = mj.daligner_job_array(range(1, 101),
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(len(job.rowids), 100)
    assert_equals(job.rowid_str(), '1-100')

def test_array_tricky_taskids():
    job = mj.daligner_job_array([1, 2, 6, 7, 8, 10, 12, 13, 14, 15],
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    assert_equals(len(job.rowids), 10)
    assert_equals(job.rowid_str(), '1-2,6-8,10,12-15')

def test_array_submit():
    job = mj.daligner_job_array([1, 2, 6, 7, 8, 10, 12, 13, 14, 15],
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

    job = mj.daligner_job_array([1, 2, 6, 7, 8, 10, 12, 13, 14, 15],
                                config.get('general', 'database'),
                                config.get('general', 'script_directory'))
    cmd = job.start(dryrun=True)
    assert_true('-o daligner_array_%A_%a.log' in cmd)
    os.remove(job.filename)

def test_file_creation():
    job1 = mj.daligner_job_array([1, 2, 6, 7, 8, 10, 12, 13, 14, 15],
                                 config.get('general', 'database'),
                                 config.get('general', 'script_directory'),
                                 log_directory=config.get('general',
                                                          'log_directory'))
    job1.start(dryrun=True)
    modtime = os.path.getmtime(job1.filename)

    job2 = mj.daligner_job_array(range(1, 101),
                                 config.get('general', 'database'),
                                 config.get('general', 'script_directory'))
    job2.start(dryrun=True)
    assert_equals(modtime, os.path.getmtime(job2.filename))

    os.remove(job2.filename)
    assert_false(os.path.exists(job1.filename))
