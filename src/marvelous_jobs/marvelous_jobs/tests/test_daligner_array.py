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
