from nose.tools import assert_true, assert_false, assert_equal
import os

import marvelous_jobs as mj
from marvelous_jobs.tests import db, config

def test_marvel_job_single_line():
    script_filename = os.path.join(config.get('general', 'script_directory'),
                                   'single_line_script.sh')
    job = mj.job.marvel_job(['executable', '-o', 'test_output'], 'testjob',
                            script_filename)
    assert_equal(job.filename, script_filename)
    assert_false(os.path.isfile(script_filename))
    job.save_script()
    assert_true(os.path.isfile(script_filename))
    # Should have shebang, set statement, and the call to the executable
    assert_equal(len(str(job).splitlines()), 3)

def test_marvel_job_multiple_lines():
    script_filename = os.path.join(config.get('general', 'script_directory'),
                                   'multiline_script.sh')
    job = mj.job.marvel_job([['x=$(prepare stuff)'],
                             ['executable', '-o', 'test_output'],
                             ['postprocess', '--for-real']],
                            'multiline_test', script_filename)
    assert_equal(job.filename, script_filename)
    assert_false(os.path.isfile(job.filename))
    job.save_script()
    assert_true(os.path.isfile(job.filename))
    assert_equal(len(str(job).splitlines()), 5)
