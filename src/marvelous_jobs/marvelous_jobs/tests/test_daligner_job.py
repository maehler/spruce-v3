from nose.tools import assert_true, assert_false, assert_equal, assert_in
import os

import marvelous_jobs as mj
from marvelous_jobs.tests import db, config

def test_daligner_job():
    dj = mj.daligner_job(1, config.get('general', 'database'),
                         config.get('general', 'script_directory'))
    assert_equal(dj.jobname, 'daligner_1')
    assert_equal(dj.filename,
                 os.path.join(config.get('general', 'script_directory'),
                              'daligner.sh'))
    assert_false(os.path.isfile(dj.filename))
    dj.save_script()
    assert_true(os.path.isfile(dj.filename))

def test_daligner_job_sbatch_args():
    dj = mj.daligner_job(1, config.get('general', 'database'),
                         config.get('general', 'script_directory'),
                         account=config.get('general', 'account'),
                         threads=8)
    assert_in('#SBATCH -A {0}'.format(config.get('general', 'account')),
              str(dj).splitlines())
    assert_in('#SBATCH -n 8', str(dj).splitlines())
    assert_in('-j 8', dj.commandline())
