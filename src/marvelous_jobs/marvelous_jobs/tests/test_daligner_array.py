import marvelous_jobs as mj
from nose.tools import assert_equals

from marvelous_jobs.tests import db, n_blocks, config

def test_array_script():
    mj.daligner_job_array(range(1, 101),
                          config.get('general', 'database'))
