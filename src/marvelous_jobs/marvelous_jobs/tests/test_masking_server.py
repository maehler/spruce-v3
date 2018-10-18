
from nose.tools import assert_equals
from nose.tools import assert_true
from nose.tools import assert_false
from nose.tools import assert_count_equal
import os

import marvelous_jobs as mj
from marvelous_jobs.tests import db, n_blocks, config

def test_masking_server_script():
    job = mj.masking_server_job('test', 20,
                                config.get('DMserver', 'checkpoint_file'),
                                script_directory=config.get('general',
                                                            'script_directory'),
                                log_directory=config.get('general',
                                                         'log_directory'),
                                port=12345,
                                threads=4)
    assert_false(os.path.isfile(job.filename))
    job.save_script()
    assert_true(os.path.isfile(job.filename))

def test_masking_server_sbatch_args():
    job = mj.masking_server_job('test', 20,
                                config.get('DMserver', 'checkpoint_file'),
                                script_directory=config.get('general',
                                                            'script_directory'),
                                log_directory=config.get('general',
                                                         'log_directory'),
                                port=12345,
                                threads=4,
                                cluster='snowy')
    cmd = job.start(dryrun=True)
    with open(job.filename) as f:
        str_script = f.read()
    assert_true('-M snowy' in cmd)
    assert_false('#SBATCH -C' in str_script)

    job = mj.masking_server_job('test', 20,
                                config.get('DMserver', 'checkpoint_file'),
                                script_directory=config.get('general',
                                                            'script_directory'),
                                log_directory=config.get('general',
                                                         'log_directory'),
                                port=12345,
                                threads=4,
                                cluster='rackham',
                                constraint='mem256GB')
    cmd = job.start(dryrun=True)
    with open(job.filename) as f:
        str_script = f.read()
    assert_false('-M snowy' in cmd)
    assert_true('-M rackham' in cmd)
    assert_true('#SBATCH -C mem256GB' in str_script)
