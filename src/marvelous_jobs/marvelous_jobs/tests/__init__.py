import os
import sys
import tempfile
import shutil

import marvelous_jobs as mj

testdir = os.path.join(tempfile.gettempdir(), 'marvelous_test')
if os.path.isdir(testdir):
    shutil.rmtree(testdir)
elif os.path.isfile(testdir):
    os.remove(testdir)
os.mkdir(testdir)

script_dir = os.path.join(testdir, 'scripts')
log_dir = os.path.join(testdir, 'logs')
os.mkdir(script_dir)
os.mkdir(log_dir)

db_filename = os.path.join(testdir, 'marveldb')
config_filename = os.path.join(testdir, 'config.ini')

n_blocks = 500
n_daligner_jobs = n_blocks + n_blocks * (n_blocks - 1) // 2
db = mj.marvel_db(db_filename, 'test', 20)
config = mj.marvelous_config(
    filename=config_filename,
    cdict={
    'general': {
        'account': 'test',
        'database': db_filename,
        'directory': testdir,
        'script_directory': script_dir,
        'log_directory': log_dir,
        'blocksize': 20,
        'max_number_of_jobs': 10,
        'fasta': os.path.join(testdir, 'dummy_fasta.fa')
    },
    'daligner': {
        'verbose': True,
        'identity': True,
        'tuple_suppression_frequency': 20,
        'correlation_rate': 0.7,
        'threads': 4,
        'timelimit': '1-00:00:00'
    },
    'DMserver': {
        'threads': 4,
        'port': 12345,
        'cluster': 'snowy',
        'checkpoint_file': os.path.join(testdir, 'masking_checkpoint'),
        'timelimit': '10-00:00:00'
    }
})

def setup():
    global db, n_blocks, config, config_filename
    current_job = 0

    jobs = []

    # Diagonal
    for i in range(1, n_blocks + 1):
        current_job += 1
        db.add_block(i, 'test.{0}'.format(i))
        jobs.append((current_job, i, i, 1, False))

    # Off-diagonal
    for i in range(1, n_blocks + 1):
        for j in range(i + 1, n_blocks + 1):
            current_job += 1
            jobs.append((current_job, i, j, 1, False))
            if current_job % 1000 == 0:
                db.add_daligner_jobs(jobs)
                jobs = []

    db.add_daligner_jobs(jobs)

def teardown():
    global testdir
    shutil.rmtree(testdir)
