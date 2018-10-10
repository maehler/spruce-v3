import os
import sys
import tempfile

import marvelous_jobs as mj

tmp = tempfile.mkstemp()
db_filename = tmp[1]
# I have to force the creation of the database since
# mkstemp creates the file, and thus the initalisation
# of marvel_db won't do anything.
db = mj.marvel_db(db_filename, 'test', 20, force=True)
n_blocks = 10

def setup():
    global db, n_blocks
    current_job = 0

    jobs = []

    # Diagonal
    for i in range(n_blocks):
        current_job += 1
        db.add_block(i, 'test.{0}'.format(i))
        jobs.append((current_job, i, i, 1, False))

    # Off-diagonal
    for i in range(n_blocks):
        for j in range(i + 1, n_blocks):
            current_job += 1
            jobs.append((current_job, i, j, 1, False))

    db.add_daligner_jobs(jobs)

def teardown():
    global db_filename
    os.remove(db_filename)
