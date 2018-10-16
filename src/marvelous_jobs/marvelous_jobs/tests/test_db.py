import marvelous_jobs as mj
from nose.tools import assert_equals
from nose.tools import assert_is_instance

from marvelous_jobs.tests import db, n_blocks, n_daligner_jobs

def set_jobs_as_running():
    query = 'UPDATE daligner_job SET status = "RUNNING" WHERE rowid IN (1, 2, 3)'
    db._c.execute(query)
    db._db.commit()

def test_number_of_daligner_jobs():
    expected_number_of_jobs = n_blocks + n_blocks * (n_blocks - 1) / 2
    number_of_jobs = db.n_daligner_jobs()
    assert_equals(number_of_jobs, expected_number_of_jobs)

def test_rowid():
    db._c.execute('SELECT min(rowid), max(rowid) FROM daligner_job')
    min_rowid, max_rowid = db._c.fetchone()
    assert_equals(min_rowid, 1)
    assert_equals(max_rowid, db.n_daligner_jobs())

def test_getting_daligner_jobs():
    jobs = db.get_daligner_jobs(5)
    assert_equals(len(jobs), 5)
    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.completed)
    assert_equals(len(jobs), 0)
    jobs = db.get_daligner_jobs()
    assert_equals(len(jobs), n_daligner_jobs)
    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.notstarted)
    assert_equals(len(jobs), n_daligner_jobs)
    jobs = db.get_daligner_jobs(100)
    assert_equals(len(jobs), 100)

def test_update_daligner_jobs():
    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.notstarted)
    db.update_daligner_jobs(jobs)
    jobs = db.get_daligner_jobs()
    db.update_daligner_jobs(jobs)

    set_jobs_as_running()
    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.running)
    assert_equals(len(jobs), 3)

    db.update_daligner_jobs(jobs)

    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.running)
    assert_equals(len(jobs), 0)
