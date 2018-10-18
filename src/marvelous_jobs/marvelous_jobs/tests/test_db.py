import marvelous_jobs as mj
from nose.tools import assert_equals
from nose.tools import assert_is_instance
from nose.tools import assert_dict_equal
from nose.tools import with_setup

from marvelous_jobs.tests import db, n_blocks, n_daligner_jobs

def set_dummy_jobs():
    query1 = '''UPDATE daligner_job
        SET jobid = 1, status = "COMPLETED"
        WHERE rowid <= 100'''
    db._c.execute(query1)
    query2 = '''UPDATE daligner_job
        SET jobid = 2, status = "RUNNING"
        WHERE rowid > 100 AND rowid <= 200'''
    db._c.execute(query2)
    db._db.commit()

def reset_dummy_jobs():
    query = '''UPDATE daligner_job
        SET jobid = NULL, status = "NOTSTARTED"'''
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

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_getting_daligner_jobs():
    jobs = db.get_daligner_jobs(5)
    assert_equals(len(jobs), 5)
    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.completed)
    assert_equals(len(jobs), 5)
    jobs = db.get_daligner_jobs()
    assert_equals(len(jobs), n_daligner_jobs)
    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.notstarted)
    assert_equals(len(jobs), n_daligner_jobs - 200)
    jobs = db.get_daligner_jobs(100)
    assert_equals(len(jobs), 100)

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_update_daligner_jobs():
    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.notstarted)
    db.update_daligner_jobs(jobs)

    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.running)
    assert_equals(len(jobs), 100)
    db.update_daligner_jobs(jobs)
    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.running)
    assert_equals(len(jobs), 0)

    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.completed)
    assert_equals(len(jobs), 100)

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_daligner_jobids():
    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.running)
    jobids = db.get_daligner_jobids(jobs)
    assert_dict_equal(jobids, {101: '2_101', 102: '2_102', 103: '2_103',
                               104: '2_104', 105: '2_105'})

    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.completed)
    jobids = db.get_daligner_jobids(jobs)
    assert_dict_equal(jobids, {1: '1_1', 2: '1_2', 3: '1_3', 4: '1_4', 5: '1_5'})
