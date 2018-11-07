from functools import reduce
from nose.tools import assert_dict_equal
from nose.tools import assert_equals
from nose.tools import assert_is_instance
from nose.tools import assert_raises
from nose.tools import assert_true
from nose.tools import raises
from nose.tools import with_setup
import os
import sqlite3
import subprocess
import threading

import marvelous_jobs as mj
from marvelous_jobs.tests import config, db, n_blocks, n_daligner_jobs

def set_dummy_jobs():
    query1 = '''UPDATE daligner_job
        SET jobid = "1_" || rowid, status = "COMPLETED",
        reservation_token = "test_token"
        WHERE rowid <= 100'''
    db._c.execute(query1)
    query2 = '''UPDATE daligner_job
        SET jobid = "2_" || ?, status = "RUNNING",
        reservation_token = "test_token"
        WHERE rowid = ?'''
    for i, ri in zip(range(1, 101), range(101, 201)):
        db._c.execute(query2, (i, ri))
    db._db.commit()

def reset_dummy_jobs():
    query = '''UPDATE daligner_job
        SET jobid = NULL,
        status = "NOTSTARTED",
        reservation_token = null'''
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
def test_database_integrity():
    assert_true(db.integrity_check())

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_database_backup():
    backup_filename = '{0}.backup'.format(config.get('general', 'database'))
    db.backup(backup_filename)
    assert_true(os.path.isfile(backup_filename))

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
    db.update_daligner_jobs(jobs, config.get('general', 'log_directory'))

    jobs = db.get_daligner_jobs(75, status=mj.slurm_utils.status.running)
    assert_equals(len(jobs), 75)
    db.update_daligner_jobs(jobs, config.get('general', 'log_directory'))
    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.running)
    assert_equals(len(jobs), 25)

    jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.completed)
    assert_equals(len(jobs), 100)

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_daligner_jobids():
    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.running)
    jobids = db.get_daligner_jobids(jobs)
    assert_dict_equal(jobids, {101: '2_1', 102: '2_2', 103: '2_3',
                               104: '2_4', 105: '2_5'})

    jobs = db.get_daligner_jobs(5, status=mj.slurm_utils.status.completed)
    jobids = db.get_daligner_jobids(jobs)
    assert_dict_equal(jobids, {1: '1_1', 2: '1_2', 3: '1_3', 4: '1_4', 5: '1_5'})

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_reserving_jobs():
    jobs = db.reserve_daligner_jobs(token='test-token', max_jobs=5)
    reserved_jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.reserved)
    assert_equals(len(jobs), 5)
    assert_equals(len(reserved_jobs), 5)

    db.cancel_daligner_reservation()
    reserved_jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.reserved)
    assert_equals(len(reserved_jobs), 0)

@with_setup(None, reset_dummy_jobs)
def test_reserving_multiblocks():
    # These jobs will be the diagonal, and they should only contain
    # a single comparison.
    jobs = db.reserve_daligner_jobs(token='test-token', max_jobs=n_blocks,
                                    comparisons_per_job=16)
    assert_equals(len(jobs), n_blocks)
    for j in jobs:
        assert_equals(len(j['target_blocks']), 1)

    all_rowids = reduce(lambda x, y: x + y,
                        [x['rowids'] for x in jobs])
    assert_true(all(len(x['rowids']) == 1 for x in jobs))
    assert_equals(len(set(all_rowids)), n_blocks)

    # Off-diagonal jobs should be done column by column.
    jobs = db.reserve_daligner_jobs(token='test-token2', max_jobs=100,
                                    comparisons_per_job=16)
    # The first few jobs should have the full number of comparisons...
    assert_equals(len(jobs), 100)
    n_full_jobs = (n_blocks-1)//16
    for j in jobs[:n_full_jobs]:
        assert_equals(len(j['target_blocks']), 16)
    # .. while the remaining job should only contain comparisons up until
    # the end of the column.
    assert_equals(len(jobs[n_full_jobs]['target_blocks']),
                  (n_blocks - 1) - 16 * n_full_jobs)
    # And then it should again be full jobs.
    assert_equals(len(jobs[n_full_jobs+1]['target_blocks']), 16)

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_reserving_jobs_in_parallel():
    class dbworker(threading.Thread):
        def __init__(self, n_jobs, config):
            super().__init__()
            self.n_jobs = n_jobs
            self.config = config
            self.jobs = []
        def run(self):
            local_db = mj.marvel_db.from_file(self.config.get('general', 'database'))
            self.jobs = local_db.reserve_daligner_jobs(token='test-token',
                                                       max_jobs=self.n_jobs)

    jobs_per_thread = 100
    n_threads = 10
    threads = []
    for i in range(n_threads):
        t = dbworker(jobs_per_thread, config)
        threads.append(t)

    for t in threads:
        t.start()

    results = []
    for t in threads:
        t.join()
        results.append(t.jobs)

    assert_equals(len(results), n_threads)
    assert_equals(len(results[0]), jobs_per_thread)
    reserved_jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.reserved)
    assert_equals(len(reserved_jobs), n_threads * jobs_per_thread)

    all_rowids = reduce(lambda x, y: x + y,
                        [x['rowids'] for y in results for x in y])
    assert_equals(len(all_rowids), n_threads * jobs_per_thread)
    assert_equals(len(set(all_rowids)), len(all_rowids))

    db.cancel_daligner_reservation()
    reserved_jobs = db.get_daligner_jobs(status=mj.slurm_utils.status.reserved)
    assert_equals(len(reserved_jobs), 0)

@with_setup(set_dummy_jobs, reset_dummy_jobs)
def test_exclusive():
    db1 = sqlite3.connect(config.get('general', 'database'), timeout=0)
    c1 = db1.cursor()
    db2 = sqlite3.connect(config.get('general', 'database'), timeout=0)
    c2 = db2.cursor()

    select_query = '''SELECT rowid FROM daligner_job
        WHERE status = "NOTSTARTED" LIMIT 10'''

    c1.execute('BEGIN EXCLUSIVE');
    with assert_raises(sqlite3.OperationalError) as oe:
        c2.execute(select_query)
    c1.execute(select_query)
    rowids1 = [x[0] for x in c1.fetchall()]
    with assert_raises(sqlite3.OperationalError) as oe:
        c2.execute(select_query)
    c1.execute('UPDATE daligner_job SET status = "RESERVED" WHERE rowid IN({0})' \
              .format(','.join('?' for x in rowids1)), tuple(rowids1))
    with assert_raises(sqlite3.OperationalError) as oe:
        c2.execute(select_query)
    db1.commit()
    c2.execute(select_query)
    rowids2 = [x[0] for x in c2.fetchall()]

    assert_true(len(rowids1), len(rowids2))
    assert_true(len(set(rowids1 + rowids2)), len(rowids1) + len(rowids2))
