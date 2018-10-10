import marvelous_jobs as mj
from nose.tools import assert_equals

from marvelous_jobs.tests import db, n_blocks

def test_number_of_daligner_jobs():
    expected_number_of_jobs = n_blocks + n_blocks * (n_blocks - 1) / 2
    number_of_jobs = db.n_daligner_jobs()
    assert_equals(number_of_jobs, expected_number_of_jobs)

def test_rowid():
    db._c.execute('SELECT min(rowid), max(rowid) FROM daligner_job')
    min_rowid, max_rowid = db._c.fetchone()
    assert_equals(min_rowid, 1)
    assert_equals(max_rowid, db.n_daligner_jobs())
