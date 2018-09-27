import os
import sqlite3

from marvelous_jobs import slurm_utils
from marvelous_jobs.job import daligner_job

class marvel_db:

    def __init__(self, filename, name, coverage, force=False):
        is_new = not os.path.exists(filename)
        self._db = sqlite3.connect(filename)
        self._db.row_factory = sqlite3.Row
        self._c = self._db.cursor()

        if force:
            self._c.execute('DROP TABLE IF EXISTS prepare_job')
            self._c.execute('DROP TABLE IF EXISTS daligner_job')
            self._c.execute('DROP TABLE IF EXISTS masking_job')
            self._c.execute('DROP TABLE IF EXISTS block')
            self._c.execute('DROP TABLE IF EXISTS project')

        if is_new or force:
            self._c.execute('''CREATE TABLE project (
                                name TEXT PRIMARY KEY NOT NULL,
                                coverage INT NOT NULL,
                                started_on TEXT NOT NULL,
                                prepared_on TEXT
                               )''')
            self._c.execute('''CREATE TABLE block (
                                id INT PRIMARY KEY NOT NULL,
                                name TEXT NOT NULL
                               )''')
            self._c.execute('''CREATE TABLE daligner_job
                                (block_id1 INT NOT NULL,
                                 block_id2 INT NOT NULL,
                                 priority INT NOT NULL,
                                 status TEXT NOT NULL DEFAULT 'NOTSTARTED',
                                 jobid INT,
                                 last_update TEXT,
                                 PRIMARY KEY(block_id1, block_id2),
                                 FOREIGN KEY(block_id1) REFERENCES block(id),
                                 FOREIGN KEY(block_id2) REFERENCES block(id))''')
            self._c.execute('''CREATE TABLE masking_job
                                (node TEXT,
                                 ip TEXT,
                                 status TEXT NOT NULL DEFAULT 'NOTSTARTED',
                                 jobid INT,
                                 last_update TEXT,
                                 PRIMARY KEY(jobid))''')
            self._c.execute('''CREATE TABLE prepare_job
                                (jobid INT,
                                 status TEXT NOT NULL DEFAULT 'NOTSTARTED',
                                 last_update TEXT,
                                 PRIMARY KEY (jobid))''')

            self._c.execute('''INSERT INTO project
                                (name, coverage, started_on)
                                VALUES (?, ?, datetime('now', 'localtime'))''', (name, coverage))
            self._db.commit()

    @classmethod
    def from_file(cls, filename):
        db = sqlite3.connect(filename)
        c = db.cursor()
        c.execute('SELECT name, coverage FROM project')
        name, coverage = c.fetchone()
        return cls(filename, name, coverage, False)

    def remove_blocks(self):
        self._c.execute('DELETE FROM block')
        self._db.commit()

    def remove_daligner_jobs(self):
        self._c.execute('DELETE FROM daligner_job')
        self._db.commit()

    def add_block(self, id, name):
        try:
            self._c.execute('''INSERT INTO block
                                (id, name)
                                VALUES (?, ?)''', (id, name))
        except sqlite3.IntegrityError:
            raise RuntimeError('block already exists')
        self._db.commit()

    def add_daligner_job(self, id1, id2, priority):
        self._c.execute('''INSERT INTO daligner_job
                        (block_id1, block_id2, priority, last_update)
                        VALUES (?, ?, ?, datetime('now', 'localtime'))''', (id1, id2, priority))
        self._db.commit()

    def update_daligner_job_id(self, id1, id2, jobid):
        self._c.execute('''UPDATE daligner_job
                            SET jobid = ?
                            WHERE block_id1 = ? AND block_id2 = ?''',
                        (jobid, id1, id2))
        self._db.commit()

    def update_daligner_job_status(self):
        self._c.execute('SELECT jobid FROM daligner_job')

        for (jobid,) in self._c.fetchall():
            status = slurm_utils.get_job_status(jobid)
            self._c.execute('''UPDATE daligner_job
                                SET status = ?
                                WHERE jobid = ?''',
                           (status, jobid))

        self._db.commit()

    def get_daligner_jobs(self):
        self._c.execute('''SELECT jobid, block1.name, block2.name
                        FROM daligner_job
                        LEFT JOIN block AS block1 ON block1.id = block_id1
                        LEFT JOIN block AS block2 ON block2.id = block_id2''')
        jobs = []
        for j in self._c.fetchall():
            jobs.append(daligner_job(j[1], j[2], jobid=j[0]))

        return jobs

    def add_prepare_job(self):
        self._c.execute('''INSERT INTO prepare_job (last_update)
                        VALUES (datetime('now', 'localtime'))''')
        self._db.commit()

    def update_prepare_job_id(self, jobid):
        self._c.execute('UPDATE prepare_job SET jobid = ?', (jobid,))
        self._db.commit()
        self.update_prepare_job_status()

    def update_prepare_job_status(self):
        self._c.execute('SELECT jobid FROM prepare_job')
        res = self._c.fetchone()

        if res is None:
            return

        jobid = res[0]

        try:
            job_status = slurm_utils.get_job_status(jobid)
        except ValueError:
            raise
        self._c.execute('''UPDATE prepare_job
                        SET status = ?, last_update = datetime('now',
                        'localtime')''',
                        (job_status,))
        self._db.commit()

    def prepare_status(self):
        self._c.execute('SELECT status FROM prepare_job')
        res = self._c.fetchone()

        if res is None:
            return res

        return res[0]

    def has_masking_job(self):
        self._c.execute('SELECT COUNT(node) FROM masking_job')
        return self._c.fetchone()[0] > 0

    def add_masking_job(self, job):
        if self.has_masking_job():
            self._c.execute('DELETE FROM masking_job')
        self._c.execute('''INSERT INTO masking_job
                        (node, ip, last_update)
                        VALUES (?, ?, datetime('now', 'localtime'))''', (job.node, job.ip))
        self._db.commit()

    def update_masking_job_id(self, jobid):
        self._c.execute('UPDATE masking_job SET jobid = ?', (jobid,))
        self._db.commit()
        self.update_masking_job_status()

    def update_masking_job_status(self):
        if not self.has_masking_job():
            return

        self._c.execute('SELECT jobid FROM masking_job')
        res = self._c.fetchone()

        jobid = res[0]

        try:
            job_status = slurm_utils.get_job_status(jobid)
        except ValueError:
            raise
        self._c.execute('''UPDATE masking_job
                        SET status = ?, last_update = datetime('now',
                        'localtime')''',
                        (job_status,))

        self._db.commit()

    def masking_status(self):
        if not self.has_masking_job():
            return None
        self._c.execute('SELECT jobid, status, last_update FROM masking_job')
        return self._c.fetchone()

    def get_masking_node(self):
        if not self.has_masking_job():
            return None
        self._c.execute('SELECT node FROM masking_job')
        return self._c.fetchone()[0]

    def get_masking_ip(self):
        if not self.has_masking_job():
            return None
        self._c.execute('SELECT ip FROM masking_job')
        return self._c.fetchone()[0]

    def get_masking_jobid(self):
        if not self.has_masking_job():
            return None
        self._c.execute('SELECT jobid FROM masking_job')
        return self._c.fetchone()[0]

    def is_prepared(self):
        self._c.execute('SELECT prepared_on FROM project')
        return self._c.fetchone()[0] is not None

    def prepare(self, force=False):
        if not self.is_prepared() or force:
            self._c.execute('''UPDATE project
                            SET prepared_on = datetime('now', 'localtime')''')
            self._db.commit()
        else:
            raise RuntimeError('database is already prepared')

    def n_blocks(self):
        self._c.execute('SELECT COUNT(id) FROM block')
        return self._c.fetchone()[0]

    def n_daligner_jobs(self):
        self._c.execute('SELECT COUNT(block_id1) FROM daligner_job')
        return self._c.fetchone()[0]

    def n_daligner_jobs_not_started(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = ?''', (slurm_utils.status.notstarted,))
        return self._c.fetchone()[0]

    def n_daligner_jobs_running(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = ?''', (slurm_utils.status.running,))
        return self._c.fetchone()[0]

    def n_daligner_jobs_finished(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = ?''', (slurm_utils.status.completed,))
        return self._c.fetchone()[0]

    def n_daligner_jobs_cancelled(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = ?''', (slurm_utils.status.cancelled,))
        return self._c.fetchone()[0]

    def n_daligner_jobs_failed(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = ?''', (slurm_utils.status.failed,))
        return self._c.fetchone()[0]

    def n_daligner_jobs_pending(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = ?''', (slurm_utils.status.pending,))
        return self._c.fetchone()[0]

    def get_project_name(self):
        self._c.execute('SELECT name FROM project')
        return self._c.fetchone()[0]

    def get_coverage(self):
        self._c.execute('SELECT coverage FROM project')
        return self._c.fetchone()[0]

    def info(self, key=None):
        self._c.execute('''SELECT name, coverage, started_on, prepared_on FROM project''')
        res = self._c.fetchone()
        if key is None:
            return {'name': res[0],
                    'coverage': res[1],
                    'started on': res[2],
                    'prepared on': res[3] if res[3] is not None else 'Not prepared',
                    'blocks': self.n_blocks(),
                    'daligner jobs': self.n_daligner_jobs(),
                    'daligner jobs running': self.n_daligner_jobs_running(),
                    'daligner jobs finished': self.n_daligner_jobs_finished(),
                    'daligner jobs pending': self.n_daligner_jobs_pending(),
                    'daligner jobs cancelled': self.n_daligner_jobs_cancelled(),
                    'daligner jobs failed': self.n_daligner_jobs_failed(),
                    'daligner jobs not started': self.n_daligner_jobs_not_started()}
        else:
            if key not in res.keys():
                raise KeyError('"{0}" not a valid key'.format(key))
            return res[res.keys().index(key)]
