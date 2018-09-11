import os
import sqlite3

from marvelous_jobs import slurm_utils

class marvel_db:

    def __init__(self, filename, name, coverage, force=False):
        is_new = not os.path.exists(filename)
        self._db = sqlite3.connect(filename)
        self._db.row_factory = sqlite3.Row
        self._c = self._db.cursor()

        if force:
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
                                 status TEXT NOT NULL DEFAULT 'notstarted',
                                 jobid INT,
                                 last_update TEXT,
                                 PRIMARY KEY(block_id1, block_id2),
                                 FOREIGN KEY(block_id1) REFERENCES block(id),
                                 FOREIGN KEY(block_id2) REFERENCES block(id))''')
            self._c.execute('''CREATE TABLE masking_job
                                (node TEXT,
                                 ip TEXT,
                                 status TEXT NOT NULL DEFAULT 'notstarted',
                                 jobid INT,
                                 last_update TEXT,
                                 PRIMARY KEY(jobid))''')

            self._c.execute('''INSERT INTO project
                                (name, coverage, started_on)
                                VALUES (?, ?, datetime('now'))''', (name, coverage))
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
        self._c.execute('''INSERT INTO block
                            (id, name)
                            VALUES (?, ?)''', (id, name))
        self._db.commit()

    def add_daligner_job(self, id1, id2, priority):
        self._c.execute('''INSERT INTO daligner_job
                        (block_id1, block_id2, priority, last_update)
                        VALUES (?, ?, ?, datetime('now'))''', (id1, id2, priority))
        self._db.commit()

    def add_masking_job(self, job):
        self._c.execute('''SELECT COUNT(node) FROM masking_job''')
        n = self._c.fetchone()[0]

        if n > 0:
            self._c.execute('DELETE FROM masking_job')
        self._c.execute('''INSERT INTO masking_job
                        (node, ip, last_update)
                        VALUES (?, ?, datetime('now'))''', (job.node, job.ip))
        self._db.commit()

    def update_masking_job_id(self, jobid):
        self._c.execute('UPDATE masking_job SET jobid = ?', (jobid,))
        self._db.commit()
        self.update_masking_job_status()

    def update_masking_job_status(self):
        self._c.execute('SELECT jobid FROM masking_job')
        jobid = self._c.fetchone()[0]

        job_status = slurm_utils.get_job_status(jobid)
        self._c.execute('''UPDATE masking_job
                        SET status = ?, last_update = datetime('now')''',
                        (job_status,))

        self._db.commit()

    def masking_status(self):
        self._c.execute('SELECT jobid, status, last_update FROM masking_job')
        return self._c.fetchone()

    def is_prepared(self):
        self._c.execute('SELECT prepared_on FROM project')
        return self._c.fetchone()[0] is not None

    def prepare(self, force=False):
        if not self.is_prepared() or force:
            self._c.execute('''UPDATE project
                            SET prepared_on = datetime('now')''')
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
                        WHERE status = 'notstarted' ''')
        return self._c.fetchone()[0]

    def n_daligner_jobs_running(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = 'running' ''')
        return self._c.fetchone()[0]

    def n_daligner_jobs_finished(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM daligner_job
                        WHERE status = 'finished' ''')
        return self._c.fetchone()[0]

    def info(self, key=None):
        self._c.execute('''SELECT name, coverage, started_on, prepared_on FROM project''')
        res = self._c.fetchone()
        if key is None:
            return {'name': res[0],
                    'coverage': res[1],
                    'started on': res[2],
                    'prepared on': res[3],
                    'blocks': self.n_blocks(),
                    'daligner jobs': self.n_daligner_jobs(),
                    'daligner jobs running': self.n_daligner_jobs_running(),
                    'daligner jobs finished': self.n_daligner_jobs_finished(),
                    'daligner jobs not started': self.n_daligner_jobs_not_started()}
        else:
            if key not in res.keys():
                raise KeyError('"{0}" not a valid key'.format(key))
            return res[res.keys().index(key)]
