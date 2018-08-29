import os
import sqlite3

class marvel_db:

    def __init__(self, filename, name, coverage, force=False):
        is_new = not os.path.exists(filename)
        self._db = sqlite3.connect(filename)
        self._db.row_factory = sqlite3.Row
        self._c = self._db.cursor()

        if force:
            self._c.execute('DROP TABLE IF EXISTS job')
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
            self._c.execute('''CREATE TABLE job
                                (block_id1 INT NOT NULL,
                                 block_id2 INT NOT NULL,
                                 status TEXT NOT NULL DEFAULT 'notstarted',
                                 last_update TEXT,
                                 PRIMARY KEY(block_id1, block_id2),
                                 FOREIGN KEY(block_id1) REFERENCES block(id),
                                 FOREIGN KEY(block_id2) REFERENCES block(id))''')

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

    def add_block(self, id, name):
        self._c.execute('''INSERT INTO block
                            (id, name)
                            VALUES (?, ?)''', (id, name))
        self._db.commit()

    def add_job(self, id1, id2):
        self._c.execute('''INSERT INTO job
                        (block_id1, block_id2, last_update)
                        VALUES (?, ?, datetime('now'))''', (id1, id2))
        self._db.commit()

    def is_prepared(self):
        self._c.execute('SELECT prepared_on FROM project')
        return self._c.fetchone()[0] is not None

    def prepare(self):
        if not self.is_prepared():
            self._c.execute('''UPDATE project
                            SET prepared_on = datetime('now')''')
            self._db.commit()
        else:
            raise RuntimeError('database is already prepared')

    def n_blocks(self):
        self._c.execute('SELECT COUNT(id) FROM block')
        return self._c.fetchone()[0]

    def n_jobs(self):
        self._c.execute('SELECT COUNT(block_id1) FROM job')
        return self._c.fetchone()[0]

    def n_jobs_not_started(self):
        self._c.execute('''SELECT COUNT(block_id1)
                        FROM job
                        WHERE status = 'notstarted' ''')
        return self._c.fetchone()[0]

    def info(self, key=None):
        self._c.execute('''SELECT name, coverage, started_on FROM project''')
        res = self._c.fetchone()
        if key is None:
            return {'name': res[0], 'coverage': res[1], 'started on': res[2],
                    'blocks': self.n_blocks(),
                    'jobs': self.n_jobs(),
                    'jobs not started': self.n_jobs_not_started()}
        else:
            if key not in res.keys():
                raise KeyError('"{0}" not a valid key'.format(key))
            return res[res.keys().index(key)]
