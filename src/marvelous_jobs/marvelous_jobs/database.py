import os
import sqlite3

class marvel_db:

    def __init__(self, filename, name, coverage, force=False):
        is_new = not os.path.exists(filename)
        self._db = sqlite3.connect(filename)
        self._c = self._db.cursor()

        if force:
            self._c.execute('DROP TABLE IF EXISTS jobs')
            self._c.execute('DROP TABLE IF EXISTS project')

        if is_new or force:
            self._c.execute('''CREATE TABLE jobs
                                (name TEXT,
                                 block1 INT,
                                 block2 INT,
                                 status TEXT,
                                 last_update TEXT)''')
            self._c.execute('''CREATE TABLE project (
                                name TEXT,
                                coverage INT,
                                started_on TEXT
                               )''')

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

    def add_block(self):
        pass

    def info(self):
        self._c.execute('''SELECT name, coverage, started_on FROM project''')
        res = self._c.fetchone();
        return {'name': res[0], 'coverage': res[1], 'started on': res[2]}
