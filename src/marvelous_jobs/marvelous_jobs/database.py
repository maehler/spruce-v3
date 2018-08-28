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
                                name TEXT,
                                coverage INT,
                                started_on TEXT
                               )''')
            self._c.execute('''CREATE TABLE block (
                                id INT,
                                name TEXT
                               )''')
            self._c.execute('''CREATE TABLE job
                                (block_id1 INT,
                                 block_id2 INT,
                                 status TEXT,
                                 last_update TEXT)''')

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

    def info(self, key=None):
        self._c.execute('''SELECT name, coverage, started_on FROM project''')
        res = self._c.fetchone()
        if key is None:
            return {'name': res[0], 'coverage': res[1], 'started on': res[2]}
        else:
            if key not in res.keys():
                raise KeyError('"{0}" not a valid key'.format(key))
            return res[res.keys().index(key)]
