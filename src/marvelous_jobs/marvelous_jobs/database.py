from functools import reduce
import os
import re
import sqlite3
import subprocess
import time

from marvelous_jobs import slurm_utils
from marvelous_jobs.job import daligner_job

class marvel_db:

    def __init__(self, filename, name, coverage, force=False):
        is_new = not os.path.exists(filename)
        self.filename = filename
        self._db = sqlite3.connect(filename, timeout=60.0)
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
                                 use_masking INT NOT NULL DEFAULT 1,
                                 jobid TEXT,
                                 last_update TEXT,
                                 reservation_token TEXT,
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
        db = sqlite3.connect(filename, timeout=60.0)
        c = db.cursor()
        c.execute('SELECT name, coverage FROM project')
        name, coverage = c.fetchone()
        db.close()
        return cls(filename, name, coverage, False)

    def backup(self, filename):
        p = subprocess.Popen([
            'sqlite3', self.filename,
            '.backup {0}'.format(filename)
        ], shell=False, stderr=subprocess.PIPE)
        output = p.communicate()
        if len(output[1].strip()) > 0:
            raise RuntimeError(output[1].strip().decode('utf-8'))

    def integrity_check(self):
        self._c.execute('PRAGMA integrity_check')
        return self._c.fetchone()[0] == 'ok'

    def begin_exclusive(self):
        self._c.execute('BEGIN EXCLUSIVE')

    def stop_exclusive(self):
        self._c.execute('COMMIT')

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

    def add_daligner_job(self, rowid, id1, id2, priority, use_masking_server=True):
        self._c.execute('''INSERT INTO daligner_job
                        (rowid, block_id1, block_id2, priority, use_masking, last_update)
                        VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'))''',
                        (rowid, id1, id2, priority, 1 if use_masking_server else 0))
        self._db.commit()

    def add_daligner_jobs(self, jobs):
        query = '''INSERT INTO daligner_job
                (rowid, block_id1, block_id2, priority, use_masking, last_update)
                VALUES '''

        for jtuple in jobs:
            query += '(?, ?, ?, ?, ?, datetime("now", "localtime")),'

        query = query.rstrip(',')

        self._c.execute(query, tuple(y for x in jobs for y in x))
        self._db.commit()

    def any_using_masking(self):
        self._c.execute('SELECT COUNT(*) FROM daligner_job WHERE use_masking = 1')
        return self._c.fetchone()[0] > 0

    def get_daligner_jobids(self, rowids):
        query = 'SELECT rowid, jobid FROM daligner_job WHERE rowid IN ({0})' \
                .format(','.join('?' for x in rowids))
        self._c.execute(query, tuple(rowids))
        return {x[0]: x[1] for x in self._c.fetchall()}

    def get_daligner_tokens(self, rowids):
        query = 'SELECT rowid, reservation_token from daligner_job WHERE rowid in ({0})' \
                .format(','.join('?' for x in rowids))
        self._c.execute(query, tuple(rowids))
        return {x[0]: x[1] for x in self._c.fetchall()}

    def update_daligner_jobs(self, rowids, log_directory):
        if type(rowids) is not list:
            rowids = [rowids]

        if len(rowids) == 0:
            return

        tokens = self.get_daligner_tokens(rowids)
        unique_tokens = set(x for x in tokens.values() if x is not None)

        if len(unique_tokens) == 0:
            return

        token_regex = re.compile('|'.join(unique_tokens))
        jobid_regex = re.compile(r'_(\d+_\d+)\.log$')
        started_regex = re.compile(r'^Starting job (\d+)')
        completed_regex = re.compile(r'^Finished job (\d+)')

        start = time.time()

        statuses = {ri: {'started': False,
                         'completed': False,
                         'jobid': None} for ri in rowids}

        for fname in os.listdir(log_directory):
            full_fname = os.path.join(log_directory, fname)
            if not os.path.isfile(full_fname):
                continue
            if not token_regex.search(full_fname):
                continue
            jobid_match = jobid_regex.search(full_fname)
            if not jobid_match:
                raise ValueError('no job id found in logfile')
            jobid = jobid_match.group(1)
            with open(full_fname) as f:
                for line in f:
                    start_match = started_regex.search(line)
                    if start_match:
                        rowid = int(start_match.group(1))
                        if rowid not in rowids:
                            continue
                        statuses[rowid]['started'] = True
                        statuses[rowid]['jobid'] = jobid

                    end_match = completed_regex.search(line)
                    if end_match:
                        rowid = int(end_match.group(1))
                        if rowid not in rowids:
                            continue
                        statuses[rowid]['completed'] = True
                        statuses[rowid]['jobid'] = jobid

        print('fetched status in {0}'.format(time.time() - start))

        query = '''UPDATE daligner_job SET
            status = ?,
            jobid = ?,
            last_update = datetime("now", "localtime")
        WHERE rowid = ?'''

        start = time.time()
        for ri, status in statuses.items():
            textstatus = slurm_utils.status.reserved
            if status['completed']:
                textstatus = slurm_utils.status.completed
            elif status['started']:
                textstatus = slurm_utils.status.running
            self._c.execute(query, (textstatus, status['jobid'], ri))
        self._db.commit()
        print('updated database in {0}'.format(time.time() - start))

    def get_daligner_jobs(self, max_jobs=None, status=()):
        query = 'SELECT rowid FROM daligner_job'
        if type(status) is not str and len(status) > 0:
            query += ' WHERE '
            for i in range(len(status) - 1):
                query += 'status = ? OR '
            query += 'status = ?'
        elif type(status) is str:
            query += ' WHERE status = ?'
            status = (status,)
        query += ' ORDER BY priority'
        if max_jobs is not None:
            query += ' LIMIT ?'
            args = status + (max_jobs,)
        else:
            args = status
        self._c.execute(query, args)

        return [j[0] for j in self._c.fetchall()]

    def reserve_daligner_jobs(self, token, max_jobs=1, comparisons_per_job=1):
        self.begin_exclusive()

        reservation = []

        for ji in range(max_jobs):
            rowids = self.get_daligner_jobs(max_jobs=comparisons_per_job,
                                            status=slurm_utils.status.notstarted)
            source_query = 'SELECT block_id1 FROM daligner_job WHERE rowid = ?'
            self._c.execute(source_query, (rowids[0],))
            source_block = self._c.fetchone()[0]

            query = '''SELECT rowid, block_id1, block_id2
                FROM daligner_job
                WHERE block_id1 = ? AND rowid IN ({0})''' \
                        .format(','.join('?' for ri in rowids))
            self._c.execute(query, (source_block,) + tuple(rowids))
            jobs = self._c.fetchall()

            assert(len(set(x[1] for x in jobs)) == 1)

            reservation.append({
                'source_block': source_block,
                'target_blocks': [x[2] for x in jobs],
                'rowids': [x[0] for x in jobs]
            })

            reserve_query = '''UPDATE daligner_job
                SET status = "{0}",
                reservation_token = "{1}",
                last_update = datetime('now', 'localtime')
                WHERE rowid IN ({2})''' \
                        .format(slurm_utils.status.reserved, token,
                                ','.join('?' for ri in range(len(jobs))))
            self._c.execute(reserve_query, reservation[-1]['rowids'])

        self.stop_exclusive()

        return reservation

    def reset_daligner_jobs(self, rowids):
        query = 'UPDATE daligner_job SET status = ? WHERE rowid IN ({0})' \
                .format(','.join('?' for x in rowids))
        self._c.execute(query,
                        (slurm_utils.status.notstarted,) + tuple(rowids))
        self._db.commit()

    def get_n_running_tasks(self):
        query = '''SELECT COUNT(DISTINCT jobid)
        FROM daligner_job WHERE status IN (?, ?, ?, ?, ?)'''
        self._c.execute(query, (slurm_utils.status.running,
                                slurm_utils.status.reserved,
                                slurm_utils.status.pending,
                                slurm_utils.status.completing,
                                slurm_utils.status.configuring))
        return self._c.fetchone()[0]

    def cancel_daligner_reservation(self):
        query = 'UPDATE daligner_job SET status = ? WHERE status = ?'
        self._c.execute(query, (slurm_utils.status.notstarted,
                                slurm_utils.status.reserved))
        self._db.commit()

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
        self._c.execute('SELECT COUNT(status) FROM masking_job')
        return self._c.fetchone()[0] > 0

    def add_masking_job(self, job):
        if self.has_masking_job():
            self._c.execute('DELETE FROM masking_job')
        self._c.execute('''INSERT INTO masking_job
                        (ip, last_update)
                        VALUES (?, datetime('now', 'localtime'))''',
                        (job.ip,))
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
        node_ip = None
        if job_status == slurm_utils.status.running:
            node = slurm_utils.get_job_node(jobid)
            node_ip = slurm_utils.get_node_ip(node)
        self._c.execute('''UPDATE masking_job
                        SET ip = ?, status = ?, last_update = datetime('now',
                        'localtime')''',
                        (node_ip, job_status))

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

    def n_daligner_jobs(self, *args):
        query = 'SELECT COUNT(block_id1) FROM daligner_job'
        if len(args) > 0:
            query += ' WHERE '
            if len(args) > 1:
                for i in range(0, len(args) - 1):
                    query += 'status = ? OR '
            query += 'status = ?'
            self._c.execute(query, args)
        else:
            self._c.execute(query)

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
                    'daligner jobs running':
                        self.n_daligner_jobs(slurm_utils.status.running),
                    'daligner jobs finished':
                        self.n_daligner_jobs(slurm_utils.status.completed),
                    'daligner jobs pending':
                        self.n_daligner_jobs(slurm_utils.status.pending),
                    'daligner jobs reserved':
                        self.n_daligner_jobs(slurm_utils.status.reserved),
                    'daligner jobs cancelled':
                        self.n_daligner_jobs(slurm_utils.status.cancelled),
                    'daligner jobs failed':
                        self.n_daligner_jobs(slurm_utils.status.failed),
                    'daligner jobs not started':
                        self.n_daligner_jobs(slurm_utils.status.notstarted)}
        else:
            if key not in res.keys():
                raise KeyError('"{0}" not a valid key'.format(key))
            return res[res.keys().index(key)]
