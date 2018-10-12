import hashlib
import itertools
import os
from subprocess import Popen, PIPE

import marvel
import marvelous_jobs as mj

class marvel_job:

    def __init__(self, args, jobname, filename,
                 log_filename=None, account=None,
                 jobid=None, **kwargs):
        config = mj.marvelous_config()
        self.sbatch_args = kwargs
        self.args = args
        self.jobname = jobname
        self.jobid = jobid
        self.account = account
        self.filename = filename
        self.logfile = log_filename

        if type(self.args[0]) is not list:
            self.args = [self.args]

    def commandline(self):
        lines = []
        for args in self.args:
            lines.append(' '.join(x for x in args if len(x) > 0))
        return '\n'.join(lines)

    def save_script(self):
        str_script = str(self)
        if os.path.isfile(self.filename):
            with open(self.filename) as f:
                file_md5 = hashlib.md5(f.read().encode('utf8')).digest()
            str_md5 = hashlib.md5(str_script.encode('utf8')).digest()
            if str_md5 != file_md5:
                with open(self.filename, 'w') as f:
                    f.write(str_script)
        else:
            with open(self.filename, 'w') as f:
                f.write(str_script)

    def start(self, *args):
        if not os.path.isfile(self.filename):
            self.save_script()
        p = Popen(['sbatch',
                   '-o', self.logfile,
                   '-J', self.jobname,
                   self.filename,
                   *args],
                  shell=False, stdout=PIPE, stderr=PIPE)
        output = p.communicate()
        if len(output[1].strip()) > 0:
            raise RuntimeError(output[1].decode('utf-8'))
        self.jobid = int(output[0].strip().split()[-1])
        return self.jobid

    def cancel(self):
        if self.jobid is None:
            raise RuntimeError('job has not been queued yet')
        p = Popen(['scancel', str(self.jobid)], shell=False)
        p.wait()

    def __str__(self):
        cmd_lines = ['#!/bin/bash -l',
                     '#SBATCH -A {0}'.format(self.account) \
                        if self.account is not None else '',
                     '#SBATCH -w {0}'.format(self.sbatch_args.get('node')) \
                        if self.sbatch_args.get('node') is not None else '',
                     '#SBATCH -C {0}'.format(self.sbatch_args.get('constraint')) \
                        if self.sbatch_args.get('constraint') is not None else '',
                     '#SBATCH -t {0}'.format(self.sbatch_args.get('timelimit')) \
                        if self.sbatch_args.get('timelimit') is not None else '',
                     '#SBATCH -p {0}'.format(self.sbatch_args.get('partition')) \
                        if self.sbatch_args.get('partition') is not None else '',
                     '#SBATCH -n {0}'.format(self.sbatch_args.get('cores')) \
                        if self.sbatch_args.get('cores') is not None else '',
                     '#SBATCH --dependency after:{0}'\
                        .format(self.sbatch_args.get('after')) \
                        if self.sbatch_args.get('after') is not None else '',
                     'set -eu',
                     self.commandline()]
        return '\n'.join([x for x in cmd_lines if len(x) > 0])+'\n'

class prepare_job(marvel_job):

    filename = 'marvel_prepare.sh'

    def __init__(self, name, fasta):
        config = mj.marvelous_config()
        args = [os.path.join(marvel.config.PATH_SCRIPTS, 'DBprepare.py'),
                '--blocksize', config.get('general', 'blocksize'),
                name, fasta]
        super().__init__(args,
                         'marvel_prepare',
                         prepare_job.filename,
                         timelimit='1-00:00:00')

class daligner_job_array(marvel_job):

    filename = 'daligner_array.sh'

    def __init__(self, rowids, database_filename, script_directory=None,
                 masking_jobid=None, masking_port=None, account=None,
                 timelimit='1:00:00', verbose=True, identity=True,
                 tuple_suppression_frequency=20, correlation_rate=0.7,
                 threads=4):
        self.rowids = rowids
        self.use_masking_server = masking_jobid is not None
        if script_directory is None:
            self.filename = daligner_job_array.filename
        else:
            self.filename = os.path.join(script_directory,
                                         daligner_job_array.filename)

        args = [
            ['project=$(sqlite3 {0} "SELECT name FROM project")' \
                .format(database_filename)],
            ['block1=$(sqlite3 {0}'.format(database_filename),
             '"SELECT block_id1 FROM daligner_job',
             'WHERE rowid = $SLURM_ARRAY_TASK_ID")'],
            ['block2=$(sqlite3 {0}'.format(database_filename),
             '"SELECT block_id2 FROM daligner_job',
             'WHERE rowid = $SLURM_ARRAY_TASK_ID")'],
            [],
            # Masking server
            ['maskip=$(sqlite3 {0} "SELECT ip FROM masking_job")' \
                .format(database_filename)],
            ['if [[ {0} = true ]] && [[ -z $maskip ]]; then' \
                .format('true' if self.use_masking_server else 'false')],
            ['\techo >&2 "error: no masking server available"'],
            ['\texit 1'],
            ['fi'],
            [],
            # daligner
            [os.path.join(marvel.config.PATH_BIN, 'daligner'),
             '-v' if verbose else '',
             '-I' if identity else '',
             '-t', str(tuple_suppression_frequency),
             '-e', str(correlation_rate),
             '-D' if self.use_masking_server else '',
             '${{maskip}}:{0}'.format(masking_port) \
                if self.use_masking_server else '',
             '-j', str(threads),
             '"${project}.${block1}"', '"${project}.${block2}"']
        ]

        super().__init__(args, 'daligner_array',
                         self.filename,
                         timelimit=timelimit,
                         cores=threads,
                         after=masking_jobid,
                         account=account)

    def rowid_str(self):
        id_groups = []
        for k, g in itertools.groupby(enumerate(self.rowids),
                                      lambda x: x[0] - x[1]):
            group = list(map(lambda x: x[1], g))
            gmin = min(group)
            gmax = max(group)
            if gmin == gmax:
                id_groups.append(str(gmin))
            else:
                id_groups.append('{0}-{1}'.format(gmin, gmax))
        return ','.join(id_groups)

class daligner_job(marvel_job):

    filename = 'daligner.sh'

    def __init__(self, rowid, database_filename, script_directory=None,
                 masking_jobid=None, masking_port=None, jobid=None,
                 priority=None, status=None, account=None,
                 timelimit='1:00:00', verbose=True, identity=True,
                 tuple_suppression_frequency=20, correlation_rate=0.7,
                 threads=4):
        self.rowid = rowid
        self.priority = priority
        self.status = status
        self.use_masking_server = masking_jobid is not None
        if script_directory is None:
            self.filename = daligner_job.filename
        else:
            self.filename = os.path.join(script_directory,
                                         daligner_job.filename)

        args = [
            ['rowid=$1'],
            ['project=$(sqlite3', database_filename,
             '"SELECT name FROM project")'],
            # Get block information
            ['block1=$(sqlite3', database_filename,
             '"SELECT block_id1 FROM daligner_job WHERE rowid = $rowid")'],
            ['block2=$(sqlite3', database_filename,
             '"SELECT block_id2 FROM daligner_job WHERE rowid = $rowid")'],
            [],
            # Masking server
            ['maskip=$(sqlite3 {0} "SELECT ip FROM masking_job")' \
                .format(database_filename)],
            ['if [[ {0} = true ]] && [[ -z $maskip ]]; then' \
                .format('true' if self.use_masking_server else 'false')],
            ['\techo >&2 "error: no masking server available"'],
            ['\texit 1'],
            ['fi'],
            [],
            # daligner
            [os.path.join(marvel.config.PATH_BIN, 'daligner'),
             '-v' if verbose else '',
             '-I' if identity else '',
             '-t', str(tuple_suppression_frequency),
             '-e', str(correlation_rate),
             '-D' if self.use_masking_server else '',
             '${{maskip}}:{0}'.format(masking_port) \
                if self.use_masking_server else '',
             '-j', str(threads),
             '"${project}.${block1}"', '"${project}.${block2}"']
        ]

        super().__init__(args,
                         'daligner_{0}'.format(self.rowid),
                         self.filename,
                         timelimit=timelimit,
                         jobid=jobid, cores=threads,
                         after=masking_jobid,
                         account=account)

    def start(self):
        return super().start(str(self.rowid))

class masking_server_job(marvel_job):

    filename = 'marvel_masking.sh'

    def __init__(self, name, coverage, jobid=None):
        config = mj.marvelous_config()
        jobname = 'marvel_masking'
        self.port = config.getint('DMserver', 'port')
        self.threads = config.getint('DMserver', 'threads')
        args = [
            os.path.join(marvel.config.PATH_BIN, 'DMserver'),
            '-t', config.get('DMserver', 'threads'),
            '-p', config.get('DMserver', 'port'),
            name, str(coverage),
            config.get('DMserver', 'checkpoint_file')
        ]
        self.constraint = config.get('DMserver', 'constraint')
        self.ip = None
        if jobid is not None:
            node = mj.slurm_utils.get_job_node(jobid)
            if node is not None:
                self.ip = mj.slurm_utils.get_node_ip(node)
        super().__init__(args,
                         jobname,
                         masking_server_job.filename,
                         jobid=jobid,
                         timelimit=config.get('DMserver', 'timelimit'),
                         partition='node',
                         cores=1,
                         constraint=self.constraint)

    def stop(self):
        dmctl = os.path.join(marvel.config.PATH_BIN, 'DMctl')
        p = Popen([dmctl,
                   '-h', self.ip,
                   '-p', str(self.port),
                   'shutdown'],
                  shell=False)
        p.wait()
