import hashlib
import itertools
import os
import re
from subprocess import Popen, PIPE

import marvel
import marvelous_jobs as mj

class marvel_job:

    def __init__(self, args, jobname, filename,
                 log_filename=None, account=None,
                 jobid=None, **kwargs):
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
            lines.append(' '.join(str(x) for x in args if x is not None \
                                  and len(str(x)) > 0))
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

    def start(self, dryrun=False, force_write=False, *args):
        if not os.path.isfile(self.filename) or force_write:
            self.save_script()
        run_args = ['sbatch',
                    '--parsable',
                    '--array' if 'array' in self.sbatch_args else '',
                    self.sbatch_args.get('array') \
                        if 'array' in self.sbatch_args else '',
                    '-M' if self.sbatch_args.get('cluster') is not None \
                        else '',
                    self.sbatch_args.get('cluster') \
                        if self.sbatch_args.get('cluster') is not None \
                            else '',
                    '-o' if self.logfile is not None else '',
                    self.logfile if self.logfile is not None else '',
                    '-J', self.jobname,
                    self.filename,
                    *args]
        clean_args = [str(x) for x in run_args if len(str(x)) > 0]
        if dryrun:
            return ' '.join(clean_args)
        p = Popen(clean_args, shell=False, stdout=PIPE, stderr=PIPE)
        output = p.communicate()
        if len(output[1].strip()) > 0:
            raise RuntimeError(output[1].decode('utf-8'))
        self.jobid = int(output[0].decode('utf-8').split(';')[0])
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

    def __init__(self, name, fasta, blocksize,
                 script_directory=None, log_directory=None,
                 account=None):
        args = [os.path.join(marvel.config.PATH_SCRIPTS, 'DBprepare.py'),
                '--blocksize', blocksize,
                name, fasta]
        jobname = 'marvel_prepare'
        if script_directory is not None:
            self.filename = os.path.join(script_directory, prepare_job.filename)
        else:
            self.filename = prepare_job.filename
        if log_directory is not None:
            self.logfile = os.path.join(log_directory, '{0}.log'.format(jobname))
        super().__init__(args,
                         jobname,
                         self.filename,
                         log_filename=self.logfile,
                         account=account,
                         timelimit='1-00:00:00')

    def start(self, dryrun=False, *args):
        return super().start(dryrun=dryrun, force_write=True, *args)

class daligner_job_array(marvel_job):

    filename = 'daligner_array.sh'

    def __init__(self, n_tasks, database_filename, script_directory=None,
                 run_directory=None, reservation_token=None,
                 log_directory=None, jobs_per_task=100,
                 max_simultaneous_tasks=None, masking_jobid=None,
                 masking_port=None,account=None, timelimit='1-00:00:00',
                 verbose=True, identity=True, tuple_suppression_frequency=20,
                 correlation_rate=0.7, threads=4):
        self.n_tasks = n_tasks
        self.jobs_per_task = jobs_per_task
        self.array_indices = range(1, n_tasks + 1)
        self.use_masking_server = masking_jobid is not None
        self.max_simultaneous_tasks = max_simultaneous_tasks

        if reservation_token is None:
            raise ValueError('reservation token must not be None')
        self.reservation_token = reservation_token

        if run_directory is None:
            self.run_directory = os.path.abspath('.')
        else:
            self.run_directory = run_directory

        if script_directory is None:
            self.filename = daligner_job_array.filename
        else:
            self.filename = os.path.join(script_directory,
                                         daligner_job_array.filename)
        if log_directory is None:
            self.logfile = '{0}_{1}_%A_%a.log' \
                    .format(os.path.splitext(daligner_job_array.filename)[0],
                            self.reservation_token)
        else:
            self.logfile = os.path.join(log_directory, '{0}_{1}_%A_%a.log' \
                                        .format(os.path.splitext(daligner_job_array.filename)[0],
                                                self.reservation_token))

        sqlite_timeout = '-init <(echo .timeout 30000)'
        args = [
            # Setup
            ['reservation=$1'],
            ['reservation_filename="{0}/daligner_task_${{reservation}}_${{SLURM_ARRAY_TASK_ID}}.txt"' \
             .format(run_directory)],
            ['echo', '"Using reservation in $reservation_filename"'],
            ['project=$(sqlite3 {0} {1} "SELECT name FROM project")' \
                .format(sqlite_timeout, database_filename)],
            [],
            # Masking server
            ['maskip=$(sqlite3 {0} {1} "SELECT ip FROM masking_job")' \
                .format(sqlite_timeout, database_filename)],
            ['if [[ {0} = true ]] && [[ -z $maskip ]]; then' \
                .format('true' if self.use_masking_server else 'false')],
            ['\techo >&2 "error: no masking server available"'],
            ['\texit 1'],
            ['fi'],
            [],
            # daligner
            ['while', 'IFS=$\'\\t\'', 'read', '-ra', 'line;', 'do'],
            ['\tsource_block=${line[0]}'],
            ['\tn=$(expr ${#line[@]} - 1)'],
            ['\tn_comparisons=$(expr $n \/ 2)'],
            ['\trowids=(${line[@]:1:$n_comparisons})'],
            ['\tblocks=(${line[@]:$(expr $n_comparisons + 1)})'],
            ['\techo "Starting job(s) ${rowids[@]}: '
             '${source_block} vs ${blocks[@]}"'],
            ['\t{0}'.format(os.path.join(marvel.config.PATH_BIN, 'daligner')),
             '-v' if verbose else '',
             '-I' if identity else '',
             '-t', tuple_suppression_frequency,
             '-e', correlation_rate,
             '-D' if self.use_masking_server else '',
             '${{maskip}}:{0}'.format(masking_port) \
                if self.use_masking_server else '',
             '-j', threads,
             '"${project}.${source_block}"', '"${blocks[@]/#/${project}.}"'],
            ['\techo "Finished job(s) ${rowids[@]}: '
             '${source_block} vs ${blocks[@]}"'],
            ['done', '<', '$reservation_filename']
        ]

        super().__init__(args, 'daligner_array',
                         self.filename,
                         log_filename=self.logfile,
                         timelimit=timelimit,
                         cores=threads,
                         after=masking_jobid,
                         account=account,
                         array=self._array_index_str())

    def _array_index_str(self):
        id_groups = []
        for k, g in itertools.groupby(enumerate(self.array_indices),
                                      lambda x: x[0] - x[1]):
            group = list(map(lambda x: x[1], g))
            gmin = min(group)
            gmax = max(group)
            if gmin == gmax:
                id_groups.append(str(gmin))
            else:
                id_groups.append('{0}-{1}'.format(gmin, gmax))
        array_str = ','.join(id_groups)
        if self.max_simultaneous_tasks is not None:
            array_str += '%{0}'.format(self.max_simultaneous_tasks)
        return array_str

    def start(self, dryrun=False, force_write=False):
        return super().start(dryrun, force_write, self.reservation_token)

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
             '-t', tuple_suppression_frequency,
             '-e', correlation_rate,
             '-D' if self.use_masking_server else '',
             '${{maskip}}:{0}'.format(masking_port) \
                if self.use_masking_server else '',
             '-j', threads,
             '"${project}.${block1}"', '"${project}.${block2}"']
        ]

        super().__init__(args,
                         'daligner_{0}'.format(self.rowid),
                         self.filename,
                         timelimit=timelimit,
                         jobid=jobid, cores=threads,
                         after=masking_jobid,
                         account=account)

    def start(self, dryrun=False):
        return super().start(dryrun=dryrun, force_write=False, *[self.rowid])

class masking_server_job(marvel_job):

    filename = 'marvel_masking.sh'

    def __init__(self, name, coverage, checkpoint_file,
                 script_directory=None, log_directory=None,
                 jobid=None, port=12345, threads=4, constraint=None,
                 cluster=None, account=None, timelimit='10-00:00:00'):
        jobname = 'marvel_masking'
        self.port = port
        self.threads = threads
        if script_directory is not None:
            self.filename = os.path.join(script_directory,
                                         masking_server_job.filename)
        else:
            self.filename = masking_server_job.filename
        if log_directory is not None:
            self.logfile = os.path.join(log_directory,
                                        '{0}.log'.format(jobname))
        else:
            self.logfile = '{0}.log'.format(jobname)
        args = [
            os.path.join(marvel.config.PATH_BIN, 'DMserver'),
            '-t', self.threads,
            '-p', self.port,
            name, coverage,
            checkpoint_file
        ]
        self.constraint = constraint
        self.cluster = cluster
        self.ip = None
        if jobid is not None:
            node = mj.slurm_utils.get_job_node(jobid)
            if node is not None:
                self.ip = mj.slurm_utils.get_node_ip(node)
        super().__init__(args,
                         jobname,
                         self.filename,
                         log_filename=self.logfile,
                         jobid=jobid,
                         account=account,
                         timelimit=timelimit,
                         partition='node',
                         cores=1,
                         constraint=self.constraint,
                         cluster=self.cluster)

    def start(self, dryrun=False, *args):
        return super().start(dryrun=dryrun, force_write=True, *args)

    def stop(self):
        dmctl = os.path.join(marvel.config.PATH_BIN, 'DMctl')
        p = Popen([dmctl,
                   '-h', self.ip,
                   '-p', str(self.port),
                   'shutdown'],
                  shell=False)
        p.wait()
