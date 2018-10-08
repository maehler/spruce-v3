import hashlib
import os
from subprocess import Popen, PIPE

import marvel
import marvelous_jobs as mj

class marvel_job:

    def __init__(self, args, jobname, filename, jobid=None, **kwargs):
        config = mj.marvelous_config()
        self.sbatch_args = kwargs
        self.args = args
        self.jobname = jobname
        self.jobid = jobid
        self.account = config.get('general', 'account')
        self.filename = os.path.join(config.get('general', 'script_directory'), filename)
        self.logfile = os.path.join(config.get('general', 'log_directory'),
                                    '{0}.log'.format(self.jobname))

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

class daligner_job(marvel_job):

    filename = 'daligner.sh'

    def __init__(self, block_id1, block_id2, use_masking_server=False,
                 jobid=None, priority=None, status=None):
        config = mj.marvelous_config()
        db = mj.marvel_db.from_file(config.get('general', 'database'))

        self.priority = priority
        self.status = status
        self.use_masking_server = use_masking_server
        if use_masking_server:
            masking_jobid = db.get_masking_jobid()
        else:
            masking_jobid = None

        self.block_id1 = block_id1
        self.block_id2 = block_id2

        project_name = db.get_project_name()
        block1 = '{0}.{1}'.format(project_name, self.block_id1)
        block2 = '{0}.{1}'.format(project_name, self.block_id2)
        jobname = '{0}.{1}.dalign'.format(block1, block2)

        mask_ip_args = [
            ['maskip=$(sqlite3 {} "SELECT ip FROM masking_job")' \
                .format(config.get('general', 'database'))],
            ['if [[ {0} = true ]] && [[ -z $maskip ]]; then' \
                .format('true' if use_masking_server else 'false')],
            ['\techo >&2 "error: no masking server available"'],
            ['\texit 1'],
            ['fi']
        ]

        daligner_args = [
            os.path.join(marvel.config.PATH_BIN, 'daligner'),
            '-v' if config.getboolean('daligner', 'verbose') else '',
            '-I' if config.getboolean('daligner', 'identity') else '',
            '-t', config.get('daligner', 'tuple_suppression_frequency'),
            '-e', config.get('daligner', 'correlation_rate'),
            '-D' if use_masking_server else '',
            '${{maskip}}:{0}'.format(config.get('DMserver', 'port')) \
                if use_masking_server else '',
            '-j', config.get('daligner', 'threads'),
            '"{0}.$1"'.format(project_name), '"{0}.$2"'.format(project_name)
        ]

        gzip_args = [
            ['gzip', '-f',
             '{0}/d001_$(printf "%05d" $1)/{1}.$1.{1}.$2.las' \
                .format(config.get('general', 'directory'), project_name)],
            ['if [[ $1 != $2 ]]; then'],
            ['\tgzip', '-f',
             '{0}/d001_$(printf "%05d" $2)/{1}.$2.{1}.$1.las' \
                .format(config.get('general', 'directory'), project_name)],
            ['fi']
        ]

        super().__init__(mask_ip_args + [daligner_args] + gzip_args,
                         jobname,
                         daligner_job.filename,
                         timelimit=config.get('daligner', 'timelimit'),
                         jobid=jobid, cores=config.get('daligner', 'threads'),
                         after=masking_jobid)

    def start(self):
        return super().start(str(self.block_id1), str(self.block_id2))

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
