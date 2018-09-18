import os
from subprocess import Popen, PIPE

import marvel
from marvelous_jobs import slurm_utils

class marvel_job:

    def __init__(self, executable, jobname, args, config, jobid=None, **kwargs):
        self.sbatch_args = kwargs
        self.executable = executable
        self.args = args
        self.jobname = jobname
        self.jobid = jobid
        self.account = config.get('general', 'account')
        self.filename = os.path.join(config.get('general', 'script_directory'),
                                     '{0}.sh'.format(self.jobname))
        self.logfile = os.path.join(config.get('general', 'log_directory'),
                                    '{0}.log'.format(self.jobname))

    def commandline(self):
        return ' '.join([self.executable] + self.args)

    def save_script(self):
        with open(self.filename, 'w') as f:
            f.write(str(self))

    def start(self):
        if not os.path.isfile(self.filename):
            self.save_script()
        p = Popen(['sbatch', self.filename], shell=False,
                  stdout=PIPE, stderr=PIPE)
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
                     '#SBATCH -t {0}'.format(self.sbatch_args.get('timelimit')) \
                        if self.sbatch_args.get('timelimit') is not None else '',
                     'set -eu',
                     self.commandline()]
        return '\n'.join([x for x in cmd_lines if len(x) > 0])+'\n'

class daligner_job(marvel_job):

    def __init__(self, block1, block2, config):
        jobname = '{0}.{1}.dalign'.format(block1, block2)
        args = [
            '-v' if config.getboolean('daligner', 'verbose') else '',
            '-I' if config.getboolean('daligner', 'identity') else '',
            '-t', config.get('daligner', 'tuple_suppression_frequency'),
            '-e', config.get('daligner', 'correlation_rate'),
            '-j', config.get('daligner', 'threads'),
            block1, block2
        ]
        super().__init__(os.path.join(marvel.config.PATH_BIN, 'daligner'),
                         jobname, args, config,
                         timelimit=config.get('daligner', 'timelimit'))

class masking_server_job(marvel_job):

    def __init__(self, name, coverage, node, config, jobid=None):
        jobname = 'marvel_masking'
        self.port = config.getint('DMserver', 'port')
        self.threads = config.getint('DMserver', 'threads')
        args = [
            '-t', config.get('DMserver', 'threads'),
            '-p', config.get('DMserver', 'port'),
            name, str(coverage)
        ]
        self.node = node
        self.ip = slurm_utils.get_node_ip(node)
        super().__init__(os.path.join(marvel.config.PATH_BIN, 'DMserver'),
                         jobname, args, config, jobid=jobid, node=node,
                         timelimit=config.get('DMserver', 'timelimit'))

    def stop(self):
        dmctl = os.path.join(marvel.config.PATH_BIN, 'DMctl')
        p = Popen([dmctl,
                   '-h', self.ip,
                   '-p', str(self.port),
                   'shutdown'],
                  shell=False)
        p.wait()
