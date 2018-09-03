import os

import marvel

class marvel_job:

    def __init__(self, executable, jobname, args, config):
        self.executable = executable
        self.args = args
        self.jobname = jobname
        self.account = config.get('general', 'account')

    def commandline(self):
        return ' '.join([self.executable] + self.args)

    def save(self):
        pass

    def __str__(self):
        cmd_lines = ['#!/bin/bash -l',
                     'set -eu',
                     '#SBATCH -A {0}'.format(self.account) \
                        if self.account is not None else '',
                     self.commandline()]
        return '\n'.join([x for x in cmd_lines if len(x) > 0])

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
                         jobname, args, config)

