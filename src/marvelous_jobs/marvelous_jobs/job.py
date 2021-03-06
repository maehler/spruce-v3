import hashlib
import itertools
import os
import re
from subprocess import Popen, PIPE

import marvel
import marvelous_jobs as mj

class marvel_job:
    """Base class for all MARVEL jobs.

    This class should most likely not be instantiated
    directly, but rather be the parent of a more specialised
    class.
    """

    def __init__(self, args, jobname, filename,
                 log_filename=None, account=None,
                 jobid=None, **kwargs):
        """Instantiate a marvel_job.

        Parameters
        ---------
        args : list of list of str
            A list of lists, each representing a line
            in the generated script.
        jobname : str
            Name of the job as used by SLURM.
        filename : str
            Absolute path where the script should be
            saved.
        log_filename : str, optional
            Absolute path to the SLURM log file. If
            not given, the default log file will be
            generated.
        account : str, optional
            SLURM account that the job should run under.
        jobid : int, optional
            Set the jobid if this is a job that is
            already running.
        """
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
        """Generate a (possibly multiline) command line string.

        Returns
        -------
        str
            The generated command line.
        """
        lines = ['']
        for args in self.args:
            lines.append(' '.join(str(x) for x in args if x is not None \
                                  and len(str(x)) > 0))
        return '\n'.join(lines)

    def save_script(self):
        """Save the script on disk.

        The script is saved in the following cases:
            1. The file does not already exist.
            2. The file exists and the content
               differs from what will be written
               (based on the md5 digest).
        """
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
        """Start the job.

        Parameters
        ----------
        dryrun : bool
            If True, then return the command line for
            submitting the script. If False, submit the
            job.
        force_write : bool
            If True, write the script to file, regardless
            of whether the script already exists or not.
            If False, only write the script if it doesn't
            already exist.
        *args
            Additional arguments to be passed to the script.

        Returns
        -------
        int
            The job ID of the submission.

        Raises
        ------
        RuntimeError
            If the job submission prints on stderr.
        """
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
        """Cancel the job.

        Cancel the SLURM job, if the job has a job ID.

        Raises
        ------
        RuntimeError
            If the job does not have a jobid.
        """
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
                     '#SBATCH -C {0}' \
                        .format(self.sbatch_args.get('constraint')) \
                        if self.sbatch_args.get('constraint') is not None \
                        else '',
                     '#SBATCH -t {0}' \
                        .format(self.sbatch_args.get('timelimit')) \
                        if self.sbatch_args.get('timelimit') is not None \
                        else '',
                     '#SBATCH -p {0}' \
                        .format(self.sbatch_args.get('partition')) \
                        if self.sbatch_args.get('partition') is not None \
                        else '',
                     '#SBATCH -n {0}' \
                        .format(self.sbatch_args.get('cores')) \
                        if self.sbatch_args.get('cores') is not None else '',
                     '#SBATCH --dependency after:{0}'\
                        .format(self.sbatch_args.get('after')) \
                        if self.sbatch_args.get('after') is not None else '',
                     'set -eu',
                     self.commandline()]
        return '\n'.join([x for x in cmd_lines if len(x) > 0])+'\n'

class prepare_job(marvel_job):

    filename = 'marvel_prepare.sh'

    def __init__(self, name, fasta, blocksize, annotation_tracks=None,
                 script_directory=None, log_directory=None,
                 account=None, repeats=False):
        args = [os.path.join(marvel.config.PATH_SCRIPTS, 'DBprepare.py'),
                '--blocksize', blocksize]

        if annotation_tracks is not None:
            for at in annotation_tracks:
                args += ['-c', at]

        args += [name, fasta]
        jobname = 'marvel_prepare'
        if script_directory is not None:
            self.filename = os.path.join(script_directory, prepare_job.filename)
        else:
            self.filename = prepare_job.filename
        if log_directory is not None:
            self.logfile = os.path.join(
                log_directory, '{0}.log'.format(jobname))
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
                 masking_port=None, repeat_annotations=None,
                 account=None, timelimit='1-00:00:00',
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
            self.logfile = '{0}_{1}_%a_%A_%a.log' \
                    .format(os.path.splitext(daligner_job_array.filename)[0],
                            self.reservation_token)
        else:
            self.logfile = os.path.join(
                log_directory, '{0}_{1}_%a_%A_%a.log' \
                .format(os.path.splitext(daligner_job_array.filename)[0],
                        self.reservation_token))

        sqlite_timeout = '-init <(echo .timeout 30000)'
        args = [
            # Setup
            ['reservation=$1'],
            ['reservation_filename="{0}/daligner_task_${{reservation}}'
             '_${{SLURM_ARRAY_TASK_ID}}.txt"' \
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
            ['\techo "[$(date "+%F %T")] Starting job(s) ${rowids[@]}: '
             '${source_block} vs ${blocks[@]}"'],
            ['\tif {0}'.format(
                os.path.join(marvel.config.PATH_BIN, 'daligner')),
             '-v' if verbose else '',
             '-I' if identity else '',
             '-t', tuple_suppression_frequency,
             '-e', correlation_rate,
             '-D' if self.use_masking_server else '',
             '${{maskip}}:{0}'.format(masking_port) \
                if self.use_masking_server else '',
             '-m' if repeat_annotations is not None else '',
             repeat_annotations if repeat_annotations is not None else '',
             '-j', threads,
             '"${project}.${source_block}"',
             '"${blocks[@]/#/${project}.}"; then'],
            ['\t\techo "[$(date "+%F %T")] Finished job(s) ${rowids[@]}: '
             '${source_block} vs ${blocks[@]}"'],
            ['\telse'],
            ['\t\techo "[$(date "+%F %T")] Failed job(s) ${rowids[@]}: '
             '${source_block} vs ${blocks[@]}"'],
            ['\tfi'],
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

class merge_job_array(marvel_job):

    filename = 'las_merge.sh'

    def __init__(self, blocks, project, n_files=32,
                 max_simultaneous_tasks=None, script_directory=None,
                 log_directory=None, reservation_token=None,
                 run_directory=None, account=None,
                 timelimit='1-00:00:00', verbose=True):
        jobname = 'las_merge'

        if reservation_token is None:
            raise ValueError('reservation token must not be None')
        self.reservation_token = reservation_token

        if run_directory is None:
            self.run_directory = os.path.abspath('.')
        else:
            self.run_directory = run_directory

        if script_directory is None:
            self.filename = merge_job_array.filename
        else:
            self.filename = os.path.join(script_directory,
                                         merge_job_array.filename)

        if log_directory is None:
            self.logfile = '{}_{}_%a_%A_%a.log' \
                    .format(os.path.splitext(merge_job_array.filename)[0],
                            self.reservation_token)
        else:
            self.logfile = os.path.join(
                log_directory, '{}_{}_%a_%A_%a.log' \
                .format(os.path.splitext(merge_job_array.filename)[0],
                        self.reservation_token))

        if len(blocks) > 1000:
            raise ValueError('maximum 1000 blocks can be merged, tried to '
                             'merge {}'.format(len(blocks)))

        self.array_indices = '1-{}'.format(len(blocks))
        if max_simultaneous_tasks is not None:
            self.array_indices += '%{}'.format(max_simultaneous_tasks)

        sqlite_timeout = '-init <(echo .timeout 30000)'
        args = [
            ['reservation=$1'],
            ['reservation_filename="{}/merge_task_${{reservation}}_'
             '${{SLURM_ARRAY_TASK_ID}}.txt"' \
             .format(run_directory)],
            ['echo', '"Using reservation in $reservation_filename"'],
            ['block=$(cat ${reservation_filename})'],
            ['echo',
             '"Merging block ${block} in job ${SLURM_JOB_ID}',
             '(array ${SLURM_ARRAY_JOB_ID})"'],
            ['db="{}"'.format(project)],
            [],
            ['LAmerge',
             '-v' if verbose else '',
             '-s',
             '-C', 'A',
             '-n', str(n_files),
             '${db}',
             '${db}.${block}.las',
             '$(printf "d001_%05d" ${block})'],
            [],
            ['echo', '"Deleting input files..."'],
            ['rm', '-r', '$(printf "d001_%05d" ${block})']
        ]

        super().__init__(args,
                         jobname,
                         self.filename,
                         log_filename=self.logfile,
                         timelimit=timelimit,
                         account=account,
                         array=self.array_indices,
                         cores=2)

    def start(self, dryrun=False, force_write=False):
        return super().start(dryrun, force_write, self.reservation_token)

class annotate_job_array(marvel_job):

    filename = 'annotate_block.sh'

    def __init__(self, blocks, project,
                 max_simultaneous_tasks=None, script_directory=None,
                 log_directory=None, reservation_token=None,
                 run_directory=None, account=None,
                 timelimit='1-00:00:00'):
        jobname = 'annotation'

        if reservation_token is None:
            raise ValueError('reservation token must not be None')
        self.reservation_token = reservation_token

        if run_directory is None:
            self.run_directory = os.path.abspath('.')
        else:
            self.run_directory = run_directory

        if script_directory is None:
            self.filename = merge_job_array.filename
        else:
            self.filename = os.path.join(script_directory,
                                         annotate_job_array.filename)

        if log_directory is None:
            self.logfile = '{}_{}_%a_%A_%a.log' \
                    .format(os.path.splitext(annotate_job_array.filename)[0],
                            self.reservation_token)
        else:
            self.logfile = os.path.join(
                log_directory, '{}_{}_%a_%A_%a.log' \
                .format(os.path.splitext(annotate_job_array.filename)[0],
                        self.reservation_token))

        if len(blocks) > 1000:
            raise ValueError('maximum 1000 blocks can be run, tried to '
                             'merge {}'.format(len(blocks)))

        self.array_indices = '1-{}'.format(len(blocks))
        if max_simultaneous_tasks is not None:
            self.array_indices += '%{}'.format(max_simultaneous_tasks)

        sqlite_timeout = '-init <(echo .timeout 30000)'
        args = [
            ['reservation=$1'],
            ['reservation_filename="{}/annotate_task_${{reservation}}'
             '_${{SLURM_ARRAY_TASK_ID}}.txt"' \
             .format(run_directory)],
            ['echo', '"Using reservation in $reservation_filename"'],
            ['block=$(cat ${reservation_filename})'],
            ['echo', '"Annotating block ${block}"'],
            ['db="{}"'.format(project)],
            [],
            ['LAq',
             '-b', '${block}',
             '${db}',
             '${db}.${block}.las']
        ]

        super().__init__(args,
                         jobname,
                         self.filename,
                         log_filename=self.logfile,
                         timelimit=timelimit,
                         account=account,
                         array=self.array_indices,
                         cores=2)

    def start(self, dryrun=False, force_write=False):
        return super().start(dryrun, force_write, self.reservation_token)

class patch_job_array(marvel_job):

    filename = 'patch_block.sh'
    out_filename = '{db}.{block}.patched{trim}.fasta'

    def __init__(self,
                 blocks,
                 max_simultaneous_tasks,
                 update_repeat_annotations,
                 config,
                 reservation_token=None):

        jobname = 'patching'

        if reservation_token is None:
            raise ValueError('reservation token must not be None')
        self.reservation_token = reservation_token

        try:
            run_directory = config.get('patch_blocks', 'run_directory')
            script_directory = config.get('general', 'script_directory')
            log_directory = config.get('general', 'log_directory')
            project = config.get('general', 'name')
            timelimit = config.get('patch_blocks', 'timelimit')
            account = config.get('general', 'account')
            self.trim = config.getboolean('patch_blocks', 'trim')
            min_read_length = config.getint('patch_blocks', 'min_read_length')
        except KeyError:
            raise

        self.filename = os.path.join(script_directory,
                                     patch_job_array.filename)
        self.logfile = os.path.join(
            log_directory, '{}_{}_%a_%A_%a.log' \
            .format(os.path.splitext(patch_job_array.filename)[0],
                    self.reservation_token))

        if len(blocks) > 1000:
            raise ValueError('maximum 1000 blocks can be run, tried to '
                             'merge {}'.format(len(blocks)))

        self.array_indices = '1-{}'.format(len(blocks))
        if max_simultaneous_tasks is not None:
            self.array_indices += '%{}'.format(max_simultaneous_tasks)

        args = [
            ['reservation=$1'],
            ['trim=$2'],
            [],
            ['reservation_filename="{}/patch_task_${{reservation}}'
             '_${{SLURM_ARRAY_TASK_ID}}.txt"' \
             .format(run_directory)],
            ['echo', '"Using reservation in $reservation_filename"'],
            ['block=$(cat ${reservation_filename})'],
            ['echo', '"Patching block ${block}"'],
            ['db="{}"'.format(project)],
            [],
            ['if [[ ${trim} == "True"  ]]; then'],
            ['\tLAfix',
             '-g', '-1',
             '-x', min_read_length,
             '-c' if update_repeat_annotations else '',
             'repeats'.format(update_repeat_annotations) \
                if update_repeat_annotations else '',
             '-q', 'q',
             '-t', 'trim',
             '${db}',
             '${db}.${block}.las',
             patch_job_array.out_filename. \
             format(db='${db}', block='${block}',
                    trim='.trimmed')],
            ['else'],
            ['\tLAfix',
             '-g', '-1',
             '-x', min_read_length,
             '-c' if update_repeat_annotations else '',
             'repeats'.format(update_repeat_annotations) \
                if update_repeat_annotations else '',
             '-q', 'q',
             '${db}',
             '${db}.${block}.las',
             patch_job_array.out_filename. \
             format(db='${db}', block='${block}',
                    trim='')],
            ['fi']
        ]

        super().__init__(args,
                         jobname,
                         self.filename,
                         log_filename=self.logfile,
                         timelimit=timelimit,
                         account=account,
                         array=self.array_indices,
                         cores=6)

    def start(self, dryrun=False):
        return super().start(dryrun, True,
                             self.reservation_token,
                             self.trim)

class stats_job_array(marvel_job):

    filename = 'block_stats.sh'

    def __init__(self,
                 blocks,
                 project,
                 out_filename_template,
                 max_simultaneous_tasks=None,
                 script_directory=None,
                 log_directory=None,
                 reservation_token=None,
                 run_directory=None,
                 account=None,
                 timelimit='1-00:00:00'):

        jobname = 'stats'

        if reservation_token is None:
            raise ValueError('reservation token must not be None')
        self.reservation_token = reservation_token

        if run_directory is None:
            self.run_directory = os.path.abspath('.')
        else:
            self.run_directory = run_directory

        if script_directory is None:
            self.filename = stats_job_array.filename
        else:
            self.filename = os.path.join(script_directory,
                                         stats_job_array.filename)

        if log_directory is None:
            self.logfile = '{}_{}_%a_%A_%a.log' \
                    .format(os.path.splitext(stats_job_array.filename)[0],
                            self.reservation_token)
        else:
            self.logfile = os.path.join(
                log_directory, '{}_{}_%a_%A_%a.log' \
                .format(os.path.splitext(stats_job_array.filename)[0],
                        self.reservation_token))

        if len(blocks) > 1000:
            raise ValueError('maximum 1000 blocks can be run, tried to '
                             'merge {}'.format(len(blocks)))

        self.array_indices = '1-{}'.format(len(blocks))
        if max_simultaneous_tasks is not None:
            self.array_indices += '%{}'.format(max_simultaneous_tasks)

        args = [
            ['reservation=$1'],
            ['reservation_filename="{}/stats_task_${{reservation}}_'
             '${{SLURM_ARRAY_TASK_ID}}.txt"' \
             .format(run_directory)],
            ['echo', '"# Using reservation in $reservation_filename"'],
            ['block=$(cat ${reservation_filename})'],
            ['echo', '"# Getting stats for ${block}"'],
            ['db="{}"'.format(project)],
            [],
            ['LAstats',
             '${db}',
             '${db}.${block}.las',
             '>', out_filename_template.format('${db}', '${block}')]
        ]

        super().__init__(args,
                         jobname,
                         self.filename,
                         log_filename=self.logfile,
                         timelimit=timelimit,
                         account=account,
                         array=self.array_indices,
                         cores=1)

    def start(self, dryrun=False, force_write=False):
        return super().start(dryrun, force_write, self.reservation_token)

class check_job(marvel_job):

    filename = 'check_block.sh'

    def __init__(self, block, run, project,
                 script_directory=None,
                 log_directory=None,
                 account=None,
                 cores=16,
                 timelimit='5:00:00'):

        self.project = project
        self.block = block
        self.run = run

        jobname = '{}_{}'.format(os.path.splitext(check_job.filename)[0],
                                 self.block)

        if script_directory is None:
            self.filename = check_job.filename
        else:
            self.filename = os.path.join(script_directory, check_job.filename)

        if log_directory is None:
            logfile = '{}.log'.format(jobname)
        else:
            logfile = os.path.join(log_directory, '{}.log'.format(jobname))

        args = [
            ['module', 'load', 'gnuparallel/20180822'],
            ['project=$1'],
            ['block=$2'],
            ['run=$3'],
            ['input_dir=$(printf "d%03d_%05d" ${run} ${block})'],
            [],
            ['check_las() {'],
            ['\tif ! output=$(LAcheck $1 $2 2>&1); then'],
            ['\t\techo "$2"'],
            ['\tfi'],
            ['}'],
            ['export', '-f', 'check_las'],
            [],
            ['find ${input_dir} -type f -name "*.las" |',
             'parallel -j {} check_las ${{project}} {{}}'.format(cores)]
        ]

        super().__init__(args,
                         jobname,
                         filename=self.filename,
                         log_filename=logfile,
                         account=account,
                         timelimit=timelimit,
                         cores=cores)

    def start(self, dryrun=False, force_write=False):
        return super().start(dryrun,
                             force_write,
                             self.project,
                             self.block,
                             self.run)

class annotation_merge_job(marvel_job):

    filename = 'merge_annotations.sh'

    def __init__(self, config):

        script_directory = config.get('general', 'script_directory')
        log_directory = config.get('general', 'log_directory')

        self.jobname = os.path.splitext(annotation_merge_job.filename)[0]

        if script_directory is None:
            self.filename = annotation_merge_job.filename
        else:
            self.filename = os.path.join(script_directory,
                                         annotation_merge_job.filename)
        if log_directory is None:
            logfile = '{}.log'.format(self.jobname)
        else:
            logfile = os.path.join(log_directory, '{}.log'.format(self.jobname))

        args = [
            ['TKmerge', config.get('general', 'name'), 'q'],
            ['TKmerge', config.get('general', 'name'), 'trim'],
            ['TKmerge', config.get('general', 'name'), 'repeats']
        ]

        super().__init__(args,
                         self.jobname,
                         filename=self.filename,
                         log_filename=logfile,
                         account=config.get('general', 'account'),
                         timelimit=config.get(self.jobname, 'timelimit'))

class repeat_annotation_array(marvel_job):

    filename = 'repeat_annotation.sh'

    def __init__(self,
                 blocks,
                 max_simultaneous_tasks,
                 config,
                 reservation_token):

        jobname = 'annotate_repeats'

        if reservation_token is None:
            raise ValueError('reservation token must not be None')
        self.reservation_token = reservation_token

        try:
            run_directory = config.get('repeat_annotation', 'run_directory')
            script_directory = config.get('general', 'script_directory')
            log_directory = config.get('general', 'log_directory')
            project = config.get('general', 'name')
            timelimit = config.get('repeat_annotation', 'timelimit')
            account = config.get('general', 'account')
            coverage = config.getint('general', 'coverage')
        except KeyError:
            raise

        self.filename = os.path.join(script_directory,
                                     repeat_annotation_array.filename)

        logfile = os.path.join(log_directory,
                               '{}_{}_%a_%A_%a.log' \
                               .format(jobname, self.reservation_token))

        if len(blocks) > 1000:
            raise ValueError('maximum 1000 blocks can be run, tried to '
                             'annotate {}'.format(len(blocks)))

        self.array_indices = '1-{}'.format(len(blocks))
        if max_simultaneous_tasks is not None:
            self.array_indices += '%{}'.format(max_simultaneous_tasks)

        args = [
            ['reservation=$1'],
            [],
            ['reservation_filename="{rundir}/{jobname}_${{reservation}}'
             '_${{SLURM_ARRAY_TASK_ID}}.txt"' \
             .format(rundir=run_directory, jobname=jobname)],
            ['echo', '"# Using reservation in ${reservation_filename}"'],
            ['block=$(cat ${reservation_filename})'],
            ['echo', '"# Annotating block ${block}"'],
            ['db="{}"'.format(project)],
            [],
            ['LArepeat',
             '-c', coverage,
             '-b', '${block}',
             '${db}',
             '${db}.${block}.las']
        ]

        super().__init__(args,
                         jobname,
                         self.filename,
                         log_filename=logfile,
                         timelimit=timelimit,
                         account=account,
                         array=self.array_indices,
                         cores=1)

    def start(self, dryrun=False):
        return super().start(dryrun, True,
                             self.reservation_token)
