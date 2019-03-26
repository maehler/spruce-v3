import pyslurm
import socket
from subprocess import Popen,PIPE

class status:
    pending = 'PENDING'
    running = 'RUNNING'
    completed = 'COMPLETED'
    completing = 'COMPLETING'
    configuring = 'CONFIGURING'
    failed = 'FAILED'
    notstarted = 'NOTSTARTED' # not a slurm job state
    reserved = 'RESERVED' # not a slurm job state
    timeout = 'TIMEOUT'
    cancelled = 'CANCELLED'

def is_node(n):
    try:
        pyslurm.node().find_id(n)
    except IndexError:
        return False

    return True

def get_job_node(jobid):
    try:
        return pyslurm.job().find_id(str(jobid))[0]['nodes']
    except ValueError:
        raise

def get_node_ip(n):
    return socket.gethostbyname(n)

def get_job_status(jobid):
    """Get the current state of a SLURM job

    Parameters
    ----------
    jobid : int or str
        Job ID that job state should be fetched for.
        Can be on the form <job_id>_<task_id> or just
        <job_id>.

    Returns
    -------
    str
        Job state associated with the job ID.
    """
    global status


    if jobid is None:
        return status.notstarted

    try:
        jobs = pyslurm.job().find_id(str(jobid))
        if len(jobs) > 1:
            raise ValueError('job is an array job, but the id '
                             'does not imply this')
        return jobs[0]['job_state']
    except ValueError:
        p = Popen(['sacct', '-j', str(jobid),
                   '--format', 'JobID,State',
                   '--noheader', '--parsable2'],
                  shell=False, stdout=PIPE, stderr=PIPE,
                  encoding='utf8')
        (output, err) = p.communicate()
        for line in output.splitlines():
            str_jobid, job_status = line.strip().split('|')
            if str_jobid == str(jobid):
                return job_status.split()[0]

def cancel_jobs(jobids):
    args = ['scancel', *map(str, jobids)]
    p = Popen(args, shell=False, encoding='utf8')
    p.wait()
