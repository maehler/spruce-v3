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
    global status

    if jobid is None:
        return status.notstarted
    try:
        return pyslurm.job().find_id(str(jobid))[0]['job_state']
    except ValueError:
        p = Popen(['sacct', '-j', str(jobid),
                   '--format', 'JobID,State',
                   '--noheader', '--parsable2'],
                  shell=False, stdout=PIPE, stderr=PIPE,
                  encoding='utf8')
        (output, err) = p.communicate()
        for line in output.splitlines():
            str_jobid, job_status = line.strip().split('|')
            if str(jobid) == str_jobid:
                return job_status.strip().split()[0]

    raise ValueError('invalid job id specified')
