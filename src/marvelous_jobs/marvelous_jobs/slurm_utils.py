import pyslurm
import socket

class status:
    pending = 'PENDING'
    running = 'RUNNING'
    completed = 'COMPLETED'
    completing = 'COMPLETING'
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

def get_node_ip(n):
    return socket.gethostbyname(n)

def get_job_status(jobid):
    if jobid is None:
        return status.notstarted
    try:
        return pyslurm.job().find_id(str(jobid))[0]['job_state']
    except ValueError:
        raise
