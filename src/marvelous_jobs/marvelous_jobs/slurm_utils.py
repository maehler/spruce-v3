import pyslurm
import socket

def is_node(n):
    try:
        pyslurm.node().find_id(n)
    except IndexError:
        return False

    return True

def get_node_ip(n):
    return socket.gethostbyname(n)
