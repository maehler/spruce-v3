#!/usr/bin/env python

import argparse
import os
import re
from subprocess import Popen, PIPE
import sys

def get_id_clusters(array_id):
    args = [
        'sacct', '-j', str(array_id),
        '--format',
        'JobID,JobIDRaw,Node,Cluster,AveDiskRead,AveDiskWrite,Elapsed,State',
        '--parsable2',
        '--delimiter', '\t',
        '--noheader'
    ]

    batch_regex = re.compile(r'\.batch$')

    p = Popen(args, stdout=PIPE)
    stdout_data = p.communicate()[0].decode('utf-8')

    jobnodes = {}
    for line in stdout_data.splitlines():
        jobtask, batchjobid, node, cluster, \
        diskread, diskwrite, elapsedtime, state = line.strip().split('\t')
        if not batch_regex.search(batchjobid):
            continue
        jobid = batchjobid.rstrip('.batch')
        arrayid, taskid = jobtask.rstrip('.batch').split('_')
        jobnodes[jobid] = {
            'cluster': cluster,
            'node': node,
            'taskid': taskid,
            'arrayid': arrayid,
            'diskread': diskread,
            'diskwrite': diskwrite,
            'elapsed': elapsedtime,
            'state': state
        }

    return jobnodes

def get_run_data(job_nodes):
    stats_path = '/sw/share/slurm/{cluster}/uppmax_jobstats/{node}'
    all_stats = {}
    n_cores = None
    for jobid, v in job_nodes.items():
        stat_fname = os.path.join(stats_path.format(**v), jobid)
        if not os.path.isfile(stat_fname):
            print('warning: no jobstats for {0}_{1} found' \
                  .format(v['arrayid'], v['taskid']), file=sys.stderr)
            continue
        with open(stat_fname) as f:
            for i, line in enumerate(f):
                if i == 0:
                    continue
                cols = line.strip().split()
                time, raw_time, mem_limit, mem_used, swap_used = cols[:5]
                core_percentages = list(map(float, cols[5:]))
                if n_cores is None:
                    n_cores = len(core_percentages)
                elif n_cores != len(core_percentages):
                    print('error: this does not look like an array job: '
                          'number of cores is different across tasks', file=sys.stderr)
                if jobid not in all_stats:
                    all_stats[jobid] = []
                all_stats[jobid].append({
                    'raw_jobid': jobid,
                    'jobid': '{arrayid}_{taskid}'.format(**v),
                    'node': v['node'],
                    'localtime': time,
                    'raw_time': int(raw_time),
                    'elapsed_time': v['elapsed'],
                    'mem_limit_gb': float(mem_limit),
                    'mem_used_gb': float(mem_used),
                    'swap_used_gb': float(swap_used),
                    'diskread': v['diskread'],
                    'diskwrite': v['diskwrite'],
                    'core_percentage_used': core_percentages,
                    'state': v['state']
                })

    return all_stats, n_cores

def print_stats(s, n_cores, filename=None):
    if filename is not None:
        outfile = open(filename, 'w')
    else:
        outfile = sys.stdout
    print('\t'.join(['jobid_raw', 'jobid', 'node', 'state', 'localtime', 'raw_time',
                     'elapsed_time', 'mem_limit_gb', 'mem_used_gb', 'swap_used_gb',
                     'diskread', 'diskwrite',
                     '\t'.join('core{0}'.format(x) for x in range(1, n_cores + 1))]),
          file=outfile)
    for jobid, lines in s.items():
        for v in lines:
            print('{raw_jobid}\t{jobid}\t{node}\t{state}\t{localtime}\t{raw_time}\t{elapsed_time}\t'
                  '{mem_limit_gb}\t{mem_used_gb}\t{swap_used_gb}\t{diskread}\t{diskwrite}\t'
                  '{0}'.format('\t'.join(map(str, v['core_percentage_used'])), **v),
                  file=outfile)
    outfile.close()

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('array_id', help='ID of the job array to get '
                        'stats for', type=int)
    parser.add_argument('-o', help='file to write results to (default: stdout)')

    args = parser.parse_args()

    return args

def main():
    args = parse_args()

    job_nodes = get_id_clusters(args.array_id)
    all_stats, n_cores = get_run_data(job_nodes)

    if len(all_stats) == 0:
        print('warning: no job statistics found', file=sys.stderr)
        sys.exit(1)

    try:
        print_stats(all_stats, n_cores, filename=args.o)
    except FileNotFoundError as e:
        print('error: {0}'.format(e))
        sys.exit(1)

if __name__ == '__main__':
    main()
