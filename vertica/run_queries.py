'''
Run a set of tpcds benchmark queries on Vertica
in a single-user or multi-user (concurrent) setting.
'''

from common import get_ssh_client, vsql, get_parser
import os
import json
import workerpool
import Queue
import random
import copy
import re
import threading
import csv
import datetime

INTERACTIVE_QUERIES = ['q19', 'q42', 'q52', 'q55', 'q63', 'q68', 'q73', 'q98']
REPORTING_QUERIES = ['q3', 'q7', 'q27', 'q54', 'q53', 'q89']
ANALYTIC_QUERIES = ['q34', 'q46', 'q59', 'q79', 'ss_max']

OUTPUT_LOCK = threading.Lock()

def _daemon_worker_factory(job_queue):
    '''
    Helper function used when created a workerpool Worker.
    Set the thread to run as a daemon.
    '''
    w = workerpool.Worker(job_queue)
    w.setDaemon(True)
    return w

def _get_time_from_result_str(result_str):
    '''
    Parse vsql output to get the time it took to run a query
    '''
    match = re.search('All rows formatted: (\d+\.\d+) ms', result_str)
    if match:
        return float(match.group(1))

def _run_query_and_get_time(_vsql, sql):
    '''
    Run a query with vsql with \timing enabled,
    and parse the time the query took
    '''
    stdout, stderr = _vsql(r"\timing\\\\{sql}".format(sql=sql))
    timing_str = stdout[-1].strip()
    t_ms = _get_time_from_result_str(timing_str)
    return t_ms/1000.0

def _run_queries_for_user(_vsql, output_dir, run_number, user_num, query_list):
    '''
    Run queries in `query_list` for user `user_num`
    Queries run sequentially for the user
    Write the query times to file as they finish
    '''
    for q_name, sql in query_list:
        time_s = _run_query_and_get_time(_vsql, sql)
        _mark_time_for_user_query_run(output_dir, run_number, user_num, q_name, time_s)

def _mark_time_for_user_query_run(output_dir, run_number, user_num, q_name, time_s, mode='a+b'):
    '''
    Write to output_dir/`query_times.csv` the run number, user number, query name, and time it took.
    Multiple threads will call this as they finish
    '''
    with OUTPUT_LOCK:
        with open(os.path.join(output_dir, 'query_times.csv'), mode) as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
            row = [run_number, user_num, q_name, time_s]
            writer.writerow(row)
            print row

def _get_results_dir(opts):
    return os.path.join(opts.base_results_dir,
            '{0}-scale-{1}-users-{2}-runs'.format(
                os.environ.get('TPCDS_SCALE_FACTOR'),
                opts.num_users,
                opts.num_runs))

def _write_metadata(results_dir, opts, queries):
    with open(os.path.join(results_dir, 'metadata.json'), 'wb') as fh:
        json.dump({
            'metadata': {
                'num_users': opts.num_users,
                'num_runs': opts.num_runs,
                'scale_factor': os.environ.get('TPCDS_SCALE_FACTOR'),
                'queries': [q[0] for q in queries],
                'start_time': datetime.datetime.now().__str__()
            }
        }, fh, indent=4)

def run_queries(opts):
    '''
    Main entry point.

    For each run,
        asynchronously for each user,
            randomize the list of queries
            run the queries sequentially
    '''
    query_fnames = []
    if opts.interactive:
        query_fnames += INTERACTIVE_QUERIES
    if opts.reporting:
        query_fnames += REPORTING_QUERIES
    if opts.analytic:
        query_fnames += ANALYTIC_QUERIES

    queries = []
    for qname in query_fnames:
        fname = qname + '.sql'
        with open(os.path.join(opts.query_dir, fname)) as fh:
            queries.append((qname, fh.read().strip()))

    print 'running {0} queries {1} times {2}'.format(len(queries), opts.num_runs, [q[0] for q in queries])

    results_dir = _get_results_dir(opts)
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    # write some metadata about the run before we begin
    _mark_time_for_user_query_run(results_dir, 'run_number', 'user', 'query', 'time_s', 'wb')
    _write_metadata(results_dir, opts, queries)

    workers = workerpool.WorkerPool(size=opts.num_users,
                                    worker_factory=_daemon_worker_factory)
    results = Queue.Queue()
    print 'starting runs, writing to ', results_dir
    for run_num in xrange(1, opts.num_runs + 1):
        for user_num in xrange(1, opts.num_users + 1):
            query_list = copy.copy(queries)
            random.shuffle(query_list)
            ssh_client = get_ssh_client(opts)
            _vsql = lambda s: vsql(ssh_client, opts, s)
            workers.put(workerpool.SimpleJob(results,
                                             _run_queries_for_user,
                                             [_vsql, results_dir, run_num,
                                              user_num, query_list]))
        print 'waiting for jobs to complete in run ', run_num
        workers.join()
        print 'finished run', run_num

    workers.shutdown()
    workers.join()

if __name__ == '__main__':
    parser = get_parser('Create TPC-SD tables in vertica')
    parser.add_argument('--interactive', action='store_true', help='Run interactive queries')
    parser.add_argument('--reporting', action='store_true', help='Run reporting queries')
    parser.add_argument('--analytic', action='store_true', help='Run analytic queries')
    parser.add_argument('--num-users', type=int, help='Number of users to simulate', default=1)
    parser.add_argument('--num-runs', type=int, help='Number of runs for each test', default=3)
    parser.add_argument('--results-dir',
        dest='base_results_dir',
        default=os.path.join(os.path.dirname(__file__), '../results/'),
        help='Where to store results')
    parser.add_argument('--query-dir',
        default=os.path.join(os.path.dirname(__file__), '../queries-sql92-modified/queries/'),
        help='path to directory where queries are located')
    opts = parser.parse_args()

    run_queries(opts)
