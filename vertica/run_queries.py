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

INTERACTIVE_QUERIES = ['q19', 'q42', 'q52', 'q55', 'q63', 'q68', 'q73', 'q98']
REPORTING_QUERIES = ['q3', 'q7', 'q27', 'q54', 'q53', 'q89']
ANALYTIC_QUERIES = ['q34', 'q46', 'q59', 'q79', 'ss_max']

OUTPUT_LOCK = threading.Lock()

def run_query():
    pass

def _daemon_worker_factory(job_queue):
    w = workerpool.Worker(job_queue)
    w.setDaemon(True)
    return w

def _get_time_from_result_str(result_str):
  match = re.search('All rows formatted: (\d+\.\d+) ms', result_str)
  if match:
    return float(match.group(1))

def _run_query_and_get_time(_vsql, sql):
    stdout, stderr = _vsql(r"\timing\\\\{sql}".format(sql=sql))
    timing_str = stdout[-1].strip()
    t_ms = _get_time_from_result_str(timing_str)
    return t_ms/1000.0

def _run_queries_for_user(_vsql, output_file, run_number, user_num, query_list):
    for q_name, query in query_list:
        time_s = _run_query_and_get_time(_vsql, user_num, q_name, query)
        _mark_time_for_user_query_run(output_file, run_number, user_num, q_name, time_s)

def _mark_time_for_user_query_run(output_file, run_number, user_num, q_name, time_s, mode='a+b'):
    with OUTPUT_LOCK:
        with open(output_file, mode) as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
            row = [run_number, user_num, q_name, time_s]
            writer.writerow(row)
            print row

def run_queries(opts):
    query_fnames = []
    if opts.interactive:
        query_fnames += INTERACTIVE_QUERIES
    if opts.reporting:
        query_fnames += REPORTING_QUERIES
    if opts.analytic:
        query_fnames += ANALYTIC_QUERIES

    queries = []
    for fname in query_fnames:
        with open(os.path.join(opts.query_dir, fname)) as fh:
            queries.append((fname, fh.read().strip()))

    results_dir = os.path.dirname(opts.results_file)
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    # write csv header
    _mark_time_for_user_query_run(opts.results_file, 'run_number', 'user', 'query', 'time_s', 'wb')

    workers = workerpool.WorkerPool(size=opts.num_users, 
                                    worker_factory=_daemon_worker_factory)
    results = Queue.Queue()
    print 'starting runs, writing to ', opts.results_file
    for run_num in xrange(opts.num_runs):
        for user_num in xrange(opts.num_users):
            query_list = copy.copy(queries)
            random.shuffle(query_list)
            ssh_client = get_ssh_client(opts)
            _vsql = lambda s: vsql(ssh_client, opts, s)
            workers.put(workerpool.SimpleJob(results, 
                                             _run_queries_for_user, 
                                             [_vsql, opts.results_file, run_num, 
                                              user_num, query_list]))
            workers.join()
            print 'finished run', run_num

    workers.shutdown()
    workers.join()

if __name__ == '__main__':
    parser = get_parser('Create TPC-SD tables in vertica')
    parser.add_argument('--query-dir', 
        default=os.path.join(os.path.dirname(__file__), '../queries-sql92-modified/queries/'))
    parser.add_argument('--results-file', required=True, help='Where to store results')
    parser.add_argument('--interactive', action='store_true', help='Run interactive queries')
    parser.add_argument('--reporting', action='store_true', help='Run reporting queries')
    parser.add_argument('--analytic', action='store_true', help='Run analytic queries')
    parser.add_argument('--num-users', action='store_true', help='Number of users to simulate', default=1)
    parser.add_argument('--num-runs', action='store_true', help='Number of runs for each test', default=3)
    opts = parser.parse_args()
    run_queries(opts)
