#!/usr/bin/python
# Convert a flat store_sales table into a partitioned store_sales table.
# In general, a simple insert overwrite is not safe to do, as the Impala may end up using
# more memory than it has available. In order to alleviate that problem, we determine the
# number of partitions to insert into in one query based on the impalad's memory limit and
# the number of impalads in the system.

import os
import socket
import re
import urllib

from math import ceil
from subprocess import call

TPCDS_DB = os.getenv('TPCDS_DBNAME')
IMPALAD = socket.getfqdn()
LOAD_FILE = "load_catalog_returns_tmp.sql"

def get_mem_limit():
  """Get the memory limit of an Impala daemon"""
  content = urllib.urlopen("http://{0}:25000/varz?raw".format(IMPALAD)).read()
  # memz has the mem limit in bytes
  mem_limit_gb = float(re.findall('--mem_limit=(\d+)', content)[0])/(1024**3)
  return mem_limit_gb

def get_num_backends():
  """Get the number of Impala daemons in the cluster"""
  content = urllib.urlopen("http://{0}:25000/backends?raw".format(IMPALAD)).read()
  return len([b for b in content.strip().split('\n') if '22000' in b])

def generate_queries(ss_sold_dates):
  num_part_per_query = int(ceil(0.5 * get_mem_limit())) * get_num_backends()
  partition_ranges = [ss_sold_dates[i: i + num_part_per_query] for\
      i in range(0, len(ss_sold_dates), num_part_per_query)]
  assert sum([len(r) for r in partition_ranges]) == len(ss_sold_dates)
  queries = []
  for partition_range in partition_ranges:
    query = """insert overwrite table catalog_returns
              partition(cr_returned_date_sk) [shuffle]
              select
    cr_returned_time_sk,
    cr_item_sk,
    cr_refunded_customer_sk ,
    cr_refunded_cdemo_sk,
    cr_refunded_hdemo_sk,
    cr_refunded_addr_sk,
    cr_returning_customer_sk,
    cr_returning_cdemo_sk,
    cr_returning_hdemo_sk ,
    cr_returning_addr_sk ,
    cr_call_center_sk,
    cr_catalog_page_sk,
    cr_ship_mode_sk,
    cr_warehouse_sk,
    cr_reason_sk,
    cr_order_number,
    cr_return_quantity,
    cr_return_amount,
    cr_return_tax ,
    cr_return_amt_inc_tax,
    cr_fee  ,
    cr_return_ship_cost ,
    cr_refunded_cash,
    cr_reversed_charge,
    cr_store_credit,
    cr_net_loss,
    cr_returned_date_sk
              from et_catalog_returns
              where cr_returned_date_sk
              between {0} and {1}""".format(partition_range[0], partition_range[-1])
    queries.append(query)
  return queries

def _main():

  with open("distinct-ss-sold-date.txt", 'r') as f:
    ss_sold_dates = sorted([d.strip() for d in f.readlines()])
  queries = generate_queries(ss_sold_dates)
  with open(LOAD_FILE, 'w') as f:
    f.write('USE {0};\n'.format(TPCDS_DB))
    f.write(';\n'.join(queries))
  try:
    os.system('impala-shell -f {0}'.format(LOAD_FILE))
  except Exception, e:
    print "Data Loading failed: %s" % e
  finally:
    os.remove(LOAD_FILE)

if __name__ == "__main__":
  assert TPCDS_DB, "The TPCDS_DBNAME environment variable is required"
  assert get_mem_limit() > 2.0, "The impalad's memory limit is too low"
  _main()
