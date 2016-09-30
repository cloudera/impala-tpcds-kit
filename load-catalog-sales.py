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
LOAD_FILE = "load_catalog_sales_tmp.sql"

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
    query = """insert overwrite table catalog_sales
              partition(cs_sold_date_sk) [shuffle]
              select
              cs_sold_time_sk ,
    cs_ship_date_sk   ,
    cs_bill_customer_sk ,
    cs_bill_cdemo_sk  ,
    cs_bill_hdemo_sk  ,
    cs_bill_addr_sk   ,
    cs_ship_customer_sk ,
    cs_ship_cdemo_sk ,
    cs_ship_hdemo_sk ,
    cs_ship_addr_sk ,
    cs_call_center_sk,
    cs_catalog_page_sk ,
    cs_ship_mode_sk ,
    cs_warehouse_sk,
    cs_item_sk  ,
    cs_promo_sk   ,
    cs_order_number  ,
    cs_quantity ,
    cs_wholesale_cost,
    cs_list_price  ,
    cs_sales_price  ,
    cs_ext_discount_amt ,
    cs_ext_sales_price  ,
    cs_ext_wholesale_cost ,
    cs_ext_list_price  ,
    cs_ext_tax  ,
    cs_coupon_amt,
    cs_ext_ship_cost ,
    cs_net_paid,
    cs_net_paid_inc_tax,
    cs_net_paid_inc_ship,
    cs_net_paid_inc_ship_tax,
    cs_net_profit,
    cs_sold_date_sk
              from et_catalog_sales
              where cs_sold_date_sk
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
