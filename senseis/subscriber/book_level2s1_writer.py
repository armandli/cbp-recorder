import argparse
import logging
import json
import math
from datetime import datetime
import pandas as pd
import numpy as np
import asyncio

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import is_book_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath
from senseis.utility import setup_logging, build_subscriber_parser
from senseis.extraction_producer_consumer import consume_extraction, extraction_subscriber, extraction_writer
from senseis.metric_utility import setup_gateway, create_live_gauge, create_write_success_gauge, create_row_count_gauge, create_error_gauge, get_error_gauge

# special writer that compress the amount of data in book level2
# determine how many levels of bids and asks we take per second based on market trading volume
# by default we take 6 levels

DEFAULT_TAKE_LEVEL = 6

def create_ba_state(exchange_name):
  pids = get_exchange_pids(exchange_name)
  global last_pid_bids
  global last_pid_asks
  last_pid_bids = {pid : None for pid in pids}
  last_pid_asks = {pid : None for pid in pids}

def compute_take_level(pid, cur_bids, cur_asks):
  last_bids = last_pid_bids[pid]
  if last_bids is None or len(cur_bids) == 0:
    blevel = DEFAULT_TAKE_LEVEL
  else:
    missing_volume = 0.
    lidx = 0
    while lidx < len(last_bids) and float(last_bids[lidx][0]) > float(cur_bids[0][0]):
      try:
        missing_volume += float(last_bids[lidx][1])
      except ValueError:
        get_error_gauge().inc()
      lidx += 1
    if lidx < len(last_bids) and float(last_bids[lidx][0]) == float(cur_bids[0][0]):
      try:
        missing_volume += max(0., float(last_bids[lidx][1]) - float(cur_bids[0][1]))
      except ValueError:
        get_error_gauge().inc()
    level_count = 0
    cidx = 0
    while cidx < len(cur_bids) and missing_volume > 0.:
      try:
        missing_volume -= float(cur_bids[cidx][1])
      except ValueError:
        get_error_gauge().inc()
      level_count += 1
      cidx += 1
    blevel = max(DEFAULT_TAKE_LEVEL, int(level_count))

  last_asks = last_pid_asks[pid]
  if last_asks is None or len(cur_asks) == 0:
    alevel = DEFAULT_TAKE_LEVEL
  else:
    missing_volume = 0.
    lidx = 0
    while lidx < len(last_asks) and float(last_asks[lidx][0]) < float(cur_asks[0][0]):
      try:
        missing_volume += float(last_asks[lidx][1])
      except ValueError:
        get_error_gauge().inc()
      lidx += 1
    if lidx < len(last_asks) and float(last_asks[lidx][0]) == float(cur_asks[0][0]):
      try:
        missing_volume += max(0., float(last_asks[lidx][1]) - float(cur_asks[0][1]))
      except ValueError:
        get_error_gauge().inc()
    level_count = 0
    cidx = 0
    while cidx < len(cur_asks) and missing_volume > 0.:
      try:
        missing_volume -= float(cur_asks[cidx][1])
      except ValueError:
        get_error_gauge().inc()
      level_count += 1
      cidx += 1
    alevel = max(DEFAULT_TAKE_LEVEL, int(level_count))
  return (blevel, alevel)

def set_last_bids_asks(pid, cur_bids, cur_asks):
  last_pid_bids[pid] = cur_bids
  last_pid_asks[pid] = cur_asks

def data_to_df(data, exchange_name):
  pids = get_exchange_pids(exchange_name)
  columns = [pid + ':' + ty for pid in pids for ty in ['bids', 'asks', 'sequence']]
  columns.append(STIME_COLNAME)
  columns.append(RTIME_COLNAME)
  data.sort(key=lambda x: datetime.strptime(x[STIME_COLNAME], DATETIME_FORMAT))
  d = {colname : [] for colname in columns}
  for row in data:
    for pid in pids:
      pid_data = json.loads(row[pid])
      if not pid_data:
        d[pid + ':' + 'bids'].append(np.array([], dtype=np.float32))
        d[pid + ':' + 'asks'].append(np.array([], dtype=np.float32))
        d[pid + ':' + 'sequence'].append(math.nan)
      else:
        cur_bids = pid_data['bids']
        cur_asks = pid_data['asks']
        bid_level, ask_level = compute_take_level(pid, cur_bids, cur_asks)
        set_last_bids_asks(pid, cur_bids, cur_asks)
        taking_bids = [b[:2] for b in pid_data['bids'][:bid_level]]
        taking_asks = [a[:2] for a in pid_data['asks'][:ask_level]]
        d[pid + ':' + 'bids'].append(np.array(taking_bids, dtype=np.float32).flatten())
        d[pid + ':' + 'asks'].append(np.array(taking_asks, dtype=np.float32).flatten())
        d[pid + ':' + 'sequence'].append(int(pid_data['sequence']))
    d[STIME_COLNAME].append(row[STIME_COLNAME])
    d[RTIME_COLNAME].append(row[RTIME_COLNAME])
  df = pd.DataFrame(data=d)
  return df

def main():
  parser = build_subscriber_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_book_exchange_name(args.exchange) or 'level2' not in args.exchange:
    logging.error("Invalid exchange name. exit.")
    return
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange) + '-s1'
  create_ba_state(args.exchange)
  app_name = 'cbp_{}_s_writer'.format(args.exchange)
  setup_gateway(app_name)
  create_live_gauge(app_name)
  create_write_success_gauge(app_name)
  create_row_count_gauge(app_name)
  create_error_gauge(app_name)
  asyncio.run(
    consume_extraction(
        extraction_subscriber,
        extraction_writer,
        data_to_df,
        args.exchange,
        s3bucket,
        s3outdir,
        args.period * 60,
    )
  )

if __name__ == '__main__':
  main()
