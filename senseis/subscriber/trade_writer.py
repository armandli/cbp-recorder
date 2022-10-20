import argparse
import logging
import json
import math
from datetime import datetime
import pandas as pd
import numpy as np
import asyncio

from senseis.configuration import DATETIME_FORMAT, TICKER_TIME_FORMAT1, TICKER_TIME_FORMAT2
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import is_trade_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath
from senseis.utility import setup_logging, build_subscriber_parser
from senseis.extraction_producer_consumer import consume_extraction, extraction_subscriber, extraction_writer
from senseis.metric_utility import setup_gateway, create_live_gauge, create_write_success_gauge, create_row_count_gauge

def convert_trade_time(time_str):
  try:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT1)
  except ValueError:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT2)

def data_to_df(data, exchange_name):
  try:
    pids = get_exchange_pids(exchange_name)
    columns = ["pid", STIME_COLNAME, RTIME_COLNAME, "time", "trade_id", "price", "size", "side"]
    d = {colname : [] for colname in columns}
    for row in data:
      for pid in pids:
        pid_data = json.loads(row[pid])
        if pid_data:
          for i in range(-1, -1 * len(pid_data) - 1, -1):
            trade = pid_data[i]
            try:
              trade_id = int(trade['trade_id'])
              trade_time = convert_trade_time(trade['time'])
              trade_price = float(trade['price'])
              trade_size = float(trade['size'])
              trade_side = trade['side']
            except ValueError:
              logging.error("Unable to convert trade data {}. data skipped".format(trade))
              continue
            d["pid"].append(pid)
            d[STIME_COLNAME].append(row[STIME_COLNAME])
            d[RTIME_COLNAME].append(row[RTIME_COLNAME])
            d['time'].append(trade_time)
            d['trade_id'].append(trade_id)
            d['price'].append(trade_price)
            d['size'].append(trade_size)
            d['side'].append(trade_side)
    df = pd.DataFrame(data=d)
    return df
  except Exception as e:
    logging.error("Unknown exception {}. creating empty data frame.".format(e))
    pids = get_exchange_pids(exchange_name)
    columns = ["pid", STIME_COLNAME, RTIME_COLNAME, "time", "trade_id", "price", "size", "side"]
    d = {colname : [] for colname in columns}
    df = pd.DataFrame(data=d)
    return df

def main():
  parser = build_subscriber_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_trade_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange)
  setup_gateway('cbp_{}_writer'.format(args.exchange))
  create_live_gauge('cbp_{}_writer'.format(args.exchange))
  create_write_success_gauge('cbp_{}_writer'.format(args.exchange))
  create_row_count_gauge('cbp_{}_writer'.format(args.exchange))
  asyncio.run(
    consume_extraction(
      extraction_subscriber,
      extraction_writer,
      data_to_df,
      args.exchange,
      s3bucket,
      s3outdir,
      args.period * 60,
    ))

if __name__ == '__main__':
  main()
