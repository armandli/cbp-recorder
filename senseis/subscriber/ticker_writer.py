import argparse
import logging
import json
import math
from datetime import datetime
import pandas as pd
import asyncio

from senseis.configuration import DATETIME_FORMAT, TICKER_TIME_FORMAT1, TICKER_TIME_FORMAT2
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import is_ticker_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath
from senseis.utility import setup_logging, build_subscriber_parser
from senseis.extraction_producer_consumer import consume_extraction, extraction_subscriber, extraction_writer
from senseis.extraction_producer_consumer import create_interval_state
from senseis.metric_utility import setup_gateway, setup_subscriber_gauges

def convert_ticker_time(time_str):
  try:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT1)
  except ValueError:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT2)

def data_to_df(data, exchange_name):
  pids = get_exchange_pids(exchange_name)
  columns = [pid + ':' + ty for pid in pids for ty in ['bid','ask','volume','price','size','trade_id','time']]
  columns.append(STIME_COLNAME)
  columns.append(RTIME_COLNAME)
  data.sort(key=lambda x: datetime.strptime(x[STIME_COLNAME], DATETIME_FORMAT))
  d = {colname : [] for colname in columns}
  for row in data:
    for pid in pids:
      pid_data = json.loads(row[pid])
      if not pid_data:
        d[pid + ':' + 'bid'].append(math.nan)
        d[pid + ':' + 'ask'].append(math.nan)
        d[pid + ':' + 'volume'].append(math.nan)
        d[pid + ':' + 'price'].append(math.nan)
        d[pid + ':' + 'size'].append(math.nan)
        d[pid + ':' + 'trade_id'].append(math.nan)
        d[pid + ':' + 'time'].append(None)
      else:
        d[pid + ':' + 'bid'].append(float(pid_data['bid']))
        d[pid + ':' + 'ask'].append(float(pid_data['ask']))
        d[pid + ':' + 'volume'].append(float(pid_data['volume']))
        d[pid + ':' + 'price'].append(float(pid_data['price']))
        d[pid + ':' + 'size'].append(float(pid_data['size']))
        d[pid + ':' + 'trade_id'].append(int(pid_data['trade_id']))
        d[pid + ':' + 'time'].append(convert_ticker_time(pid_data['time']))
    d[STIME_COLNAME].append(row[STIME_COLNAME])
    d[RTIME_COLNAME].append(row[RTIME_COLNAME])
  df = pd.DataFrame(data=d)
  return df

def main():
  parser = build_subscriber_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_ticker_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange)
  app_name = 'cbp_{}_writer'.format(args.exchange)
  setup_gateway(app_name)
  setup_subscriber_gauges(app_name)
  create_interval_state()
  try:
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
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
