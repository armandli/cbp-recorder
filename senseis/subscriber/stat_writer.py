import argparse
import logging
import json
import math
from datetime import datetime
import pandas as pd
import asyncio

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import is_stat_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath
from senseis.utility import setup_logging, build_subscriber_parser
from senseis.extraction_producer_consumer import consume_extraction, extraction_subscriber, extraction_writer
from senseis.metric_utility import setup_gateway, create_live_gauge, create_write_success_gauge, create_row_count_gauge, create_error_gauge

def data_to_df(data, exchange_name):
  pids = get_exchange_pids(exchange_name)
  columns = [pid + ':' + ty for pid in pids for ty in ['open','last','high','low','volume','volume_30day']]
  columns.append(STIME_COLNAME)
  columns.append(RTIME_COLNAME)
  data.sort(key=lambda x: datetime.strptime(x[STIME_COLNAME], DATETIME_FORMAT))
  d = {colname : [] for colname in columns}
  for row in data:
    for pid in pids:
      pid_data = json.loads(row[pid])
      if not pid_data:
        d[pid + ':' + 'open'].append(math.nan)
        d[pid + ':' + 'last'].append(math.nan)
        d[pid + ':' + 'high'].append(math.nan)
        d[pid + ':' + 'low'].append(math.nan)
        d[pid + ':' + 'volume'].append(math.nan)
        d[pid + ':' + 'volume_30day'].append(math.nan)
      else:
        d[pid + ':' + 'open'].append(float(pid_data['open']))
        d[pid + ':' + 'last'].append(float(pid_data['last']))
        d[pid + ':' + 'high'].append(float(pid_data['high']))
        d[pid + ':' + 'low'].append(float(pid_data['low']))
        d[pid + ':' + 'volume'].append(float(pid_data['volume']))
        d[pid + ':' + 'volume_30day'].append(float(pid_data['volume_30day']))
    d[STIME_COLNAME].append(row[STIME_COLNAME])
    d[RTIME_COLNAME].append(row[RTIME_COLNAME])
  df = pd.DataFrame(data=d)
  return df

def main():
  parser = build_subscriber_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_stat_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange)
  setup_gateway('cbp_{}_writer'.format(args.exchange))
  create_live_gauge('cbp_{}_writer'.format(args.exchange))
  create_write_success_gauge('cbp_{}_writer'.format(args.exchange))
  create_row_count_gauge('cbp_{}_writer'.format(args.exchange))
  create_error_gauge('cbp_{}_writer'.format(args.exchange))
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
