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
from senseis.metric_utility import setup_gateway, create_live_gauge, create_write_success_gauge, create_row_count_gauge, create_error_gauge

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
        bids = np.array(pid_data['bids'], dtype=np.float32).flatten()
        asks = np.array(pid_data['asks'], dtype=np.float32).flatten()
        d[pid + ':' + 'bids'].append(bids)
        d[pid + ':' + 'asks'].append(asks)
        d[pid + ':' + 'sequence'].append(int(pid_data['sequence']))
    d[STIME_COLNAME].append(row[STIME_COLNAME])
    d[RTIME_COLNAME].append(row[RTIME_COLNAME])
  df = pd.DataFrame(data=d)
  return df

def main():
  parser = build_subscriber_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_book_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange)
  setup_gateway('cbp_{}_writer'.format(args.exchange))
  create_live_gauge('cbp_{}_writer'.format(args.exchange))
  create_write_success_gauge('cbp_{}_writer'.format(args.exchange))
  create_row_count_gauge('cbp_{}_writer'.format(args.exchange))
  create_error_gauge('cbp_{}_writer'.format(args.exchange))
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
