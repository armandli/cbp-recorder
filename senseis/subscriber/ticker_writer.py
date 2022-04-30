import argparse
import logging
import json
from datetime import datetime
import pandas as pd
import asyncio

from senseis.configuration import DATETIME_FORMAT, TICKER_TIME_FORMAT
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import is_ticker_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath
from senseis.utility import setup_logging, build_subscriber_parser
from senseis.extraction_producer_consumer import consume_extraction, extraction_subscriber, extraction_writer

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
      d[pid + ':' + 'bid'].append(float(pid_data['bid']))
      d[pid + ':' + 'ask'].append(float(pid_data['ask']))
      d[pid + ':' + 'volume'].append(float(pid_data['volume']))
      d[pid + ':' + 'price'].append(float(pid_data['price']))
      d[pid + ':' + 'size'].append(float(pid_data['size']))
      d[pid + ':' + 'trade_id'].append(int(pid_data['trade_id']))
      d[pid + ':' + 'time'].append(datetime.strptime(pid_data['time'], TICKER_TIME_FORMAT))
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