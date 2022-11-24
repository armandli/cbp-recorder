import argparse
import logging
import json
from datetime import datetime
import pandas as pd
import asyncio

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import STIME_COLNAME
from senseis.configuration import is_etl_exchange_name, get_s3_bucket, get_s3_outpath
from senseis.utility import setup_logging, build_subscriber_parser
from senseis.extraction_producer_consumer import consume_extraction, extraction_subscriber, extraction_writer
from senseis.extraction_producer_consumer import create_interval_state
from senseis.metric_utility import setup_gateway, setup_subscriber_gauges

def data_to_df(data, exchange_name):
  data.sort(key=lambda x: datetime.strptime(x[STIME_COLNAME], DATETIME_FORMAT))
  d = dict()
  for row in data:
    if not d:
      columns = row.keys()
      d = {colname : [] for colname in columns}
    for column in columns:
      if column in row:
        d[column].append(row[column])
      else:
        d[column].append(float("nan"))
  df = pd.DataFrame(data=d)
  return df

def main():
  parser = build_subscriber_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_etl_exchange_name(args.exchange):
    logging.error("Invalid ETL exchange name. exist")
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
        args.period * 60
      )
    )
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
