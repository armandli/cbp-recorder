from io import BytesIO
import argparse
import logging
import json
import pytz
from datetime import datetime
from functools import partial
import pandas as pd
import asyncio
import aio_pika
import aiobotocore.session as abcsession

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import S3_ENDPOINT, S3_KEY, S3_SECRET
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME, PERIOD_COLNAME
from senseis.configuration import is_valid_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath

def setup_logging(args):
  logger = logging.getLogger()
  logger.setLevel(logging.DEBUG)
  fhandler = logging.FileHandler(args.logfile, mode='w')
  formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-5s %(message)s', datefmt='%H:%M:%s')
  fhandler.setFormatter(formatter)
  logger.addHandler(fhandler)
  chandler = logging.StreamHandler()
  chandler.setLevel(logging.INFO)
  chandler.setFormatter(formatter)
  logger.addHandler(chandler)

def build_parser():
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('--period', type=int, help='output period, in minutes', default=3600)
  parser.add_argument('--exchange', type=str, help='queue exchange name', required=True)
  parser.add_argument('--logfile', type=str, help='logfile path', required=True)
  return parser

def data_to_df(data, exchange_name):
  #TODO
  pass

async def write_candle_to_s3(s3bucket, s3outdir, periodicity, data, exchange_name, msg: aio_pika.IncomingMessage):
  async with msg.process():
    dat = json.loads(msg.body)
    dat_period = get_period(int(datetime.strptime(dat[STIME_COLNAME], DATETIME_FORMAT).timestamp()), periodicity)
    #TODO
    if data:
      data_period = get_period(int(datetime.strptime(data[0][STIME_COLNAME], DATETIME_FORMAT).timestamp()), periodicity)
    else:
      data_period = dat_period
    if data_period == dat_period:
      data.append(dat)
      return
    #TODO

async def consume_candle(exchange_name, s3bucket, s3outdir, periodicity):
  data = []
  candle_handler = partial(write_candle_to_s3, s3bucket, s3outdir, periodicity, data, exchange_name)
  connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  async with connection:
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(name=exchange_name, type='fanout')
    queue = await channel.declare_queue('', auto_delete=True)
    await queue.bind(exchange=exchange)
    await queue.consume(candle_handler)
    await asyncio.Future()

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_valid_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange)
  asyncio.run(
    consume_candle(
        args.exchange,
        s3bucket,
        s3outdir,
        args.period * 60,
    )
  )

if __name__ == '__main__':
  main()
