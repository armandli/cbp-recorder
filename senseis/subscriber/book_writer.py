from io import BytesIO
import argparse
import logging
import json
import pytz
from datetime import datetime
from functools import partial
import pandas as pd
import numpy as np
import asyncio
import aio_pika
import aiobotocore.session as abcsession

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import S3_ENDPOINT, S3_KEY, S3_SECRET
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import is_book_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath

def setup_logging(args):
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)
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
  parser.add_argument('--period', type=int, help='output period, in minutes', default=1440)
  parser.add_argument('--exchange', type=str, help='queue exchange name', required=True)
  parser.add_argument('--logfile', type=str, help='logfile path', required=True)
  return parser

def data_to_df(data, exchange_name):
  pids = get_exchange_pids(exchange_name)
  columns = [pid + ':' + ty for pid in pids for ty in ['bids', 'asks']]
  columns.append(STIME_COLNAME)
  columns.append(RTIME_COLNAME)
  data.sort(key=lambda x: datetime.strptime(x[STIME_COLNAME], DATETIME_FORMAT))
  d = {colname : [] for colname in columns}
  for row in data:
    for pid in pids:
      pid_data = json.loads(row[pid])
      bids = np.array(pid_data['bids'], dtype=np.float32).flatten()
      asks = np.array(pid_data['asks'], dtype=np.float32).flatten()
      d[pid + ':' + 'bids'].append(bids)
      d[pid + ':' + 'asks'].append(asks)
    d[STIME_COLNAME].append(row[STIME_COLNAME])
    d[RTIME_COLNAME].append(row[RTIME_COLNAME])
  df = pd.DataFrame(data=d)
  return df

def get_period(epoch, periodicity):
  period = epoch // periodicity
  return period

async def book_writer(exchange_name, s3bucket, s3outdir, periodicity, que):
  data = []
  while True:
    msg = await que.get()
    dat = json.loads(msg)
    dat_period = get_period(int(datetime.strptime(dat[STIME_COLNAME], DATETIME_FORMAT).timestamp()), periodicity)
    if data:
      data_period = get_period(int(datetime.strptime(data[0][STIME_COLNAME], DATETIME_FORMAT).timestamp()), periodicity)
    else:
      data_period = dat_period
    if data_period == dat_period:
      data.append(dat)
      que.task_done()
      continue
    start_epoch = data_period * periodicity
    end_epoch = (data_period + 1) * periodicity
    filename = s3outdir + '/' + exchange_name + '_' + str(start_epoch) + '_' + str(end_epoch) + '.parquet'
    logging.info("Write s3://{}/{}".format(s3bucket, filename))
    df = data_to_df(data, exchange_name)
    logging.info("Dataframe size {}".format(len(df)))
    data.clear()
    data.append(dat)
    que.task_done()
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    session = abcsession.get_session()
    async with session.create_client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET) as client:
      resp = await client.put_object(Bucket=s3bucket, Key=filename, Body=parquet_buffer.getvalue())
      logging.info(resp)

async def push_book_to_queue(que, msg: aio_pika.IncomingMessage):
  async with msg.process():
    await que.put(msg.body)

async def book_subscriber(exchange_name, que):
  connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  book_handler = partial(push_book_to_queue, que)
  async with connection:
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(name=exchange_name, type='fanout')
    queue = await channel.declare_queue('', auto_delete=True)
    await queue.bind(exchange=exchange)
    await queue.consume(book_handler)
    await asyncio.Future()

async def consume_book(exchange_name, s3bucket, s3outdir, periodicity):
  que = asyncio.Queue()
  subscriber = asyncio.create_task(book_subscriber(exchange_name, que))
  writer = asyncio.create_task(book_writer(exchange_name, s3bucket, s3outdir, periodicity, que))
  await asyncio.gather(subscriber, return_exceptions=True)
  await que.join()
  writer.cancel()

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_book_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange)
  asyncio.run(
    consume_book(
        args.exchange,
        s3bucket,
        s3outdir,
        args.period * 60,
    )
  )

if __name__ == '__main__':
  main()
