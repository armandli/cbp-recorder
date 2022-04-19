import sys
import argparse
import logging
import json
import time
from datetime import datetime, timedelta
import pytz
import asyncio
import aiohttp
import aio_pika

from senseis.configuration import DATETIME_FORMAT, MICROSECONDS, RETRY_TIME
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME, PERIOD_COLNAME
from senseis.configuration import CANDLE_GRANULARITY
from senseis.configuration import is_candle_exchange_name, get_exchange_pids

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
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument("--exchange", type=str, help='queue exchange name', required=True)
  parser.add_argument("--period", type=int, help='candle report interval, in minutes', default=60)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

async def candle_extraction(pid, periodicity, session, que):
  # synchronize at the start of the next period
  utc = pytz.timezone('UTC')
  url = 'https://api.exchange.coinbase.com/products/{}/candles'.format(pid)
  header = {"Accept": "application/json"}
  t = datetime.now(utc).timestamp()
  nxt_epoch = (t // periodicity + 1) * periodicity
  nxt_time = datetime.fromtimestamp(nxt_epoch)
  delta = nxt_epoch - t
  await asyncio.sleep(delta)
  now = datetime.now(utc)
  logging.info("Starting {} at {}".format(pid, now))
  while True:
    start_timestamp = (t // periodicity) * periodicity
    params = {'granularity': CANDLE_GRANULARITY, 'start': int(start_timestamp), 'end': int(nxt_epoch) - 1}
    time_record = datetime.now(utc)
    while True:
      resp = await session.request(method="GET", url=url, headers=header, params=params)
      if resp.ok:
        break
      logging.info("Request {} failed: retcode {} reason {}. retrying in 10 milliseconds".format(pid, resp.status, resp.reason))
      await asyncio.sleep(RETRY_TIME / MICROSECONDS) # retry in 10 milliseconds
    data = await resp.text()
    await que.put((nxt_time, time_record, pid, data))
    t = datetime.now(utc).timestamp()
    nxt_epoch = (t // periodicity + 1) * periodicity
    nxt_time = datetime.fromtimestamp(nxt_epoch)
    delta = nxt_epoch - t
    await asyncio.sleep(delta)

def create_message(periodic_time, time_record, periodicity, data):
  data[STIME_COLNAME] = periodic_time.strftime(DATETIME_FORMAT)
  data[RTIME_COLNAME] = periodic_time.strftime(DATETIME_FORMAT)
  data[PERIOD_COLNAME] = periodicity
  message = json.dumps(data)
  return message.encode()

#TODO: book and candle consumer look almost exactly the same
async def candle_consumer(pids, exchange_name, periodicity, que):
  utc = pytz.timezone("UTC")
  records = dict()
  mq_connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  async with mq_connection:
    channel = await mq_connection.channel()
    exchange = await channel.declare_exchange(name=exchange_name, type='fanout')
    logging.info("Pushing to {}".format(exchange_name))
    while True:
      periodic_time, time_record, pid, data = await que.get()
      if periodic_time in records:
        records[periodic_time][pid] = data
      else:
        records[periodic_time] = {pid : data}
      all_found = True
      for known_pid in pids:
        if known_pid not in records[periodic_time]:
          all_found = False
          break
      if all_found:
        body = create_message(periodic_time, time_record, periodicity, records[periodic_time])
        msg = aio_pika.Message(body=body)
        logging.info("Sending {}".format(periodic_time))
        await exchange.publish(message=msg, routing_key='')
        records.pop(periodic_time, None)
      que.task_done()

async def candle_extraction_producer_consumer(pids, periodicity, exchange_name):
  que = asyncio.Queue()
  async with aiohttp.ClientSession() as session:
    producers = []
    for pid in pids:
      producers.append(asyncio.create_task(candle_extraction(pid, periodicity, session, que)))
    consumers = [asyncio.create_task(candle_consumer(pids, exchange_name, periodicity, que))]
    await asyncio.gather(*producers)
    await que.join()
    for c in consumers:
      c.cancel()

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_candle_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  pids = get_exchange_pids(args.exchange)
  asyncio.run(candle_extraction_producer_consumer(pids, args.period * 60, args.exchange))

if __name__ == '__main__':
  main()
