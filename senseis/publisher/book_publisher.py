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
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import is_book_exchange_name, get_exchange_pids, get_book_level

#TODO: how to do you deal with incomplete sets that build up over time ?

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

def get_period(args):
  if args.period < 1:
    logging.error("Coinbase Pro rate limit would exceed, need periodicity >= 1, exiting")
    sys.exit(1)
  return args.period

def build_parser():
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument("--period", type=int, help="periodicity in seconds", default=1)
  parser.add_argument("--exchange", type=str, help="queue exchange name", required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

async def book_extraction(pid, level, period, session, que):
  # synchronize at the start of next minute
  utc = pytz.timezone("UTC")
  t = datetime.now(utc)
  nxt_min = t + timedelta(seconds=60)
  nxt_min = nxt_min - timedelta(seconds=nxt_min.second, microseconds=nxt_min.microsecond)
  delta = nxt_min - t
  await asyncio.sleep((delta.seconds * MICROSECONDS + delta.microseconds) / MICROSECONDS)
  logging.info("Starting {}".format(pid))
  while True:
    url = 'https://api.exchange.coinbase.com/products/{}/book?level={}'.format(pid, level)
    header = {"Accept": "application/json"}
    time_record = datetime.now(utc)
    periodic_time = time_record - timedelta(microseconds=time_record.microsecond)
    while True:
      resp = await session.request(method="GET", url=url, headers=header)
      if resp.ok:
        break
      logging.info("Request {} failed: retcode {} reason {}. retrying in 10 milliseconds".format(pid, resp.status, resp.reason))
      await asyncio.sleep(RETRY_TIME / MICROSECONDS) # retry in 10 milliseconds
    data = await resp.text()
    await que.put((periodic_time, time_record, pid, data))
    t = datetime.now(utc)
    delta = t - periodic_time
    diff = MICROSECONDS * period - (delta.seconds * MICROSECONDS + delta.microseconds)
    await asyncio.sleep(diff / MICROSECONDS)

def create_message(periodic_time, time_record, data):
  data[STIME_COLNAME] = periodic_time.strftime(DATETIME_FORMAT)
  data[RTIME_COLNAME] = time_record.strftime(DATETIME_FORMAT)
  message = json.dumps(data)
  return message.encode()

async def book_consumer(pids, exchange_name, que):
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
        body = create_message(periodic_time, time_record, records[periodic_time])
        msg = aio_pika.Message(body=body)
        logging.info("Sending {}".format(periodic_time))
        await exchange.publish(message=msg, routing_key='')
        records.pop(periodic_time, None)
      que.task_done()

async def book_extraction_producer_consumer(pids, level, period, exchange_name):
  que = asyncio.Queue()
  async with aiohttp.ClientSession() as session:
    producers = []
    for pid in pids:
      producers.append(asyncio.create_task(book_extraction(pid, level, period, session, que)))
    consumers = [asyncio.create_task(book_consumer(pids, exchange_name, que))]
    await asyncio.gather(*producers, return_exceptions=True)
    await que.join()
    for c in consumers:
      c.cancel()

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_book_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  pids = get_exchange_pids(args.exchange)
  level = get_book_level(args.exchange)
  period = get_period(args)
  asyncio.run(book_extraction_producer_consumer(pids, level, period, args.exchange))

if __name__ == '__main__':
  main()
