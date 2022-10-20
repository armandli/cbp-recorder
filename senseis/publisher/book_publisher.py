import argparse
import logging
from datetime import datetime, timedelta
import pytz
import asyncio
import aiohttp

from senseis.configuration import MICROSECONDS, RETRY_TIME, NUM_RETRIES
from senseis.configuration import BOOK_REQUEST_URL
from senseis.configuration import is_book_exchange_name, get_exchange_pids, get_book_level
from senseis.utility import setup_logging, build_publisher_parser
from senseis.extraction_producer_consumer import extraction_producer_consumer, extraction_consumer, create_message
from senseis.metric_utility import setup_gateway, create_live_gauge

#TODO: how to do you deal with incomplete sets that build up over time ?

def get_period(args):
  if args.period < 1:
    logging.error("Coinbase Pro rate limit would exceed, need periodicity >= 1, exiting")
  return args.period

async def book_extraction(url, pid, period, session, que, level):
  # synchronize at the start of next second
  utc = pytz.timezone("UTC")
  params = {"level":level}
  header = {"Accept": "application/json"}
  t = datetime.now(utc)
  nxt_sec = t + timedelta(seconds=1)
  nxt_sec = nxt_sec - timedelta(seconds=0, microseconds=nxt_sec.microsecond)
  delta = nxt_sec - t
  await asyncio.sleep((delta.seconds * MICROSECONDS + delta.microseconds) / MICROSECONDS)
  logging.info("Starting {}".format(pid))
  while True:
    time_record = datetime.now(utc)
    periodic_time = time_record - timedelta(microseconds=time_record.microsecond)
    data_good = False
    for _ in range(NUM_RETRIES):
      try:
        resp = await session.request(method="GET", url=url.format(pid), headers=header, params=params)
        if resp.ok:
          data_good = True
          break
        if resp.status >= 300 and resp.status < 400:
          logging.error("Request {} {} failed: retcode {} reason {}.".format(pid, periodic_time, resp.status, resp.reason))
          break
        elif resp.status >= 400 and resp.status < 500:
          logging.error("Request {} {} failed: retcode {} reason {}.".format(pid, periodic_time, resp.status, resp.reason))
          break
        elif resp.status >= 500:
          logging.info("Request {} {} failed: retcode {} reason {}. retrying in 10 milliseconds".format(pid, periodic_time, resp.status, resp.reason))
          await asyncio.sleep(RETRY_TIME / MICROSECONDS) # retry in 100 milliseconds
      except asyncio.TimeoutError as err:
        logging.info("TimeoutError {}".format(err))
    if not data_good:
      logging.info("enqueue None {} {}".format(pid, periodic_time))
      await que.put((periodic_time, time_record, pid, "\"\""))
    else:
      try:
        data = await resp.text()
        logging.debug("enqueue {} {}".format(pid, periodic_time))
        await que.put((periodic_time, time_record, pid, data))
      except aiohttp.client_exceptions.ClientPayloadError as err:
        logging.error("Client Payload Error {}".format(err))
        await que.put((periodic_time, time_record, pid, "\"\""))
    t = datetime.now(utc)
    delta = t - periodic_time
    diff = MICROSECONDS * period - (delta.seconds * MICROSECONDS + delta.microseconds)
    await asyncio.sleep(diff / MICROSECONDS)

def main():
  parser = build_publisher_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_book_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  pids = get_exchange_pids(args.exchange)
  level = get_book_level(args.exchange)
  period = get_period(args)
  setup_gateway('cbp_{}_publisher'.format(args.exchange))
  create_live_gauge('cbp_{}_publisher'.format(args.exchange))
  asyncio.run(
    extraction_producer_consumer(
      book_extraction,
      extraction_consumer,
      create_message,
      pids,
      BOOK_REQUEST_URL,
      period,
      args.exchange,
      level=level
    ))

if __name__ == '__main__':
  main()
