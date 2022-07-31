import argparse
import logging
import json
from datetime import datetime, timedelta
import pytz
import asyncio

from senseis.configuration import MICROSECONDS, RETRY_TIME, NUM_RETRIES, TRADE_SIZE_LIMIT
from senseis.configuration import TRADE_REQUEST_URL
from senseis.configuration import is_trade_exchange_name, get_exchange_pids
from senseis.utility import setup_logging, build_publisher_parser
from senseis.extraction_producer_consumer import extraction_producer_consumer, extraction_consumer, create_message

#TODO: we can push this into utility
def get_period(args):
  if args.period < 1:
    logging.error("Coinbase Pro rate limit would exceed, need periodicity >= 1, exiting")
  return args.period

def filter_trade_data(data, last_trade_id):
  if last_trade_id is None:
    return data
  try:
    data_dict = json.loads(data)
    filtered_data_dict = []
    for d in data_dict:
      try:
        if int(d['trade_id']) > last_trade_id:
          filtered_data_dict.append(d)
      except ValueError:
        logging.error("Got empty trade id {}. skip".format(d))
    out_data = json.dumps(filtered_data_dict)
    return out_data
  except json.decoder.JSONDecodeError:
    return "\"\""
  except TypeError:
    return "\"\""

def get_last_trade_id(data, last_trade_id):
  if data is None:
    return last_trade_id
  try:
    data_dict = json.loads(data)
    if len(data_dict) == 0:
      return last_trade_id
    idx = 0
    while idx < len(data_dict):
      try:
        trade_id = int(data_dict[idx]['trade_id'])
        if last_trade_id is None:
          return trade_id
        return max(trade_id, last_trade_id)
      except ValueError:
        idx += 1
    return last_trade_id
  except json.decoder.JSONDecodeError:
    return last_trade_id
  except ValueError:
    return last_trade_id

async def trade_extraction(url, pid, period, session, que):
  # synchronize at the start of next second
  utc = pytz.timezone("UTC")
  params = {"limit" : TRADE_SIZE_LIMIT}
  header = {"Accept": "application/json"}
  t = datetime.now(utc)
  nxt_sec = t + timedelta(seconds=1)
  nxt_sec = nxt_sec - timedelta(seconds=0, microseconds=nxt_sec.microsecond)
  delta = nxt_sec - t
  await asyncio.sleep((delta.seconds * MICROSECONDS + delta.microseconds) / MICROSECONDS)
  logging.info("Starting {}".format(pid))
  last_trade_id = None
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
        data = filter_trade_data(data, last_trade_id)
        logging.debug("enqueue {} {}".format(pid, periodic_time))
        await que.put((periodic_time, time_record, pid, data))
        # update param for pagination
        last_trade_id = get_last_trade_id(data, last_trade_id)
        params = {"limit" : TRADE_SIZE_LIMIT, "after" : last_trade_id}
      except aiohttp.client_exception.ClientPayloadError as err:
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
  if not is_trade_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  pids = get_exchange_pids(args.exchange)
  period = get_period(args)
  asyncio.run(
    extraction_producer_consumer(
      trade_extraction,
      extraction_consumer,
      create_message,
      pids,
      TRADE_REQUEST_URL,
      period,
      args.exchange
    ))

if __name__ == '__main__':
  main()
