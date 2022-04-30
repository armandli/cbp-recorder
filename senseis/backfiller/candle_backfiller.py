from io import BytesIO
import argparse
import logging
import math
import json
from datetime import datetime, timedelta
import pytz
import pandas as pd
import asyncio
import aiohttp
import aiobotocore.session as abcsession

from senseis.configuration import DATE_FORMAT, CANDLE_TIMESTAMP_FORMAT
from senseis.configuration import S3_ENDPOINT, S3_KEY, S3_SECRET
from senseis.configuration import CANDLE_GRANULARITY, CANDLE_SIZE
from senseis.configuration import is_candle_exchange_name, get_exchange_pids, get_s3_bucket, get_s3_outpath
from senseis.utility import setup_logging

def build_parser():
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument("--exchange", type=str, help='queue exchange name', required=True)
  parser.add_argument("--start-date", type=str, help='start datetime in YYYY-mm-dd', required=True)
  parser.add_argument("--end-date", type=str, help='end datetime in YYYY-mm-dd', required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

async def candle_extraction(pid, start_date, end_date, session, que):
  utc = pytz.timezone("UTC")
  url = "https://api.exchange.coinbase.com/products/{}/candles".format(pid)
  header = {"Accept": "application/json"}
  start_timestamp = int(utc.localize(datetime.strptime(start_date, DATE_FORMAT)).timestamp())
  end_timestamp = int(utc.localize(datetime.strptime(end_date, DATE_FORMAT)).timestamp())
  length = CANDLE_GRANULARITY * CANDLE_SIZE

  logging.info("Starting {} extraction".format(pid))

  for s in range(start_timestamp, end_timestamp, length):
    e = min(s + length, end_timestamp)
    st = datetime.utcfromtimestamp(s).strftime(CANDLE_TIMESTAMP_FORMAT)
    et = datetime.utcfromtimestamp(e).strftime(CANDLE_TIMESTAMP_FORMAT)
    params = {'granularity': CANDLE_GRANULARITY, 'start': st, 'end': et}
    while True:
      resp = await session.request(method="GET", url=url, headers=header, params=params)
      if resp.ok:
        break
      logging.info("Request {} failed: retcode {} reason {}. retrying in 10 milliseconds".format(pid, resp.status, resp.reason))
      await asyncio.sleep(RETRY_TIME / MICROSECONDS)
    data = await resp.text()
    await que.put((s, e, pid, data))
    await asyncio.sleep(1.)

  logging.info("pid {} extraction complete".format(pid))

def candle_data_to_df(data, pids, the_date, start_timestamp, end_timestamp):
  # timestamp, low, high, open, close, volume
  array_keys = ['low','high','open','close','volume']
  columns = [pid + ':' + ty for pid in pids for ty in array_keys]
  columns.append('timestamp')
  tdata = dict()
  for stime, dat in data.items():
    for pidx, pid in enumerate(pids):
      pid_data = json.loads(dat[pid])
      for row in pid_data:
        timestamp = row[0]
        if not (timestamp >= start_timestamp and timestamp < end_timestamp):
          continue
        if timestamp not in tdata:
          tdata[timestamp] = [math.nan for _ in range(len(pids) * 5)]
        tdata[timestamp][pidx * 5]     = row[1]
        tdata[timestamp][pidx * 5 + 1] = row[2]
        tdata[timestamp][pidx * 5 + 2] = row[3]
        tdata[timestamp][pidx * 5 + 3] = row[4]
        tdata[timestamp][pidx * 5 + 4] = row[5]
  d = {key : [] for key in columns}
  for t, row in tdata.items():
    for pidx, pid in enumerate(pids):
      for aidx, col in enumerate(array_keys):
        d[pid + ':' + col].append(row[pidx * 5 + aidx])
    d['timestamp'].append(t)
  df = pd.DataFrame(data=d)
  df.sort_values(by=['timestamp'], inplace=True)
  return df

async def candle_writer(pids, exchange_name, start_date, end_date, s3bucket, s3outdir, que):
  utc = pytz.timezone("UTC")
  start_timestamp = int(utc.localize(datetime.strptime(start_date, DATE_FORMAT)).timestamp())
  end_timestamp = int(utc.localize(datetime.strptime(end_date, DATE_FORMAT)).timestamp())
  records = dict()
  e = start_timestamp
  all_found = False

  logging.info("Start Consumer/Writer")

  while e < end_timestamp or not all_found:
    s, e, pid, data = await que.get()
    if s in records:
      records[s][pid] = data
    else:
      records[s] = {pid: data}
    if e == end_timestamp:
      all_found = True
      for known_pid in pids:
        if known_pid not in records[s]:
          all_found = False
          break
    que.task_done()

  logging.info("Extraction complete. Writing to S3")

  the_date = datetime.strptime(start_date, DATE_FORMAT)
  date_end = datetime.strptime(end_date, DATE_FORMAT)
  while the_date < date_end:
    time_begin = int(utc.localize(the_date).timestamp())
    time_end = int(utc.localize(the_date + timedelta(days=1)).timestamp())
    day_data = dict()
    logging.info("gothere {} {}".format(time_begin, time_end))
    for stime, data in records.items():
      if stime + CANDLE_GRANULARITY * CANDLE_SIZE >= time_begin and stime < time_end:
        day_data[stime] = data
    df = candle_data_to_df(day_data, pids, the_date, time_begin, time_end)
    logging.info("Dataframe size {}".format(len(df)))
    filename = s3outdir + '/' + exchange_name + '_' + str(time_begin) + '_' + str(time_end) + '.parquet'
    logging.info("Write s3://{}/{}".format(s3bucket, filename))
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    session = abcsession.get_session()
    async with session.create_client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET) as client:
      resp = await client.put_object(Bucket=s3bucket, Key=filename, Body=parquet_buffer.getvalue())
      logging.info(resp)

    the_date += timedelta(days=1)

  logging.info("Write Complete")

async def candle_extraction_producer_writer(pids, exchange_name, start_date, end_date, s3bucket, s3outdir):
  que = asyncio.Queue()
  async with aiohttp.ClientSession() as session:
    producers = []
    for pid in pids:
      producers.append(asyncio.create_task(candle_extraction(pid, start_date, end_date, session, que)))
    consumers = [asyncio.create_task(candle_writer(pids, exchange_name, start_date, end_date, s3bucket, s3outdir, que))]
    await asyncio.gather(*producers, return_exceptions=True)
    await que.join()
    #don't cancel consumer because there is more work there

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_candle_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  pids = get_exchange_pids(args.exchange)
  s3bucket = get_s3_bucket(args.exchange)
  s3outdir = get_s3_outpath(args.exchange)
  asyncio.run(candle_extraction_producer_writer(pids, args.exchange, args.start_date, args.end_date, s3bucket, s3outdir))

if __name__ == '__main__':
  main()
