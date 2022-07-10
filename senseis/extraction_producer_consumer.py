from io import BytesIO
import logging
import json
import pytz
from datetime import datetime, timedelta
from functools import partial
import asyncio
import aiohttp
import aio_pika
import aiobotocore.session as abcsession

from senseis.configuration import DATETIME_FORMAT, MICROSECONDS, RETRY_TIME, NUM_RETRIES
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import S3_ENDPOINT, S3_BUCKET, S3_KEY, S3_SECRET
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME

async def product_extraction_producer(url, pid, period, session, que):
  # synchronize at the start of next minute
  utc = pytz.timezone("UTC")
  header = {"Accept": "application/json"}
  t = datetime.now(utc)
  nxt_min = t + timedelta(seconds=60)
  nxt_min = nxt_min - timedelta(seconds=nxt_min.second, microseconds=nxt_min.microsecond)
  delta = nxt_min - t
  await asyncio.sleep((delta.seconds * MICROSECONDS + delta.microseconds) / MICROSECONDS)
  logging.info("Starting {}".format(pid))
  while True:
    time_record = datetime.now(utc)
    periodic_time = time_record - timedelta(microseconds=time_record.microsecond)
    data_good = False
    for _ in range(NUM_RETRIES):
      resp = await session.request(method="GET", url=url.format(pid), headers=header)
      if resp.ok:
        data_good = True
        break
      if resp.status >= 300 and resp.status < 400:
        logging.error("Request {} {} failed: retcode {} reason {}.".format(pid, periodic_time, resp.status, resp.reason))
        break
      # one error code 400, 429 too many requests
      elif resp.status >= 400 and resp.status < 500:
        logging.error("Request {} {} failed: retcode {} reason {}.".format(pid, periodic_time, resp.status, resp.reason))
        break
      # error code 524, 504
      elif resp.status >= 500:
        logging.info("Request {} failed: retcode {} reason {}. retrying in 10 milliseconds".format(pid, resp.status, resp.reason))
        await asyncio.sleep(RETRY_TIME / MICROSECONDS) # retry in 100 milliseconds
    if not data_good:
      logging.info("Enqueue None {} {}".format(pid, periodic_time))
      await que.put((periodic_time, time_record, pid, "\"\""))
    else:
      data = await resp.text()
      logging.debug("Enqueue {} {}".format(pid, periodic_time))
      await que.put((periodic_time, time_record, pid, data))
    t = datetime.now(utc)
    delta = t - periodic_time
    diff = MICROSECONDS * period - (delta.seconds * MICROSECONDS + delta.microseconds)
    await asyncio.sleep(diff / MICROSECONDS)

async def extraction_consumer(pids, exchange_name, create_message, que):
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

async def extraction_producer_consumer(producer, consumer, create_message, pids, url, period, exchange_name, **args):
  while True:
    tasks = []
    try:
      que = asyncio.Queue()
      async with aiohttp.ClientSession() as session:
        for pid in pids:
          tasks.append(asyncio.create_task(producer(url=url, pid=pid, period=period, session=session, que=que, **args)))
        tasks.append(asyncio.create_task(consumer(pids=pids, exchange_name=exchange_name, create_message=create_message, que=que)))
        await asyncio.gather(*tasks, return_exceptions=False)
        await que.join()
    except asyncio.CancelledError as err:
      logging.info("CancelledError {}".format(err))
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")

def create_message(periodic_time, time_record, data):
  data[STIME_COLNAME] = periodic_time.strftime(DATETIME_FORMAT)
  data[RTIME_COLNAME] = periodic_time.strftime(DATETIME_FORMAT)
  message = json.dumps(data)
  return message.encode()

async def consume_extraction(subscriber_f, writer_f, data_to_df_f, exchange_name, s3bucket, s3outdir, periodicity):
  tasks = []
  while True:
    try:
      que = asyncio.Queue()
      tasks.append(asyncio.create_task(subscriber_f(exchange_name, que)))
      tasks.append(asyncio.create_task(writer_f(data_to_df_f, exchange_name, s3bucket, s3outdir, periodicity, que)))
      await asyncio.gather(*tasks, return_exceptions=False)
      await que.join()
    except asyncio.CancelledError as err:
      logging.info("CancelledError {}".format(err))
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")

async def push_to_queue(que, msg: aio_pika.IncomingMessage):
  async with msg.process():
    await que.put(msg.body)

async def extraction_subscriber(exchange_name, que):
  connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  handler = partial(push_to_queue, que)
  async with connection:
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1) #TODO: needed ?
    exchange = await channel.declare_exchange(name=exchange_name, type='fanout')
    queue = await channel.declare_queue('', auto_delete=True)
    await queue.bind(exchange=exchange)
    await queue.consume(handler)
    await asyncio.Future()

def get_period(epoch, periodicity):
  period = epoch // periodicity
  return period

async def extraction_writer(data_to_df_f, exchange_name, s3bucket, s3outdir, periodicity, que):
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
      logging.info("Received {}".format(dat[STIME_COLNAME]))
      que.task_done()
      continue
    start_epoch = data_period * periodicity
    end_epoch = (data_period + 1) * periodicity
    filename = s3outdir + '/' + exchange_name + '_' + str(start_epoch) + '_' + str(end_epoch) + '.parquet'
    logging.info("Write s3://{}/{}".format(s3bucket, filename))
    #TODO: did it stuck here ? or somewhere else ?
    df = data_to_df_f(data, exchange_name)
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
