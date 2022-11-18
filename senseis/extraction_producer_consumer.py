from io import BytesIO
import logging
import json
import pytz
import time
from datetime import datetime, timedelta
from functools import partial
import botocore
import asyncio
import aiohttp
import aio_pika
import aiobotocore.session as abcsession

from prometheus_client import push_to_gateway

from senseis.configuration import DATETIME_FORMAT, TICKER_TIME_FORMAT1, TICKER_TIME_FORMAT2
from senseis.configuration import MICROSECONDS, RETRY_TIME, NUM_RETRIES
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import S3_ENDPOINT, S3_BUCKET, S3_KEY, S3_SECRET
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import S3_RETRY_TIME_SECOND
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import get_collector_registry, get_job_name
from senseis.metric_utility import get_live_gauge, get_write_success_gauge, get_row_count_gauge, get_error_gauge, get_output_data_process_time_gauge

def convert_trade_time(time_str):
  try:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT1)
  except ValueError:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT2)

def is_all_found(names, data):
  all_found = True
  for name in names:
    if name not in data:
      all_found = False
      break
  return all_found

async def product_extraction_producer(url, pid, period, session, que):
  # synchronize at the start of next second
  utc = pytz.timezone("UTC")
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
        resp = await session.request(method="GET", url=url.format(pid), headers=header)
        if resp.ok:
          data_good = True
          break
        if resp.status >= 300 and resp.status < 400:
          logging.error("Request {} {} failed: retcode {} reason {}.".format(pid, periodic_time, resp.status, resp.reason))
          get_error_gauge().inc()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          break
        # one error code 400, 429 too many requests
        elif resp.status >= 400 and resp.status < 500:
          logging.error("Request {} {} failed: retcode {} reason {}.".format(pid, periodic_time, resp.status, resp.reason))
          get_error_gauge().inc()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          break
        # error code 524, 504
        elif resp.status >= 500:
          logging.info("Request {} failed: retcode {} reason {}. retrying in 10 milliseconds".format(pid, resp.status, resp.reason))
          get_error_gauge().inc()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          await asyncio.sleep(RETRY_TIME / MICROSECONDS) # retry in 100 milliseconds
      except asyncio.TimeoutError as err:
        logging.info("TimeoutError {}".format(err))
        get_error_gauge().inc()
        push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
    if not data_good:
      logging.info("Enqueue None {} {}".format(pid, periodic_time))
      await que.put((periodic_time, time_record, pid, "\"\""))
    else:
      try:
        data = await resp.text()
        logging.debug("Enqueue {} {}".format(pid, periodic_time))
        await que.put((periodic_time, time_record, pid, data))
      except aiohttp.client_exceptions.ClientPayloadError as err:
        logging.error("Client Payload Error {}".format(err))
        get_error_gauge().inc()
        push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
        await que.put((periodic_time, time_record, pid, "\"\""))
      except asyncio.exceptions.TimeoutError as err:
        logging.error("Timeout Error {}".format(err))
        get_error_gauge().inc()
        push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
        await que.put((periodic_time, time_record, pid, "\"\""))
    t = datetime.now(utc)
    delta = t - periodic_time
    diff = MICROSECONDS * period - (delta.seconds * MICROSECONDS + delta.microseconds)
    await asyncio.sleep(diff / MICROSECONDS)

async def extraction_consumer(pids, exchange_name, create_message_f, que):
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
      all_found = is_all_found(pids, records[periodic_time])
      if all_found:
        body = create_message_f(periodic_time, time_record, records[periodic_time])
        msg = aio_pika.Message(body=body)
        logging.info("Sending {}".format(periodic_time))
        await exchange.publish(message=msg, routing_key='')
        get_live_gauge().set_to_current_time()
        push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
        records.pop(periodic_time, None)
      que.task_done()

async def extraction_producer_consumer(producer_f, consumer_f, create_message_f, pids, url, period, exchange_name, **args):
  while True:
    tasks = []
    try:
      que = asyncio.Queue()
      async with aiohttp.ClientSession() as session:
        tasks.append(asyncio.create_task(consumer_f(pids=pids, exchange_name=exchange_name, create_message_f=create_message_f, que=que)))
        for pid in pids:
          tasks.append(asyncio.create_task(producer_f(url=url, pid=pid, period=period, session=session, que=que, **args)))
        await asyncio.gather(*tasks, return_exceptions=False)
        await que.join()
    except asyncio.CancelledError as err:
      logging.info("CancelledError {}".format(err))
      get_error_gauge().inc()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")
    except aiohttp.client_exceptions.ClientOSError as err:
      logging.info("ClientOSError {}".format(err))
      get_error_gauge().inc()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")
    except aiohttp.client_exceptions.ServerDisconnectedError as err:
      logging.info("ServerDisconnectedError {}".format(err))
      get_error_gauge().inc()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
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
      tasks.append(asyncio.create_task(writer_f(data_to_df_f, exchange_name, s3bucket, s3outdir, periodicity, que)))
      tasks.append(asyncio.create_task(subscriber_f(exchange_name, que)))
      await asyncio.gather(*tasks, return_exceptions=False)
      await que.join()
    except asyncio.CancelledError as err:
      logging.info("CancelledError {}".format(err))
      get_error_gauge().inc()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
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
      get_live_gauge().set_to_current_time()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
      logging.info("Received {}".format(dat[STIME_COLNAME]))
      que.task_done()
      continue
    start_epoch = data_period * periodicity
    end_epoch = (data_period + 1) * periodicity
    filename = s3outdir + '/' + exchange_name + '_' + str(start_epoch) + '_' + str(end_epoch) + '.parquet'
    logging.info("Write s3://{}/{}".format(s3bucket, filename))
    perf_output_time_start = time.perf_counter()
    df = data_to_df_f(data, exchange_name)
    logging.info("Dataframe size {}".format(len(df)))
    perf_output_time = time.perf_counter() - perf_output_time_start
    get_output_data_process_time_gauge().set(perf_output_time)
    push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
    logging.info("Clearing Data")
    data.clear()
    data.append(dat)
    que.task_done()
    logging.info("Creating dataframe")
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    get_row_count_gauge().set(df.shape[0])
    push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
    session = abcsession.get_session()
    while True:
      try:
        async with session.create_client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET) as client:
          logging.info("Pushing to S3")
          resp = await client.put_object(Bucket=s3bucket, Key=filename, Body=parquet_buffer.getvalue())
          get_write_success_gauge().set_to_current_time()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          logging.info(resp)
          break
      except botocore.exceptions.ClientError as err:
        logging.info("botocore ClientError: {}".format(err))
        get_error_gauge().inc()
        push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
        await asyncio.sleep(S3_RETRY_TIME_SECOND)
      except Exception as err:
        logging.error("Unexpected write error: {}".fomat(err))
        get_error_gauge().inc()
        push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
        await asyncio.sleep(S3_RETRY_TIME_SECOND)
