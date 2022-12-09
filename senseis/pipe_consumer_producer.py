import logging
import json
import math
from datetime import datetime, timedelta
import pytz
import zlib
from functools import partial
from abc import ABC, abstractmethod

import asyncio
import aio_pika

from prometheus_client import push_to_gateway

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import MICROSECONDS
from senseis.configuration import STIME_COLNAME
from senseis.configuration import ETL_QUEUE_SIZE
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import get_exchange_pids, is_book_exchange_name, is_trade_exchange_name
from senseis.extraction_producer_consumer import get_period, is_all_found
from senseis.extraction_producer_consumer import get_interval
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import get_collector_registry, get_job_name
from senseis.metric_utility import get_live_gauge, get_restarted_counter, get_interval_gauge

def process_etl_data(period, data, state):
  book_data = dict()
  trade_data = dict()
  for exchange, msg in data.items():
    dat = json.loads(msg)
    if is_book_exchange_name(exchange):
      book_data.update(dat)
    elif is_trade_exchange_name(exchange):
      trade_data.update(dat)
    else:
      logging.info("Unexpected data type from exchange {}, neither book nor trade".format(exchange))
  logging.info("Inserting period {} data into state".format(period))
  state.insert(period, book_data, trade_data)
  logging.info("Producing ETL output data from state")
  output = state.produce_output(period)
  logging.info("Output production complete")
  message = json.dumps(output)
  return zlib.compress(message.encode())

async def etl_processor(etl_f, create_etl_state_f, get_history_size_f, output_exchange_name, input_exchange_names, periodicity, que):
  utc = pytz.timezone("UTC")
  etl_state = create_etl_state_f()
  records = dict()
  pids = set()
  for input_exchange_name in input_exchange_names:
    pids.update(get_exchange_pids(input_exchange_name))
  etl_state.set_pids(list(pids))
  mq_connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  async with mq_connection:
    channel = await mq_connection.channel()
    exchange = await channel.declare_exchange(name=output_exchange_name, type='fanout')
    logging.info("Pushing to {}".format(output_exchange_name))
    while True:
      ie_name, msg = await que.get()
      dat = json.loads(msg)
      cur_epoch = int(datetime.strptime(dat[STIME_COLNAME], DATETIME_FORMAT).timestamp())
      epoch_interval = get_interval(cur_epoch)
      get_interval_gauge().set(epoch_interval)
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
      #TODO: this is a bad idea, period other than 1 will not represent time, but it is treated like time in seconds already
      dat_period = get_period(cur_epoch, periodicity)
      if dat_period in records:
        records[dat_period][ie_name] = msg
      else:
        records[dat_period] = {ie_name : msg}
      for period in sorted(records.keys()):
        period_str = utc.localize(datetime.utcfromtimestamp(period)).strftime(DATETIME_FORMAT)
        if period < dat_period - get_history_size_f():
          records.pop(period, None)
        elif not is_all_found(input_exchange_names, records[period]) or period > dat_period:
          break
        else:
          logging.info("Producing output data for period {}".format(period))
          output = etl_f(period, records[period], etl_state)
          msg = aio_pika.Message(body=output)
          logging.info("Sending {}".format(period_str))
          await exchange.publish(message=msg, routing_key='')
          logging.info("Published ETL data for period {}".format(period_str))
          get_live_gauge().set_to_current_time()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          records.pop(period, None)
      que.task_done()

async def push_incoming_to_queue(que, utc, exchange_name, msg: aio_pika.IncomingMessage):
  async with msg.process():
    logging.info("Received from {}".format(exchange_name))
    data = zlib.decompress(msg.body).decode()
    await que.put((exchange_name, data))

async def data_subscriber(exchange_name, que):
  utc = pytz.timezone("UTC")
  connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  handler = partial(push_incoming_to_queue, que, utc, exchange_name)
  async with connection:
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1) #TODO: needed ?
    exchange = await channel.declare_exchange(name=exchange_name, type='fanout')
    queue = await channel.declare_queue('', auto_delete=True)
    await queue.bind(exchange=exchange)
    await queue.consume(handler)
    await asyncio.Future()

async def etl_consumer_producer(
    data_subscriber_f,
    etl_processor_f,
    process_etl_data_f,
    create_etl_state_f,
    get_history_size_f,
    output_exchange_name,
    input_exchange_names,
    periodicity
  ):
  tasks = []
  while True:
    try:
      que = asyncio.Queue(maxsize=ETL_QUEUE_SIZE)
      tasks.append(asyncio.create_task(etl_processor_f(process_etl_data_f, create_etl_state_f, get_history_size_f, output_exchange_name, input_exchange_names, periodicity, que)))
      for input_exchange in input_exchange_names:
          tasks.append(asyncio.create_task(data_subscriber_f(input_exchange, que)))
      await asyncio.gather(*tasks, return_exceptions=False)
      await que.join()
    except asyncio.CancelledError as err:
      logging.info("Cancelled Error: {}".format(err))
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")
      get_restarted_counter().inc()
      push_to_gateway(GETWAY_URL, job=get_job_name(), registry=get_collector_registry())

class ETLState(ABC):
  def __init__(self):
    self.timestamps = None

  @abstractmethod
  def set_pids(self, pids):
    pass

  @abstractmethod
  def insert(self, timestamp, pid_book, pid_trade):
    pass

  @abstractmethod
  def produce_output(self, timestamp):
    pass

  @abstractmethod
  def hist_size(self):
    pass

  #note: assume lengths is already sorted
  def rolling_avg_sum_max_min_multi_k(self, data, idx, timestamp, lengths, count_nan=False):
    s = 0.
    s_min = float("inf")
    s_max = float("-inf")
    nan_count = 0
    count = 0
    length_idx = 0
    max_length = max(lengths)
    min_timestamp = timestamp - max_length
    ret = []
    for i in range(max_length + 1):
      if i == lengths[length_idx]:
        if count_nan:
          ret.append([s / float(count),             s, s_max, s_min])
        elif count - nan_count <= 0:
          ret.append([float("nan"),                 s, s_max, s_min])
        else:
          ret.append([s / float(count - nan_count), s, s_max, s_min])
        length_idx += 1
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        continue
      if not math.isnan(data[(idx - i) % self.hist_size()]):
        s += data[(idx - i) % self.hist_size()]
        s_max = max(s_max, data[(idx - i) % self.hist_size()])
        s_min = min(s_min, data[(idx - i) % self.hist_size()])
      else:
        nan_count += 1
      count += 1
    return ret

  def rolling_avg_sum_max_min_aoa_multi_k(self, data, idx, ioidx, timestamp, lengths, nan_count=False):
    s = 0.
    s_max = float("-inf")
    s_min = float("inf")
    nan_count = 0
    count = 0
    length_idx = 0
    max_length = max(lengths)
    min_timestamp = timestamp - max_length
    ret = []
    for i in range(max_length + 1):
      if i == lengths[length_idx]:
        if nan_count:
          ret.append([s / float(count),             s, s_max, s_min])
        elif count - nan_count == 0:
          ret.append([float("nan"),                 s, s_max, s_min])
        else:
          ret.append([s / float(count - nan_count), s, s_max, s_min])
        length_idx += 1
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        continue
      if len(data[(idx - i) % self.hist_size()]) > ioidx and not math.isnan(data[(idx - i) % self.hist_size()][ioidx]):
        s += data[(idx - i) % self.hist_size()][ioidx]
        s_max = max(s_max, data[(idx - i) % self.hist_size()][ioidx])
        s_min = min(s_min, data[(idx - i) % self.hist_size()][ioidx])
      else:
        nan_count += 1
      count += 1
    return ret

  def rolling_abs_avg_sum_max_min_multi_k(self, data, idx, timestamp, lengths, count_nan=False):
    s = 0.
    s_max = float("-inf")
    s_min = float("inf")
    count = 0
    nan_count = 0
    max_length = max(lengths)
    min_timestamp = timestamp - max_length
    length_idx = 0
    ret = []
    for i in range(max_length + 1):
      if i == lengths[length_idx]:
        if count_nan:
          ret.append([s / float(count),             s, s_max, s_min])
        elif count - nan_count == 0:
          ret.append([float("nan"),                 s, s_max, s_min])
        else:
          ret.append([s / float(count - nan_count), s, s_max, s_min])
        length_idx += 1
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        continue
      if not math.isnan(data[(idx - i) % self.hist_size()]):
        s += abs(data[(idx - i) % self.hist_size()])
        s_max = max(s, abs(data[(idx - i) % self.hist_size()]))
        s_min = min(s, abs(data[(idx - i) % self.hist_size()]))
      else:
        nan_count += 1
      count += 1
    return ret

  def rolling_sum(self, data, idx, timestamp, length):
    s = 0.
    min_timestamp = timestamp - length
    for i in range(length):
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        break
      if not math.isnan(data[(idx - i) % self.hist_size()]):
        s += data[(idx - i) % self.hist_size()]
    return s

  def rolling_volatility_multi_k(self, data, idx, timestamp, lengths):
    s = 0.
    min_timestamp = timestamp - max(lengths)
    length_idx = 0
    ret = []
    for i in range(max(lengths) + 1):
      if i == lengths[length_idx]:
        ret.append(s)
        length_idx += 1
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        continue
      if not math.isnan(data[(idx - i) % self.hist_size()]):
        s += data[(idx - i) % self.hist_size()] ** 2
    return [math.sqrt(r) for r in ret]

