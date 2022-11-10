import argparse
import logging
import json
from datetime import datetime
import pytz
from functools import partial
import asyncio
import aio_pika

from prometheus_client import push_to_gateway

from senseis.utility import setup_logging
from senseis.configuration import DATETIME_FORMAT, TICKER_TIME_FORMAT1, TICKER_TIME_FORMAT2
from senseis.configuration import STIME_COLNAME
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import get_exchange_pids
from senseis.configuration import is_etl_exchange_name, is_book_exchange_name, is_trade_exchange_name
from senseis.extraction_producer_consumer import get_period
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name, create_live_gauge, create_error_gauge, get_live_gauge, get_error_gauge

#TODO: setup metrics
#TODO: add logging

HIST_SIZE = 960

def build_parser():
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument('--period', type=int, help='periodicity in seconds', default=1)
  parser.add_argument('--b10name', type=str, help='book level1 0 exchange name', default='book_level1_0')
  parser.add_argument('--b11name', type=str, help='book level1 1 exchange name', default='book_level1_1')
  parser.add_argument('--t0name', type=str, help='trade 0 exchange name', default='trade_exchange_0')
  parser.add_argument('--t1name', type=str, help='trade 1 exchange name', default='trade_exchange_1')
  parser.add_argument('--exchange', type=str, help='etl exchange name', required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

def compute_book_inbalance(cbprice, cbsize, caprice, casize, pbprice, pbsize, paprice, pasize):
  if math.isnan(pbprice) or math.isnan(pbsize) or math.isnan(paprice) or math.isnan(pasize):
    return float("nan")
  ge_bid_price = cbprice >= pbprice
  le_bid_price = cbprice <= pbprice
  ge_ask_price = caprice >= paprice
  le_ask_price = caprice <= paprice
  inba = ge_bid_price * cbsize - le_bid_price * pbsize + ge_ask_price * pasize - le_ask_price * casize
  return inba

def compute_weighted_average_price(bprice, bsize, aprice, asize):
  return (bprice * asize + aprice * bsize) / (bsize + asize)

def compute_return(price1, price2):
  if math.isnan(price1):
    return float("nan")
  return math.log(price2 / price1)

def compute_bidask_spread(bid_price, ask_price):
  return (ask_price - bid_price) / ask_price

def compute_volatility(returns):
  #TODO: double check formula
  return math.sqrt(sum([r**2 for r in returns]) / (len(returns) - 1))

def convert_trade_time(time_str):
  try:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT1)
  except ValueError:
    return datetime.strptime(time_str, TICKER_TIME_FORMAT2)

class ETLS1State:
  def __init__(self):
    self.timestamps = [None for _ in range(HIST_SIZE)]
    self.utc = pytz.timezone("UTC")
    self.pids = []
    # book data
    self.bbprice = dict()
    self.bbsize = dict()
    self.baprice = dict()
    self.basize = dict()
    self.bba_inbalance = dict()
    self.waprice = dict()
    self.breturn = dict()
    self.bbaspread = dict()
    # trade data
    self.tnbuys = dict()
    self.tnsells = dict()
    self.tsize = dict()
    self.tvolume = dict()
    self.tavgprice = dict()
    self.nidx = 0

  def set_pids(self, pids):
    self.pids = pids
    for pid in self.pids:
      if pid not in self.bbprice:
        # update book data
        self.bbprice[pid] =       [float("nan") for _ in range(HIST_SIZE)]
        self.bbsize[pid] =        [float("nan") for _ in range(HIST_SIZE)]
        self.baprice[pid] =       [float("nan") for _ in range(HIST_SIZE)]
        self.basize[pid] =        [float("nan") for _ in range(HIST_SIZE)]
        self.bba_inbalance[pid] = [float("nan") for _ in range(HIST_SIZE)]
        self.waprice[pid] =       [float("nan") for _ in range(HIST_SIZE)]
        self.breturn[pid] =       [float("nan") for _ in range(HIST_SIZE)]
        self.bbaspread[pid] =     [float("nan") for _ in range(HIST_SIZE)]
        # update trade data
        self.tnbuys[pid] =        [0 for _ in range(HIST_SIZE)]
        self.tnsells[pid] =       [0 for _ in range(HIST_SIZE)]
        self.tsize[pid] =         [float("nan") for _ in range(HIST_SIZE)]
        self.tvolume[pid] =       [float("nan") for _ in range(HIST_SIZE)]
        self.tavgprice[pid] =     [float("nan") for _ in range(HIST_SIZE)]

  def insert(self, timestamp, pid_book, pid_trade):
    nidx = self.nidx
    pidx = (nidx - 1) % HIST_SIZE
    self.timestamps[nidx] = timestamp
    for pid in self.pids:
      self.bbprice[pid][nidx] = float(pid_book[pid]['bids'][0][0])
      self.bbsize[pid][nidx] = float(pid_book[pid]['bids'][0][1])
      self.baprice[pid][nidx] = float(pid_book[pid]['asks'][0][0])
      self.basize[pid][nidx] = float(pid_book[pid]['asks'][0][1])
      self.bba_inbalance[pid][nidx] = compute_book_inbalance(
        self.bbprice[pid][nidx], self.bbsize[pid][nidx], self.baprice[pid][nidx], self.basize[pid][nidx],
        self.bbprice[pid][pidx], self.bbsize[pid][pidx], self.baprice[pid][pidx], self.basize[pid][pidx]
      )
      self.waprice[pid][nidx] = compute_weighted_average_price(
        self.bbprice[pid][nidx], self.bbsize[pid][nidx], self.baprice[pid][nidx], self.basize[pid][nidx]
      )
      self.breturn[pid][nidx] = compute_return(self.waprice[pid][pidx], self.waprice[pid][nidx])
      self.bbaspread[pid][nidx] = compute_bidask_spread(self.bbprice[pid][nidx], self.baprice[pid][nidx])

      total_size = 0.
      total_volume = 0.
      count_buys = 0
      count_sells = 0
      for t in pid_trade[pid]:
        trade_epoch = convert_trade_time(t['time']).timestamp()
        if trade_epoch >= timestamp - 1:
          total_size += float(t['size'])
          total_volume += float(t['price']) * float(t['size'])
          side = t['side']
          if side == 'buy':
            count_buys += 1
          elif side == 'sell':
            count_sells += 1
      self.tnbuys[pid][nidx] = total_buys
      self.tnsells[pid][nidx] = total_sells
      self.tsize[pid][nidx] = total_size
      self.tvolume[pid][nidx] = total_volume
      if total_size == 0:
        self.tavgprice[pid][nidx] = float("nan")
      else:
        self.tavgprice[pid][nidx] = total_volume / total_size
    self.nidx = (self.nidx + 1) % HIST_SIZE

  def rolling_avg(self, data, idx, length):
    s = 0.
    nan_count = 0
    for i in range(length):
      if not math.isnan(data[(idx - i) % HIST_SIZE]):
        s += data[(idx - i) % HIST_SIZE]
      else:
        nan_count += 1
    if length - nan_count == 0:
      return float("nan")
    return s / float(length - nan_count)

  def rolling_sum(self, data, idx, length):
    s = 0.
    for i in range(length):
      if not math.isnan(data[(idx - i) % HIST_SIZE]):
        s += data[(idx - i) % HIST_SIZE]
    return s

  def rolling_volatility(self, data, idx, length):
    s = 0.
    nan_count = 0
    for i in range(length):
      if not math.isnan(data[(idx - i) % HIST_SIZE]):
        s += data[(idx - i) % HIST_SIZE] ** 2.
      else:
        nan_count += 1
    if length - nan_count == 0:
      return float("nan")
    #TODO: double check formula
    return math.sqrt(s / float(length - nan_count))

  def produce_output(self, timestamp):
    idx = -1
    for i in range(len(self.timestamps)):
      if self.timestamps[i] == timestamp:
        idx = i
        break
    if idx == -1:
      logging.error("Cannot find timestamp {} in state data".format(timestamp))
      return "\"\""
    data = dict()
    for pid in self.pids:
      data[pid + ":best_bid_price"] = self.bbprice[pid][idx]
      data[pid + ":best_ask_price"] = self.baprice[pid][idx]
      data[pid + ":best_bid_size"]  = self.bbsize[pid][idx]
      data[pid + ":best_ask_size"]  = self.basize[pid][idx]
      data[pid + ":ba_inbalance"]   = self.bba_inbalance[pid][idx]
      data[pid + ":weighted_average_price"] = self.wapprice[pid][idx]
      data[pid + ":book_return"]    = self.breturn[pid][idx]
      data[pid + ":ba_spread"]      = self.bbaspread[pid][idx]
      data[pid + ":trade_buys_count"] = self.tnbuys[pid][idx]
      data[pid + ":trade_sells_count"] = self.tnsells[pid][idx]
      data[pid + ":trade_size"]     = self.tsize[pid][idx]
      data[pid + ":trade_volume"]   = self.tvolume[pid][idx]
      data[pid + ":trade_avg_price"] = self.tavgprice[pid][idx]
      #TODO: research other signals to add
      #TODO: add signal on log of bid/ask price over average for given window
      # rolling average 3 seconds
      data[pid + ":best_bid_price_3avg"] = self.rolling_avg(self.bbprice[pid], idx, 3)
      data[pid + ":best_ask_price_3avg"] = self.rolling_avg(self.baprice[pid], idx, 3)
      data[pid + ":best_bid_size_3avg"] = self.rolling_avg(self.bbsize[pid], idx, 3)
      data[pid + ":best_ask_size_3avg"] = self.rolling_avg(self.basize[pid], idx, 3)
      data[pid + ":ba_inbalance_3avg"] = self.rolling_avg(self.bba_inbalance[pid], idx, 3)
      data[pid + ":wap_3avg"] = self.rolling_avg(self.wapprice[pid], idx, 3)
      data[pid + ":book_return_3avg"] = self.rolling_avg(self.breturn[pid], idx, 3)
      data[pid + ":book_return_3sum"] = self.rolling_sum(self.breturn[pid], idx, 3)
      data[pid + ":ba_spread_3avg"] = self.rolling_avg(self.bbaspread[pid], idx, 3)
      data[pid + ":trade_buys_count_3sum"] = self.rolling_sum(self.tnbuys[pid], idx, 3)
      data[pid + ":trade_sells_count_3sum"] = self.rolling_sum(self.tnsells[pid], idx, 3)
      data[pid + ":trade_size_3avg"] = self.rolling_avg(self.tsize[pid], idx, 3)
      data[pid + ":trade_avg_price_3avg"] = self.rolling_avg(self.tavgprice[pid], idx, 3)
      # rolling average 9 seconds
      data[pid + ":best_bid_price_9avg"] = self.rolling_avg(self.bbprice[pid], idx, 9)
      data[pid + ":best_ask_price_9avg"] = self.rolling_avg(self.baprice[pid], idx, 9)
      data[pid + ":best_bid_size_9avg"] = self.rolling_avg(self.bbsize[pid], idx, 9)
      data[pid + ":best_ask_size_9avg"] = self.rolling_avg(self.basize[pid], idx, 9)
      data[pid + ":ba_inbalance_9avg"] = self.rolling_avg(self.bba_inbalance[pid], idx, 9)
      data[pid + ":wap_9avg"] = self.rolling_avg(self.wapprice[pid], idx, 9)
      data[pid + ":book_return_9avg"] = self.rolling_avg(self.breturn[pid], idx, 9)
      data[pid + ":book_return_9sum"] = self.rolling_sum(self.breturn[pid], idx, 9)
      data[pid + ":ba_spread_9avg"] = self.rolling_avg(self.bbaspread[pid], idx, 9)
      data[pid + ":trade_buys_count_9sum"] = self.rolling_sum(self.tnbuys[pid], idx, 9)
      data[pid + ":trade_sells_count_9sum"] = self.rolling_sum(self.tnsells[pid], idx, 9)
      data[pid + ":trade_size_9avg"] = self.rolling_avg(self.tsize[pid], idx, 9)
      data[pid + ":trade_avg_price_9avg"] = self.rolling_avg(self.tavgprice[pid], idx, 9)
      # rolling average 27 seconds
      data[pid + ":best_bid_price_27avg"] = self.rolling_avg(self.bbprice[pid], idx, 27)
      data[pid + ":best_ask_price_27avg"] = self.rolling_avg(self.baprice[pid], idx, 27)
      data[pid + ":best_bid_size_27avg"] = self.rolling_avg(self.bbsize[pid], idx, 27)
      data[pid + ":best_ask_size_27avg"] = self.rolling_avg(self.basize[pid], idx, 27)
      data[pid + ":ba_inbalance_27avg"] = self.rolling_avg(self.bba_inbalance[pid], idx, 27)
      data[pid + ":wap_27avg"] = self.rolling_avg(self.wapprice[pid], idx, 27)
      data[pid + ":book_return_27avg"] = self.rolling_avg(self.breturn[pid], idx, 27)
      data[pid + ":book_return_27sum"] = self.rolling_sum(self.breturn[pid], idx, 27)
      data[pid + ":ba_spread_27avg"] = self.rolling_avg(self.bbaspread[pid], idx, 27)
      data[pid + ":volatility_27s"] = self.rolling_volatility(self.breturn[pid], idx, 27)
      data[pid + ":trade_buys_count_27sum"] = self.rolling_sum(self.tnbuys[pid], idx, 27)
      data[pid + ":trade_sells_count_27sum"] = self.rolling_sum(self.tnsells[pid], idx, 27)
      data[pid + ":trade_size_27avg"] = self.rolling_avg(self.tsize[pid], idx, 27)
      data[pid + ":trade_avg_price_27avg"] = self.rolling_avg(self.tavgprice[pid], idx, 27)
      # rolling average 60 seconds
      data[pid + ":best_bid_price_60avg"] = self.rolling_avg(self.bbprice[pid], idx, 60)
      data[pid + ":best_ask_price_60avg"] = self.rolling_avg(self.baprice[pid], idx, 60)
      data[pid + ":best_bid_size_60avg"] = self.rolling_avg(self.bbsize[pid], idx, 60)
      data[pid + ":best_ask_size_60avg"] = self.rolling_avg(self.basize[pid], idx, 60)
      data[pid + ":ba_inbalance_60avg"] = self.rolling_avg(self.bba_inbalance[pid], idx, 60)
      data[pid + ":wap_60avg"] = self.rolling_avg(self.wapprice[pid], idx, 60)
      data[pid + ":book_return_60avg"] = self.rolling_avg(self.breturn[pid], idx, 60)
      data[pid + ":book_return_60sum"] = self.rolling_sum(self.breturn[pid], idx, 60)
      data[pid + ":volatility_60s"] = self.rolling_volatility(self.breturn[pid], idx, 60)
      data[pid + ":ba_spread_60avg"] = self.rolling_avg(self.bbaspread[pid], idx, 60)
      data[pid + ":trade_buys_count_60sum"] = self.rolling_sum(self.tnbuys[pid], idx, 60)
      data[pid + ":trade_sells_count_60sum"] = self.rolling_sum(self.tnsells[pid], idx, 60)
      data[pid + ":trade_size_60avg"] = self.rolling_avg(self.tsize[pid], idx, 60)
      data[pid + ":trade_avg_price_60avg"] = self.rolling_avg(self.tavgprice[pid], idx, 60)
      # rolling average 120 seconds
      #TODO
      # rolling average 240 seconds
      #TODO
      # rolling average 480 seconds
      #TODO
      # rolling average 960 seconds
      #TODO
    data[STIME_COLNAME] = self.utc.localize(datetime.utcfromtimestamp(timestamp)).strftime(DATETIME_FORMAT)
    return data

def etl_s1(period, data, state):
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
  state.insert(period, book_data, trade_data)
  output = state.produce_output(period)
  message = json.dumps(output)
  return message.encode()

def is_all_found(exchange_names, data):
  all_found = True
  for name in exchange_names:
      if name not in data:
        all_found = False
        break
  return all_found

async def etl_processor(etl_f, output_exchange_name, input_exchange_names, periodicity, que):
  utc = pytz.timezone("UTC")
  etl_state = ETLS1State()
  records = dict()
  pids = set()
  for input_exchange_name in input_exchange_names:
    pids.update(get_exchange_pids(input_exchange_name))
  etl_state.set_pids(list(pids))
  mq_connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  async with mq_connection:
    channel = await mq_connection.channel()
    exchange = await channel.declare_exchange(name=output_exchange_name, type='fanout')
    while True:
      ie_name, msg = await que.get()
      dat = json.loads(msg)
      #TODO: this is a bad idea, period other than 1 will not represent time, but it is treated like time in seconds already
      dat_period = get_period(int(datetime.strptime(dat[STIME_COLNAME], DATETIME_FORMAT).timestamp()), periodicity)
      if dat_period in records:
        records[dat_period][ie_name] = msg
      else:
        records[dat_period] = {ie_name : msg}
      for period in sorted(records.keys()):
        period_str = utc.localize(datetime.utcfromtimestamp(period)).strftime(DATETIME_FORMAT)
        if period < dat_period - HIST_SIZE:
          records.pop(period, None)
        elif not is_all_found(input_exchange_names, records[period]) or period > dat_period:
          break
        else:
          output = etl_f(period, records[period], etl_state)
          msg = aio_pika.Message(body=output)
          logging.info("Sending {}".format(period_str))
          await exchange_publish(message=msg, routing_key='')
          records.pop(period, None)
      que.task_done()

async def push_incoming_to_queue(que, exchange_name, msg: aio_pika.IncomingMessage):
  async with msg.process():
    logging.debug("Received from {}".format(exchange_name))
    await que.put((exchange_name, msg.body))

async def data_subscriber(exchange_name, que):
  connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  handler = partial(push_incoming_to_queue, que, exchange_name)
  async with connection:
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1) #TODO: needed ?
    exchange = await channel.declare_exchange(name=exchange_name, type='fanout')
    queue = await channel.declare_queue('', auto_delete=True)
    await queue.bind(exchange=exchange)
    await queue.consume(handler)
    await asyncio.Future()

async def etl_consumer_producer(output_exchange_name, input_exchange_names, periodicity):
  tasks = []
  while True:
    try:
      que = asyncio.Queue()
      for input_exchange in input_exchange_names:
          tasks.append(asyncio.create_task(data_subscriber(input_exchange, que)))
      tasks.append(asyncio.create_task(etl_processor(etl_s1, output_exchange_name, input_exchange_names, periodicity, que)))
      await asyncio.gather(*tasks, return_exceptions=False)
      await que.join()
    except asyncio.CancelledError as err:
      logging.info("Cancelled Error: {}".format(err))
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_etl_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  name = 'cbp_etl_s1_pipe'
  setup_gateway(name)
  create_live_gauge(name)
  create_error_gauge(name)
  try:
    asyncio.run(
      etl_consumer_producer(
        args.exchange,
        [args.b10name, args.b11name, args.t0name, args.t1name],
        args.period,
    ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
