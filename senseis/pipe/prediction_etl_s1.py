import argparse
import logging
import json
import time
from datetime import datetime
import pytz
import math
import asyncio

from prometheus_client import push_to_gateway

from senseis.utility import setup_logging
from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import STIME_COLNAME
from senseis.configuration import is_etl_exchange_name
from senseis.calculation import compute_book_imbalance, compute_weighted_average_price, compute_return, compute_bidask_spread
from senseis.extraction_producer_consumer import convert_trade_time
from senseis.pipe_consumer_producer import etl_consumer_producer, data_subscriber, etl_processor, process_etl_data
from senseis.pipe_consumer_producer import ETLState

from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name, setup_basic_gauges
from senseis.metric_utility import get_live_gauge, get_error_gauge
from senseis.metric_utility import create_etl_process_time_gauge, get_etl_process_time_gauge

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

class ETLS1State(ETLState):
  def __init__(self):
    self.timestamps = [None for _ in range(HIST_SIZE)]
    self.utc = pytz.timezone("UTC")
    self.pids = []
    # book data
    self.bbprice = dict()
    self.bbsize = dict()
    self.baprice = dict()
    self.basize = dict()
    self.bba_imbalance = dict()
    self.waprice = dict()
    self.breturn = dict()
    self.bbaspread = dict()
    self.bmreturn27 = [None for _ in range(HIST_SIZE)]
    # trade data
    self.tnbuys = dict()
    self.tnsells = dict()
    self.tsize = dict()
    self.tvolume = dict()
    self.tavgprice = dict()
    self.treturn = dict()
    self.nidx = 0

  def hist_size(self):
    return HIST_SIZE

  def set_pids(self, pids):
    self.pids = pids
    for pid in self.pids:
      if pid not in self.bbprice:
        # update book data
        self.bbprice[pid] =       [float("nan") for _ in range(self.hist_size())]
        self.bbsize[pid] =        [float("nan") for _ in range(self.hist_size())]
        self.baprice[pid] =       [float("nan") for _ in range(self.hist_size())]
        self.basize[pid] =        [float("nan") for _ in range(self.hist_size())]
        self.bba_imbalance[pid] = [float("nan") for _ in range(self.hist_size())]
        self.waprice[pid] =       [float("nan") for _ in range(self.hist_size())]
        self.breturn[pid] =       [float("nan") for _ in range(self.hist_size())]
        self.bbaspread[pid] =     [float("nan") for _ in range(self.hist_size())]
        # update trade data
        self.tnbuys[pid] =        [0 for _ in range(self.hist_size())]
        self.tnsells[pid] =       [0 for _ in range(self.hist_size())]
        self.tsize[pid] =         [float("nan") for _ in range(self.hist_size())]
        self.tvolume[pid] =       [float("nan") for _ in range(self.hist_size())]
        self.tavgprice[pid] =     [float("nan") for _ in range(self.hist_size())]
        self.treturn[pid] =       [float("nan") for _ in range(self.hist_size())]

  def insert(self, timestamp, pid_book, pid_trade):
    nidx = self.nidx
    pidx = (nidx - 1) % self.hist_size()
    self.timestamps[nidx] = timestamp
    for pid in self.pids:
      book_data = json.loads(pid_book[pid])
      if 'bids' not in book_data:
        logging.info("{} book data for {} missing bids".format(pid, timestamp))
        self.bbprice[pid][nidx] = float("nan")
        self.bbsize[pid][nidx] = float("nan")
      else:
        try:
          self.bbprice[pid][nidx] = float(book_data['bids'][0][0])
        except ValueError:
          logging.error("cannot parse bid price: {}".format(book_data['bids'][0][0]))
          get_error_gauge().inc()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          self.bbprice[pid][nidx] = float("nan")
        try:
          self.bbsize[pid][nidx] = float(book_data['bids'][0][1])
        except ValueError:
          logging.error("cannot parse bid size: {}".format(book_data['bids'][0][1]))
          get_error_gauge().inc()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          self.bbsize[pid][nidx] = float("nan")
      if 'asks' not in book_data:
        logging.info("{} book data for {} missing asks".format(pid, timestamp))
        self.baprice[pid][nidx] = float("nan")
        self.basize[pid][nidx] = float("nan")
      else:
        try:
          self.baprice[pid][nidx] = float(book_data['asks'][0][0])
        except ValueError:
          logging.error("cannot parse ask price: {}".format(book_data['asks'][0][0]))
          get_error_gauge().inc()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          self.baprice[pid][nidx] = float("nan")
        try:
          self.basize[pid][nidx] = float(book_data['asks'][0][1])
        except ValueError:
          logging.error("cannot parse ask size: {}".format(book_data['asks'][0][1]))
          get_error_gauge().inc()
          push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          self.basize[pid][nidx] = float("nan")
      self.bba_imbalance[pid][nidx] = compute_book_imbalance(
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
      trade_data = json.loads(pid_trade[pid])
      #TODO: if empty, is it going to be a [] ? or a ""
      for t in trade_data:
        trade_epoch = convert_trade_time(t['time']).timestamp()
        if trade_epoch >= timestamp - 1:
          try:
            total_size += float(t['size'])
          except ValueError:
            logging.error("cannot parse trade size: {}".format(t['size']))
            get_error_gauge().inc()
            push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          try:
            total_volume += float(t['price']) * float(t['size'])
          except ValueError:
            logging.error("cannot parse trade price or size: {} {}".format(t['price'], t['size']))
            get_error_gauge().inc()
            push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
          side = t['side']
          if side == 'buy':
            count_buys += 1
          elif side == 'sell':
            count_sells += 1
      self.tnbuys[pid][nidx] = count_buys
      self.tnsells[pid][nidx] = count_sells
      self.tsize[pid][nidx] = total_size
      self.tvolume[pid][nidx] = total_volume
      if total_size == 0:
        self.tavgprice[pid][nidx] = float("nan")
        self.treturn[pid][nidx] = float("nan")
      else:
        self.tavgprice[pid][nidx] = total_volume / total_size
        self.treturn[pid][nidx] = float("nan")
        idx = pidx
        while idx != nidx:
          if self.timestamps[idx] is None or self.timestamps[idx] > self.timestamps[nidx]:
            break
          if not math.isnan(self.tavgprice[pid][idx]):
            self.treturn[pid][nidx] = compute_return(self.tavgprice[pid][idx], self.tavgprice[pid][nidx])
            break
          idx = (idx - 1) % self.hist_size()
    self.bmreturn27[nidx] = self.rolling_mean_return(nidx, timestamp, 27)
    self.nidx = (self.nidx + 1) % self.hist_size()

  def rolling_mean_return(self, idx, timestamp, length):
    rs = [self.rolling_sum(self.breturn[pid], idx, timestamp, length) for pid in self.pids]
    s = sum(rs)
    return s / float(len(self.pids))

  def rolling_beta(self, prdata, idx, timestamp, length):
    m2sum = 0.
    mrsum = 0.
    count = 0
    nan_count = 0
    min_timestamp = timestamp - length
    for i in range(length):
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        return float("nan")
      if not math.isnan(self.bmreturn27[(idx - i) % self.hist_size()]) and not math.isnan(prdata[(idx - i) % self.hist_size()]):
        m2sum += self.bmreturn27[(idx - i) % self.hist_size()] ** 2
        mrsum += self.bmreturn27[(idx - i) % self.hist_size()] * prdata[(idx - i) % self.hist_size()]
      else:
        nan_count += 1
      count += 1
    if count - nan_count == 0:
      return float("nan")
    return (mrsum / float(count - nan_count)) / (m2sum / float(count - nan_count))

  def produce_output_rolling_k(self, data, pid, idx, timestamp, k):
    data[pid + ":best_bid_price_{}avg".format(k)] = self.rolling_avg(self.bbprice[pid], idx, timestamp, k)
    data[pid + ":best_bid_price_{}max".format(k)] = self.rolling_max(self.bbprice[pid], idx, timestamp, k)
    data[pid + ":best_bid_price_{}min".format(k)] = self.rolling_min(self.bbprice[pid], idx, timestamp, k)
    data[pid + ":best_ask_price_{}avg".format(k)] = self.rolling_avg(self.baprice[pid], idx, timestamp, k)
    data[pid + ":best_ask_price_{}max".format(k)] = self.rolling_max(self.baprice[pid], idx, timestamp, k)
    data[pid + ":best_ask_price_{}min".format(k)] = self.rolling_min(self.baprice[pid], idx, timestamp, k)
    data[pid + ":best_bid_size_{}avg".format(k)]  = self.rolling_avg(self.bbsize[pid], idx, timestamp, k)
    data[pid + ":best_bid_size_{}max".format(k)]  = self.rolling_max(self.bbsize[pid], idx, timestamp, k)
    data[pid + ":best_bid_size_{}min".format(k)]  = self.rolling_min(self.bbsize[pid], idx, timestamp, k)
    data[pid + ":best_ask_size_{}avg".format(k)]  = self.rolling_avg(self.basize[pid], idx, timestamp, k)
    data[pid + ":best_ask_size_{}max".format(k)]  = self.rolling_max(self.basize[pid], idx, timestamp, k)
    data[pid + ":best_ask_size_{}min".format(k)]  = self.rolling_min(self.basize[pid], idx, timestamp, k)
    data[pid + ":ba_imbalance_{}avg".format(k)]   = self.rolling_avg(self.bba_imbalance[pid], idx, timestamp, k)
    data[pid + ":ba_imbalance_{}max".format(k)]   = self.rolling_max(self.bba_imbalance[pid], idx, timestamp, k)
    data[pid + ":ba_imbalance_{}min".format(k)]   = self.rolling_min(self.bba_imbalance[pid], idx, timestamp, k)
    data[pid + ":wap_{}avg".format(k)]            = self.rolling_avg(self.waprice[pid], idx, timestamp, k)
    data[pid + ":wap_{}max".format(k)]            = self.rolling_max(self.waprice[pid], idx, timestamp, k)
    data[pid + ":wap_{}min".format(k)]            = self.rolling_min(self.waprice[pid], idx, timestamp, k)
    data[pid + ":book_return_{}avg".format(k)]    = self.rolling_avg(self.breturn[pid], idx, timestamp, k)
    data[pid + ":book_return_{}max".format(k)]    = self.rolling_max(self.breturn[pid], idx, timestamp, k)
    data[pid + ":book_return_{}min".format(k)]    = self.rolling_min(self.breturn[pid], idx, timestamp, k)
    data[pid + ":book_return_{}sum".format(k)]    = self.rolling_sum(self.breturn[pid], idx, timestamp, k)
    data[pid + ":ba_spread_{}avg".format(k)]      = self.rolling_avg(self.bbaspread[pid], idx, timestamp, k)
    data[pid + ":ba_spread_{}max".format(k)]      = self.rolling_max(self.bbaspread[pid], idx, timestamp, k)
    data[pid + ":ba_spread_{}min".format(k)]      = self.rolling_min(self.bbaspread[pid], idx, timestamp, k)

    data[pid + ":trade_buys_count_{}sum".format(k)] = self.rolling_sum(self.tnbuys[pid], idx, timestamp, k)
    data[pid + ":trade_buys_count_{}avg".format(k)] = self.rolling_avg(self.tnbuys[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_buys_count_{}max".format(k)] = self.rolling_max(self.tnbuys[pid], idx, timestamp, k)
    data[pid + ":trade_sells_count_{}sum".format(k)] = self.rolling_sum(self.tnsells[pid], idx, timestamp, k)
    data[pid + ":trade_sells_count_{}avg".format(k)] = self.rolling_avg(self.tnsells[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_sells_count_{}max".format(k)] = self.rolling_max(self.tnsells[pid], idx, timestamp, k)
    data[pid + ":trade_size_{}sum".format(k)] = self.rolling_sum(self.tsize[pid], idx, timestamp, k)
    data[pid + ":trade_size_{}avg".format(k)] = self.rolling_avg(self.tsize[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_size_{}max".format(k)] = self.rolling_max(self.tsize[pid], idx, timestamp, k)
    data[pid + ":trade_volume_{}sum".format(k)] = self.rolling_sum(self.tvolume[pid], idx, timestamp, k)
    data[pid + ":trade_volume_{}avg".format(k)] = self.rolling_avg(self.tvolume[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_volume_{}max".format(k)] = self.rolling_max(self.tvolume[pid], idx, timestamp, k)
    data[pid + ":trade_avg_price_{}avg".format(k)] = self.rolling_avg(self.tavgprice[pid], idx, timestamp, k)
    data[pid + ":trade_avg_price_{}max".format(k)] = self.rolling_max(self.tavgprice[pid], idx, timestamp, k)
    data[pid + ":trade_avg_price_{}min".format(k)] = self.rolling_min(self.tavgprice[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}avg".format(k)] = self.rolling_avg(self.treturn[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}sum".format(k)] = self.rolling_sum(self.treturn[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}max".format(k)] = self.rolling_max(self.treturn[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}min".format(k)] = self.rolling_min(self.treturn[pid], idx, timestamp, k)

  def produce_output(self, timestamp):
    perf_start_time = time.perf_counter()
    idx = -1
    for i in range(len(self.timestamps)):
      if self.timestamps[i] == timestamp:
        idx = i
        break
    data = dict()
    data[STIME_COLNAME] = self.utc.localize(datetime.utcfromtimestamp(timestamp)).strftime(DATETIME_FORMAT)
    if idx == -1:
      logging.error("Unexpected Error: Cannot find timestamp {} in state data".format(timestamp))
      get_error_gauge().inc()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
      return data
    for pid in self.pids:
      data[pid + ":best_bid_price"] = self.bbprice[pid][idx]
      data[pid + ":best_ask_price"] = self.baprice[pid][idx]
      data[pid + ":best_bid_size"]  = self.bbsize[pid][idx]
      data[pid + ":best_ask_size"]  = self.basize[pid][idx]
      data[pid + ":ba_imbalance"]   = self.bba_imbalance[pid][idx]
      data[pid + ":wap"]            = self.waprice[pid][idx]
      data[pid + ":book_return"]    = self.breturn[pid][idx]
      data[pid + ":ba_spread"]      = self.bbaspread[pid][idx]

      data[pid + ":trade_buys_count"] = self.tnbuys[pid][idx]
      data[pid + ":trade_sells_count"] = self.tnsells[pid][idx]
      data[pid + ":trade_size"]     = self.tsize[pid][idx]
      data[pid + ":trade_volume"]   = self.tvolume[pid][idx]
      data[pid + ":trade_avg_price"] = self.tavgprice[pid][idx]
      data[pid + ":trade_return"]   = self.treturn[pid][idx]

      self.produce_output_rolling_k(data, pid, idx, timestamp, 3)
      self.produce_output_rolling_k(data, pid, idx, timestamp, 9)
      self.produce_output_rolling_k(data, pid, idx, timestamp, 27)
      self.produce_output_rolling_k(data, pid, idx, timestamp, 81)
      self.produce_output_rolling_k(data, pid, idx, timestamp, 162)
      self.produce_output_rolling_k(data, pid, idx, timestamp, 324)
      self.produce_output_rolling_k(data, pid, idx, timestamp, 648)
      self.produce_output_rolling_k(data, pid, idx, timestamp, 960)

      data[pid + ":book_volatility_27"] = self.rolling_volatility(self.breturn[pid], idx, timestamp, 27)
      data[pid + ":book_volatility_81"] = self.rolling_volatility(self.breturn[pid], idx, timestamp, 81)
      data[pid + ":book_volatility_162"] = self.rolling_volatility(self.breturn[pid], idx, timestamp, 162)
      data[pid + ":book_volatility_324"] = self.rolling_volatility(self.breturn[pid], idx, timestamp, 324)
      data[pid + ":book_volatility_648"] = self.rolling_volatility(self.breturn[pid], idx, timestamp, 648)
      data[pid + ":book_volatility_960"] = self.rolling_volatility(self.breturn[pid], idx, timestamp, 960)

      data[pid + ":book_beta_648"] = self.rolling_beta(self.breturn[pid], idx, timestamp, 648)

      data[pid + ":trade_volatility_27"] = self.rolling_volatility(self.treturn[pid], idx, timestamp, 27)
      data[pid + ":trade_volatility_81"] = self.rolling_volatility(self.treturn[pid], idx, timestamp, 81)
      data[pid + ":trade_volatility_162"] = self.rolling_volatility(self.treturn[pid], idx, timestamp, 162)
      data[pid + ":trade_volatility_324"] = self.rolling_volatility(self.treturn[pid], idx, timestamp, 324)
      data[pid + ":trade_volatility_648"] = self.rolling_volatility(self.treturn[pid], idx, timestamp, 648)
      data[pid + ":trade_volatility_960"] = self.rolling_volatility(self.treturn[pid], idx, timestamp, 960)

    data["book_mean_return_27"] = self.bmreturn27[idx]
    perf_time_taken = time.perf_counter() - perf_start_time
    get_etl_process_time_gauge().set(perf_time_taken)
    push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
    return data

def create_state():
  return ETLS1State()

def get_history_size():
  return HIST_SIZE

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_etl_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  app_name = 'cbp_etl_s1_pipe'
  setup_gateway(app_name)
  setup_basic_gauges(app_name)
  create_etl_process_time_gauge(app_name)
  try:
    asyncio.run(
      etl_consumer_producer(
        data_subscriber,
        etl_processor,
        process_etl_data,
        create_state,
        get_history_size,
        args.exchange,
        [args.b10name, args.b11name, args.t0name, args.t1name],
        args.period,
    ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
