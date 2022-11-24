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
from senseis.calculation import compute_book_first_tick, compute_book_avg_tick
from senseis.calculation import compute_bid_size_change, compute_ask_size_change
from senseis.calculation import compute_bid_volume_change, compute_ask_volume_change
from senseis.calculation import compute_book_level_line
from senseis.extraction_producer_consumer import convert_trade_time
from senseis.extraction_producer_consumer import create_interval_state
from senseis.pipe_consumer_producer import etl_consumer_producer, data_subscriber, etl_processor, process_etl_data
from senseis.pipe_consumer_producer import ETLState

from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name, setup_basic_gauges
from senseis.metric_utility import get_live_gauge
from senseis.metric_utility import create_etl_process_time_histogram, get_etl_process_time_histogram

def build_parser():
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument('--period', type=int, help='periodicity in seconds', default=1)
  parser.add_argument('--b20name', type=str, help='book level2 0 exchange name', default='book_level2_0')
  parser.add_argument('--b11name', type=str, help='book level2 1 exchange name', default='book_level2_1')
  parser.add_argument('--t0name', type=str, help='trade 0 exchange name', default='trade_exchange_0')
  parser.add_argument('--t1name', type=str, help='trade 1 exchange name', default='trade_exchange_1')
  parser.add_argument('--exchange', type=str, help='etl exchange name', required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

HIST_SIZE = 960

class ETLS2State(ETLState):
  def __init__(self):
    self.timestamps = [None for _ in range(HIST_SIZE)]
    self.utc = pytz.timezone("UTC")
    self.pids = []
    # book data
    self.bbidprices = dict()
    self.baskprices = dict()
    self.bbidsizes = dict()
    self.basksizes = dict()
    self.bbidstacksize = dict()
    self.baskstacksize = dict()

    self.bbidtick1 = dict()
    self.basktick1 = dict()
    self.bbidavgtick = dict()
    self.baskavgtick = dict()
    self.bbidsizechange = dict()
    self.basksizechange = dict()
    self.bbidvolchange = dict()
    self.baskvolchange = dict()
    self.bbidlevelslope = dict()
    self.bbidlevelintercept = dict()
    self.basklevelslope = dict()
    self.basklevelintercept = dict()

    self.bba_imbalance = dict()
    self.wapprice = dict()
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
      if pid not in self.bbidprices:
        # update book data
        self.bbidprices[pid]    = [[] for _ in range(self.hist_size())]
        self.baskprices[pid]    = [[] for _ in range(self.hist_size())]
        self.bbidsizes[pid]     = [[] for _ in range(self.hist_size())]
        self.basksizes[pid]     = [[] for _ in range(self.hist_size())]
        self.bbidstacksize[pid] = [float("nan") for _ in range(self.hist_size())]
        self.baskstacksize[pid] = [float("nan") for _ in range(self.hist_size())]

        self.bbidtick1[pid]     = [float("nan") for _ in range(self.hist_size())]
        self.basktick1[pid]     = [float("nan") for _ in range(self.hist_size())]
        self.bbidavgtick[pid]   = [float("nan") for _ in range(self.hist_size())]
        self.baskavgtick[pid]   = [float("nan") for _ in range(self.hist_size())]
        self.bbidsizechange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.basksizechange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbidvolchange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.baskvolchange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbidlevelslope[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbidlevelintercept[pid] = [float("nan") for _ in range(self.hist_size())]
        self.basklevelslope[pid] = [float("nan") for _ in range(self.hist_size())]
        self.basklevelintercept[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bba_imbalance[pid] = [float("nan") for _ in range(self.hist_size())]
        self.wapprice[pid]      = [float("nan") for _ in range(self.hist_size())]
        self.breturn[pid]       = [float("nan") for _ in range(self.hist_size())]
        self.bbaspread[pid]     = [float("nan") for _ in range(self.hist_size())]
        # update trade data
        self.tnbuys[pid]        = [0 for _ in range(self.hist_size())]
        self.tnsells[pid]       = [0 for _ in range(self.hist_size())]
        self.tsize[pid]         = [float("nan") for _ in range(self.hist_size())]
        self.tvolume[pid]       = [float("nan") for _ in range(self.hist_size())]
        self.tavgprice[pid]     = [float("nan") for _ in range(self.hist_size())]
        self.treturn[pid]       = [float("nan") for _ in range(self.hist_size())]

  def insert(self, timestamp, pid_book, pid_trade):
    nidx = self.nidx
    pix = (nidx - 1) % self.hist_size()
    self.timestamps[nidx] = timestamp
    for pid in self.pids:
      # book data insertion
      book_data = json.loads(pid_book[pid])
      bid_prices = []
      bid_sizes = []
      if 'bids' not in book_data:
        logging.info("{} book data for {} missing bids".format(pid, timestamp))
      else:
        for i in range(1, len(book_data['bids'])):
          try:
            bid_prices.append(float(book_data['bids'][i][0]))
          except ValueError:
            bid_prices.append(float("nan"))
            logging.error("cannot parse bid price: {}".format(book_data['bids'][i][0]))
          try:
            bid_sizes.append(float(book_data['bids'][i][1]))
          except ValueError:
            bid_sizes.append(float("nan"))
            logging.error("cannot parse bid size: {}".format(book_data['bids'][i][1]))
      self.bbidprices[pid][nidx] = bid_prices
      self.bbidsizes[pid][nidx] = bid_sizes
      ask_prices = []
      ask_sizes = []
      if 'asks' not in book_data:
        logging.info("{} book data for {} missing asks".format(pid, timestamp))
      else:
        for i in range(1, len(book_data['asks'])):
          try:
            ask_prices.append(float(book_data['asks'][i][0]))
          except ValueError:
            ask_prices.append(float("nan"))
            logging.error("cannot parse ask price: {}".format(book_data['asks'][i][0]))
          try:
            ask_sizes.append(float(book_data['asks'][i][1]))
          except ValueError:
            ask_sizes.append(float("nan"))
            logging.error("cannot parse ask size: {}".format(book_data['asks'][i][1]))
      self.baskprices[pid][nidx] = ask_prices
      self.basksizes[pid][nidx] = ask_sizes
      if 'bids' not in book_data:
        self.bbidsstacksize[pid][nidx] = 0
      else:
        try:
          self.bbidstacksize[pid][nidx] = int(book_data['bids'][0])
        except ValueError:
          logging.error("cannot convert bid stack size value")
          self.bbidstacksize[pid][nidx] = 0
      if 'asks' in book_data:
        self.baskstacksize[pid][nidx] = 0
      else:
        try:
          self.baskstacksize[pid][nidx] = int(book_data['asks'][0])
        except ValueError:
          logging.error("cannot convert ask stack size value")
          self.baskstacksize[pid][nidx] = 0

      self.bbidtick1[pid][nidx] = compute_book_first_tick(self.bbidprices[pid][nidx])
      self.basktick1[pid][nidx] = compute_book_first_tick(self.baskprices[pid][nidx])
      self.bbidavgtick[pid][nidx] = compute_book_avg_tick(self.bbidprices[pid][nidx])
      self.baskavgtick[pid][nidx] = compute_book_avg_tick(self.baskprices[pid][nidx])
      self.bbidsizechange[pid][nidx] = compute_bid_size_change(
          self.bbidprices[pid][nidx], self.bbidsizes[pid][nidx], self.bbidprices[pid][pidx], self.bbidsizes[pid][pidx]
      )
      self.basksizechange[pid][nidx] = compute_ask_size_change(
          self.baskprices[pid][nidx], self.basksizes[pid][nidx], self.baskprices[pid][pidx], self.basksizes[pid][pidx]
      )
      self.bbidvolchange[pid][nidx] = compute_bid_volume_change(
          self.bbidprices[pid][nidx], self.bbidsizes[pid][nidx], self.bbidprices[pid][pidx], self.bbidsizes[pid][pidx]
      )
      self.baskvolchange[pid][nidx] = compute_ask_volume_change(
          self.baskprices[pid][nidx], self.basksizes[pid][nidx], self.baskprices[pid][pidx], self.basksizes[pid][pidx]
      )
      (bid_slope, bid_intercept) = compute_book_level_line(self.bbidprices[pid][nidx], self.bbidsizes[pid][nidx])
      (ask_slope, ask_intercept) = compute_book_level_line(self.baskprices[pid][nidx], self.basksizes[pid][nidx])
      self.bbidlevelslope[pid][nidx] = bid_slope
      self.bbidlevelintercept[pid][nidx] = bid_intercept
      self.basklevelslope[pid][nidx] = ask_slope
      self.basklevelintercept[pid][nidx] = ask_intercept

      if len(self.bbidprices[pid][nidx]) == 0 or len(self.baskprices[pid][nidx]) == 0 or len(self.bbidsizes[pid][nidx]) == 0 or len(self.basksizes[pid][nidx]) == 0 or \
         len(self.bbidprices[pid][pidx]) == 0 or len(self.baskprices[pid][pidx]) == 0 or len(self.bbidsizes[pid][pidx]) == 0 or len(self.basksizes[pid][pidx]) == 0:
        self.bba_imbalance[pid][nidx] = float("nan")
      else:
        self.bba_imbalance[pid][nidx] = compute_book_imbalance(
          self.bbidprices[pid][nidx][0], self.bbidsizes[pid][nidx][0], self.baskprices[pid][nidx][0], self.basksizes[pid][nidx][0],
          self.bbidprices[pid][pidx][0], self.bbidsizes[pid][pidx][0], self.baskprices[pid][pidx][0], self.basksizes[pid][pidx][0],
        )
      if len(self.bbidprices[pid][nidx]) == 0 or len(self.baskprices[pid][nidx]) == 0 or len(self.bbidsizes[pid][nidx]) == 0 or len(self.basksizes[pid][nidx]) == 0:
        self.wapprice[pid][nidx] = float("nan")
      else:
        self.wapprice[pid][nidx] = compute_weighted_average_price(
          self.bbidprices[pid][nidx][0], self.bbidsizes[pid][nidx][0], self.baskprices[pid][nidx][0], self.basksizes[pid][nidx][0],
        )
      self.breturn[pid][nidx] = compute_return(self.wapprice[pid][pidx], self.wrapprice[pid][nidx])
      if len(self.bbidprices[pid][nidx]) == 0 or len(self.baskprices[pid][nidx]) == 0:
        self.bbaspread[pid][nidx] = float("nan")
      else:
        self.bbaspread[pid][nidx] = compute_bidask_spread(self.bbidprices[pid][nidx][0], self.baskprices[pid][nidx][0])

      # trade data insertion
      total_size = 0.
      total_volume = 0.
      count_buys = 0
      count_sells = 0
      trade_data = json.loads(pid_trade[pid])
      for t in trade_data:
        trade_epoch = convert_trade_time(t['time']).timestamp()
        if trade_epoch >= timestamp - 1:
          try:
            total_size += float(t['size'])
          except ValueError:
            logging.error("cannot parse trade size: {}".format(t['size']))
          try:
            total_volume += float(t['price']) * float(t['size'])
          except ValueError:
            logging.error("cannot parse trade price or trade size: {} {}".format(t['price'], t['size']))
          side = t['side']
          if side == 'buy':
            count_buys += 1
          if side == 'sell':
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
          if math.isnan(self.timestamsp[idx]) or self.timestamps[idx] > self.timestamps[nidx]:
            break
          if not math.isnan(self.tavgprice[pid][idx]):
            self.treturn[pid][nidx] = compute_return(self.tavgprice[pid][idx], self.tavgprice[pid][nidx])
            break
          idx = (idx - 1) % self.hist_size()
    self.bmreturn27[nidx] = self.rolling_mean_return(nidx, timestamp, 27)
    self.nidx = (self.nidx + 1) % self.hist_size()

  def rolling_avg_aoa(self, data, idx, ioidx, timestamp, k, count_nan=False):
    s = 0.
    nan_count = 0
    count = 0
    min_timestamp = timestamp - length
    for i in range(length):
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        break
      if len(data[(idx - i) % self.hist_size()]) > ioidx and not math.isnan(data[(idx - i) % self.hist_size()][ioidx]):
        s += data[(idx - i) % self.hist_size()][ioidx]
      else:
        nan_count += 1
      count += 1
    if count - nan_count == 0:
      return float("nan")
    if count_nan:
      return s / float(count)
    else:
      return s / float(count - nan_count)

  def rolling_abs_avg(self, data, idx, timestamp, k, count_nan=False):
    s = 0.
    nan_count = 0
    count = 0
    min_timestamp = timestamp - length
    for i in range(length):
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        break
      if not math.isnan(data[(idx - i) % self.hist_size()]):
        s += abs(data[(idx - i) % self.hist_size()])
      else:
        nan_count += 1
      count += 1
    if count - nan_count == 0:
      return float("nan")
    if count_nan:
      return s / float(count)
    else:
      return s / float(count - nan_count)

  def rolling_max_aoa(self, data, idx, ioidx, timestamp, k):
    s = float("nan")
    min_timestamp = timestamp - length
    for i in range(length):
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamsp[(idx - i) % self.hist_size()] <= min_timestamp:
        break
      if len(data[(idx - i) % self.hist_size()]) > ioidx:
        s = max(s, data[(idx - i) % self.hist_size()][ioidx])
    return s

  def rolling_min_aoa(self, data, idx, ioidx, timestamp, k):
    s = float("nan")
    min_timestamp = timestamp - length
    for i in range(length):
      if self.timestamps[(idx - i) % self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.hist_size()] <= min_timestamp:
        break
      if len(data[(idx - i) % self.hist_size()]) > ioidx:
        s = min(s, data[(idx - i) % self.hist_size()][ioidx])
    return s

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
      if self.timestamps[(idx - i) % self.self.hist_size()] is None or \
         self.timestamps[(idx - i) % self.self.hist_size()] > timestamp or \
         self.timestamps[(idx - i) % self.self.hist_size()] <= min_timestamp:
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
    data[pid + ":best_bid_price_{}avg".format(k)]    = self.rolling_avg_aoa(self.bbidprices[pid], idx, 0, timestamp, k)
    data[pid + ":best_bid_price_{}max".format(k)]    = self.rolling_max_aoa(self.bbidprices[pid], idx, 0, timestamp, k)
    data[pid + ":best_bid_price_{}min".format(k)]    = self.rolling_min_aoa(self.bbidprices[pid], idx, 0, timestamp, k)
    data[pid + ":best_ask_price_{}avg".format(k)]    = self.rolling_avg_aoa(self.baskprices[pid], idx, 0, timestamp, k)
    data[pid + ":best_ask_price_{}max".format(k)]    = self.rolling_max_aoa(self.baskprices[pid], idx, 0, timestamp, k)
    data[pid + ":best_ask_price_{}min".format(k)]    = self.rolling_min_aoa(self.baskprices[pid], idx, 0, timestamp, k)
    data[pid + ":best_bid_size_{}avg".format(k)]     = self.rolling_avg_aoa(self.bbidsizes[pid], idx, 0, timestamp, k)
    data[pid + ":best_bid_size_{}max".format(k)]     = self.rolling_max_aoa(self.bbidsizes[pid], idx, 0, timestamp, k)
    data[pid + ":best_bid_size_{}min".format(k)]     = self.rolling_min_aoa(self.bbidsizes[pid], idx, 0, timestamp, k)
    data[pid + ":best_ask_size_{}avg".format(k)]     = self.rolling_avg_aoa(self.basksizes[pid], idx, 0, timestamp, k)
    data[pid + ":best_ask_size_{}max".format(k)]     = self.rolling_max_aoa(self.basksizes[pid], idx, 0, timestamp, k)
    data[pid + ":best_ask_size_{}min".format(k)]     = self.rolling_min_aoa(self.basksizes[pid], idx, 0, timestamp, k)
    data[pid + ":bid_tick1_{}max".format(k)]         = self.rolling_max(self.bbidtick1[pid], idx, timestamp, k)
    data[pid + ":bid_tick1_{}min".format(k)]         = self.rolling_min(self.bbidtick1[pid], idx, timestamp, k)
    data[pid + ":ask_tick1_{}max".format(k)]         = self.rolling_max(self.basktick1[pid], idx, timestamp, k)
    data[pid + ":ask_tick1_{}min".format(k)]         = self.rolling_min(self.basktick1[pid], idx, timestamp, k)
    data[pid + ":bid_avg_tick_{}max".format(k)]      = self.rolling_max(self.bbidavgtick[pid], idx, timestamp, k)
    data[pid + ":bid_avg_tick_{}min".format(k)]      = self.rolling_min(self.bbidavgtick[pid], idx, timestamp, k)
    data[pid + ":ask_avg_tick_{}max".format(k)]      = self.rolling_max(self.baskavgtick[pid], idx, timestamp, k)
    data[pid + ":ask_avg_tick_{}min".format(k)]      = self.rolling_min(self.baskavgtick[pid], idx, timestamp, k)
    data[pid + ":bid_size_change_{}max".format(k)]   = self.rolling_max(self.bbidsizechange[pid], idx, timestamp, k)
    data[pid + ":bid_size_change_{}min".format(k)]   = self.rolling_min(self.bbidsizechange[pid], idx, timestamp, k)
    data[pid + ":bid_size_change_{}absavg".format(k)] = self.rolling_abs_avg(self.bbidsizechange[pid], idx, timestamp, k)
    data[pid + ":ask_size_change_{}max".format(k)]   = self.rolling_max(self.basksizechange[pid], idx, timestamp, k)
    data[pid + ":ask_size_change_{}min".format(k)]   = self.rolling_min(self.basksizechange[pid], idx, timestamp, k)
    data[pid + ":ask_size_change_{}absavg".format(k)] = self.rolling_abs_avg(self.basksizechange[pid], idx, timestamp, k)
    data[pid + ":bid_volume_change_{}max".format(k)] = self.rolling_max(self.bbidvolchange[pid], idx, timestamp, k)
    data[pid + ":bid_volume_change_{}min".format(k)] = self.rolling_min(self.bbidvolchange[pid], idx, timestamp, k)
    data[pid + ":bid_volume_change_{}absavg".format(k)] = self.rolling_abs_avg(self.bbidvolchange[pid], idx, timestamp, k)
    data[pid + ":ask_volume_change_{}max".format(k)] = self.rolling_max(self.baskvolchange[pid], idx, timestamp, k)
    data[pid + ":ask_volume_change_{}min".format(k)] = self.rolling_min(self.baskvolchange[pid], idx, timestamp, k)
    data[pid + ":ask_volume_change_{}absavg".format(k)] = self.rolling_abs_avg(self.baskvolchange[pid], idx, timestamp, k)
    data[pid + ":bid_level_intercept_{}avg".format(k)] = self.rolling_avg(self.bbidlevelintercept[pid], idx, timestamp, k)
    data[pid + ":bid_level_slope_{}avg".format(k)]   = self.rolling_avg(self.bbidlevelslope[pid], idx, timestamp, k)
    data[pid + ":ask_level_intercept_{}avg".format(k)] = self.rolling_avg(self.basklevelintercept[pid], idx, timestamp, k)
    data[pid + ":ask_level_slope_{}avg".format(k)]   = self.rolling_avg(self.basklevelslope[pid], idx, timestamp, k)
    data[pid + ":ba_imbalance_{}avg".format(k)]      = self.rolling_avg(self.bba_imbalance[pid], idx, timestamp, k)
    data[pid + ":ba_imbalance_{}max".format(k)]      = self.rolling_max(self.bba_imbalance[pid], idx, timestamp, k)
    data[pid + ":ba_imbalance_{}min".format(k)]      = self.rolling_min(self.bba_imbalance[pid], idx, timestamp, k)
    data[pid + ":wap_{}avg".format(k)]               = self.rolling_avg(self.wapprice[pid], idx, timestamp, k)
    data[pid + ":wap_{}max".format(k)]               = self.rolling_max(self.wapprice[pid], idx, timestamp, k)
    data[pid + ":wap_{}min".format(k)]               = self.rolling_min(self.wapprice[pid], idx, timestamp, k)
    data[pid + ":book_return_{}avg".format(k)]       = self.rolling_avg(self.breturn[pid], idx, timestamp, k)
    data[pid + ":book_return_{}max".format(k)]       = self.rolling_max(self.breturn[pid], idx, timestamp, k)
    data[pid + ":book_return_{}min".format(k)]       = self.rolling_min(self.breturn[pid], idx, timestamp, k)
    data[pid + ":ba_spread_{}avg".format(k)]         = self.rolling_avg(self.bbaspread[pid], idx, timestamp, k)
    data[pid + ":ba_spread_{}max".format(k)]         = self.rolling_max(self.bbaspread[pid], idx, timestamp, k)
    data[pid + ":ba_spread_{}min".format(k)]         = self.rolling_min(self.bbaspread[pid], idx, timestamp, k)

    data[pid + ":trade_buys_count_{}sum".format(k)]  = self.rolling_sum(self.tnbuys[pid], idx, timestamp, k)
    data[pid + ":trade_buys_count_{}avg".format(k)]  = self.rolling_avg(self.tnbuys[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_buys_count_{}max".format(k)]  = self.rolling_max(self.tnbuys[pid], idx, timestamp, k)
    data[pid + ":trade_sells_count_{}sum".format(k)] = self.rolling_sum(self.tnsells[pid], idx, timestamp, k)
    data[pid + ":trade_sells_count_{}avg".format(k)] = self.rolling_avg(self.tnsells[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_sells_count_{}max".format(k)] = self.rolling_max(self.tnsells[pid], idx, timestamp, k)
    data[pid + ":trade_size_{}sum".format(k)]        = self.rolling_sum(self.tsize[pid], idx, timestamp, k)
    data[pid + ":trade_size_{}avg".format(k)]        = self.rolling_avg(self.tsize[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_size_{}max".format(k)]        = self.rolling_max(self.tsize[pid], idx, timestamp, k)
    data[pid + ":trade_volume_{}sum".format(k)]      = self.rolling_sum(self.tvolume[pid], idx, timestamp, k)
    data[pid + ":trade_volume_{}avg".format(k)]      = self.rolling_avg(self.tvolume[pid], idx, timestamp, k, count_nan=True)
    data[pid + ":trade_volume_{}max".format(k)]      = self.rolling_max(self.tvolume[pid], idx, timestamp, k)
    data[pid + ":trade_avg_price_{}avg".format(k)]   = self.rolling_avg(self.tavgprice[pid], idx, timestamp, k)
    data[pid + ":trade_avg_price_{}max".format(k)]   = self.rolling_max(self.tavgprice[pid], idx, timestamp, k)
    data[pid + ":trade_avg_price_{}min".format(k)]   = self.rolling_min(self.tavgprice[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}avg".format(k)]      = self.rolling_avg(self.treturn[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}sum".format(k)]      = self.rolling_sum(self.treturn[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}max".format(k)]      = self.rolling_max(self.treturn[pid], idx, timestamp, k)
    data[pid + ":trade_return_{}min".format(k)]      = self.rolling_min(self.treturn[pid], idx, timestamp, k)

  def produce_output(self, timestamp):
    perf_start_time = time.perf_counter()
    idx = -1
    for i in range(len(self.timestsamps)):
      if self.timestamps[i] == timestamp:
        idx = i
        break
    data = dict()
    data[STIME_COLNAME] = self.utc.localize(datetime.utcfromtimestamp(timestamp)).strftime(DATETIME_FORMAT)
    if idx == -1:
      logging.error("Unexpected Error: cannot find timestamp {} in state data.".format(timestamp))
      return data
    for pid in self.pids:
      if len(self.bbidprices[pid][idx]) == 0:
        data[pid + ":best_bid_price"] = float("nan")
      else:
        data[pid + ":best_bid_price"] = self.bbidprices[pid][idx][0]
      if len(self.baskprices[pid][idx]) == 0:
        data[pid + ":best_ask_price"] = float("nan")
      else:
        data[pid + ":best_ask_price"] = self.baskprices[pid][idx][0]
      if len(self.bbidsizes[pid][idx]) == 0:
        data[pid + ":best_bid_size"] = float("nan")
      else:
        data[pid + ":best_bid_size"] = self.bbidsizes[pid][idx][0]
      if len(self.basksizes[pid][idx]) == 0:
        data[pid + ":best_ask_size"] = float("nan")
      else:
        data[pid + ":best_ask_size"] = self.basksizes[pid][idx][0]
      data[pid + ":bid_level_size"] = self.bbidstacksize[pid][idx]
      data[pid + ":ask_level_size"] = self.baskstacksize[pid][idx]
      data[pid + ":bid_tick1"]      = self.bbidtick1[pid][idx]
      data[pid + ":ask_tick1"]      = self.basktick1[pid][idx]
      data[pid + ":bid_avg_tick"]   = self.bbidavgtick[pid][idx]
      data[pid + ":ask_avg_tick"]   = self.baskavgtick[pid][idx]
      data[pid + ":bid_size_change"] = self.bbidsizechange[pid][idx]
      data[pid + ":ask_size_change"] = self.basksizechange[pid][idx]
      data[pid + ":bid_volume_change"]  = self.bbidvolchange[pid][idx]
      data[pid + ":ask_volume_change"]  = self.baskvolchange[pid][idx]
      data[pid + ":bid_level_slope"] = self.bbidlevelslope[pid][idx]
      data[pid + ":ask_level_slope"] = self.basklevelslope[pid][idx]
      data[pid + ":bid_level_intercept"] = self.bbidlevelintercept[pid][idx]
      data[pid + ":ask_level_intercept"] = self.basklevelintercept[pid][idx]
      data[pid + ":ba_imbalance"]     = self.bba_imbalance[pid][idx]
      data[pid + ":wap"]             = self.wapprice[pid][idx]
      data[pid + ":book_return"]     = self.breturn[pid][idx]
      data[pid + ":ba_spread"]       = self.bbaspread[pid][idx]

      data[pid + ":trade_buys_count"] = self.tnbuys[pid][idx]
      data[pid + ":trade_sells_count"] = self.tnsells[pid][idx]
      data[pid + ":trade_size"]      = self.tsize[pid][idx]
      data[pid + ":trade_volume"]    = self.tvolume[pid][idx]
      data[pid + ":trade_avg_price"] = self.tavgprice[pid][idx]
      data[pid + ":trade_return"]    = self.treturn[pid][idx]

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
    get_etl_process_time_histogram().observe(perf_time_taken)
    push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
    return data

def create_state():
  return ETLS2State()

def get_history_size():
  return HIST_SIZE

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_etl_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  app_name = 'cbp_etl_s2_pipe'
  setup_gateway(app_name)
  setup_basic_gauges(app_name)
  create_etl_process_time_histogram(app_name)
  create_interval_state()
  try:
    asyncio.run(
      etl_consumer_producer(
        data_subscriber,
        etl_processor,
        process_etl_data,
        create_state,
        get_history_size,
        args.exchange,
        [args.b20name, args.b21name, args.t0name, args.t1name],
        args.period,
    ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
