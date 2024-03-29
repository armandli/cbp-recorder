import argparse
import logging
import json
import time
from datetime import datetime
import pytz
import math
import asyncio

from prometheus_client import push_to_gateway

import cppext as m

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
from senseis.metric_utility import create_etl_process_time_histogram

def build_parser():
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument('--period', type=int, help='periodicity in seconds', default=1)
  parser.add_argument('--b20name', type=str, help='book level2 0 exchange name', default='book_level2_0')
  parser.add_argument('--b21name', type=str, help='book level2 1 exchange name', default='book_level2_1')
  parser.add_argument('--t0name', type=str, help='trade 0 exchange name', default='trade_exchange_0')
  parser.add_argument('--t1name', type=str, help='trade 1 exchange name', default='trade_exchange_1')
  parser.add_argument('--exchange', type=str, help='etl exchange name', required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

HIST_SIZE = 1920
MAX_WAIT_SECONDS = 26

class ETLS2State(ETLState):
  def __init__(self):
    self.timestamps = [None for _ in range(HIST_SIZE)]
    self.book_lengths = [3, 9, 27, 81, 162, 324, 648, 960, 1440, 1920]
    self.trade_lengths = [27, 81, 162, 324, 648, 960, 1440, 1920]
    self.return_lengths = [27, 81, 162, 324, 648, 960, 1440, 1920]
    self.utc = pytz.timezone("UTC")
    self.pids = []
    # book data
    self.bbidprices = dict()
    self.baskprices = dict()
    self.bbidsizes = dict()
    self.basksizes = dict()
    self.bbidhands = dict()
    self.bbidhands_cache = dict()
    self.baskhands = dict()
    self.baskhands_cache = dict()
    self.bbidstacksize = dict()
    self.baskstacksize = dict()

    self.bbidtick1 = dict()
    self.bbidtick1_cache = dict()
    self.basktick1 = dict()
    self.basktick1_cache = dict()
    self.bbidavgtick = dict()
    self.bbidavgtick_cache = dict()
    self.baskavgtick = dict()
    self.baskavgtick_cache = dict()
    self.bbidsizechange = dict()
    self.basksizechange = dict()
    self.bbidvolchange = dict()
    self.bbidvolchange_cache = dict()
    self.baskvolchange = dict()
    self.baskvolchange_cache = dict()
    self.bbidlevelslope = dict()
    self.bbidlevelslope_cache = dict()
    self.bbidlevelintercept = dict()
    self.bbidlevelintercept_cache = dict()
    self.basklevelslope = dict()
    self.basklevelslope_cache = dict()
    self.basklevelintercept = dict()
    self.basklevelintercept_cache = dict()

    self.bba_imbalance = dict()
    self.wapprice = dict()
    self.breturn = dict()
    self.breturn_cache = dict()
    self.bbaspread = dict()
    self.bmreturn27 = [None for _ in range(HIST_SIZE)]
    # trade data
    self.tnbuys = dict()
    self.tnbuys_cache = dict()
    self.tnsells = dict()
    self.tnsells_cache = dict()
    self.tsize = dict()
    self.tvolume = dict()
    self.tvolume_cache = dict()
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
        self.bbidhands[pid]     = [[] for _ in range(self.hist_size())]
        self.bbidhands_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.baskhands[pid]     = [[] for _ in range(self.hist_size())]
        self.baskhands_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.bbidstacksize[pid] = [float("nan") for _ in range(self.hist_size())]
        self.baskstacksize[pid] = [float("nan") for _ in range(self.hist_size())]

        self.bbidtick1[pid]     = [float("nan") for _ in range(self.hist_size())]
        self.bbidtick1_cache[pid] = {k: [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.basktick1[pid]     = [float("nan") for _ in range(self.hist_size())]
        self.basktick1_cache[pid] = {k: [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.bbidavgtick[pid]   = [float("nan") for _ in range(self.hist_size())]
        self.bbidavgtick_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.baskavgtick[pid]   = [float("nan") for _ in range(self.hist_size())]
        self.baskavgtick_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.bbidsizechange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.basksizechange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbidvolchange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbidvolchange_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.baskvolchange[pid] = [float("nan") for _ in range(self.hist_size())]
        self.baskvolchange_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.bbidlevelslope[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbidlevelslope_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.bbidlevelintercept[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbidlevelintercept_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.basklevelslope[pid] = [float("nan") for _ in range(self.hist_size())]
        self.basklevelslope_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.basklevelintercept[pid] = [float("nan") for _ in range(self.hist_size())]
        self.basklevelintercept_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.book_lengths}
        self.bba_imbalance[pid] = [float("nan") for _ in range(self.hist_size())]
        self.wapprice[pid]      = [float("nan") for _ in range(self.hist_size())]
        self.breturn[pid]       = [float("nan") for _ in range(self.hist_size())]
        self.breturn_cache[pid] = [float("nan") for _ in range(self.hist_size())]
        self.bbaspread[pid]     = [float("nan") for _ in range(self.hist_size())]
        # update trade data
        self.tnbuys[pid]        = [0 for _ in range(self.hist_size())]
        self.tnbuys_cache[pid]  = {k : [float("nan") for _ in range(self.hist_size())] for k in self.trade_lengths}
        self.tnsells[pid]       = [0 for _ in range(self.hist_size())]
        self.tnsells_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.trade_lengths}
        self.tsize[pid]         = [float("nan") for _ in range(self.hist_size())]
        self.tvolume[pid]       = [float("nan") for _ in range(self.hist_size())]
        self.tvolume_cache[pid] = {k : [float("nan") for _ in range(self.hist_size())] for k in self.trade_lengths}
        self.tavgprice[pid]     = [float("nan") for _ in range(self.hist_size())]
        self.treturn[pid]       = [float("nan") for _ in range(self.hist_size())]

  def insert(self, timestamp, pid_book, pid_trade):
    nidx = self.nidx
    pidx = (nidx - 1) % self.hist_size()
    self.timestamps[nidx] = timestamp
    for pid in self.pids:
      # book data insertion
      book_data = json.loads(pid_book[pid])
      bid_prices = []
      bid_sizes = []
      bid_hands = []
      if 'bids' not in book_data:
        logging.info("{} book data for {} missing bids. data {}".format(pid, timestamp, book_data))
      else:
        for i in range(1, len(book_data['bids'])):
          try:
            bid_prices.append(float(book_data['bids'][i][0]))
          except ValueError:
            logging.error("cannot parse bid price: {}".format(book_data['bids'][i][0]))
            bid_prices.append(float("nan"))
          try:
            bid_sizes.append(float(book_data['bids'][i][1]))
          except ValueError:
            logging.error("cannot parse bid size: {}".format(book_data['bids'][i][1]))
            bid_sizes.append(float("nan"))
          try:
            bid_hands.append(float(book_data['bids'][i][2]))
          except ValueError:
            logging.error("cannot parse bid hand: {}".format(book_data['bids'][i][2]))
            bid_hands.append(float("nan"))
      self.bbidprices[pid][nidx] = bid_prices
      self.bbidsizes[pid][nidx] = bid_sizes
      self.bbidhands[pid][nidx] = bid_hands
      ask_prices = []
      ask_sizes = []
      ask_hands = []
      if 'asks' not in book_data:
        logging.info("{} book data for {} missing asks. data {}".format(pid, timestamp, book_data))
      else:
        for i in range(1, len(book_data['asks'])):
          try:
            ask_prices.append(float(book_data['asks'][i][0]))
          except ValueError:
            logging.error("cannot parse ask price: {}".format(book_data['asks'][i][0]))
            ask_prices.append(float("nan"))
          try:
            ask_sizes.append(float(book_data['asks'][i][1]))
          except ValueError:
            logging.error("cannot parse ask size: {}".format(book_data['asks'][i][1]))
            ask_sizes.append(float("nan"))
          try:
            ask_hands.append(float(book_data['asks'][i][2]))
          except ValueError:
            logging.error("cannot parse ask hand: {}".format(book_data['asks'][i][2]))
            ask_hands.append(float("nan"))
      self.baskprices[pid][nidx] = ask_prices
      self.basksizes[pid][nidx] = ask_sizes
      self.baskhands[pid][nidx] = ask_hands
      if 'bids' not in book_data:
        self.bbidstacksize[pid][nidx] = 0
      else:
        try:
          self.bbidstacksize[pid][nidx] = int(book_data['bids'][0])
        except ValueError:
          logging.error("cannot convert bid stack size value")
          self.bbidstacksize[pid][nidx] = 0
      if 'asks' not in book_data:
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
      self.breturn[pid][nidx] = compute_return(self.wapprice[pid][pidx], self.wapprice[pid][nidx])
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
        if not isinstance(t, dict):
          logging.info("trade data {} contains non-dictionary".format(trade_data))
          continue
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
          if self.timestamps[idx] is None or self.timestamps[idx] > self.timestamps[nidx]:
            break
          if not math.isnan(self.tavgprice[pid][idx]):
            self.treturn[pid][nidx] = compute_return(self.tavgprice[pid][idx], self.tavgprice[pid][nidx])
            break
          idx = (idx - 1) % self.hist_size()
    self.bmreturn27[nidx] = self.rolling_mean_return(nidx, timestamp, 27)
    self.nidx = (self.nidx + 1) % self.hist_size()

  def rolling_mean_return(self, idx, timestamp, length):
    rs = [self.rolling_avg_sum(self.breturn[pid], self.breturn_cache[pid], idx, timestamp, length) for pid in self.pids]
    for i, pid in enumerate(self.pids):
      self.breturn_cache[pid][idx] = rs[i][0]
    s = sum([r[1] for r in rs])
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

  def produce_book_output_rolling_multi_k(self, data, pid, idx, timestamp, ks):
    output = self.rolling_avg_sum_max_min_aoa_multi_k(self.bbidprices[pid], idx, 0, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":best_bid_price_{}avg".format(k)] = output[i][0]
      data[pid + ":best_bid_price_{}max".format(k)] = output[i][2]
      data[pid + ":best_bid_price_{}min".format(k)] = output[i][3]

    output = self.rolling_avg_sum_max_min_aoa_multi_k(self.baskprices[pid], idx, 0, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":best_ask_price_{}avg".format(k)] = output[i][0]
      data[pid + ":best_ask_price_{}max".format(k)] = output[i][2]
      data[pid + ":best_ask_price_{}min".format(k)] = output[i][3]

    output = self.rolling_avg_sum_max_min_aoa_multi_k(self.bbidsizes[pid], idx, 0, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":best_bid_size_{}avg".format(k)] = output[i][0]
      data[pid + ":best_bid_size_{}max".format(k)] = output[i][2]
      data[pid + ":best_bid_size_{}min".format(k)] = output[i][3]

    output = self.rolling_avg_sum_max_min_aoa_multi_k(self.basksizes[pid], idx, 0, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":best_ask_size_{}avg".format(k)] = output[i][0]
      data[pid + ":best_ask_size_{}max".format(k)] = output[i][2]
      data[pid + ":best_ask_size_{}min".format(k)] = output[i][3]

    output = self.rolling_ema_ems_aoa_multi_k(self.bbidhands[pid], self.bbidhands_cache[pid], idx, 0, timestamp, ks)
    for k in ks:
      data[pid + ":best_bid_hand_{}avg".format(k)] = output[k][0]
      self.bbidhands_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_aoa_multi_k(self.baskhands[pid], self.baskhands_cache[pid], idx, 0, timestamp, ks)
    for k in ks:
      data[pid + ":best_ask_hand_{}avg".format(k)] = output[k][0]
      self.baskhands_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.bbidtick1[pid], self.bbidtick1_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":bid_tick1_{}avg".format(k)] = output[k][0]
      self.bbidtick1_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.basktick1[pid], self.basktick1_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":ask_tick1_{}avg".format(k)] = output[k][0]
      self.basktick1_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.bbidavgtick[pid], self.bbidavgtick_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":bid_avg_tick_{}avg".format(k)] = output[k][0]
      self.bbidavgtick_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.baskavgtick[pid], self.baskavgtick_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":ask_avg_tick_{}avg".format(k)] = output[k][0]
      self.baskavgtick_cache[pid][k][idx] = output[k][0]

    output = self.rolling_abs_avg_sum_max_min_multi_k(self.bbidsizechange[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":bid_size_change_{}max".format(k)] = output[i][2]
      data[pid + ":bid_size_change_{}min".format(k)] = output[i][3]
      data[pid + ":bid_size_change_{}absavg".format(k)] = output[i][0]

    output = self.rolling_abs_avg_sum_max_min_multi_k(self.basksizechange[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":ask_size_change_{}max".format(k)] = output[i][2]
      data[pid + ":ask_size_change_{}min".format(k)] = output[i][3]
      data[pid + ":ask_size_change_{}absavg".format(k)] = output[i][0]

    output = self.rolling_abs_ema_ems_multi_k(self.bbidvolchange[pid], self.bbidvolchange_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":bid_volume_change_{}absavg".format(k)] = output[k][0]
      self.bbidvolchange_cache[pid][k][idx] = output[k][0]

    output = self.rolling_abs_ema_ems_multi_k(self.baskvolchange[pid], self.baskvolchange_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":ask_volume_change_{}absavg".format(k)] = output[k][0]
      self.baskvolchange_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.bbidlevelslope[pid], self.bbidlevelslope_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":bid_level_slope_{}avg".format(k)] = output[k][0]
      self.bbidlevelslope_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.bbidlevelintercept[pid], self.bbidlevelintercept_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":bid_level_intercept_{}avg".format(k)] = output[k][0]
      self.bbidlevelintercept_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.basklevelslope[pid], self.basklevelslope_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":ask_level_slope_{}avg".format(k)] = output[k][0]
      self.basklevelslope_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.basklevelintercept[pid], self.basklevelintercept_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":ask_level_intercept_{}avg".format(k)] = output[k][0]
      self.basklevelintercept_cache[pid][k][idx] = output[k][0]

    output = self.rolling_avg_sum_max_min_multi_k(self.bba_imbalance[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":ba_imbalance_{}avg".format(k)] = output[i][0]
      data[pid + ":ba_imbalance_{}max".format(k)] = output[i][2]
      data[pid + ":ba_imbalance_{}min".format(k)] = output[i][3]

    output = self.rolling_avg_sum_max_min_multi_k(self.wapprice[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":wap_{}avg".format(k)] = output[i][0]
      data[pid + ":wap_{}max".format(k)] = output[i][2]
      data[pid + ":wap_{}min".format(k)] = output[i][3]

    output = self.rolling_avg_sum_max_min_multi_k(self.breturn[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":book_return_{}avg".format(k)] = output[i][0]
      data[pid + ":book_return_{}max".format(k)] = output[i][2]
      data[pid + ":book_return_{}min".format(k)] = output[i][3]

    output = self.rolling_avg_sum_max_min_multi_k(self.bbaspread[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":ba_spread_{}avg".format(k)] = output[i][0]
      data[pid + ":ba_spread_{}max".format(k)] = output[i][2]
      data[pid + ":ba_spread_{}min".format(k)] = output[i][3]

  def produce_trade_output_rolling_multi_k(self, data, pid, idx, timestamp, ks):
    output = self.rolling_ema_ems_multi_k(self.tnbuys[pid], self.tnbuys_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":trade_buys_count_{}sum".format(k)] = output[k][1]
      self.tnbuys_cache[pid][k][idx] = output[k][0]

    output = self.rolling_ema_ems_multi_k(self.tnsells[pid], self.tnsells_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":trade_sells_count_{}sum".format(k)] = output[k][1]
      self.tnsells_cache[pid][k][idx] = output[k][0]

    output = self.rolling_avg_sum_max_min_multi_k(self.tsize[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":trade_size_{}sum".format(k)] = output[i][1]
      data[pid + ":trade_size_{}avg".format(k)] = output[i][0]
      data[pid + ":trade_size_{}max".format(k)] = output[i][2]

    output = self.rolling_ema_ems_multi_k(self.tvolume[pid], self.tvolume_cache[pid], idx, timestamp, ks)
    for k in ks:
      data[pid + ":trade_volume_{}sum".format(k)] = output[k][1]
      self.tvolume_cache[pid][k][idx] = output[k][0]

    output = self.rolling_avg_sum_max_min_multi_k(self.tavgprice[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":trade_avg_price_{}avg".format(k)] = output[i][0]
      data[pid + ":trade_avg_price_{}max".format(k)] = output[i][2]
      data[pid + ":trade_avg_price_{}min".format(k)] = output[i][3]

    output = self.rolling_avg_sum_max_min_multi_k(self.treturn[pid], idx, timestamp, ks)
    for i, k in enumerate(ks):
      data[pid + ":trade_return_{}avg".format(k)] = output[i][0]
      data[pid + ":trade_return_{}sum".format(k)] = output[i][1]
      data[pid + ":trade_return_{}max".format(k)] = output[i][2]
      data[pid + ":trade_return_{}min".format(k)] = output[i][3]

  def produce_output(self, timestamp):
    idx = -1
    for i in range(len(self.timestamps)):
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
      if len(self.bbidhands[pid][idx]) == 0:
        data[pid + ":best_bid_hand"] = float("nan")
      else:
        data[pid + ":best_bid_hand"] = self.bbidhands[pid][idx][0]
      if len(self.baskhands[pid][idx]) == 0:
        data[pid + ":best_ask_hand"] = float("nan")
      else:
        data[pid + ":best_ask_hand"] = self.baskhands[pid][idx][0]
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

      self.produce_book_output_rolling_multi_k(data, pid, idx, timestamp, self.book_lengths)
      self.produce_trade_output_rolling_multi_k(data, pid, idx, timestamp, self.trade_lengths)

      output = self.rolling_volatility_multi_k(self.breturn[pid], idx, timestamp, self.return_lengths)
      for i, k in enumerate(self.return_lengths):
        data[pid + ":book_volatility_{}".format(k)] = output[i]

      data[pid + ":book_beta_648"] = self.rolling_beta(self.breturn[pid], idx, timestamp, 648)

      output = self.rolling_volatility_multi_k(self.treturn[pid], idx, timestamp, self.return_lengths)
      for i, k in enumerate(self.return_lengths):
        data[pid + ":trade_volatility_{}".format(k)] = output[i]

    data["book_mean_return_27"] = self.bmreturn27[idx]
    return data

def create_state():
  return m.ETLS2State()

def get_max_wait_seconds():
  return MAX_WAIT_SECONDS

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
        get_max_wait_seconds,
        args.exchange,
        [args.b20name, args.b21name, args.t0name, args.t1name],
        args.period,
    ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
