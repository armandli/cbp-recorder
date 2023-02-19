import logging
import math
import json
import asyncio

from prometheus_client import push_to_gateway
from prometheus_client import Gauge

from senseis.configuration import is_trade_exchange_name, get_exchange_pids
from senseis.utility import setup_logging, build_monitor_parser
from senseis.extraction_producer_consumer import monitor_extraction, extraction_monitor, extraction_subscriber
from senseis.extraction_producer_consumer import create_interval_state
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name
from senseis.metric_utility import setup_basic_gauges

def create_trade_price_gauges(app_name, pids):
  global TRADE_PRICE_GAUGES
  TRADE_PRICE_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_last_trade_price", "{} last trade price".format(pid), registry=get_collector_registry()) for pid in pids}

def get_trade_price_gauges():
  return TRADE_PRICE_GAUGES

def create_trade_size_gauges(app_name, pids):
  global TRADE_SIZE_GAUGES
  TRADE_SIZE_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_last_trade_size", "{} last trade size".format(pid), registry=get_collector_registry()) for pid in pids}

def get_trade_size_gauges():
  return TRADE_SIZE_GAUGES

def create_trade_volume_gauges(app_name, pids):
  global TRADE_VOLUME_GAUGES
  TRADE_VOLUME_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "last_trade_volume", "{} last trade volume".format(pid), registry=get_collector_registry()) for pid in pids}

def get_trade_volume_gauges():
  return TRADE_VOLUME_GAUGES

def create_trade_price_avg_gauges(app_name, pids):
  global TRADE_PRICE_AVG_GAUGES
  TRADE_PRICE_AVG_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_trade_price_avg", "{} average trade price this second".format(pid), registry=get_collector_registry()) for pid in pids}

def get_trade_price_avg_gauges():
  return TRADE_PRICE_AVG_GAUGES

def create_trade_size_sum_gauges(app_name, pids):
  global TRADE_SIZE_SUM_GAUGES
  TRADE_SIZE_SUM_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_trade_size_sum", "{} total trade size sum this second".format(pid), registry=get_collector_registry()) for pid in pids}

def get_trade_size_sum_gauges():
  return TRADE_SIZE_SUM_GAUGES

def create_trade_volume_sum_gauges(app_name, pids):
  global TRADE_VOLUME_SUM_GAUGES
  TRADE_VOLUME_SUM_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_trade_volume_sum", "{} total trade size volume this second".format(pid), registry=get_collector_registry()) for pid in pids}

def get_trade_volume_sum_gauges():
  return TRADE_VOLUME_SUM_GAUGES

def trade_monitor(data, exchange_name):
  pids = get_exchange_pids(exchange_name)
  tpgs = get_trade_price_gauges()
  tsgs = get_trade_size_gauges()
  tvgs = get_trade_volume_gauges()
  tpags = get_trade_price_avg_gauges()
  tssgs = get_trade_size_sum_gauges()
  tvsgs = get_trade_volume_sum_gauges()
  for pid in pids:
    trade_data = json.loads(data[pid])
    total_size = 0.
    total_volume = 0.
    last_trade_price = float("nan")
    last_trade_size = float("nan")
    last_trade_volume = float("nan")
    max_id_no = 0
    for t in trade_data:
      try:
        trade_sz = float(t['size'])
        trade_price = float(t['price'])
        trade_id = int(t['trade_id'])
        if trade_id > max_id_no:
          max_id_no = trade_id
          last_trade_price = trade_price
          last_trade_size = trade_sz
          last_trade_volume = trade_price * trade_sz
        total_size += trade_sz
        total_volume += trade_price * trade_sz
      except ValueError:
        logging.info("Error reading pid {} trade: {} ".format(pid, t))
    if not math.isnan(last_trade_price) and not math.isnan(last_trade_size):
      tpgs[pid].set(last_trade_price)
      tsgs[pid].set(last_trade_size)
      tvgs[pid].set(last_trade_volume)
    tvsgs[pid].set(total_volume)
    tssgs[pid].set(total_size)
    if total_volume > 0. and total_size > 0.:
      tpags[pid].set(total_volume / total_size)
  push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())

def main():
  parser = build_monitor_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_trade_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  app_name = 'cbp_{}_monitor'.format(args.exchange)
  setup_gateway(app_name)
  setup_basic_gauges(app_name)
  create_interval_state()
  pids = get_exchange_pids(args.exchange)
  create_trade_price_gauges(app_name, pids)
  create_trade_size_gauges(app_name, pids)
  create_trade_volume_gauges(app_name, pids)
  create_trade_price_avg_gauges(app_name, pids)
  create_trade_size_sum_gauges(app_name, pids)
  create_trade_volume_sum_gauges(app_name, pids)
  try:
    asyncio.run(
      monitor_extraction(
        extraction_subscriber,
        extraction_monitor,
        trade_monitor,
        args.exchange,
      ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
