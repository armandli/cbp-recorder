import logging
import json
import asyncio

from prometheus_client import push_to_gateway
from prometheus_client import Gauge

from senseis.configuration import is_book_exchange_name, get_exchange_pids
from senseis.utility import setup_logging, build_monitor_parser
from senseis.extraction_producer_consumer import monitor_extraction, extraction_monitor, extraction_subscriber
from senseis.extraction_producer_consumer import create_interval_state
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name
from senseis.metric_utility import setup_basic_gauges

def create_best_bid_price_gauges(app_name, pids):
  global BEST_BID_PRICE_GAUGES
  BEST_BID_PRICE_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_best_bid_price", "{} best bid price".format(pid), registry=get_collector_registry()) for pid in pids}

def get_best_bid_price_gauges():
  return BEST_BID_PRICE_GAUGES

def create_best_ask_price_gauges(app_name, pids):
  global BEST_ASK_PRICE_GAUGES
  BEST_ASK_PRICE_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_best_ask_price", "{} best ask price".format(pid), registry=get_collector_registry()) for pid in pids}

def get_best_ask_price_gauges():
  return BEST_ASK_PRICE_GAUGES

def create_best_bid_size_gauges(app_name, pids):
  global BEST_BID_SIZE_GAUGES
  BEST_BID_SIZE_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_best_bid_size", "{} best bid size".format(pid), registry=get_collector_registry()) for pid in pids}

def get_best_bid_size_gauges():
  return BEST_BID_SIZE_GAUGES

def create_best_ask_size_gauges(app_name, pids):
  global BEST_ASK_SIZE_GAUGES
  BEST_ASK_SIZE_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + "_best_ask_size", "{} best ask size".format(pid), registry=get_collector_registry()) for pid in pids}

def get_best_ask_size_gauges():
  return BEST_ASK_SIZE_GAUGES

def create_best_bid_volume_gauges(app_name, pids):
  global BEST_BID_VOLUME_GAUGES
  BEST_BID_VOLUME_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + '_best_bid_volume', "{} best bid volume".format(pid), registry=get_collector_registry()) for pid in pids}

def get_best_bid_volume_gauges():
  return BEST_BID_VOLUME_GAUGES

def create_best_ask_volume_gauges(app_name, pids):
  global BEST_ASK_VOLUME_GAUGES
  BEST_ASK_VOLUME_GAUGES = {pid : Gauge(app_name + "_" + pid.replace('-','_') + '_best_ask_volume', "{} best ask volume".format(pid), registry=get_collector_registry()) for pid in pids}

def get_best_ask_volume_gauges():
  return BEST_ASK_VOLUME_GAUGES

def book_monitor(data, exchange_name):
  pids = get_exchange_pids(exchange_name)
  bpgs = get_best_bid_price_gauges()
  apgs = get_best_ask_price_gauges()
  bsgs = get_best_bid_size_gauges()
  asgs = get_best_ask_size_gauges()
  bvgs = get_best_bid_volume_gauges()
  avgs = get_best_ask_volume_gauges()
  for pid in pids:
    pid_data = json.loads(data[pid])
    if not pid_data:
      continue
    if 'bids' in pid_data and len(pid_data['bids']) > 0:
      bids = pid_data['bids']
      if isinstance(bids[0], list):
        try:
          bpgs[pid].set(float(bids[0][0]))
          bsgs[pid].set(float(bids[0][1]))
          bvgs[pid].set(float(bids[0][0]) * float(bids[0][1]))
        except ValueError:
          logging.info("Detected invalid bids value in pid {} data: {}".format(pid, pid_data))
      elif isinstance(bids[0], int) and len(bids) > 1 and isinstance(bids[1], list):
        try:
          bpgs[pid].set(float(bids[1][0]))
          bsgs[pid].set(float(bids[1][1]))
          bvgs[pid].set(float(bids[1][0]) * float(bids[1][1]))
        except ValueError:
          logging.info("Detected invalid bids value in pid {} data: {}".format(pid, pid_data))
    if 'asks' in pid_data and len(pid_data['asks']) > 0:
      asks = pid_data['asks']
      if isinstance(asks[0], list):
        try:
          apgs[pid].set(float(asks[0][0]))
          asgs[pid].set(float(asks[0][1]))
          avgs[pid].set(float(asks[0][0]) * float(asks[0][1]))
        except ValueError:
          logging.info("Detected invalid asks value in pid {} data: {}".format(pid, pid_data))
      elif isinstance(asks[0], int) and len(asks) > 1 and isinstance(asks[1], list):
        try:
          apgs[pid].set(float(asks[1][0]))
          asgs[pid].set(float(asks[1][1]))
          avgs[pid].set(float(asks[1][0]) * float(asks[1][1]))
        except ValueError:
          logging.info("Detected invalid asks value in pid {} data: {}".format(pid, pid_data))
  push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())

def main():
  parser = build_monitor_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_book_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit.")
    return
  app_name = 'cbp_{}_monitor'.format(args.exchange)
  setup_gateway(app_name)
  setup_basic_gauges(app_name)
  create_interval_state()
  pids = get_exchange_pids(args.exchange)
  create_best_bid_price_gauges(app_name, pids)
  create_best_ask_price_gauges(app_name, pids)
  create_best_bid_size_gauges(app_name, pids)
  create_best_ask_size_gauges(app_name, pids)
  try:
    asyncio.run(
      monitor_extraction(
        extraction_subscriber,
        extraction_monitor,
        book_monitor,
        args.exchange,
        ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
