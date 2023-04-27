import argparse
import logging
import math
import asyncio

from prometheus_client import push_to_gateway
from prometheus_client import Gauge

from senseis.configuration import get_all_pids
from senseis.utility import setup_logging
from senseis.extraction_producer_consumer import monitor_extraction, extraction_monitor, extraction_subscriber
from senseis.extraction_producer_consumer import create_interval_state
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name
from senseis.metric_utility import setup_basic_gauges

def create_book_return_gauges(app_name, pids):
  global BOOK_RETURN_GAUGES
  BOOK_RETURN_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + "_book_return",
                "{} book return".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_return_gauges():
  return BOOK_RETURN_GAUGES

def create_ba_imbalance_gauges(app_name, pids):
  global BOOK_BA_IMBALANCE_GAUGES
  BOOK_BA_IMBALANCE_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_ba_imbalance',
                "{} book bid ask imbalance".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_ba_imbalance_gauges():
  return BOOK_BA_IMBALANCE_GAUGES

def create_extended_ba_imbalance_gauges(app_name, pids):
  global BOOK_EXTENDED_BA_IMBALANCE_GAUGES
  BOOK_EXTENDED_BA_IMBALANCE_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_extended_ba_imbalance',
                "{} book extended bid ask imbalance".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_extended_ba_imbalance_gauges():
  return BOOK_EXTENDED_BA_IMBALANCE_GAUGES

def create_ba_spread_gauges(app_name, pids):
  global BOOK_BA_SPREAD_GAUGES
  BOOK_BA_SPREAD_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_ba_spread',
                "{} book bid ask spread".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_ba_spread_gauges():
  return BOOK_BA_SPREAD_GAUGES

def create_wap_price_gauges(app_name, pids):
  global BOOK_WAP_PRICE_GAUGES
  BOOK_WAP_PRICE_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-', '_') + '_book_wap_price',
                "{} book wap".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_wap_price_gauges():
  return BOOK_WAP_PRICE_GAUGES

def create_bid_volume_change_gauges(app_name, pids):
  global BOOK_BID_VOLUME_CHANGE_GAUGES
  BOOK_BID_VOLUME_CHANGE_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_bid_volume_change',
                "{} bid volume change".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_bid_volume_change_gauges():
  return BOOK_BID_VOLUME_CHANGE_GAUGES

def create_ask_volume_change_gauges(app_name, pids):
  global BOOK_ASK_VOLUME_CHANGE_GAUGES
  BOOK_ASK_VOLUME_CHANGE_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_ask_volume_change',
                "{} ask volume change".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_ask_volume_change_gauges():
  return BOOK_ASK_VOLUME_CHANGE_GAUGES

def create_book_volatility_27_gauges(app_name, pids):
  global BOOK_VOLATILITY_27_GAUGES
  BOOK_VOLATILITY_27_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_volatility_27',
                "{} book volatility 27s".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_volatility_27_gauges():
  return BOOK_VOLATILITY_27_GAUGES

def create_book_volatility_81_gauges(app_name, pids):
  global BOOK_VOLATILITY_81_GAUGES
  BOOK_VOLATILITY_81_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_volatility_81',
                "{} book volatility 81s".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_volatility_81_gauges():
  return BOOK_VOLATILITY_81_GAUGES

def create_book_volatility_324_gauges(app_name, pids):
  global BOOK_VOLATILITY_324_GAUGES
  BOOK_VOLATILITY_324_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_volatility_324',
                "{} book volatility 324s".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_volatility_324_gauges():
  return BOOK_VOLATILITY_324_GAUGES

def create_book_volatility_960_gauges(app_name, pids):
  global BOOK_VOLATILITY_960_GAUGES
  BOOK_VOLATILITY_960_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_volatility_960',
                "{} book volatility 960s".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_volatility_960_gauges():
  return BOOK_VOLATILITY_960_GAUGES

def create_book_beta_648_gauges(app_name, pids):
  global BOOK_BETA_648_GAUGES
  BOOK_BETA_648_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_book_beta_648',
                "{} beta 648s".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_beta_648_gauges():
  return BOOK_BETA_648_GAUGES

def create_trade_volume_27_sum_gauges(app_name, pids):
  global TRADE_VOLUME_27_SUM_GAUGES
  TRADE_VOLUME_27_SUM_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_volume_27_sum',
                "{} trade volume 27s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_volume_27_sum_gauges():
  return TRADE_VOLUME_27_SUM_GAUGES

def create_trade_volume_81_sum_gauges(app_name, pids):
  global TRADE_VOLUME_81_SUM_GAUGES
  TRADE_VOLUME_81_SUM_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_volume_81_sum',
                "{} trade volume 81s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_volume_81_sum_gauges():
  return TRADE_VOLUME_81_SUM_GAUGES

def create_trade_volume_324_sum_gauges(app_name, pids):
  global TRADE_VOLUME_324_SUM_GAUGES
  TRADE_VOLUME_324_SUM_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_volume_324_sum',
                "{} trade volume 324s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_volume_324_sum_gauges():
  return TRADE_VOLUME_324_SUM_GAUGES

def create_trade_volume_960_sum_gauges(app_name, pids):
  global TRADE_VOLUME_960_SUM_GAUGES
  TRADE_VOLUME_960_SUM_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_volume_960_sum',
                "{} trade volume 960s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_volume_960_sum_gauges():
  return TRADE_VOLUME_960_SUM_GAUGES

def create_trade_price_27_avg_gauges(app_name, pids):
  global TRADE_PRICE_27_AVG_GAUGES
  TRADE_PRICE_27_AVG_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_price_27_avg',
                "{} trade price 27s avg".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_price_27_avg_gauges():
  return TRADE_PRICE_27_AVG_GAUGES

def create_trade_price_81_avg_gauges(app_name, pids):
  global TRADE_PRICE_81_AVG_GAUGES
  TRADE_PRICE_81_AVG_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_price_81_avg',
                "{} trade price 81s avg".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_price_81_avg_gauges():
  return TRADE_PRICE_81_AVG_GAUGES

def create_trade_price_324_avg_gauges(app_name, pids):
  global TRADE_PRICE_324_AVG_GAUGES
  TRADE_PRICE_324_AVG_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_price_324_avg',
                "{} trade price 324s avg".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_price_324_avg_gauges():
  return TRADE_PRICE_324_AVG_GAUGES

def create_trade_price_960_avg_gauges(app_name, pids):
  global TRADE_PRICE_960_AVG_GAUGES
  TRADE_PRICE_960_AVG_GAUGES = {
    pid : Gauge(app_name + "_" + pid.replace('-','_') + '_trade_price_960_avg',
                "{} trade price 960s avg".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_trade_price_960_avg_gauges():
  return TRADE_PRICE_960_AVG_GAUGES

def create_book_return_9_sum_gauges(app_name, pids):
  global BOOK_RETURN_9_SUM_GAUGES
  BOOK_RETURN_9_SUM_GAUGES = {
    pid : Gauge(app_name + '_' + pid.replace('-','_') + '_book_return_9_sum',
                "{} book return 9s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_return_9_sum_gauges():
  return BOOK_RETURN_9_SUM_GAUGES

def create_book_return_27_sum_gauges(app_name, pids):
  global BOOK_RETURN_27_SUM_GAUGES
  BOOK_RETURN_27_SUM_GAUGES = {
    pid : Gauge(app_name + '_' + pid.replace('-','_') + '_book_return_27_sum',
                "{} book return 27s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_return_27_sum_gauges():
  return BOOK_RETURN_27_SUM_GAUGES

def create_book_return_81_sum_gauges(app_name, pids):
  global BOOK_RETURN_81_SUM_GAUGES
  BOOK_RETURN_81_SUM_GAUGES = {
    pid : Gauge(app_name + '_' + pid.replace('-','_') + '_book_return_81_sum',
                "{} book return 81s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_return_81_sum_gauges():
  return BOOK_RETURN_81_SUM_GAUGES

def create_book_return_324_sum_gauges(app_name, pids):
  global BOOK_RETURN_324_SUM_GAUGES
  BOOK_RETURN_324_SUM_GAUGES = {
    pid : Gauge(app_name + '_' + pid.replace('-','_') + '_book_return_324_sum',
                "{} book return 324s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_return_324_sum_gauges():
  return BOOK_RETURN_324_SUM_GAUGES

def create_book_return_960_sum_gauges(app_name, pids):
  global BOOK_RETURN_960_SUM_GAUGES
  BOOK_RETURN_960_SUM_GAUGES = {
    pid : Gauge(app_name + '_' + pid.replace('-','_') + '_book_return_960_sum',
                "{} book return 960s sum".format(pid),
                registry=get_collector_registry())
    for pid in pids
  }

def get_book_return_960_sum_gauges():
  return BOOK_RETURN_960_SUM_GAUGES


def prediction_etl_s2_monitor(data, exchange_name):
  pids = get_all_pids()
  book_return_gs = get_book_return_gauges()
  book_baimba_gs = get_ba_imbalance_gauges()
  book_baeimba_gs = get_extended_ba_imbalance_gauges()
  book_baspread_gs = get_ba_spread_gauges()
  book_wap_gs = get_wap_price_gauges()
  book_bvc_gs = get_bid_volume_change_gauges()
  book_avc_gs = get_ask_volume_change_gauges()
  book_volatility27_gs = get_book_volatility_27_gauges()
  book_volatility81_gs = get_book_volatility_81_gauges()
  book_volatility324_gs = get_book_volatility_324_gauges()
  book_volatility960_gs = get_book_volatility_960_gauges()
  book_beta648_gs = get_book_beta_648_gauges()
  trade_volume27s_gs = get_trade_volume_27_sum_gauges()
  trade_volume81s_gs = get_trade_volume_81_sum_gauges()
  trade_volume324s_gs = get_trade_volume_324_sum_gauges()
  trade_volume960s_gs = get_trade_volume_960_sum_gauges()
  trade_price27a_gs = get_trade_price_27_avg_gauges()
  trade_price81a_gs = get_trade_price_81_avg_gauges()
  trade_price324a_gs = get_trade_price_324_avg_gauges()
  trade_price960a_gs = get_trade_price_960_avg_gauges()
  book_return9s_gs = get_book_return_9_sum_gauges()
  book_return27s_gs = get_book_return_27_sum_gauges()
  book_return81s_gs = get_book_return_81_sum_gauges()
  book_return324s_gs = get_book_return_324_sum_gauges()
  book_return960s_gs = get_book_return_960_sum_gauges()
  for pid in pids:
    book_return_gs[pid].set(data["{}:book_return".format(pid)])
    book_baimba_gs[pid].set(data["{}:ba_imbalance".format(pid)])
    book_baeimba_gs[pid].set(data["{}:ba_imbalance_extended".format(pid)])
    book_baspread_gs[pid].set(data["{}:ba_spread".format(pid)])
    book_wap_gs[pid].set(data["{}:wap".format(pid)])
    book_bvc_gs[pid].set(data["{}:bid_volume_change".format(pid)])
    book_avc_gs[pid].set(data["{}:ask_volume_change".format(pid)])
    book_volatility27_gs[pid].set(data["{}:book_volatility_27".format(pid)])
    book_volatility81_gs[pid].set(data["{}:book_volatility_81".format(pid)])
    book_volatility324_gs[pid].set(data["{}:book_volatility_324".format(pid)])
    book_volatility960_gs[pid].set(data["{}:book_volatility_960".format(pid)])
    book_beta648_gs[pid].set(data["{}:book_beta_648".format(pid)])
    book_return9s_gs[pid].set(data["{}:book_return_9sum".format(pid)])
    book_return27s_gs[pid].set(data["{}:book_return_27sum".format(pid)])
    book_return81s_gs[pid].set(data["{}:book_return_81sum".format(pid)])
    book_return324s_gs[pid].set(data["{}:book_return_324sum".format(pid)])
    book_return960s_gs[pid].set(data["{}:book_return_960sum".format(pid)])

    tv27s = data["{}:trade_volume_27sum".format(pid)]
    if not math.isnan(tv27s):
      trade_volume27s_gs[pid].set(tv27s)
    tv81s = data["{}:trade_volume_81sum".format(pid)]
    if not math.isnan(tv81s):
      trade_volume81s_gs[pid].set(tv81s)
    tv324s = data["{}:trade_volume_324sum".format(pid)]
    if not math.isnan(tv324s):
      trade_volume324s_gs[pid].set(tv324s)
    tv960s = data["{}:trade_volume_960sum".format(pid)]
    if not math.isnan(tv960s):
      trade_volume960s_gs[pid].set(tv960s)
    tp27a = data["{}:trade_avg_price_27avg".format(pid)]
    if not math.isnan(tp27a):
      trade_price27a_gs[pid].set(tp27a)
    tp81a = data["{}:trade_avg_price_81avg".format(pid)]
    if not math.isnan(tp81a):
      trade_price81a_gs[pid].set(tp81a)
    tp324a = data["{}:trade_avg_price_324avg".format(pid)]
    if not math.isnan(tp324a):
      trade_price324a_gs[pid].set(tp324a)
    tp960a = data["{}:trade_avg_price_960avg".format(pid)]
    if not math.isnan(tp960a):
      trade_price960a_gs[pid].set(tp960a)

  push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())

def build_parser():
  parser = argparse.ArgumentParser(description='')
  parser.add_argument("--exchange", type=str, help="queue exchange name", default='etl_exchange_s2')
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  app_name = 'cbp_prediction_etl_s2_monitor'
  setup_gateway(app_name)
  setup_basic_gauges(app_name)
  create_interval_state()
  pids = get_all_pids()
  create_book_return_gauges(app_name, pids)
  create_ba_imbalance_gauges(app_name, pids)
  create_extended_ba_imbalance_gauges(app_name, pids)
  create_ba_spread_gauges(app_name, pids)
  create_wap_price_gauges(app_name, pids)
  create_bid_volume_change_gauges(app_name, pids)
  create_ask_volume_change_gauges(app_name, pids)
  create_book_volatility_27_gauges(app_name, pids)
  create_book_volatility_81_gauges(app_name, pids)
  create_book_volatility_324_gauges(app_name, pids)
  create_book_volatility_960_gauges(app_name, pids)
  create_book_beta_648_gauges(app_name, pids)
  create_trade_volume_27_sum_gauges(app_name, pids)
  create_trade_volume_81_sum_gauges(app_name, pids)
  create_trade_volume_324_sum_gauges(app_name, pids)
  create_trade_volume_960_sum_gauges(app_name, pids)
  create_trade_price_27_avg_gauges(app_name, pids)
  create_trade_price_81_avg_gauges(app_name, pids)
  create_trade_price_324_avg_gauges(app_name, pids)
  create_trade_price_960_avg_gauges(app_name, pids)
  create_book_return_9_sum_gauges(app_name, pids)
  create_book_return_27_sum_gauges(app_name, pids)
  create_book_return_81_sum_gauges(app_name, pids)
  create_book_return_324_sum_gauges(app_name, pids)
  create_book_return_960_sum_gauges(app_name, pids)
  try:
    asyncio.run(
      monitor_extraction(
        extraction_subscriber,
        extraction_monitor,
        prediction_etl_s2_monitor,
        args.exchange,
      ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
