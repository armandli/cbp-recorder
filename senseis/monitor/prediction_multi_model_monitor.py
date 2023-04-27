import argparse
import logging
import json
import boto3
from smart_open import open
import asyncio

from prometheus_client import push_to_gateway
from prometheus_client import Gauge

from senseis.configuration import S3_KEY, S3_SECRET, S3_ENDPOINT
from senseis.utility import setup_logging
from senseis.configuration import is_pred_exchange_name, get_pred_exchange_call_sign, get_pred_exchange_pid
from senseis.configuration import is_multi_volatility_pred_exchange
from senseis.pipe_consumer_producer import multi_monitor_extraction, multi_extraction_monitor, data_subscriber
from senseis.extraction_producer_consumer import create_interval_state
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name
from senseis.metric_utility import setup_basic_gauges

s3_client = boto3.client("s3", aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET, endpoint_url=S3_ENDPOINT)

def create_multi_volatility_prediction_gauges(app_name, exchanges, config_file):
  global MULTI_VOLATILITY_PREDICTION_GAUGES

  pids = [get_pred_exchange_pid(exchange) for exchange in exchanges if is_multi_volatility_pred_exchange(exchange)]
  call_signs = [get_pred_exchange_call_sign(exchange) for exchange in exchanges if is_multi_volatility_pred_exchange(exchange)]
  with open(config_file, 'r', transport_params=dict(clident=s3_client)) as fd:
    config = json.load(fd)
  target_names = [target['target_name'] for target in config['targets']]

  MULTI_VOLATILITY_PREDICTION_GAUGES = {
    pid : {
      call_sign : {
        f"{pid}:{target_name}" : Gauge(f"{app_name}_{pid.replace('-','_')}_{target_name}",
                                       f"{pid} {target_name}",
                                       registry=get_collector_registry())
        for target_name in target_names
      }
      for call_sign in call_signs
    }
    for pid in pids
  }

def get_volatility_prediction_gauges():
  return MULTI_VOLATILITY_PREDICTION_GAUGES 

def model_monitor(data, exchange_name):
  pid = get_pred_exchange_pid(exchange_name)
  call_sign = get_pred_exchange_call_sign(exchange_name)
  if is_multi_volatility_pred_exchange(exchange_name):
    gs = get_volatility_prediction_gauges()
    for (key, val) in data.items():
      if key in gs[pid][call_sign]:
        gs[pid][call_sign][key].set(data[key])
  push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())

def build_parser():
  parser = argparse.ArgumentParser(description='')
  parser.add_argument("--exchanges", nargs='+', help='list of prediction exchanges', required=True)
  parser.add_argument("--volatility-config-file", help="multi volatility prediction training configuration file in full path", required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  for exchange_name in args.exchanges:
    if not is_pred_exchange_name(exchange_name):
      logging.error(f"{exchange_name} is not a prediction exchange name. exit")
      return
  app_name = 'cbp_prediction_multi_monitor'
  setup_gateway(app_name)
  setup_basic_gauges(app_name)
  create_interval_state()
  create_multi_volatility_prediction_gauges(app_name, args.exchanges, args.volatility_config_file)
  try:
    asyncio.run(
        multi_monitor_extraction(
        data_subscriber,
        multi_extraction_monitor,
        model_monitor,
        args.exchanges,
    ))
  except Exception as err:
    logging.error(f"Complete Failure: {err}")
    print(f"Complete Failure: {err}")

if __name__ == '__main__':
  main()
