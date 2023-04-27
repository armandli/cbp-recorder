import argparse
import logging
import asyncio

from senseis.utility import setup_logging
from senseis.configuration import is_pred_exchange_name
from senseis.statedb_api import StateDBApi
from senseis.extraction_producer_consumer import create_interval_state
from senseis.pipe_consumer_producer import data_subscriber
from senseis.pred_consumer_producer import pred_consumer_producer, pred_processor, process_pred_data, create_torch_model_state

from senseis.metric_utility import setup_gateway, setup_basic_gauges
from senseis.metric_utility import create_etl_process_time_histogram

def build_parser():
  parser = argparse.ArgumentParser(description='parameters')
  parser.add_argument('--period', type=int, help='periodicity in seconds', default=1)
  parser.add_argument('--etl', type=str, help='etl exchange name', default='etl_exchange_s2')
  parser.add_argument('--exchange', type=str, help='output exchange name', required=True)
  parser.add_argument('--model-key', type=str, help='key used to retrieve model information', required=True)
  parser.add_argument('--cache-time', type=int, help='model state cache refresh time in seconds', default=1800)
  parser.add_argument('--logfile', type=str, help='log filename', required=True)
  return parser

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_pred_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit")
    return
  if StateDBApi.get_config(args.model_key) is None:
    logging.error("Invalid model key with no configuration. exit")
    return
  app_name = f"cbp_pred_{args.model_key.replace('-','_')}_pipe"
  setup_gateway(app_name)
  setup_basic_gauges(app_name)
  create_etl_process_time_histogram(app_name)
  create_interval_state()
  try:
    asyncio.run(
      pred_consumer_producer(
        data_subscriber,
        pred_processor,
        process_pred_data,
        create_torch_model_state,
        args.exchange,
        args.etl,
        args.model_key,
        args.cache_time,
        args.period,
    ))
  except Exception as err:
    logging.error("Complete Failure: {}".format(err))
    print("Complete Failure: {}".format(err))

if __name__ == '__main__':
  main()
