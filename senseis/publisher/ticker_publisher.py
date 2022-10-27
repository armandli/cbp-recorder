import argparse
import logging
import asyncio

from senseis.configuration import TICKER_REQUEST_URL
from senseis.configuration import get_exchange_pids, is_ticker_exchange_name
from senseis.utility import setup_logging, build_publisher_parser
from senseis.extraction_producer_consumer import extraction_producer_consumer, product_extraction_producer, extraction_consumer, create_message
from senseis.metric_utility import setup_gateway, create_live_gauge, create_error_gauge

def main():
  parser = build_publisher_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_ticker_exchange_name(args.exchange):
    logging.error("Invalid exchange name. exit")
    return
  pids = get_exchange_pids(args.exchange)
  setup_gateway('cbp_{}_publisher'.format(args.exchange))
  create_live_gauge('cbp_{}_publisher'.format(args.exchange))
  create_error_gauge('cbp_{}_publisher'.format(args.exchange))
  asyncio.run(
    extraction_producer_consumer(
      product_extraction_producer,
      extraction_consumer,
      create_message,
      pids,
      TICKER_REQUEST_URL,
      args.period,
      args.exchange
    ))

if __name__ == '__main__':
  main()
