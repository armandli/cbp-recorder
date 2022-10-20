import argparse
import logging
import asyncio
import aio_pika

from prometheus_client import push_to_gateway

from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD, is_valid_exchange_name
from senseis.utility import setup_logging
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import setup_gateway, get_collector_registry, get_job_name, create_live_gauge, get_live_gauge

def build_parser():
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('--exchange', type=str, help='queue exchange name', required=True)
  parser.add_argument('--logfile', type=str, help='logfile path', required=True)
  return parser

async def write_book(msg: aio_pika.IncomingMessage):
  async with msg.process():
    get_live_gauge().set_to_current_time()
    push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
    print(msg.body)

async def consume_book(exchange_name):
  connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  async with connection:
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(name=exchange_name, type='fanout')
    queue = await channel.declare_queue('', auto_delete=True)
    await queue.bind(exchange=exchange)
    logging.info("Listening to {}".format(exchange_name))
    await queue.consume(write_book)
    await asyncio.Future()

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  if not is_valid_exchange_name(args.exchange):
    logging.error("Invalid exchange. exit.")
    return
  setup_gateway('cbp_printer_{}'.format(args.exchange))
  create_live_gauge('cbp_printer_{}'.format(args.exchange))
  asyncio.run(consume_book(args.exchange))

if __name__ == '__main__':
  main()
