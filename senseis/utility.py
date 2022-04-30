import argparse
import logging

def setup_logging(args):
  logger = logging.getLogger()
  logger.setLevel(logging.DEBUG)
  fhandler = logging.FileHandler(args.logfile, mode='w')
  formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-5s %(message)s', datefmt='%H:%M:%s')
  fhandler.setFormatter(formatter)
  logger.addHandler(fhandler)
  chandler = logging.StreamHandler()
  chandler.setLevel(logging.INFO)
  chandler.setFormatter(formatter)
  logger.addHandler(chandler)

def build_publisher_parser():
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument("--period", type=int, help="periodicity in seconds", default=1)
  parser.add_argument("--exchange", type=str, help="queue exchange name", required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

def build_subscriber_parser():
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('--period', type=int, help='output period, in minutes', default=1440)
  parser.add_argument('--exchange', type=str, help='queue exchange name', required=True)
  parser.add_argument('--logfile', type=str, help='logfile path', required=True)
  return parser
