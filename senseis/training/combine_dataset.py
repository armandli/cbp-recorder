import logging
import argparse
import glob
import re
from datetime import datetime
import pytz
import pandas as pd

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import STIME_COLNAME, STIME_EPOCH_COLNAME
from senseis.utility import setup_logging


def build_parser():
  parser = argparse.ArgumentParser(description='parameters')
  parser.add_argument('--file-prefix', type=str, help='filenam prefix', required=True)
  parser.add_argument('--dir', type=str, help='data file directory', default='../data')
  parser.add_argument('--output-filename', type=str, help='output filename complete path', required=True)
  parser.add_argument('--logfile', type=str, help='log filename', required=True)
  return parser

def get_file_epochs(filename, data_dir, prefix):
  result = re.match(r'{}/{}_([0-9]+)_([0-9]+).parquet$'.format(data_dir, prefix), filename)
  return (int(result.group(1)), int(result.group(2)), filename)

def get_all_files(data_dir, prefix):
  files = glob.glob(data_dir + '/{}*.parquet'.format(prefix))
  return files

def sequence_epoch(row):
  utc = pytz.timezone('UTC')
  return int(datetime.strptime(row[STIME_COLNAME], DATETIME_FORMAT).timestamp())

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  files = get_all_files(args.dir, args.file_prefix)
  logging.info("Combining {} files".format(len(files)))
  if len(files) > 0:
    logging.info("first filename: {}".format(files[0]))
  files_epochs = [get_file_epochs(filename, args.dir, args.file_prefix) for filename in files]
  files_epochs.sort(key = lambda item : item[0])
  dfs = [pd.read_parquet(file) for _, _, file in files_epochs]
  data = pd.concat(dfs)
  data[STIME_EPOCH_COLNAME] = data.apply(lambda row: sequence_epoch(row), axis=1)
  data.set_index(STIME_EPOCH_COLNAME)
  data.sort_index(inplace=True)
  data.to_parquet(args.output_filename)

if __name__ == '__main__':
  main()
