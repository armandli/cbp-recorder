import logging
import argparse
import glob
import re
import uuid
import os
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
  if len(files) == 0:
    logging.error("No files selected using pattern {} in directory {}".format(args.file_prefix, args.dir))
    return
  logging.info("first filename: {}".format(files[0]))
  files_epochs = [get_file_epochs(filename, args.dir, args.file_prefix) for filename in files]
  files_epochs.sort(key = lambda item : item[0])

  df = pd.read_parquet(files_epochs[0][2])
  columns = df.columns
  logging.info("number of columns: {}".format(len(columns)))

  ssize = 10
  files_epochs_groups = [files_epochs[k:k+ssize] for k in range(0, len(files_epochs) - ssize, ssize)]
  tmpfiles = []
  for group in files_epochs_groups:
    dfs = [pd.read_parquet(file) for _, _, file in group]
    data = pd.concat(dfs)
    data[STIME_EPOCH_COLNAME] = data.apply(lambda row: sequence_epoch(row), axis=1)
    data.set_index(STIME_EPOCH_COLNAME)
    data.sort_index(inplace=True)
    for column in columns:
      if data[column].dtypes == 'float64':
        data[column] = data[column].astype('float16')
      elif data[column].dtypes == 'int64':
        data[column] = data[column].astype('int16')
    tmpfile_name = args.dir + "/" + uuid.uuid4().hex + ".parquet"
    tmpfiles.append(tmpfile_name)
    logging.info("write {}".format(tmpfile_name))
    data.to_parquet(tmpfile_name)

  data = pd.DataFrame(columns=columns)
  for tmpfile in tmpfiles:
    logging.info("combine {}".format(tmpfile))
    df = pd.read_parquet(tmpfile)
    data = pd.concat([data, df])
    os.remove(tmpfile)

  logging.info("writing out output file".format(args.output_filename))
  data.to_parquet(args.output_filename)

if __name__ == '__main__':
  main()
