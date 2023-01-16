import logging
import argparse
import glob
import re
import uuid
import os
from datetime import datetime
import pytz
import pandas as pd
import numpy as np

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import STIME_COLNAME, STIME_EPOCH_COLNAME
from senseis.utility import setup_logging

def build_parser():
  parser = argparse.ArgumentParser(description='parameters')
  parser.add_argument('--file-prefix', type=str, help='filenam prefix', required=True)
  parser.add_argument('--dir', type=str, help='data file directory', default='../data')
  parser.add_argument('--output-filename-prefix', type=str, help='output filename path prefix', required=True)
  parser.add_argument('--chunk-size', type=int, help='chunking size used during processing', default=16)
  parser.add_argument('--max-groups', type=int, help='maximum number of groups', default=22)
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

  ssize = args.chunk_size
  files_epochs_groups = [files_epochs[k:k+ssize] for k in range(0, len(files_epochs), ssize)]
  # filter if there are too many groups to be handled in one go
  files_epochs_groups = files_epochs_groups[:args.max_groups]
  start_epoch = files_epochs_groups[0][0][0]
  end_epoch   = files_epochs_groups[-1][-1][1]
  tmpfiles = []
  for i, group in enumerate(files_epochs_groups):
    logging.info("Process group {} of {}".format(i+1, len(files_epochs_groups)))
    dfs = [pd.read_parquet(file) for _, _, file in group]
    data = pd.concat(dfs)
    data[STIME_EPOCH_COLNAME] = data.apply(lambda row: sequence_epoch(row), axis=1)
    data.set_index(STIME_EPOCH_COLNAME)
    data.sort_index(inplace=True)
    for column in columns:
      logging.info("Converting Column {}".format(column))
      if data[column].dtypes == 'float64':
        data[column] = np.array(data[column], dtype=np.float32)
      elif data[column].dtypes == 'int64':
        data[column] = np.array(data[column], dtype=np.int16)
    tmpfile_name = args.dir + "/" + uuid.uuid4().hex + ".parquet"
    tmpfiles.append(tmpfile_name)
    logging.info("write {}".format(tmpfile_name))
    data.to_parquet(tmpfile_name)
  logging.info("combining files")
  data = pd.DataFrame(columns=columns)
  for tmpfile in tmpfiles:
    df = pd.read_parquet(tmpfile)
    data = pd.concat([data, df])
  output_filename = "{}_{}_{}.parquet".format(args.output_filename_prefix, start_epoch, end_epoch)
  logging.info("writing out output file".format(output_filename))
  data.to_parquet(output_filename)
  logging.info("Cleaning up tempfiles")
  for tmpfile in tmpfiles:
    os.remove(tmpfile)

if __name__ == '__main__':
  main()
