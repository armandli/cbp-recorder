import glob
import re
import json
import boto3
from smart_open import open
import numpy as np
import pandas as pd

from senseis.configuration import S3_KEY, S3_SECRET, S3_ENDPOINT

s3_client = boto3.client("s3", aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET, endpoint_url=S3_ENDPOINT)

def get_file_epochs(filename, data_dir, prefix):
  result = re.match(r'{}/{}_([0-9]+)_([0-9]+).parquet$'.format(data_dir, prefix), filename)
  return (int(result.group(1)), int(result.group(2)), filename)

def get_start_end_epochs(filenames, data_dir, prefix):
  epochs = [get_file_epochs(filename, data_dir, prefix) for filename in filenames]
  return (min([s for (s, _, _) in epochs]), max([e for (_, e, _) in epochs]))

def get_all_files(data_dir, prefix):
  files = glob.glob(data_dir + '/{}*.parquet'.format(prefix))
  return files

def read_train_config(config_filename):
  with open(config_filename, transport_params=dict(client=s3_client)) as fd:
    config = json.load(fd)
    return config

def get_data(files, ticker, ticker_column_names, global_column_names):
  data = []
  ticker_columns = [f'{ticker}:{colname}' for colname in ticker_column_names]
  all_columns = ticker_columns + global_column_names
  for file in files:
    df = pd.read_parquet(file, columns=all_columns)
    data.append(df)
  data = pd.concat(data)
  data.sort_index(inplace=True)
  return data

def preprocess_data(data):
  data.fillna(method='ffill', inplace=True)
  data.replace([np.inf, -np.inf], method='ffill', inplace=True)
  minmaxvals = dict()
  for colname in data.columns:
    maxval = np.nanmax(data[colname][data[colname] != np.inf])
    minval = np.nanmin(data[colname][data[colname] != -np.inf])
    data[colname].replace([np.inf, -np.inf], [maxval, minval], inplace=True)
    minmaxvals[colname] = {'min': str(minval), 'max': str(maxval)} #str for saving to json
  data.fillna(0., inplace=True)
  return (data, minmaxvals)

def ts_train_test_split(X, Y, train_pct):
  n = X.shape[0]
  train_size = int(n * train_pct)
  X_train = X[:train_size]
  Y_train = Y[:train_size]
  X_test  = X[train_size:]
  Y_test  = Y[train_size:]
  return (X_train, Y_train, X_test, Y_test)

