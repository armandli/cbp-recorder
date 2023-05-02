import logging
import argparse
import glob
import re
import json
import boto3
from smart_open import open
import zlib
import numpy as np
import pandas as pd
import torch
from torch import nn
from torch import utils
from torch import optim
from sklearn.metrics import r2_score
from scipy.stats import pearsonr

from senseis.utility import setup_logging
from senseis.configuration import S3_BUCKET
from senseis.torch_module.reporter import SReporter
from senseis.torch_module.dataset import XYDataset
from senseis.torch_module.model import RegressorV1
from senseis.statedb_api import StateDBApi
from senseis.training_utility import s3_client
from senseis.training_utility import get_file_epochs, get_start_end_epochs, get_all_files
from senseis.training_utility import read_train_config, get_data, preprocess_data, ts_train_test_split
from senseis.torch_training_utility import build_parser, get_device, get_loader_args, get_score_args
from senseis.torch_training_utility import normalize_regression_data
from senseis.torch_training_utility import regression_train_validate
from senseis.torch_training_utility import regression_validation_report

def generate_regression_targets(data, ticker, targets):
  max_shift = 0
  target_columns = []
  for ti in targets:
    data[f'{ticker}:{ti["target_name"]}'] = data[f'{ticker}:{ti["source_name"]}']
    data[f'{ticker}:{ti["target_name"]}'] = data[f'{ticker}:{ti["target_name"]}'].shift(-1 * ti['shift'])
    data[f'{ticker}:{ti["target_name"]}'] = data[f'{ticker}:{ti["target_name"]}'] - data[f'{ticker}:{ti["source_name"]}']
    max_shift = max(max_shift, ti['shift'])
    target_columns.append(f'{ticker}:{ti["target_name"]}')
  data = data.drop([data.index[k] for k in range(-1, -1 * (max_shift + 1), -1)])
  return (data, target_columns)

def save(args, model, metadata, start, end, dim):
  cs = 'peachone'
  ticker = args.ticker_name.replace('-', '')
  if args.upload:
    model_filename = f's3://{S3_BUCKET}/model/{ticker}_vdelta_{dim}_torch_{cs}_v1_{start}_{end}.pt'
    metadata_filename = f's3://{S3_BUCKET}/metadata/{ticker}_vdelta_{dim}_torch_{cs}_metadata_v1_{start}_{end}.json.gzip'
    with open(model_filename, 'wb', transport_params=dict(client=s3_client)) as fd:
      torch.save(model.state_dict(), fd)
    logging.info(f"Written out S3 model {model_filename}")
    with open(metadata_filename, 'wb', transport_params=dict(client=s3_client)) as fd:
      metadata_str = json.dumps(metadata)
      compressed = zlib.compress(metadata_str.encode())
      fd.write(compressed)
    logging.info(f"Written out S3 metadata {metadata_filename}")
    key = ticker + '_multi_vdelta_' + cs
    config = {
      'ticker' : ticker,
      'model_type' : 'torch',
      'version' : 'v1',
      'call_sign' : cs,
      'target' : 'vdelta',
      'target_dim' : dim,
      'start_epoch' : start,
      'end_epoch' : end,
    }
    config = json.dumps(config, sort_keys=True)
    StateDBApi.set_config(key, config)
    logging.info("Updated StateDB key {key}")
  else:
    model_filename = f'{args.dir}/{ticker}_vdelta_{dim}_torch_{cs}_v1_{start}_{end}.ptr'
    metadata_filename = f'{args.dir}/{ticker}_vdelta_{dim}_torch_{cs}_metadata_v1_{start}_{end}.json.gzip'
    with open(model_filename, 'wb') as fd:
      torch.save(model.state_dict(), fd)
    logging.info(f"Written out local model {model_filename}")
    with open(metadata_filename, 'wb') as fd:
      metadata_str = json.dumps(metadata)
      compressed = zlib.compress(metadata_str.encode())
      fd.write(compressed)
    logging.info(f"Written out local metadata {metadata_filename}")

#TODO: this is the same as volatility main, refactor
def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  files = get_all_files(args.dir, args.file_prefix)
  start_epoch, end_epoch = get_start_end_epochs(files, args.dir, args.file_prefix)
  device = get_device()
  cpu = torch.device('cpu')
  config = read_train_config(args.train_config_filename)
  logging.info("Generating dataset")

  data = get_data(files, args.ticker_name, config['ticker_column_names'], config['global_column_names'])
  data, minmaxvals = preprocess_data(data)
  data, target_columns = generate_regression_targets(data, args.ticker_name, config['targets'])
  input_columns = [col for col in data.columns if col not in target_columns]
  X = np.float32(data[input_columns].to_numpy())
  Y = np.float32(data[target_columns].to_numpy())
  X_train, Y_train, X_test, Y_test = ts_train_test_split(X, Y, config['train_pct'])
  X_train_norm, Y_train_norm, X_test_norm, Y_test_norm, normalization_params = normalize_regression_data(X_train, Y_train, X_test, Y_test)

  logging.info(f"Training data size: {X_train.shape[0]} Eval data size: {X_test.shape[0]}")
  logging.info(f"Input dimension: {X_train.shape[1]} output dimension: {Y_train.shape[1]}")
  logging.info(f"Training Configuration: learning_rate: {config['learning_rate']} model size: {config['nn_hidden_size']}")

  trainset = XYDataset(X_train_norm, Y_train_norm)
  evalset = XYDataset(X_test_norm, Y_test_norm)
  scoreset = XYDataset(X_test_norm, Y_test)

  loader_args = get_loader_args(config, device)
  score_args = get_score_args(config, device)

  train_loader = utils.data.DataLoader(trainset, **loader_args)
  eval_loader = utils.data.DataLoader(evalset, **loader_args)
  score_loader = utils.data.DataLoader(scoreset, **score_args)

  model = RegressorV1(len(input_columns), len(target_columns), config['nn_hidden_size'])
  model = model.to(device)

  optimizer = optim.Adam(model.parameters(recurse=True), lr=config['learning_rate'])
  scheduler = optim.lr_scheduler.ReduceLROnPlateau(
                  optimizer,
                  mode='min',
                  patience=config['patience'] / 4,
                  threshold=config['threshold']
  )
  loss = nn.MSELoss()
  reporter = SReporter()
  regression_train_validate(
      model,
      device,
      train_loader,
      eval_loader,
      optimizer,
      scheduler,
      loss,
      config['total_epochs'],
      config['patience'],
      config['patience_decay'],
      reporter,
  )

  logging.info("Training Complete")

  metadata = regression_validation_report(
      model,
      device,
      cpu,
      score_loader,
      reporter,
      input_columns,
      target_columns,
      minmaxvals,
      normalization_params,
      config,
  )

  logging.info("Final Validation Complete")
  logging.info(f"Training Loss: {metadata['train_loss']}")
  logging.info(f"Validation Loss: {metadata['validation_loss']}")
  logging.info(f"R2: {metadata['eval_r2']}")
  logging.info(f"Pearson: {metadata['eval_correlation']}")

  save(args, model, metadata, start_epoch, end_epoch, len(target_columns))


if __name__ == '__main__':
  main()
