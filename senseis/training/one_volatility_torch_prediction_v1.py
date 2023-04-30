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
from senseis.torch_training_utility import get_device, get_loader_args, get_score_args

def build_parser():
  parser = argparse.ArgumentParser(description='parameters')
  parser.add_argument('--dir', type=str, help='training data directory', required=True)
  parser.add_argument('--file-prefix', type=str, help='training data filename prefix', required=True)
  parser.add_argument('--ticker-name', type=str, help='crypto ticker name', required=True)
  parser.add_argument('--train-config-filename', type=str, help='absolute path to training configuration file, can be on S3 or local', required=True)
  parser.add_argument('--upload', action='store_true', dest='upload', help='upload to S3')
  parser.add_argument('--no-upload', action='store_false', dest='upload', help='create model locally')
  parser.set_defaults(upload=False)
  parser.add_argument('--logfile', type=str, help='log filename', required=True)
  return parser

def generate_regression_targets(data, ticker, targets):
  max_shift = 0
  target_columns = []
  for ti in targets:
    data[f'{ticker}:{ti["target_name"]}'] = data[f'{ticker}:{ti["source_name"]}']
    data[f'{ticker}:{ti["target_name"]}'] = data[f'{ticker}:{ti["target_name"]}'].shift(-1 * ti['shift'])
    max_shift = max(max_shift, ti['shift'])
    target_columns.append(f'{ticker}:{ti["target_name"]}')
  data = data.drop([data.index[k] for k in range(-1, -1 * (max_shift + 1), -1)])
  return (data, target_columns)

def normalize_regression_data(X_train, Y_train, X_test, Y_test):
  X_train_mean = X_train.mean(axis=0)
  X_train_std  = X_train.std(axis=0)
  X_train_std[X_train_std == 0.] = 1.
  X_train_norm = (X_train - X_train_mean) / X_train_std
  X_test_norm = (X_test - X_train_mean) / X_train_std
  Y_train_mean = Y_train.mean(axis=0)
  Y_train_std  = Y_train.std(axis=0)
  Y_train_std[Y_train_std == 0.] = 1.
  Y_train_norm = (Y_train - Y_train_mean) / Y_train_std
  Y_test_norm  = (Y_test - Y_train_mean) / Y_train_std
  params = {
    'X_train_mean' : X_train_mean.tolist(),
    'X_train_std'  : X_train_std.tolist(),
    'Y_train_mean' : Y_train_mean.tolist(),
    'Y_train_std'  : Y_train_std.tolist(),
  }
  return (X_train_norm, Y_train_norm, X_test_norm, Y_test_norm, params)

def regression_train(model, device, loader, optimizer, loss, epoch, reporter):
  model.train()
  total_loss = 0.
  for batch_idx, (data, target) in enumerate(loader):
    optimizer.zero_grad()
    data, target = data.to(device), target.to(device)
    output = model(data)
    l = loss(output, target)
    l.backward()
    optimizer.step()
    total_loss += l.item()
  total_loss /= float(len(loader.dataset))
  reporter.report(typ='train', epoch=epoch, loss=total_loss)
  logging.info(f"Train Epoch {epoch} Loss: {total_loss}")

def regression_validate(model, device, loader, loss, train_epoch, reporter):
  model.eval()
  total_loss = 0.
  with torch.no_grad():
    for (data, target) in loader:
      data, target = data.to(device), target.to(device)
      output = model(data)
      total_loss += loss(output, target).item()
  total_loss /= float(len(loader.dataset))
  reporter.report(typ='eval', epoch=train_epoch, loss=total_loss)

def regression_train_validate(
    model,
    device,
    train_loader,
    eval_loader,
    optimizer,
    scheduler,
    loss,
    total_epoch,
    patience,
    patience_decay,
    reporter,
):
  validation_loss = float("inf")
  patience_count = patience
  patience = int(patience * patience_decay)
  reset_patience = False
  for epoch in range(total_epoch):
    regression_train(model, device, train_loader, optimizer, loss, epoch, reporter)
    regression_validate(model, device, eval_loader, loss, epoch, reporter)
    new_validation_loss = reporter.eval_loss(-1)
    logging.info("Epoch {} Validation Loss: {}".format(epoch, new_validation_loss))
    scheduler.step(new_validation_loss)
    if new_validation_loss < validation_loss:
      validation_loss = new_validation_loss
      patience_count = patience
      if reset_patience:
        patience = int(patience * patience_decay)
        reset_patience = False
    else:
      patience_count -= 1
      reset_patience = True
      if patience_count <= 0:
        logging.info("Improvement stopped at epoch {}, validation loss {}".format(epoch, new_validation_loss))
        break

def regression_validation_report(
    model,
    device,
    cpu,
    loader,
    reporter,
    input_columns,
    target_columns,
    minmaxvals,
    normalization_params,
    config,
):
  Y_mean = torch.tensor(normalization_params['Y_train_mean']).to(device)
  Y_std  = torch.tensor(normalization_params['Y_train_std']).to(device)
  model.eval()
  outputs = []
  targets = []
  with torch.no_grad():
    for (data, target) in loader:
      data, target = data.to(device), target.to(device)
      output = model(data)
      output = (output * Y_std) + Y_mean
      outputs.append(output)
      targets.append(target)
  outputs = torch.cat(outputs, dim=0).to(cpu).numpy()
  targets = torch.cat(targets, dim=0).to(cpu).numpy()
  r2 = r2_score(targets, outputs)
  corrs = [pearsonr(targets[:,i], outputs[:,i]).statistic for i in range(outputs.shape[1])]
  return {
    'model_type' : 'torch',
    'algorithm' : 'regression',
    'version' : 'v1',
    'nn_hidden_size' : config['nn_hidden_size'],
    'eval_r2' : r2,
    'eval_correlation' : corrs,
    'validation_loss' : reporter.eval_loss(-1),
    'train_loss' : reporter.train_loss(-1),
    'column_minmax' : minmaxvals,
    'input_columns' : input_columns,
    'target_columns' : target_columns,
    'normalization_params' : normalization_params,
  }

def save(args, model, metadata, start, end, dim):
  cs = 'peachone' # call sign of the model to allow multiple models doing the same thing
  ticker = args.ticker_name.replace('-', '')
  if args.upload:
    model_filename = f's3://{S3_BUCKET}/model/{ticker}_volatility_{dim}_torch_{cs}_v1_{start}_{end}.pt'
    metadata_filename = f's3://{S3_BUCKET}/metadata/{ticker}_volatility_{dim}_torch_{cs}_metadata_v1_{start}_{end}.json.gzip'
    with open(model_filename, 'wb', transport_params=dict(client=s3_client)) as fd:
      torch.save(model.state_dict(), fd)
    logging.info(f"Written out S3 model {model_filename}")
    with open(metadata_filename, 'wb', transport_params=dict(client=s3_client)) as fd:
      metadata_str = json.dumps(metadata)
      compressed = zlib.compress(metadata_str.encode())
      fd.write(compressed)
    logging.info(f"Written out S3 metadata {metadata_filename}")
    key = ticker + '_multi_volatility_' + cs
    config = {
      'ticker' : ticker,
      'model_type' : 'torch',
      'version' : 'v1',
      'call_sign' : cs,
      'target' : 'volatility',
      'target_dim' : dim,
      'start_epoch' : start,
      'end_epoch' : end,
    }
    config = json.dumps(config, sort_keys=True)
    StateDBApi.set_config(key, config)
    logging.info(f"Updated StateDB key {key}")
  else:
    model_filename = f'{args.dir}/{ticker}_volatility_{dim}_torch_{cs}_v1_{start}_{end}.pt'
    metadata_filename = f'{args.dir}/{ticker}_volatility_{dim}_torch_{cs}_metadata_v1_{start}_{end}.json.gzip'
    with open(model_filename, 'wb') as fd:
      torch.save(model.state_dict(), fd)
    logging.info(f"Written out local model {model_filename}")
    with open(metadata_filename, 'wb') as fd:
      metadata_str = json.dumps(metadata)
      compressed = zlib.compress(metadata_str.encode())
      fd.write(compressed)
    logging.info(f"Written out local metadata {metadata_filename}")

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
