import argparse
import logging
from typing import Tuple
import glob
import re
from functools import partial
import pandas as pd
import numpy as np
import xgboost as xgb
import optuna

from senseis.utility import setup_logging

def build_parser():
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('--file-prefix', type=str, help='filename prefix', required=True)
  parser.add_argument('--dir', type=str, help='data file directory', default='data')
  parser.add_argument('--ticker', type=str, help='crypto ticker', required=True)
  parser.add_argument('--ts', type=list, help='time series intervals', default=[27, 81, 162, 324, 648, 960])
  parser.add_argument('--infval', type=float, help='the value to replace infinity with', default=1000000.)
  parser.add_argument('--ninfval', type=float, help='the value to replace negative infinity with', default=-1000000.)
  parser.add_argument('--pct', type=float, help='train test split percentage', default=0.2)
  parser.add_argument('--ntrials', type=int, help='number of hyperparameter optimization trials', default=80)
  parser.add_argument('--logfile', type=str, help='log filename', required=True)
  return parser

def get_all_files(data_dir, prefix):
  files = glob.glob(data_dir + '/{}*.parquet'.format(prefix))
  return files

def get_file_columns(files):
  if len(files) == 0:
    return
  df = pd.read_parquet(files[0])
  columns = df.columns
  return columns

def get_data(files, columns):
  data = pd.DataFrame(columns=columns)
  for file in files:
    df = pd.read_parquet(file, columns=columns)
    data = pd.concat([data, df])
  data.sort_index(inplace=True)
  return data

#TODO: compute those constants some dynamtic way
def weight(y: np.ndarray) -> np.ndarray:
  y2 = y**2.0
  return 1000.0 * y2 / (y2 + np.exp(1.0 - 480050.0 * y2)) + 1

def weighted_rmse(predt: np.ndarray, dtrain: xgb.DMatrix) -> np.ndarray:
  y = dtrain.get_label().reshape(predt.shape)
  w = weight(y)
  return np.sqrt(np.sum(w * (predt - y)**2., axis=1) / predt.shape[1])

def wmse_gradient(predt: np.ndarray, dtrain: xgb.DMatrix) -> np.ndarray:
  y = dtrain.get_label().reshape(predt.shape)
  w = weight(y)
  return (w * (predt - y)).reshape(y.size)

def wmse_hessian(predt: np.ndarray, dtrain: xgb.DMatrix) -> np.ndarray:
  y = dtrain.get_label().reshape(predt.shape)
  w = weight(y)
  return w.reshape(predt.size)

def wmse_loss(predt: np.ndarray, dtrain: xgb.DMatrix) -> Tuple[np.ndarray, np.ndarray]:
  grad = wmse_gradient(predt, dtrain)
  hess = wmse_hessian(predt, dtrain)
  return grad, hess

def wrmse_eval(predt: np.ndarray, dtrain: xgb.DMatrix) -> Tuple[str, float]:
  wr = weighted_rmse(predt, dtrain)
  v = np.sum(wr) / wr.shape[0]
  return ('WRMSE', v)

#TODO: make ranges program options
def create_objective(XYTrain, XYTest, ntarget, trial):
  logging.info("Running trial")
  param = {
      'verbosity': 0,
      'num_target': ntarget,
      'eta': trial.suggest_float('eta', 1e-8, 0.5, log=True),
      'max_depth': trial.suggest_int('max_depth', 6, 50),
      'tree_method': trial.suggest_categorical('tree_method', ['hist', 'approx', 'exact']),
  }
  rounds = trial.suggest_int('round', 1, 30)
  model = xgb.train(
      params=param,
      dtrain=XYTrain,
      num_boost_round=rounds,
      obj=wmse_loss,
  )
  Y_predict = model.inplace_predict(XYTest.get_data())
  _, loss = wrmse_eval(Y_predict, XYTest)
  return loss

def get_book_return_columns(ticker, ts):
  target_columns = [("{}:book_return_{}sum".format(ticker, t), t, "{}:book_return_{}sum=Target".format(ticker, t)) for t in ts]
  return target_columns

def ts_train_test_split(X, Y, pct):
  n = X.shape[0]
  train_size = int(n * (1 - pct))
  X_train = X[:train_size]
  Y_train = Y[:train_size]
  X_test = X[train_size:]
  Y_test = Y[train_size:]
  return (X_train, Y_train, X_test, Y_test)

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  files = get_all_files(args.dir, args.file_prefix)
  if len(files) == 0:
    logging.error("No files selected using pattern {} in directory {}".format(args.file_prefix, args.dir))
    return
  columns = [col for col in get_file_columns(files) if args.ticker in col]
  logging.info("Columns used:")
  for col in columns:
    logging.info("{}".format(col))
  data = get_data(files, columns)
  target_columns = get_book_return_columns(args.ticker, args.ts)
  target_column_names = [colname for (_, _, colname) in target_columns]
  max_offset = max(args.ts)
  for (colname, offset, tcolname) in target_columns:
    data[tcolname] = data[colname]
    data[tcolname] = data[tcolname].shift(-1 * offset)
  data = data.drop([data.index[k] for k in range(-1, -1 * (max_offset+1), -1)])
  logging.info("Created dataset of size: {}".format(data.shape))
  data.fillna(0., inplace=True)
  data.replace([float("inf"), float("-inf")], [args.infval, args.ninfval], inplace=True)
  X = data[columns]
  X = X.rename(columns=lambda x: re.sub('[^A-Za-z0-9_]+', '', x))
  Y = data[target_column_names]
  X_train, Y_train, X_test, Y_test = ts_train_test_split(X, Y, args.pct)
  XYTrain = xgb.DMatrix(X_train, Y_train)
  XYTest = xgb.DMatrix(X_test, Y_test)
  objective = partial(create_objective, XYTrain, XYTest, Y.shape[1])
  study = optuna.create_study(direction='minimize')
  logging.info("Starting optimization")
  study.optimize(objective, n_trials=args.ntrials)
  logging.info("Best Params: {}".format(study.best_params))
  print("Best params: {}".format(study.best_params))

if __name__ == '__main__':
  main()
