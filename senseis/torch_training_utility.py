import argparse
import logging
import torch

from sklearn.metrics import r2_score
from scipy.stats import pearsonr

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

def get_device():
  use_cuda = torch.cuda.is_available()
  use_mps  = torch.backends.mps.is_built()
  if use_cuda:
    logging.info("Using device CUDA")
    return torch.device('cuda')
  elif use_mps:
    logging.info("Using device MPS")
    return torch.device('mps')
  else:
    logging.info("Using device CPU")
    return torch.device('cpu')

def get_loader_args(config, device):
  args = config['loader_args']
  if device.type == 'cuda':
    args.update({'pin_memory' : True})
  return args

def get_score_args(config, device):
  args = config['score_args']
  if device.type == 'cuda':
    args.update({'pin_memory' : True})
  return args

def normalize_classification_data(X_train, Y_train, X_test, Y_test):
  X_train_mean = X_train.mean(axis=0)
  X_train_std = X_train.std(axis=0)
  X_train_std[X_train_std == 0.] = 1.
  X_train_norm = (X_train - X_train_mean) / X_train_std
  X_test_norm = (X_test - X_train_mean) / X_train_std
  params = {
    'X_train_mean' : X_train_mean.tolist(),
    'X_train_std'  : X_train_std.tolist(),
    'Y_train_mean' : [0. for _ in range(Y_train.shape[1])], # need to generate to accomodate regression
    'Y_train_std'  : [1. for _ in range(Y_train.shape[1])],
  }
  return (X_train_norm, X_test_norm, params)

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

