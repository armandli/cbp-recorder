import logging
import torch

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

