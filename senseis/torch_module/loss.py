import torch
from torch import nn
import torch.nn.functional as F

class CorrelationLoss(nn.Module):
  def __init__(self):
    super(CorrelationLoss, self).__init__()
  def forward(self, input, target):
    mo = torch.mean(input, dim=1, keepdim=True)
    mt = torch.mean(target, dim=1, keepdim=True)
    var_o = torch.var(input, dim=1, keepdim=True, unbiased=False)
    var_t = torch.var(target, dim=1, keepdim=True, unbaised=False)
    cov = torch.mean(input * target, dim=1, keepdim=True) - mo * mt
    corr = torch.mean(-1. * cov / torch.sqrt(var_o * var_t))
    return corr

class WeightedBCELoss(nn.Module):
  def __init__(self):
    super(WeightedBCELoss, self).__init__()

  def forward(self, input, target, weight):
    return F.binary_cross_entropy(input, target, weight=weight, reduction='mean')

class WeightedMSELoss(nn.Module):
  def __init__(self):
    super(WeightedMSELoss, self).__init__()

  def forward(self, input, target, weight):
    return torch.mean(weight * (input - target) ** 2.)
