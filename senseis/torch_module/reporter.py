from abc import ABC, abstractmethod

class Reporter(ABC):
  @abstractmethod
  def report(self, typ, **metric):
    pass

  @abstractmethod
  def reset(self):
    pass

class SReporter(Reporter):
  def __init__(self):
    self.log = []

  def report(self, typ, **data):
    self.log.append((typ, data))

  def reset(self):
    self.log.clear()

  def loss(self, t):
    losses = []
    for (typ, data) in self.log:
      if typ == t:
        losses.append(data['loss'])
    return losses

  def loss(self, t, idx):
    if idx >= 0:
      count = 0
      for (typ, data) in self.log:
        if typ == t:
          if count == idx:
            return data['loss']
          count += 1
    else:
      count = -1
      for (typ, data) in reversed(self.log):
        if typ == t:
          if count == idx:
            return data['loss']
          count -= 1
    return float("inf")

  def eval_loss(self):
    return self.loss('eval')

  def train_loss(self):
    return self.loss('train')

  def eval_loss(self, idx):
    return self.loss('eval', idx)

  def train_loss(self, idx):
    return self.loss('train', idx)

  def get_record(self, t, idx):
    if idx >= 0:
      count = 0
      for (typ, data) in self.log:
        if typ == t:
          if count == idx:
            return data
          count += 1
    else:
      count = -1
      for (typ, data) in reversed(self.log):
        if typ == t:
          if count == idx:
            return data
          count -= 1
    return dict()

  def eval_record(self, idx):
    return self.get_record('eval', idx)

  def train_record(self, idx):
    return self.get_record('train', idx)
