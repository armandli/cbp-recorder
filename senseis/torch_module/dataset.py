from torch import utils

class XYDataset(utils.data.Dataset):
  def __init__(self, X, Y):
    self.X = X
    self.Y = Y

  def __getitem__(self, idx):
    x = self.X[idx]
    y = self.Y[idx]
    return (x, y)

  def __len__(self):
    return self.X.shape[0]

class XYWDataset(utils.data.Dataset):
  def __init__(self, X, Y, W):
    self.X = X
    self.Y = Y
    self.W = W

  def __getitem__(self, idx):
    x = self.X[idx]
    y = self.Y[idx]
    w = self.W[idx]
    return (x, y, w)

  def __len__(self):
    return self.X.shape[0]
