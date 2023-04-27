from torch import nn

class RegressorV1(nn.Module):
  def __init__(self, isz, osz, hsz):
    super(RegressorV1, self).__init__()
    self.layers = nn.Sequential(
      nn.Linear(isz, hsz),
      nn.SiLU(inplace=True),
      nn.Linear(hsz, osz),
    )

  def forward(self, X):
    out = self.layers(X)
    return out

class BinaryClassifierV1(nn.Module):
  def __init__(self, isz, osz, hsz):
    super(BinaryClassifierV1, self).__init__()
    self.layers = nn.Sequential(
      nn.Linear(isz, hsz),
      nn.SiLU(inplace=True),
      nn.Linear(hsz, osz),
      nn.Sigmoid(),
    )

  def forward(self, X):
    out = self.layers(X)
    return out
