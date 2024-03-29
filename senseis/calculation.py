import logging
import math
import numpy as np
from sklearn.linear_model import LinearRegression

def compute_book_imbalance(cbprice, cbsize, caprice, casize, pbprice, pbsize, paprice, pasize):
  ge_bid_price = cbprice >= pbprice
  le_bid_price = cbprice <= pbprice
  ge_ask_price = caprice >= paprice
  le_ask_price = caprice <= paprice
  inba = ge_bid_price * cbsize - le_bid_price * pbsize + ge_ask_price * pasize - le_ask_price * casize
  return inba

def compute_extended_book_imbalance(cbprices, cbsizes, caprices, casizes, pbprices, pbsizes, paprices, pasizes):
  if not cbprices or not cbsizes or not caprices or not casizes or \
     not pbprices or not pbsizes or not paprices or not pasizes:
    return 0.
  if len(cbprices) != len(cbsizes) or len(caprices) != len(casizes) or \
     len(pbprices) != len(pbsizes) or len(paprices) != len(pasizes):
    return 0.

  ibsize = 0.
  cidx = 0
  while cidx < len(cbprices) and cbprices[cidx] > pbprices[0]:
    ibsize += cbsizes[cidx]
    cidx += 1
  dbsize = 0.
  pidx = 0
  while pidx < len(pbprices) and pbprices[pidx] > cbprices[0]:
    dbsize += pbsizes[pidx]
    pidx += 1
  bimbalance = ibsize - dbsize
  if cidx < len(cbprices) and pidx < len(pbprices) and cbprices[cidx] == pbprices[pidx]:
    bimbalance += (cbsizes[cidx] - pbsizes[pidx])

  dasize = 0.
  cidx = 0
  while cidx < len(caprices) and caprices[cidx] < paprices[0]:
    dasize += casizes[cidx]
    cidx += 1
  iasize = 0.
  pidx = 0
  while pidx < len(paprices) and paprices[pidx] < caprices[0]:
    iasize += pasizes[pidx]
    pidx += 1
  aimbalance = iasize - dasize
  if cidx < len(caprices) and pidx < len(paprices) and caprices[cidx] == paprices[pidx]:
    aimbalance += (pasizes[pidx] - casizes[cidx])
  return bimbalance + aimbalance

def compute_weighted_average_price(bprice, bsize, aprice, asize):
  if bsize + asize == 0:
    return float("nan")
  return (bprice * asize + aprice * bsize) / (bsize + asize)

def compute_return(price1, price2):
  if math.isnan(price1) or price1 == 0.:
    return float("nan")
  return math.log(price2 / price1)

def compute_bidask_spread(bid_price, ask_price):
  if ask_price == 0.:
    return float("nan")
  return (ask_price - bid_price) / ask_price

def compute_volatility(returns):
    return math.sqrt(sum([r**2 for r in returns if not math.isnan(r)]) / float(len(returns)))

def compute_book_avg_tick(prices):
  if len(prices) < 2:
    return 0.
  count = 0
  s = 0.
  for i in range(1, len(prices)):
    if math.isnan(prices[i]) or math.isnan(prices[i-1]):
      continue
    s += abs(prices[i] - prices[i-1])
    count += 1
  return s / float(count)

def compute_book_first_tick(prices):
  if len(prices) < 2:
    return 0.
  return abs(prices[0] - prices[1])

#TODO: there is a bug in this algorithm
def compute_bid_size_change(cbprices, cbsizes, pbprices, pbsizes):
  if len(cbprices) == 0 or len(cbsizes) == 0 or len(pbprices) == 0 or len(pbsizes) == 0:
    return 0.
  if len(cbprices) != len(cbsizes) or len(pbprices) != len(pbsizes):
    logging.info("Bid price and bid size array length mismatch. not computing compute_bid_size_change")
    return 0.
  sc = 0.
  pidx = 0
  for cidx in range(len(cbprices)):
    if cbprices[cidx] == pbprices[pidx]:
      sc += cbsizes[cidx] - pbsizes[pidx]
      break
    elif cbprices[cidx] > pbprices[pidx]:
      sc += cbsizes[cidx]
    else:
      while cbprices[cidx] < pbprices[pidx]:
        sc -= pbsizes[pidx]
        pidx += 1
        if pidx >= len(pbprices):
          break
      if pidx >= len(pbprices):
        break
  return sc

#TODO: there is a bug in this algorithm
def compute_ask_size_change(caprices, casizes, paprices, pasizes):
  if len(caprices) == 0 or len(casizes) == 0 or len(paprices) == 0 or len(pasizes) == 0:
    return 0.
  if len(caprices) != len(casizes) or len(paprices) != len(pasizes):
    logging.info("Ask price and ask size array length mismatch. not computing compute_ask_size_change")
    return 0.
  sc = 0.
  pidx = 0
  for cidx in range(len(caprices)):
    if caprices[cidx] == paprices[pidx]:
      sc += casizes[cidx] - pasizes[pidx]
      break
    elif caprices[cidx] < paprices[pidx]:
      sc += casizes[cidx]
    else:
      while caprices[cidx] > paprices[pidx]:
        sc -= pasizes[pidx]
        pidx += 1
        if pidx >= len(paprices):
          break
      if pidx >= len(paprices):
        break
  return sc

#TODO: there is a bug in this algorithm
def compute_bid_volume_change(cbprices, cbsizes, pbprices, pbsizes):
  if len(cbprices) == 0 or len(cbsizes) == 0 or len(pbprices) == 0 or len(pbsizes) == 0:
    return 0.
  if len(cbprices) != len(cbsizes) or len(pbprices) != len(pbsizes):
    logging.info("Bid price and bid size array length mismatch. not computing compute_bid_volume_change")
    return 0.
  vc = 0.
  pidx = 0
  for cidx in range(len(cbprices)):
    if cbprices[cidx] == pbprices[pidx]:
      vc += (cbsizes[cidx] - pbsizes[pidx]) * cbprices[cidx]
      break
    elif cbprices[cidx] > pbprices[pidx]:
      vc += cbsizes[cidx] * cbprices[cidx]
    else:
      while cbprices[cidx] < pbprices[pidx]:
        vc -= pbsizes[pidx] * pbprices[pidx]
        pidx += 1
        if pidx >= len(pbprices):
          break
      if pidx >= len(pbprices):
        break
  return vc

#TODO: there is a bug in this algorithm
def compute_ask_volume_change(caprices, casizes, paprices, pasizes):
  if len(caprices) == 0 or len(casizes) == 0 or len(paprices) == 0 or len(pasizes) == 0:
    return 0.
  if len(caprices) != len(casizes) or len(paprices) != len(pasizes):
    logging.info("Ask price and ask size array length mismatch. not computing compute_ask_volume_change")
    return 0.
  vc = 0.
  pidx = 0
  for cidx in range(len(caprices)):
    if caprices[cidx] == paprices[pidx]:
      vc += (casizes[cidx] - pasizes[pidx]) * caprices[cidx]
      break
    elif caprices[cidx] < paprices[pidx]:
      vc += casizes[cidx] * caprices[cidx]
    else:
      while caprices[cidx] > paprices[pidx]:
        vc -= pasizes[pidx] * paprices[pidx]
        pidx += 1
        if pidx >= len(paprices):
          break
      if pidx >= len(paprices):
        break
  return vc

def compute_book_level_line(prices, sizes):
  if len(prices) < 2 or len(sizes) < 2:
    return (0., 0.)
  x = np.array(prices).reshape((-1, 1))
  y = np.array(sizes)
  model = LinearRegression()
  model.fit(x, y)
  return (model.coef_[0], model.intercept_)
