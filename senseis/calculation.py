import math

def compute_book_inbalance(cbprice, cbsize, caprice, casize, pbprice, pbsize, paprice, pasize):
  ge_bid_price = cbprice >= pbprice
  le_bid_price = cbprice <= pbprice
  ge_ask_price = caprice >= paprice
  le_ask_price = caprice <= paprice
  inba = ge_bid_price * cbsize - le_bid_price * pbsize + ge_ask_price * pasize - le_ask_price * casize
  return inba

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
  return math.sqrt(sum([r**2 for r in returns if not math.isnan(r)]))

