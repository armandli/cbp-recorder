import os

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f %Z"
DATE_FORMAT = "%Y-%m-%d"
CANDLE_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
TICKER_TIME_FORMAT1 = "%Y-%m-%dT%H:%M:%S.%fZ"
TICKER_TIME_FORMAT2 = "%Y-%m-%dT%H:%M:%SZ"
MICROSECONDS = 1000000
RETRY_TIME = 100000
S3_RETRY_TIME_SECOND = 30
TRADE_SIZE_LIMIT = 1000
MAX_TRADE_ID_SET_SIZE = 60000
EXTRACTION_QUEUE_SIZE = 19
ETL_QUEUE_SIZE = 11
PREDICTION_QUEUE_SIZE = 9
NUM_RETRIES = 3

QUEUE_HOST =
QUEUE_PORT =
QUEUE_USER =
QUEUE_PASSWORD =

S3_ENDPOINT =
S3_BUCKET =
S3_KEY =
S3_SECRET =

DB_HOST =
DB_PORT =
DB_NAME =
DB_USER =
DB_PASSWORD =

DB_SSL_CA =

BOOK_REQUEST_URL = 'https://api.exchange.coinbase.com/products/{}/book'
TICKER_REQUEST_URL = 'https://api.exchange.coinbase.com/products/{}/ticker'
STAT_REQUEST_URL = 'https://api.exchange.coinbase.com/products/{}/stats'
TRADE_REQUEST_URL = 'https://api.exchange.coinbase.com/products/{}/trades'

BOOK_EXCHANGE_NAMES = ['book_level1_0','book_level1_1','book_level1_2','book_level2_0','book_level2_1','book_level2_2']
CANDLE_EXCHANGE_NAMES = ['candle_exchange_0','candle_exchange_1','candle_exchange_2']
TICKER_EXCHANGE_NAMES = ['ticker_exchange_0','ticker_exchange_1','ticker_exchange_2']
STAT_EXCHANGE_NAMES = ['stat_exchange_0','stat_exchange_1','stat_exchange_2']
TRADE_EXCHANGE_NAMES = ['trade_exchange_0','trade_exchange_1','trade_exchange_2']
ETL_EXCHANGE_NAMES = ['etl_exchange_s1','etl_exchange_s2']
PRED_EXCHANGE_NAMES = [
    'ETHUSD_multi_volatility_peachone','BTCUSD_multi_volatility_peachone',
    'ETHUSD_multi_return_peachone', 'BTCUSD_multi_return_peachone',
    'ETHUSD_multi_vdelta_peachone', 'BTCUSD_multi_vdelta_peachone',
]

EXCHANGE_PIDS = [
    ['BTC-USD','ETH-USD','MKR-USD','BCH-USD','COMP-USD','AAVE-USD','UNI-USD','CRV-USD','BAL-USD','LTC-USD'],
    ['ADA-USD','XLM-USD','ALGO-USD','ZEC-USD','DOGE-USD','SHIB-USD','SUSHI-USD','EOS-USD','ETC-USD','WBTC-USD'],
    ['SOL-USD', 'AVAX-USD', 'LINK-USD', 'MATIC-USD', 'GRT-USD', 'ATOM-USD', 'MANA-USD', 'APE-USD', 'XTZ-USD', 'LRC-USD'],
    ['DASH-USD', 'BAT-USD', 'REN-USD'],
]

CALL_SIGNS = ['peachone']

OUTPATHS = {
    "book_level1": "book-lvl1",
    "book_level2": "book-lvl2",
    "candle": "candle",
    "ticker" : "ticker",
    "stat" : "stat",
    "trade" : "trade",
    "etl_exchange_s1" : "etl-s1",
    "etl_exchange_s2" : "etl-s2",
    "ETHUSD_multi_volatility_peachone" : "ETHUSD-multi-volatility-peachone",
    "ETHUSD_multi_return_peachone"     : "ETHUSD-multi-return-peachone",
    "ETHUSD_multi_vdelta_peachone"     : "ETHUSD-multi-vdelta-peachone",
    "BTCUSD_multi_volatility_peachone" : "BTCUSD-multi-volatility-peachone",
    "BTCUSD_multi_return_peachone"     : "BTCUSD-multi-return-peachone",
    "BTCUSD_multi_vdelta_peachone"     : "BTCUSD-multi-vdelta-peachone",
}

STIME_COLNAME = 'sequence_time'
RTIME_COLNAME = 'record_time'
STIME_INTERVAL = 'sequence_interval_s'
PERIOD_COLNAME = 'periodicity'
STIME_EPOCH_COLNAME = 'sequence_epoch'

CANDLE_GRANULARITY = 60 # candle by each minute
CANDLE_SIZE = 300

def is_valid_exchange_name(exchange_name):
  if exchange_name in BOOK_EXCHANGE_NAMES or exchange_name in CANDLE_EXCHANGE_NAMES or exchange_name in TICKER_EXCHANGE_NAMES or exchange_name in STAT_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_book_exchange_name(exchange_name):
  if exchange_name in BOOK_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_candle_exchange_name(exchange_name):
  if exchange_name in CANDLE_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_ticker_exchange_name(exchange_name):
  if exchange_name in TICKER_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_stat_exchange_name(exchange_name):
  if exchange_name in STAT_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_trade_exchange_name(exchange_name):
  if exchange_name in TRADE_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_etl_exchange_name(exchange_name):
  if exchange_name in ETL_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_pred_exchange_name(exchange_name):
  if exchange_name in PRED_EXCHANGE_NAMES:
    return True
  else:
    return False

def is_multi_volatility_pred_exchange(exchange_name):
  return '_multi_volatility_' in exchange_name

def is_multi_return_pred_exchange(exchange_name):
  return '_multi_return_' in exchange_name

def is_multi_vdelta_pred_exchange(exchange_name):
  return '_multi_vdelta_' in exchange_name

def get_exchange_pids(exchange_name):
  idx = int(exchange_name.split('_')[2])
  return EXCHANGE_PIDS[idx]

def get_all_pids():
  all_pids = []
  all_pids.extend(EXCHANGE_PIDS[0])
  all_pids.extend(EXCHANGE_PIDS[1])
  return all_pids

def get_book_level(exchange_name):
  level = exchange_name.split('_')[1]
  if '1' in level:
    return 1
  elif '2' in level:
    return 2
  elif '3' in level:
    #not supported at this moment
    assert(False)
    return 3
  else:
    assert(False)

def get_pred_exchange_pid(exchange_name):
  words = exchange_name.split('_')
  return words[0].replace("USD", "-USD")

def get_pred_exchange_call_sign(exchange_name):
  words = exchange_name.split('_')
  return words[-1]

def get_s3_bucket(exchange_name):
  return S3_BUCKET

def get_s3_outpath(exchange_name):
  if 'book' in exchange_name:
    return OUTPATHS['_'.join(exchange_name.split('_')[:2])]
  elif 'candle' in exchange_name:
    return OUTPATHS['candle']
  elif 'ticker' in exchange_name:
    return OUTPATHS['ticker']
  elif 'stat' in exchange_name:
    return OUTPATHS['stat']
  elif 'trade' in exchange_name:
    return OUTPATHS['trade']
  elif 'etl' in exchange_name:
    return OUTPATHS[exchange_name]
  elif 'multi_volatility_peachone' in exchange_name:
    return OUTPATHS[exchange_name]
  elif 'multi_return_peachone' in exchange_name:
    return OUTPATHS[exchange_name]
  elif 'multi_vdelta_peachone' in exchange_name:
    return OUTPATHS[exchange_name]
  else:
    assert(False)
