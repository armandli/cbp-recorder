DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f %Z"
DATE_FORMAT = "%Y-%m-%d"
CANDLE_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
TICKER_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
MICROSECONDS = 1000000
RETRY_TIME = 100000
NUM_RETRIES = 3

QUEUE_HOST = ""
QUEUE_PORT = ""
QUEUE_USER = ""
QUEUE_PASSWORD = ""

S3_ENDPOINT = ""
S3_BUCKET = ""
S3_KEY = ""
S3_SECRET = ""

BOOK_REQUEST_URL = 'https://api.exchange.coinbase.com/products/{}/book'
TICKER_REQUEST_URL = 'https://api.exchange.coinbase.com/products/{}/ticker'
STAT_REQUEST_URL = 'https://api.exchange.coinbase.com/products/{}/stats'

BOOK_EXCHANGE_NAMES = ['book_level1_0','book_level1_1','book_level2_0','book_level2_1']
CANDLE_EXCHANGE_NAMES = ['candle_exchange_0','candle_exchange_1']
TICKER_EXCHANGE_NAMES = ['ticker_exchange_0', 'ticker_exchange_1']
STAT_EXCHANGE_NAMES = ['stat_exchange_0', 'stat_exchange_1']

EXCHANGE_PIDS = [
    ['BTC-USD','ETH-USD','MKR-USD','BCH-USD','COMP-USD','AAVE-USD','UNI-USD','CRV-USD','BAL-USD','LTC-USD'],
    ['ADA-USD','XLM-USD','ALGO-USD','ZEC-USD','DOGE-USD','SHIB-USD','SUSHI-USD','EOS-USD','ETC-USD','WBTC-USD'],
    ['ETH2-USD'],
]

OUTPATHS = {"book_level1": "book-lvl1", "book_level2": "book-lvl2", "candle": "candle", "ticker" : "ticker", "stat" : "stat"}

STIME_COLNAME = 'sequence_time'
RTIME_COLNAME = 'record_time'
PERIOD_COLNAME = 'periodicity'

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

def get_exchange_pids(exchange_name):
  idx = int(exchange_name.split('_')[2])
  return EXCHANGE_PIDS[idx]

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
  else:
    assert(False)
