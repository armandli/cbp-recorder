from prometheus_client import CollectorRegistry, Counter, Gauge, Summary, Histogram

GATEWAY_URL='localhost:9091'

def setup_gateway(app_name):
  global registry
  registry = CollectorRegistry()
  global JOB_NAME
  JOB_NAME=app_name

def get_collector_registry():
  return registry

def get_job_name():
  return JOB_NAME

def create_live_gauge(app_name):
  global LIVE_GAUGE
  LIVE_GAUGE = Gauge(app_name + "_last_alive", "the last time app is alive", registry=get_collector_registry())

def get_live_gauge():
  return LIVE_GAUGE

def create_write_success_gauge(app_name):
  global WRITE_SUCCESS_GAUGE
  WRITE_SUCCESS_GAUGE = Gauge(app_name + "_write_success_time", "the last time write was successful", registry=get_collector_registry())

def get_write_success_gauge():
  return WRITE_SUCCESS_GAUGE

def create_row_count_gauge(app_name):
  global ROW_COUNT_GAUGE
  ROW_COUNT_GAUGE = Gauge(app_name + "_row_count", "output row count", registry=get_collector_registry())

def get_row_count_gauge():
  return ROW_COUNT_GAUGE

def create_output_data_process_time_histogram(app_name):
  global OUTPUT_DATA_PROCESS_TIME_HISTOGRAM
  OUTPUT_DATA_PROCESS_TIME_HISTOGRAM = Histogram(app_name + "_output_data_processing_time", "the time it took to produce output data", registry=get_collector_registry())

def get_output_data_process_time_histogram():
  return OUTPUT_DATA_PROCESS_TIME_HISTOGRAM

def create_missed_book_gauge(app_name):
  global MISSED_BOOK_GAUGE
  MISSED_BOOK_GAUGE = Gauge(app_name + "_missed_book", "number of book reports missed due to http error", registry=get_collector_registry())

def get_missed_book_gauge():
  return MISSED_BOOK_GAUGE

def create_trade_count_gauge(app_name):
  global TRADE_COUNT_GAUGE
  TRADE_COUNT_GAUGE = Gauge(app_name + "_trade_count", "number of trades obtained during this period", registry=get_collector_registry())

def get_trade_count_gauge():
  return TRADE_COUNT_GAUGE

def create_etl_process_time_histogram(app_name):
  global ETL_DATA_PROCESS_TIME_HISTOGRAM
  ETL_DATA_PROCESS_TIME_HISTOGRAM = Histogram(app_name + "_etl_data_processing_time", "the time it took to process data for one period", registry=get_collector_registry())

def get_etl_process_time_histogram():
  return ETL_DATA_PROCESS_TIME_HISTOGRAM

def create_restarted_counter(app_name):
  global ASYNC_RESTARTED_COUNTER
  ASYNC_RESTARTED_COUNTER = Counter(app_name + "_restarted_count", "number of times process restarted all asynchronous tasks", registry=get_collector_registry())

def get_restarted_counter():
  return ASYNC_RESTARTED_COUNTER

def create_interval_gauge(app_name):
  global INTERVAL_GAUGE
  INTERVAL_GAUGE = Gauge(app_name + "_data_interval", "the time in seconds between each datapoint sent or received", registry=get_collector_registry())

def get_interval_gauge():
  return INTERVAL_GAUGE

def setup_basic_gauges(app_name):
  create_live_gauge(app_name)
  create_restarted_counter(app_name)
  create_interval_gauge(app_name)

def setup_subscriber_gauges(app_name):
  create_live_gauge(app_name)
  create_restarted_counter(app_name)
  create_write_success_gauge(app_name)
  create_row_count_gauge(app_name)
  create_output_data_process_time_histogram(app_name)
  create_interval_gauge(app_name)
