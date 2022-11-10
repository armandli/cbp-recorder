from prometheus_client import CollectorRegistry, Gauge

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

def create_output_data_process_time_gauge(app_name):
  global OUTPUT_DATA_PROCESS_TIME_GAUGE
  OUTPUT_DATA_PROCESS_TIME_GAUGE = Gauge(app_name + "_output_data_processing_time", "the time it took to produce output data", registry=get_collector_registry())

def get_output_data_process_time_gauge():
  return OUTPUT_DATA_PROCESS_TIME_GAUGE

def create_error_gauge(app_name):
  global ERROR_GAUGE
  ERROR_GAUGE = Gauge(app_name + '_error_count', "number of errors", registry=get_collector_registry())

def get_error_gauge():
  return ERROR_GAUGE

def create_missed_book_gauge(app_name):
  global MISSED_BOOK_GAUGE
  MISSED_BOOK_GAUGE = Gauge(app_name + "_missed_book", "number of book reports missed due to http error", registry=get_collector_registry())

def get_missed_book_gauge():
  return MISSED_BOOK_GAUGE

def create_trade_count_gauge(app_name):
  global TRADE_COUNT_GAUGE
  TRADE_COUNT_GAUGE = Gauge(app_name + "_trade_count", "number of trades obtained during this period", registry=get_collector_registry())

def get_trade_count(app_name):
  return TRADE_COUNT_GAUGE

def create_etl_process_time_gauge(app_name):
  global ETL_DATA_PROCESS_TIME_GAUGE
  ETL_DATA_PROCESS_TIME_GAUGE = Gauge(app_name + "_etl_data_processing_time", "the time it took to process data for one period", registry=get_collector_registry())

def get_etl_process_time_gauge():
  return ETL_DATA_PROCESS_TIME_GAUGE
