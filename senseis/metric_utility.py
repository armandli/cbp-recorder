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
