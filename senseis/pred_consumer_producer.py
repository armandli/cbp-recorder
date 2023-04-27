import logging
import time
import math
import json
from datetime import datetime, timedelta
import pytz
import zlib
import boto3
from smart_open import open
import aiobotocore.session as abcsession
from socket import error as SocketError
import numpy as np

import botocore
import asyncio
import aio_pika

import torch

from prometheus_client import push_to_gateway

from senseis.configuration import DATETIME_FORMAT
from senseis.configuration import STIME_COLNAME, RTIME_COLNAME
from senseis.configuration import S3_ENDPOINT, S3_BUCKET, S3_KEY, S3_SECRET, S3_BUCKET
from senseis.configuration import QUEUE_HOST, QUEUE_PORT, QUEUE_USER, QUEUE_PASSWORD
from senseis.configuration import PREDICTION_QUEUE_SIZE
from senseis.statedb_api import StateDBApi
from senseis.extraction_producer_consumer import get_period
from senseis.extraction_producer_consumer import get_interval
from senseis.torch_module.model import RegressorV1, BinaryClassifierV1
from senseis.training_utility import s3_client
from senseis.metric_utility import GATEWAY_URL
from senseis.metric_utility import get_collector_registry, get_job_name
from senseis.metric_utility import get_live_gauge, get_restarted_counter, get_interval_gauge
from senseis.metric_utility import get_etl_process_time_histogram

async def process_pred_data(period, data, state):
  perf_start_time = time.perf_counter()
  output = await state.predict(period, data)
  message = json.dumps(output)

  perf_time_taken = time.perf_counter() - perf_start_time
  get_etl_process_time_histogram().observe(perf_time_taken)
  push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())

  return zlib.compress(message.encode())

async def pred_processor(pred_f, create_pred_state_f, output_exchange_name, model_key, cache_time, periodicity, que):
  utc = pytz.timezone("UTC")
  pred_state = create_pred_state_f(model_key, cache_time)
  mq_connection = await aio_pika.connect_robust(host=QUEUE_HOST, port=QUEUE_PORT, login=QUEUE_USER, password=QUEUE_PASSWORD)
  async with mq_connection:
    channel = await mq_connection.channel()
    exchange = await channel.declare_exchange(name=output_exchange_name, type='fanout')
    logging.info("Pushing to {}".format(output_exchange_name))
    while True:
      ie_name, msg = await que.get()
      dat = json.loads(msg)
      cur_epoch = int(datetime.strptime(dat[STIME_COLNAME], DATETIME_FORMAT).timestamp())
      epoch_interval = get_interval(cur_epoch)
      get_interval_gauge().set(epoch_interval)
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
      #TODO: no need to do this, should always be 1
      dat_period = get_period(cur_epoch, periodicity)
      period_str = utc.localize(datetime.utcfromtimestamp(dat_period)).strftime(DATETIME_FORMAT)
      logging.info("Producing output data for period {}".format(period_str))
      output = await pred_f(dat[STIME_COLNAME], dat, pred_state)
      msg = aio_pika.Message(body=output)
      logging.info("Sending {}".format(period_str))
      await exchange.publish(message=msg, routing_key='')
      logging.info("Published Pred data for period {}".format(period_str))
      get_live_gauge().set_to_current_time()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
      que.task_done()

async def pred_consumer_producer(
    data_subscriber_f,
    pred_processor_f,
    process_pred_data_f,
    create_pred_state_f,
    output_exchange_name,
    input_exchange_name,
    model_key,
    cache_time,
    periodicity
  ):
  while True:
    tasks = []
    try:
      que = asyncio.Queue(maxsize=PREDICTION_QUEUE_SIZE)
      tasks.append(asyncio.create_task(data_subscriber_f(input_exchange_name, que)))
      tasks.append(
        asyncio.create_task(
          pred_processor_f(
            process_pred_data_f,
            create_pred_state_f,
            output_exchange_name,
            model_key,
            cache_time,
            periodicity,
            que
      )))
      await asyncio.gather(*tasks, return_exceptions=False)
      await que.join()
    except asyncio.CancelledError as err:
      logging.info("Cancelled Error: {}".format(err))
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")
      get_restarted_counter().inc()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())
    except SocketError as err:
      logging.info("SocketError: {}".format(err))
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      logging.info("Restarting")
      get_restarted_counter().inc()
      push_to_gateway(GATEWAY_URL, job=get_job_name(), registry=get_collector_registry())

def create_torch_model_state(model_key, cache_time):
  return TorchModel(model_key, cache_time)

class TorchModel:
  def __init__(self, key, cache_time):
    self.utc = pytz.timezone("UTC")
    self.key = key
    self.cache_time = cache_time
    self.version = None
    self.version_time = None
    self.metadata = None
    self.model = None
    self.prev_input = None

  async def predict(self, timestamp_str, data):
    await self._refresh_model()
    if self.metadata is None:
      outdict = {STIME_COLNAME : timestamp_str, RTIME_COLNAME : datetime.now(self.utc).strftime(DATETIME_FORMAT)}
      return outdict
    if self.model is None:
      outdict = {col : float("NaN") for col in self.metadata['target_columns']}
      outdict[STIME_COLNAME] = timestamp_str
      outdict[RTIME_COLNAME] = datetime.now(self.utc).strftime(DATETIME_FORMAT)
      return outdict
    #TODO: might want to speed it up by doing this once
    X_train_mean = torch.tensor(self.metadata["normalization_params"]["X_train_mean"])
    X_train_std = torch.tensor(self.metadata["normalization_params"]["X_train_std"])
    Y_train_mean = torch.tensor(self.metadata["normalization_params"]["Y_train_mean"])
    Y_train_std = torch.tensor(self.metadata["normalization_params"]["Y_train_std"])
    inp = self._create_model_input(data)
    # need to save input before normalization
    self.prev_input = inp
    inp = torch.tensor(inp)
    inp = (inp - X_train_mean) / X_train_std
    inp = torch.reshape(inp, (1, -1))
    output = self.model(inp)
    output = output * Y_train_std + Y_train_mean
    output = torch.reshape(output, (-1,)).tolist()
    outdict = dict()
    for idx, col in enumerate(self.metadata['target_columns']):
      outdict[col] = output[idx]
    outdict[STIME_COLNAME] = timestamp_str
    outdict[RTIME_COLNAME] = datetime.now(self.utc).strftime(DATETIME_FORMAT)
    return outdict

  async def _refresh_model(self):
    version = self._load_model_version()
    # if it's the same version and cache has not expired
    if version == self.version and self.version_time + timedelta(seconds=self.cache_time) >= datetime.now():
      return
    try:
      v = json.loads(version)
      self.version = version
      self.version_time = datetime.now()
    except json.decoder.JSONDecodeError as err:
      logging.error(f'Catastrophic Error: Unable to load configuration version for key: {self.key}. Error: {err}')
      return

    model_filename = f"s3://{S3_BUCKET}/model/{v['ticker']}_{v['target']}_{v['target_dim']}_{v['model_type']}_{v['call_sign']}_{v['version']}_{v['start_epoch']}_{v['end_epoch']}.pt"
    model_metadata_filename = f"metadata/{v['ticker']}_{v['target']}_{v['target_dim']}_{v['model_type']}_{v['call_sign']}_metadata_{v['version']}_{v['start_epoch']}_{v['end_epoch']}.json.gzip"

    # we must load the metadata before the model because it contains some model information
    session = abcsession.get_session()
    try:
      async with session.create_client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET) as client:
        resp = await client.get_object(Bucket=S3_BUCKET, Key=model_metadata_filename)
        async with resp['Body'] as stream:
          data = await stream.read()
          decoded = zlib.decompress(data).decode()
          self.metadata = json.loads(decoded)
    except botocore.exceptions.ClientError as err:
      logging.error(f"Catastrophic Error: failed to load metadata {model_metadata_filename}. Error: {err}")
      return
    except zlib.error as err:
      logging.error(f'Catastrophic Error: zlib failed to read metadata file: {model_metadata_filename}. Error: {err}')
      return
    except json.decoder.JSONDecodeError as err:
      logging.error(f'Catastrophic Error: Unable to load metadata {model_metadata_filename}. Error: {err}')
      return
    except Exception as err:
      logging.info(f"Unexpected read error when reading metadata file: {err}")
      return

    # assume metadata has no logical errors, metadata should not have logical errors
    with open(model_filename, 'rb', transport_params=dict(client=s3_client)) as fd:
      self.model = self._create_torch_model()
      if self.model is not None:
        #TODO: catch failures from unable to load the pt file here
        self.model.load_state_dict(torch.load(fd, map_location=torch.device("cpu")))

  def _load_model_version(self):
    if self.version_time is None or self.version_time + timedelta(seconds=self.cache_time) < datetime.now():
      version = StateDBApi.get_config(self.key)
      return version
    else:
      return self.version

  #create the torch model without loading up data
  def _create_torch_model(self):
    if 'model_type' not in self.metadata or self.metadata['model_type'] != 'torch':
      logging.error(f"Catastrophic Error: invalid model type: {self.metadata.get('model_type')}")
      return None
    if 'algorithm' not in self.metadata or 'version' not in self.metadata:
      logging.error(f"Catastrophic Error: metadata missing either version or algorithm")
      return None
    if self.metadata['algorithm'] == 'regression':
      if self.metadata['version'] == 'v1':
        return RegressorV1(
                   len(self.metadata['input_columns']),
                   len(self.metadata['target_columns']),
                   self.metadata['nn_hidden_size'],
        )
    elif self.metadata['algorithm'] == 'binary_classification':
      if self.metadata['version'] == 'v1':
        return BinaryClassifierV1(
                   len(self.metadata['input_columns']),
                   len(self.metadata['target_columns']),
                   self.metadata['nn_hidden_size'],
        )
    logging.error(f"Catastrophic Error: cannot decode torch model")
    return None

  def _create_model_input(self, data):
    inp = []
    for idx, colname in enumerate(self.metadata['input_columns']):
      if math.isnan(data[colname]):
        if self.prev_input is not None:
          inp.append(self.prev_input[idx])
        else:
          inp.append(0.)
      elif np.isneginf(data[colname]):
        if self.prev_input is not None:
          inp.append(self.prev_input[idx])
        else:
          inp.append(self.metadata['column_minmax'][colname]['min'])
      elif np.isposinf(data[colname]):
        if self.prev_input is not None:
          inp.append(self.prev_input[idx])
        else:
          inp.append(self.metadata['column_minmax'][colname]['max'])
      else:
        inp.append(data[colname])
    return inp
