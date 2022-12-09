from setuptools import setup, find_packages

setup(
  name='senseis',
  version='0.0.1',
  description='Coinbase Pro Data Recorder',
  author='senseis',
  author_email='senseisworkspace@gmail.com',
  url='https://github.com/armandli/cbp-extractor',
  packages=find_packages(exclude=['']),
  package_data={},
  data_files={},
  install_requires=[
      'sortedcontainers',
      'pytz',
      'aio-pika',
      'aiohttp',
      'aiobotocore',
      's3cmd',
      'pandas',
      'pyarrow',
      'numpy',
      'scikit-learn',
      'prometheus-client',
  ],
  entry_points={
    'console_scripts':[
        'book_publisher = senseis.publisher.book_publisher:main',
        'book_writer = senseis.subscriber.book_writer:main',
        'book_level2s1_publisher = senseis.publisher.book_level2s1_publisher:main',
        'book_level2s1_writer = senseis.subscriber.book_level2s1_writer:main',
        'ticker_publisher = senseis.publisher.ticker_publisher:main',
        'ticker_writer = senseis.subscriber.ticker_writer:main',
        'stat_publisher = senseis.publisher.stat_publisher:main',
        'stat_writer = senseis.subscriber.stat_writer:main',
        'trade_publisher = senseis.publisher.trade_publisher:main',
        'trade_writer = senseis.subscriber.trade_writer:main',
        'prediction_etl_writer = senseis.subscriber.prediction_etl_writer:main',
        'prediction_etl_s1 = senseis.pipe.prediction_etl_s1:main',
        'prediction_etl_s2 = senseis.pipe.prediction_etl_s2:main',
        'candle_backfiller = senseis.backfiller.candle_backfiller:main',
        'printer = senseis.subscriber.printer:main',
    ]
  },
  scripts=[]
)
