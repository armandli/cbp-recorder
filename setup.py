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
      'pytz',
      'aio-pika',
      'aiohttp',
      'aiobotocore',
      's3cmd',
      'pandas',
      'pyarrow',
      'numpy',
  ],
  entry_points={
    'console_scripts':[
        'book_publisher = senseis.publisher.book_publisher:main',
        'candle_publisher = senseis.publisher.candle_publisher:main',
        'book_writer = senseis.subscriber.book_writer:main',
        'candle_writer = senseis.subscriber.candle_writer:main',
        'printer = senseis.subscriber.printer:main',
    ]
  },
  scripts=[]
)
