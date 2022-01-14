from setuptools import setup, find_packages

setup(
  name='senseis',
  version='0.0.1',
  description='Coinbase Pro Data Recorder',
  authors='senseis',
  author_emails='senseisworkspace@gmail.com',
  url='https://github.com/armandli/cbp-extractor',
  packages=find_packages(exclude=['senseis']),
  package_data={},
  data_files={},
  install_requires=[
      'pytz',
      'pika',
      'aiohttp',
      'aiobotocore',
      's3fs',
      's3cmd',
  ],
  entry_points={
    'console_scripts':[
    ]
  },
  scripts=[]
)
