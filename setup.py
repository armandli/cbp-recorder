from glob import glob
from setuptools import setup, find_packages
from pybind11.setup_helpers import Pybind11Extension, build_ext, ParallelCompile, naive_recompile

ParallelCompile("NPY_NUM_BUILD_JOBS", needs_recompile=naive_recompile).install()

__version__ = '0.0.1'

ext_modules = [
    Pybind11Extension(
        "cppext",
        sorted(glob("src/*.cpp")) + sorted(glob("src/*.cc")),
    ),
]

setup(
  name='senseis',
  version=__version__,
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
      'pandas',
      'pyarrow',
      'numpy',
      'pybind11',
      'scikit-learn',
      'prometheus-client',
      's3cmd',
      'jupyter',
      'xgboost',
      'optuna',
      'shap',
      'matplotlib',
      'tqdm',
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
        'combine_dataset = senseis.training.combine_dataset:main',
        'book_monitor = senseis.monitor.book_monitor:main',
        'trade_monitor = senseis.monitor.trade_monitor:main',
        'prediction_etl_s2_monitor = senseis.monitor.prediction_etl_s2_monitor:main',
        'printer = senseis.subscriber.printer:main',
    ]
  },
  scripts=[],
  cmdclass={"build_ext": build_ext},
  ext_modules=ext_modules,
  zip_safe=False,
)
