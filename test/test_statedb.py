import asyncio

from senseis.statedb_api import AioStateDBApi

async def test_get_all_config():
  configs = await AioStateDBApi.get_all_config()
  print(configs)

def main():
  loop = asyncio.get_event_loop()
  loop.run_until_complete(test_get_all_config())

if __name__ == '__main__':
  main()
