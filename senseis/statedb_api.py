import ssl
import aiomysql
import pymysql

from senseis.configuration import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSL_CA

GET_SQL = "SELECT `config_value` FROM `config` WHERE `config_key`=%s"
SET_SQL = """
          INSERT INTO `config`(`config_key`, `config_value`) 
          VALUES (%s, %s)
          ON DUPLICATE KEY UPDATE `config_key`=%s, `config_value`=%s
          """
GET_ALL_SQL = "SELECT `config_key`,`config_value`,`update_dt` FROM `config`"
REMOVE_SQL = "DELETE FROM `config` WHERE `config_key`=%s"

class StateDBApi:

  @staticmethod
  def get_config(key):
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        ssl_ca=DB_SSL_CA,
        database=DB_NAME,
    )
    with conn:
      with conn.cursor() as cursor:
        cursor.execute(GET_SQL, (key))
        result = cursor.fetchone()
        if result is not None:
          return result[0]
        else:
          return None

  @staticmethod
  def set_config(key, value):
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        ssl_ca=DB_SSL_CA,
        database=DB_NAME,
    )
    with conn:
      with conn.cursor() as cursor:
        cursor.execute(SET_SQL, (key, value, key, value))
      conn.commit()

  @staticmethod
  def remove_config(key):
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        ssl_ca=DB_SSL_CA,
        database=DB_NAME,
    )
    with conn:
      with conn.cursor() as cursor:
        cursor.execute(REMOVE_SQL, (key))
      conn.commit()

  @staticmethod
  def get_all_config():
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        ssl_ca=DB_SSL_CA,
        database=DB_NAME,
    )
    with conn:
      with conn.cursor() as cursor:
        cursor.execute(GET_ALL_SQL)
        result = cursor.fetchall()
        return result

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.load_verify_locations(DB_SSL_CA)

class AioStateDBApi:

  @staticmethod
  async def get_config(key):
    async with aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, ssl=ssl_context, db=DB_NAME) as conn:
      async with conn.cursor() as curr:
        await curr.execute(GET_SQL, (key))
        r = await curr.fetchone()
        if r is not None:
          return r[0]
        else:
          return None

  @staticmethod
  async def set_config(key, value):
    async with aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, ssl=ssl_context, db=DB_NAME) as conn:
      async with conn.cursor() as curr:
        await curr.execute(SET_SQL, (key, value, key, value))
      await conn.commit()

  @staticmethod
  async def remove_config(key):
    async with aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, ssl=ssl_context, db=DB_NAME) as conn:
      async with conn.cursor() as curr:
        await curr.execute(REMOVE_SQL, (key))
      await conn.commit()

  @staticmethod
  async def get_all_config():
    async with aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, ssl=ssl_context, db=DB_NAME) as conn:
      async with conn.cursor() as curr:
        await curr.execute(GET_ALL_SQL)
        r = await curr.fetchall()
        return r

