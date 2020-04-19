__all__ = [
  'ApiConnectError',
  'AioRequest',
  'HttpRequest']

from aiohttp import ClientSession
from requests import HTTPError, ConnectionError, RequestException
from urllib3.exceptions import NewConnectionError, ConnectTimeoutError, MaxRetryError
from threading import RLock
import requests

# -------------------------------------------------------------- #
# AioRequest
# ---------------------------------------------------------------#
class AioRequest:
  _instance = None

  def __init__(self):
    self.conn = ClientSession()

  @classmethod
  def startFw(cls):
    cls._instance = cls()

  @classmethod
  def get(cls):
    return cls._instance.conn

  @classmethod
  async def stop(cls):
    if cls._instance:
      await cls._instance.conn.close()

# -------------------------------------------------------------- #
# ApiConnectError
# ---------------------------------------------------------------#
class ApiConnectError(Exception):
  pass

# -------------------------------------------------------------- #
# apiStatus
# ---------------------------------------------------------------#
def apiStatus(func):
  def wrapper(obj, *args, **kwargs):
    try:
      return func(obj, *args, **kwargs)
    except HTTPError as ex:
      raise Exception('api request failed\nHttp error : ' + str(ex))
    except (ConnectionError, NewConnectionError, ConnectTimeoutError, MaxRetryError) as ex:
      if 'Errno 111' in ex.__repr__():
        raise ApiConnectError('api request failed, api host is not running\nConnection error : ' + str(ex))
      else:
        raise Exception('api request failed\nConnection error: ' + str(ex))
    except RequestException as ex:
      raise Exception('api request failed\nError : ' + str(ex))
  return wrapper

# -------------------------------------------------------------- #
# HttpRequest
# ---------------------------------------------------------------#
class HttpRequest:
  _instance = None

  def __init__(self):
    self.conn = requests.Session()

  @classmethod
  def startFw(cls):
    cls._instance = obj = cls()
    return obj

  @classmethod
  def _get(cls):
    return cls._instance

  @classmethod
  def stop(cls):
    if cls._instance:
      cls._instance.conn.close()
    
  @apiStatus
  def delete(self, *args, **kwargs):
    return self.conn.delete(*args, **kwargs)

  @apiStatus
  def get(self, *args, **kwargs):
    return self.conn.get(*args, **kwargs)

  @apiStatus
  def post(self, *args, **kwargs):
    return self.conn.post(*args, **kwargs)

  @apiStatus
  def put(self, *args, **kwargs):
    return self.conn.put(*args, **kwargs)
