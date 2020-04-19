__all__ = ['LeveldbConnector']
from apibase import AbstractConnector, ConnectorError, Note, LeveldbHash
from threading import RLock
import logging
import pickle

try:
  DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL
except AttributeError:
  DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL

logger = logging.getLogger('asyncio.broker')

class DatastoreError(Exception):
  pass

#----------------------------------------------------------------#
# LeveldbConnector -
#----------------------------------------------------------------#		
class LeveldbConnector(AbstractConnector):
  
  def __init__(self, leveldb, connId, pickleMode=DEFAULT_PROTOCOL):
    self._leveldb = leveldb
    self._cid = connId
    self._pickleMode = pickleMode
    self._lock = RLock()

  @property
  def cid(self):
    return self._cid

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, leveldb, connId):
    return cls(leveldb, connId)

  #----------------------------------------------------------------#
  # __delitem__
  #----------------------------------------------------------------#		
  def __delitem__(self, key):
    self.delete(key)

  #----------------------------------------------------------------#
  # __getitem__
  #----------------------------------------------------------------#		
  def __getitem__(self, key):
    return self.get(key)

  #----------------------------------------------------------------#
  # __setitem__
  #----------------------------------------------------------------#		
  def __setitem__(self, key, value):
    self.put(key, value)

  #----------------------------------------------------------------#
  # get
  #----------------------------------------------------------------#		
  def get(self, key):
    try:
      return pickle.loads(self._leveldb.Get(key.encode()))
    except KeyError as ex:
      logger.warn(f'__getitem__ failed, dbkey : {key}')
      raise

  #----------------------------------------------------------------#
  # delete
  #----------------------------------------------------------------#		
  def delete(self, key):
    try:
      return self._leveldb.Get(key.encode())
    except KeyError as ex:
      logger.warn(f'__getitem__ failed, dbkey : {key}')
      raise

  #----------------------------------------------------------------#
  # append
  #----------------------------------------------------------------#		
  def append(self, key, value):
    try:
      valueA = pickle.loads(self._leveldb.Get(key.encode()))
    except KeyError as ex:
      valueA = []
    valueA.append(value)
    self.put(key, valueA)

  #----------------------------------------------------------------#
  # _bytes
  # returns the raw bytes or bytearray value
  #----------------------------------------------------------------#		
  def _bytes(self, key):
    try:
      return self._leveldb.Get(key.encode())
    except KeyError as ex:
      logger.error(f'_bytes failed, dbkey : {key}', exc_info=True)
      raise ConnectorError(ex)

  #----------------------------------------------------------------#
  # bput - if value is already bytes or bytearray
  #----------------------------------------------------------------#		
  def bput(self, key, value):
    try:
      bValue = value
      if not isinstance(bValue, (bytes, bytearray)):
        bValue = pickle.dumps(value, self._pickleMode)
      self._leveldb.Put(key.encode(), bValue)
    except Exception as ex:
      logger.error(f'put failed, dbkey : {key}', exc_info=True)
      raise ConnectorError(ex)

  #----------------------------------------------------------------#
  # put
  #----------------------------------------------------------------#		
  def put(self, key, value):
    try:
      with self._lock:          
        bValue = pickle.dumps(value, self._pickleMode)
        self._leveldb.Put(key.encode(), bValue)
    except Exception as ex:
      logger.error(f'put failed, dbkey : {key}', exc_info=True)
      raise ConnectorError(ex)

  #----------------------------------------------------------------#
  # select
  #----------------------------------------------------------------#		
  def select(self, startKey, endKey, incValue=True):
    try:
      dbIter = self._leveldb.RangeIter(startKey.encode(), endKey.encode(), include_value=incValue)
      return ResultSet.make(dbIter, incValue)
    except Exception as ex:
      logger.error(f'select failed, keyLow, keyHigh : {startKey}, {endKey}', exc_info=True)
      raise ConnectorError(ex)

#----------------------------------------------------------------#
# ResultSet
#----------------------------------------------------------------#		
class ResultSet():
  def __init__(self, dbIter, incValue):
    self.dbIter = dbIter
    self.__next = self.__nextVal if incValue else self.__nextKey

  @classmethod
  def make(cls, dbIter, incValue):
    resultSet = cls(dbIter, incValue)
    while True:
      try:
        yield resultSet.__next()
      except StopIteration:      
        break

  def __nextVal(self):
    key, value = self.dbIter.__next__()
    return pickle.loads(value)

  def __nextKey(self):
    key = self.dbIter.__next__()
    return key.decode()
