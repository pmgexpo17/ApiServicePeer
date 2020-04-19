from apibase import AbstractConnCache, Note, LeveldbHash, ZmqContext
from .connectorHdh import HHResponse
import leveldb
import logging
import os, subprocess, sys
import zmq

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# HardhashContext
#----------------------------------------------------------------#		
class HardhashContext:
  def __init__(self, hhId, dbPath):
    self.hhId = hhId
    self.dbPath = dbPath
    self.hhDb = leveldb.LevelDB(dbPath)

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, hhId):
    try:
      apiBase = LeveldbHash.db['apiBase']
      dbPath = f'{apiBase}/database/hardhash/{hhId}'
      if os.path.exists(dbPath):
        logger.warn(f'{cls.__name__}, datastore path already exists')
      else:
        logger.info(f'{cls.__name__}, {hhId}, creating datastorage location ...')      
        subprocess.call(['mkdir','-p',dbPath])
      return dbPath
    except Exception as ex:
      logger.error(f'{cls.__name__}, Hardhash datastore {hhId} creation failed')
      raise

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    raise NotImplementedError(f'{self.name}.close is an abstract method')

  #----------------------------------------------------------------#
  # destroy
  #----------------------------------------------------------------#		
  def destroy(self):
    logger.info(f'{self.name}, datastore will be deleted ...')
    if not os.path.exists(self.dbPath):
      logger.warn('delete failed, datastore does not exist')
    else:
      subprocess.call(['rm','-rf',self.dbPath])
    logger.info(f'{self.name} is now deleted')


#----------------------------------------------------------------#
# HHBrokerContext
#----------------------------------------------------------------#		
class HHBrokerContext(AbstractConnCache, ZmqContext, HardhashContext):
  def __init__(self, hhId, dbPath, responseAddr, connKlass=HHResponse):
    AbstractConnCache.__init__(self, hhId, connKlass)
    HardhashContext.__init__(self, hhId, dbPath)
    self.responseAddr = responseAddr
    logmsg = f'{self.name}, new hardhash broker context'
    logger.info(f'{logmsg}, responseAddr : {responseAddr}')

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, hhId, responseAddr):
    dbPath = super().make(hhId)
    return cls(hhId, dbPath, responseAddr)

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    logger.info(f'{self.name} is closing ...')
    super().close()

  #----------------------------------------------------------------#
  # makes and returns a broker backend socket
  #----------------------------------------------------------------#
  def makeSock(self, socktype, sockopt={}):
    socket = self.socket(socktype, sockopt)
    socket.connect(self.responseAddr)
    sockware = Note({
        'socket': socket,
        'address': self.responseAddr})
    return sockware

#----------------------------------------------------------------#
# HHServiceContext
#----------------------------------------------------------------#		
class HHServiceContext(AbstractConnCache, ZmqContext, HardhashContext):
  def __init__(self, hhId, dbPath, connKlass=HHResponse):
    AbstractConnCache.__init__(self, hhId, connKlass)
    HardhashContext.__init__(self, hhId, dbPath)
    logger.info(f'{self.name}, new hardhash service context')

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, hhId):
    dbPath = super().make(hhId)
    return cls(hhId, dbPath)

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    logger.info(f'{self.name} is closing ...')
    super().close()

  #----------------------------------------------------------------#
  # makes and returns a bound backend socket for direct frontend connection
  #----------------------------------------------------------------#
  def makeSock(self, socktype, sockopt={}):
    socket = self.socket(socktype, sockopt)
    port = socket.bind_to_random_port(self.hostAddr)
    sockAddr = f'{self.hostAddr}:{port}'
    sockware = Note({
      'socket': socket,
      'address': sockAddr})
    return sockware

import pickle
from threading import RLock
try:
  DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL
except AttributeError:
  DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL

# ---------------------------------------------------------------------------#
# HHLocalContext
# ---------------------------------------------------------------------------#    
class HHLocalContext(HardhashContext):
  _instance = None

  def __init__(self, hhId, dbPath, pickleProtocol=DEFAULT_PROTOCOL):
    super().__init__(hhId, dbPath)
    logger.info(f'{self.name}, new hardhash local context')
    self._instance = leveldb.LevelDB(dbPath)
    self._protocol = pickleProtocol
    self._lock = RLock()

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, hhId):
    dbPath = super().make(hhId)
    return cls(hhId, dbPath)

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  @classmethod
  def get(cls):
    return cls._instance

  #----------------------------------------------------------------#
  # __getitem__
  # for non-bytes type values
  #----------------------------------------------------------------#		
  def __getitem__(self, key):
    return pickle.loads(self._leveldb.Get(key.encode()))

  #----------------------------------------------------------------#
  # __setitem__
  #----------------------------------------------------------------#		
  def __setitem__(self, key, value):
    self.put(key,value)
    
  #----------------------------------------------------------------#
  # __delitem__
  #----------------------------------------------------------------#		
  def __delitem__(self, key):
    self._leveldb.Delete(key.encode())

  #----------------------------------------------------------------#
  # bytes
  # restores a bytes or bytearray value
  #----------------------------------------------------------------#		
  def bytes(self, key):
    return self._leveldb.Get(key.encode())

  #----------------------------------------------------------------#
  # put
  #----------------------------------------------------------------#		
  def put(self, key, value, sync=False):
    with self._lock:
      if not isinstance(value, (bytes, bytearray)):
        # use pickle to handle any kind of object value
        value = pickle.dumps(value, self._protocol)
      self._leveldb.Put(key.encode(), value, sync=sync)

  #----------------------------------------------------------------#
  # select
  #----------------------------------------------------------------#		
  def select(self, startKey, endKey, incValue=True):
    dbIter = self._leveldb.RangeIter(startKey.encode(), endKey.encode(), include_value=incValue)
    return ResultSet(dbIter, incValue)

#----------------------------------------------------------------#
# ResultSet
#----------------------------------------------------------------#		
class ResultSet():
  def __init__(self, dbIter, incValue):
    self.dbIter = dbIter
    self.__next__ = self.__nextVal if incValue else self.__nextKey

  def __iter__(self):
    return self

  def __nextVal(self):
    key, value = self.dbIter.__next__()
    return (key.decode(), pickle.loads(value)) 

  def __nextKey(self):
    key = self.dbIter.__next__()
    return key.decode()
