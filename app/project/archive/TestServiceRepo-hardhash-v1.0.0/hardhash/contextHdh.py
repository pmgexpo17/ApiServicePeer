from apibase import AbstractConnCache, Note, LeveldbHash, ZmqContext
from .connectorHdh import HHResponse
import leveldb
import logging
import os, subprocess, sys
import zmq

logger = logging.getLogger('asyncio.broker')

class HardhashDatasource(AbstractSystemUnit):
  pass

#----------------------------------------------------------------#
# HardhashContext
#----------------------------------------------------------------#		
class HardhashContext(ZmqConnectorCache, ZmqDatasource):
  def __init__(self, hhId, dbPath, connKlass=HHResponse):
    super().__init__(hhId, connKlass)
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
    logger.info(f'{self.name} is closing ...')
    super().close()

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
  # makes and returns a backend socket
  #----------------------------------------------------------------#
  def makeSock(self, socktype, sockopt={}):
    raise NotImplementedError(f'{self.name}.makeSock is an abstract method')

#----------------------------------------------------------------#
# HHBrokerContext
#----------------------------------------------------------------#		
class HHBrokerContext(HardhashContext):
  def __init__(self, hhId, dbPath, responseAddr):
    super().__init__(hhId, dbPath)
    logmsg = f'{self.name}, new hardhash broker context'
    logger.info(f'{logmsg}, responseAddr : {responseAddr}')
    self.responseAddr = responseAddr

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, hhId, responseAddr):
    dbPath = super().make(hhId)
    return cls(hhId, dbPath, responseAddr)

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
class HHServiceContext(HardhashContext):
  def __init__(self, hhId, dbPath):
    super().__init__(hhId, dbPath)
    logger.info(f'{self.name}, new hardhash service context')

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, hhId):
    dbPath = super().make(hhId)
    return cls(hhId, dbPath)

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