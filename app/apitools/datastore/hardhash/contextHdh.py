__all__ = ['HardhashContext']
from apibase import AbstractDatasource, AbstractSystemUnit, ConnectorError, Note, TaskError
from datetime import datetime
from .connectorHdh import LeveldbConnector
from .providerHdh import HardhashCache
import leveldb
import logging
import os, subprocess

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# LeveldbDatasource
#----------------------------------------------------------------#		
class LeveldbDatasource(AbstractDatasource, AbstractSystemUnit):
  def __init__(self, datastoreId, dbpath):
    self._id = datastoreId
    self._dbpath = dbpath
    self._leveldb = leveldb.LevelDB(dbpath)

  #----------------------------------------------------------------#
  # destroy
  #----------------------------------------------------------------#		
  def destroy(self):
    logger.info(f'{self.name}, datastore will be deleted ...')
    if not os.path.exists(self._dbpath):
      logger.warn('delete failed, datastore does not exist')
    else:
      subprocess.call(['rm','-rf',self._dbpath])
    logger.info(f'{self.name} is now deleted')

  #----------------------------------------------------------------#
  # get
  #----------------------------------------------------------------#
  def get(self):
    return self._leveldb

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, contextId):
    try:
      dbpath = f'{cls.apiBase}/database/metastore'
      if contextId != 'metastore':
        timestamp = datetime.now().strftime('%y%m%d%H%M%S')
        datastoreId = f'{contextId}-{timestamp}'
        dbpath = f'{cls.apiBase}/database/hardhash/{datastoreId}'
      if os.path.exists(dbpath):
        logger.warn(f'{cls.__name__}, datastore path already exists')
      else:
        logger.info(f'{cls.__name__}, {contextId}, creating datastorage location ...')      
        subprocess.call(['mkdir','-p',dbpath])
      return cls(contextId, dbpath)
    except Exception as ex:
      logger.error(f'{cls.__name__}, Hardhash datastore {datastoreId} creation failed')
      raise

# ---------------------------------------------------------------------------#
# HardhashContext
# ---------------------------------------------------------------------------#    
class HardhashContext(HardhashCache):
  _instance = {}

  def __init__(self, contextId, datasource):
    super().__init__(contextId, LeveldbConnector, datasource)

  @classmethod
  def get(cls, contextId='metastore'):
    try:
      return cls._instance[contextId]
    except KeyError:
      cls._instance[contextId] = context = cls.make(contextId)
    return context

  @classmethod
  def connector(cls, contextId='metastore', connId='hardhash'):
    try:
      context = cls._instance[contextId]
    except KeyError:
      cls._instance[contextId] = context = cls.make(contextId)
    return context._get(connId)

  @classmethod
  def destroy(cls, contextId=None):
    try:
      if not contextId:
        logger.info(f'{cls.name}, contextId is required to destroy context')
        return
      cls._instance[contextId]._destroy()
      del cls._instance[contextId]
    except KeyError:
      pass

  @property
  def name(self):
    return f'{self.__class__.__name__}-{self.contextId}'

  #----------------------------------------------------------------#
  # _get
  #----------------------------------------------------------------#
  def _get(self, connId):
    connector = self.cache.get(connId)
    if connector:
      return connector
    return self.addConn(connId)

  #----------------------------------------------------------------#
  # _destroy
  #----------------------------------------------------------------#
  def _destroy(self):
    logger.info(f'{self.name}, destroying resources ...')
    del self.cache
    self._datasource.destroy()

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, contextId=None):
    if not contextId:
      raise TaskError('{self.name}, cannot make context datasource with an id')
    datasource = LeveldbDatasource.make(contextId)
    return cls(contextId, datasource)
