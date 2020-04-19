__all__ = [
  'AbstractConnProvider',
  'AbstractConnCache',
  'ConnectorError']

# The MIT License
#
# Copyright (c) 2019 Peter A McGill
#
import logging

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# ConnectorError
#----------------------------------------------------------------#		
class ConnectorError(Exception):
  pass

#----------------------------------------------------------------#
# AbstractConnProvider
#----------------------------------------------------------------#		
class AbstractConnProvider:
  def __init__(self, contextId, connKlass, datasource):
    self.contextId = contextId
    self.connKlass = connKlass
    self._datasource = datasource
    logger.info(f'{self.name}, connector : {connKlass.__name__}')

  @property
  def name(self):
    return f'{self.__class__.__name__}-{self.contextId}'

  def makeConn(self, *args, **kwargs):
    raise NotImplementedError(f'{self.__name__}.makeConn is an abstract method')    

  def destroy(self):
    logger.info(f'{self.name}, destroying datasource resource ...')
    self._datasource.destroy()

#----------------------------------------------------------------#
# AbstractConnCache
#----------------------------------------------------------------#		
class AbstractConnCache(AbstractConnProvider):
  def __init__(self, *args):
    super().__init__(*args)
    self.cache = {}

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, *args, **kwargs):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

  #----------------------------------------------------------------#
  # add a connector
  #----------------------------------------------------------------#		
  def addConn(self, *args, **kwargs):
    raise NotImplementedError(f'{self.__name__}.addConn is an abstract method')

  #----------------------------------------------------------------#
  # contains
  #----------------------------------------------------------------#
  def contains(self, connId):
    raise NotImplementedError(f'{self.__name__}.contains is an abstract method')

  #----------------------------------------------------------------#
  # get a connector, instead of get to avoid context or connection
  # get confusion
  #----------------------------------------------------------------#		
  def connector(self, *args, **kwargs):
    raise NotImplementedError(f'{self.__name__}.connnector is an abstract method')

  #----------------------------------------------------------------#
  # keys
  #----------------------------------------------------------------#		
  def keys(self):
    raise NotImplementedError(f'{self.__name__}.keys is an abstract method')

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#
  def close(self):
    raise NotImplementedError(f'{self.__name__}.close is an abstract method')
