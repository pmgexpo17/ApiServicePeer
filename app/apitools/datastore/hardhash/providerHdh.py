__all__ = ['HardhashCache']
from apibase import AbstractConnCache, ConnectorError, Note
from copy import copy
from collections import deque
from threading import RLock
import logging
import os, sys

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# HardhashCache
#----------------------------------------------------------------#		
class HardhashCache(AbstractConnCache):

  def __init__(self, contextId, connKlass, datasource):
    super().__init__(contextId, connKlass, datasource)

  #----------------------------------------------------------------#
  # addConn
  #----------------------------------------------------------------#		
  def addConn(self, connId, connKlass=None):
    if not connId:
      raise ConnectorError('make requirement connector id is not defined')
    self.cache[connId] = connector = self.makeConn(connId, connKlass)
    return connector

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#
  def close(self):
    logger.info(f'{self.name} is shutting down ...')

  #----------------------------------------------------------------#
  # contains
  #----------------------------------------------------------------#
  def contains(self, connId):
    try:
      self.cache[connId]
    except KeyError:
      return False
    return True

  #----------------------------------------------------------------#
  # keys
  #----------------------------------------------------------------#		
  def keys(self):
    keys = copy(self.cache.keys())
    return deque(keys.sorted())

  #----------------------------------------------------------------#
  # get
  #----------------------------------------------------------------#
  def get(self, connId):
    return self.cache.get(connId)

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, *args, **kwargs):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

  #----------------------------------------------------------------#
  # makes and returns a connector
  #----------------------------------------------------------------#
  def makeConn(self, connId, connKlass=None):
    try:
      if not connKlass:
        connKlass = self.connKlass
      logger.info(f'{self.name}, connector className : {connKlass.__name__}')
      logger.info(f'{self.name}, connector id : {connId}')
      leveldb = self._datasource.get()
      return connKlass.make(leveldb, connId)
    except Exception as ex:
      raise ConnectorError('connector creation failed : ' + str(ex))

