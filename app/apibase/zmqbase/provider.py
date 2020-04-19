__all__ = [
  'Connware',
  'ZmqConnProvider',
  'ZmqConnectorCache',
  'ZmqConnectorError']

# The MIT License
#
# Copyright (c) 2019 Peter A McGill
#
from apibase import AbstractConnProvider
from collections import deque
import copy
import importlib
import logging
import sys, time
import zmq

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# ZmqConnectorError
#----------------------------------------------------------------#		
class ZmqConnectorError(Exception):
  pass

#----------------------------------------------------------------#
# ZmqConnProvider
#----------------------------------------------------------------#		
class ZmqConnProvider(AbstractConnProvider):
  def __init__(self, contextId, connKlass, datasource):
    super().__init__(contextId, connKlass, datasource)
    logger.info(f'{self.name}, connector : {connKlass.__name__}')

  @property
  def name(self):
    return f'{self.__class__.__name__}-{self.contextId}'

  #----------------------------------------------------------------#
  # makes and returns a connector
  #----------------------------------------------------------------#
  def makeConn(self, connware, connKlass=None):
    try:
      if not connKlass:
        connKlass = self.connKlass
      logMsg = f'{self.name}, connector name {connKlass.__name__}'
      logger.info(f'{logMsg}, sockArgs : {connware.sock}, {connware.sockopt}')
      sockware = self._datasource.get(*connware.sock,connware.sockopt)
      time.sleep(0.2)
      logger.info(f'{self.name}, connector args : {connware.conn}, {connware.connkw}')
      return connKlass.make(sockware, *connware.conn, **connware.connkw)
    except zmq.error.ZMQError as ex:
      raise ZmqConnectorError(f'connector creation failed by zmq error : ' + str(ex))
    except Exception as ex:
      raise ZmqConnectorError('connector creation failed : ' + str(ex))

  #----------------------------------------------------------------#
  # nothing to close, apiServer will close the datasource.broker
  #----------------------------------------------------------------#
  def close(self):
    pass

#----------------------------------------------------------------#
# ZmqConnectorCache
# - make and makeSock are abstract methods
#----------------------------------------------------------------#		
class ZmqConnectorCache(ZmqConnProvider):
  def __init__(self, contextId, connKlass, datasource):
    super().__init__(contextId, connKlass, datasource)
    # TO DO : change this to a WeakSet to possibly better enable garbage collection
    self.cache = {}

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, contextId, hostAddr='tcp://127.0.0.1'):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

  #----------------------------------------------------------------#
  # addConn
  #----------------------------------------------------------------#		
  def addConn(self, connId, connware, connKlass=None):
    if not connId:
      raise ZmqConnectorError('make requirement connector id is not defined')
    self.cache[connId] = connector = self.makeConn(connware, connKlass)
    return connector

  #----------------------------------------------------------------#
  # contains
  #----------------------------------------------------------------#
  def contains(self, taskId):
    try:
      self.cache[taskId]
    except KeyError:
      return False
    return True

  #----------------------------------------------------------------#
  # keys
  #----------------------------------------------------------------#		
  def keys(self):
    keys = copy.copy(self.cache.keys())
    return deque(keys.sorted())

  #----------------------------------------------------------------#
  # get
  #----------------------------------------------------------------#
  def get(self, taskId):
    return self.cache.get(taskId)

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#
  def close(self):
    logger.info(f'{self.name} is shutting down ...')
    [conn.sock.close() for taskId, conn in self.cache.items()]

#----------------------------------------------------------------#
# Connware
#----------------------------------------------------------------#		
class Connware:
  def __init__(self, sock=[], conn=[], sockopt={}, connkw={}):
    self.sock = sock
    self.conn = conn
    self.sockopt = sockopt
    self.connkw = connkw

  def modify(self, sock=None, conn=[], sockopt={}, connkw={}):
    cware = Connware()
    cware.sock = copy.copy(self.sock) if not sock else sock
    cware.conn = copy.copy(self.conn) if not conn else conn
    cware.sockopt = copy.copy(self.sockopt)
    if sockopt:
      cware.sockopt.update(sockopt)
    cware.connkw = copy.copy(self.connkw)
    if connkw:
      cware.connkw.update(connkw)
    return cware

