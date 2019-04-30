from abc import ABCMeta, abstractmethod
from collections import deque
from threading import RLock
import logging
import pickle
import simplejson as json
import os, sys, time
import zmq

logger = logging.getLogger('apipeer.tools')

# -------------------------------------------------------------- #
# HardHashActor
# ---------------------------------------------------------------#
class HardHashActor:
  def __init__(self, leveldb, actorId):
    self._leveldb = leveldb
    self.actorId = actorId    

  def __call__(self, jobId, *args, **kwargs):
    self.runActor(jobId, *args, **kwargs)
    HHProvider.notify(jobId, self.actorId, token=self.__class__.__name__)

#------------------------------------------------------------------#
# HHDealer
#------------------------------------------------------------------#
class HHDealer:
  def __getitem__(self, key):
    if key not in self.__dict__:
      self.__dict__[key] = []
    return self.__dict__[key]

  def __setitem__(self, key, value):
    self.__dict__[key] = [value]

  def __delitem__(self, key):
    del self.__dict__[key]

#------------------------------------------------------------------#
# HHProvider
# factory dict allows multiple client sessions, where session id = hhId
#------------------------------------------------------------------#
class HHProvider:
  lock = RLock()
  factory = {}

  def __init__(self, hhId, routerAddr):
    self.hhId = hhId
    self.routerAddr = routerAddr
    self._cache = {}
    self._keys = deque()
    self._context = zmq.Context.instance()
    self._controler = None
    self._started = HHDealer()
    self.lock = RLock()

  #------------------------------------------------------------------#
  # start -
  # - HHProvider factory creation
  #------------------------------------------------------------------#
  @staticmethod
  def create(hhId, routerAddr, clients=[]):
    with HHProvider.lock:
      try:
        factory = HHProvider.factory[hhId]
      except KeyError:
        if not routerAddr:
          raise Exception('HardHash socket address is not defined')
        factory = HHProvider.factory[hhId] = HHProvider(hhId, routerAddr)
        factory.add('control')
        return [factory.add(clientId) for clientId in clients]
      return [factory.add(clientId) for clientId in clients]

	#------------------------------------------------------------------#
	# remove
	#------------------------------------------------------------------#
  @staticmethod
  def delete(hhId):
    with HHProvider.lock:
      if hhId in HHProvider.factory:
        logger.info(f'### HHProvider {hhId} will be deleted ...')
        HHProvider.factory[hhId].terminate()
        del HHProvider.factory[hhId]  
        logger.info(f'### HHProvider {hhId} is deleted')

	#------------------------------------------------------------------#
	# close
	#------------------------------------------------------------------#
  @staticmethod
  def close(hhId, clientId):
    with HHProvider.lock:
      HHProvider.factory[hhId]._close(clientId)

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  @staticmethod
  def get(hhId, clientId=None):
    return HHProvider.factory[hhId]._get(clientId)

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  @staticmethod
  def start(hhId, actorId, clientId=None):
    return HHProvider.factory[hhId]._start(actorId,clientId)

	#------------------------------------------------------------------#
	# stop
	#------------------------------------------------------------------#
  @staticmethod
  def notify(hhId, actorId, clientId=None, token=None):
    return HHProvider.factory[hhId]._notify(actorId,clientId,token)

	#------------------------------------------------------------------#
	# _close
	#------------------------------------------------------------------#
  def _close(self, clientId):
    try:
      self._cache[clientId].close()
      del self._cache[clientId]
    except KeyError:
      raise Exception(f'{self.hhId} factory, client {clientId} not found in cache')
    except zmq.ZMQError as ex:
      raise Exception(f'{self.hhId} factory, client {clientId} closure failed',exc_info=True)

	#------------------------------------------------------------------#
	# terminate
	#------------------------------------------------------------------#
  def terminate(self):
    try:
      [client.close() for key, client in self._cache.items()]
    except zmq.ZMQError as ex:
      raise Exception(f'{self.hhId} factory, closure failed', exc_info=True)

	#------------------------------------------------------------------#
	# _get
	#------------------------------------------------------------------#
  def _get(self, clientId=None):
    with self.lock:
      if not clientId:
        clientId = self._keys[0]
        self._keys.rotate()
        return self._cache[clientId]
      elif clientId == 'control':
        if self._controler is None:
          self.add(clientId)
        return self._controler
      elif clientId not in self._cache[clientId]:
        return self.add(clientId)
      return self._cache[clientId]

	#------------------------------------------------------------------#
	# _start
	#------------------------------------------------------------------#
  def _start(self, actorId, clientId):
    with self.lock:
      if clientId == 'control':
        return self._get(clientId)
      hhClient = self._get(clientId)
      logger.debug(f'### {actorId}, {hhClient.id} is starting ...')
      if self._controler.start(hhClient.id) == 'OK':
        logger.info(f'### {actorId}, {hhClient.id} is now started')
        hhClient.join(actorId)
        self._started[actorId].append(hhClient.id)
        return hhClient

	#------------------------------------------------------------------#
	# _notify
	#------------------------------------------------------------------#
  def _notify(self, actorId, clientId, token):
    with self.lock:
      if not clientId:
        [self._cache[clientId].notify(actorId, token) for clientId in self._started[actorId]]
        del self._started[actorId]
      else:
        if clientId not in self._started:
          logger.warn(f'{self.hhId} factory, notify ignored, {clientId} is not started')
        self._started[actorId].remove(clientId)      
        self._cache[clientId].notify(actorId, token)

	#------------------------------------------------------------------#
	# add
	#------------------------------------------------------------------#
  def add(self, clientId):
    if clientId == 'control':
      self._controler = HHControler.make(self._context, clientId, self.routerAddr)
    else:
      logger.info(f'### {self.hhId} factory, client created by clientId : {clientId}')
      hhClient = HardHashClient.make(self._context, clientId, self.routerAddr)
      self._cache[clientId] = hhClient
      # append left, so we get the least used socket
      # since default mode _get reads the left most index then rotates
      self._keys.appendleft(clientId)
      return hhClient

#----------------------------------------------------------------#
# WrongTxnProtocol
#----------------------------------------------------------------#		
class WrongTxnProtocol(Exception):
  pass

#----------------------------------------------------------------#
# MessageTimeout
#----------------------------------------------------------------#		
class MessageTimeout(Exception):
  pass

#----------------------------------------------------------------#
# EmptyViewResult
#----------------------------------------------------------------#		
class EmptyViewResult(Exception):
  pass

#----------------------------------------------------------------#
# Messenger
#----------------------------------------------------------------#		
class Messenger():
  def __init__(self, clientId, pickleProtocol):
    self.clientId = clientId
    self._protocol = pickleProtocol
    self.sock = None

  @property
  def id(self):
    return self.clientId

  #----------------------------------------------------------------#
  # _make
  #----------------------------------------------------------------#		
  def _make(self, context, sockType, sockAddr, hwm=1000):
    self.sock = context.socket(sockType)
    self.sock.set_hwm(hwm)
    self.sock.identity = self.clientId.encode()
    self.sock.connect(sockAddr.encode())
    time.sleep(0.1)    

  #----------------------------------------------------------------#
  # putPacket
  #----------------------------------------------------------------#		
  def putPacket(self, packet, payload=None):
    try:      
      packet = [item.encode() for item in packet]      
      if payload:
        if not isinstance(payload, (bytes, bytearray)):
        # use pickle to handle any kind of object value
          payload = pickle.dumps(payload, self._protocol)
        packet.append(payload)
      self.sock.send_multipart(packet)
    except zmq.errno == zmq.EFSM: 
      raise WrongTxnProtocol('txn protocol is out of order')
    except Exception as ex:
      logger.error(f'!!! putPacket failed : {ex}')
      raise

  #----------------------------------------------------------------#
  # _getPacket
  #----------------------------------------------------------------#		
  def _getPacket(self):
    try:
      return self.sock.recv()
    except zmq.errno == zmq.EFSM: 
      raise WrongTxnProtocol('txn protocol is out of order')
    except Exception as ex:
      logger.error(f'!!! _getPacket failed : {ex}')
      raise

#----------------------------------------------------------------#
# HHControler
#----------------------------------------------------------------#		
class HHControler(Messenger):
  def __init__(self, clientId, pickleProtocol=pickle.HIGHEST_PROTOCOL):
    super().__init__(clientId, pickleProtocol)
    self.lock = RLock()

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#		
  @staticmethod
  def make(context, clientId, routerAddr, **kwargs):
    logger.info(f'### creating HardHash controler, routerAddr : {routerAddr}')
    client = HHControler(clientId)
    client._make(context, zmq.DEALER, routerAddr, **kwargs)
    return client

  #----------------------------------------------------------------#
  # start
  #----------------------------------------------------------------#		
  def start(self, clientId):
    with self.lock:
      try:
        logger.debug(f'!!! START : {clientId}')
        self.putPacket(['START', clientId])
        return self.getPacket('START')
      except Exception as ex:
        logger.error(f'start failed : {ex}')
        raise

  #----------------------------------------------------------------#
  # status
  #----------------------------------------------------------------#		
  def status(self, stateKey):
    with self.lock:
      try:
        logger.debug(f'!!! STATUS : {stateKey}')
        self.putPacket(['STATUS', stateKey])
        return self.getPacket('STATUS')
      except Exception as ex:
        logger.error(f'status failed : {ex}')
        raise

  #----------------------------------------------------------------#
  # getPacket
  #----------------------------------------------------------------#		
  def getPacket(self, action):
    value = self._getPacket().decode()
    logger.debug(f'!!! controler got a message, {value}')
    if value == 'KEY_ERROR':
      raise KeyError()
    if value == 'HH_ERROR':
      raise Exception(f'{action} failed')
    return value

#----------------------------------------------------------------#
# HardHashClient
#----------------------------------------------------------------#		
class HardHashClient(Messenger):
  def __init__(self, clientId, pickleProtocol=pickle.HIGHEST_PROTOCOL):
    super().__init__(clientId, pickleProtocol)
    self._debug = False
    self.restoreMode = 'PYOBJ'
    self.lock = RLock()
    self._actors = []

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#		
  @staticmethod
  def make(context, clientId, routerAddr, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # getPacket
  #----------------------------------------------------------------#		
  def getPacket(self, action):
    value = pickle.loads(self._getPacket())
    if value == 'KEY_ERROR':
      raise KeyError(f'{action} failed')
    if value == 'HH_ERROR':
      raise Exception(f'{action} failed')
    try:
      if self.restoreMode == 'JSON':
        return json.dumps(value)
      return value
    except ValueError:
      return value

  #----------------------------------------------------------------#
  # setRestoreMode
  #----------------------------------------------------------------#		
  def setRestoreMode(self, mode):
    self.restoreMode = mode

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    with self.lock:
      try:
        if not self.sock.closed:
          self.sock.setsockopt(zmq.LINGER, 0)          
          self.sock.close()
          logger.info('### client socket is closed')
      except zmq.ZMQError as ex:
        logger.error(f'### client closure failed : {ex}')

  #----------------------------------------------------------------#
  # join
  #----------------------------------------------------------------#		
  def join(self, actorId):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # notify
  #----------------------------------------------------------------#		
  def notify(self, actorId, token):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # MutableMapping methods for emulating a dict
  #----------------------------------------------------------------#		

  #----------------------------------------------------------------#
  # __getitem__
  #----------------------------------------------------------------#		
  def __getitem__(self, key):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # __setitem__
  #----------------------------------------------------------------#		
  def __setitem__(self, key, value):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # __delitem__
  #----------------------------------------------------------------#		
  def __delitem__(self, key):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # getGenerator
  #----------------------------------------------------------------#		
  def getGenerator(self):
    firstValue = self.getPacket('SELECT')
    if not firstValue:
      raise EmptyViewResult()
    yield firstValue
    while self.sock.getsockopt(zmq.RCVMORE):
      yield self.getPacket('SELECT')

  #----------------------------------------------------------------#
  # append
  #----------------------------------------------------------------#		
  def append(self, key, value):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # select
  # - use with caution - because the related socket is bound to the
  # - output generator, the socket is unusable until generator is complete
  #----------------------------------------------------------------#		
  def select(self, keyLow, keyHigh, keysOnly=False):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # minmax
  # get the resolved bounds of a range
  #----------------------------------------------------------------#		
  def minmax(self, keyLow, keyHigh):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  #----------------------------------------------------------------#
  # other methods
  #----------------------------------------------------------------#		

  def __iter__(self):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  def __len__(self):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    pass

  def __contains__(self, key):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')
