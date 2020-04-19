__all__ = [
  'AppCooperator', 
  'AppDirector', 
  'AppResolvar',
  'AppState',
  'Hashable',
  ]

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from abc import ABCMeta, abstractmethod
from apibase import Article, Note, Terminal
from asyncio import CancelledError
from concurrent.futures import CancelledError as FutCancelledError
from apitools.datastore import HardhashContext
import logging
import os

logger = logging.getLogger('asyncio.smart')

# -------------------------------------------------------------- #
# AppActor
# ---------------------------------------------------------------#
class AppActor(Terminal):

  def __init__(self, actorId):
    self.actorId = actorId
    self.resolve = None
    self.state = AppState(actorId)
    
  @property
  def id(self):
    return self.actorId

  @property
  def name(self):
    return f'{self.__class__.__qualname__}'

  def tell(self):
    return self.actorId, self.name

  # ------------------------------------------------------------ #
  # apply
  # -------------------------------------------------------------#
  def apply(self, *args, **kwargs):
    try:
      if self.state.status == 'STOPPED':
        self.state.status = 'STARTING'
        self.start(*args, **kwargs)
        self.state.status = 'STARTED'
      elif self.state.status == 'STARTED':
        self.runApp(*args, **kwargs)
    except Exception as ex:
      self.state.failed = True
      self.state.complete = True
      self.onError(ex)

  # -------------------------------------------------------------- #
  # quicken
  # -------------------------------------------------------------- #
  def quicken(self):
    state = self.state
    packet = self._quicken[state.current]
    logger.info(f'{self.name}, quicken, promote args : {packet}')
    return packet

  # -------------------------------------------------------------- #
  # destroy
  # -------------------------------------------------------------- #
  def destroy(self, *args, **kwargs):
    logger.info(f'{self.name} destroy is not required')

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.onError is an abstract method')

  # -------------------------------------------------------------- #
  # prepare
  # -------------------------------------------------------------- #
  def prepare(self):
    self.state.signal = 201
    self.state.failed = False
    return self.state

  # -------------------------------------------------------------- #
  # runApp
  # ---------------------------------------------------------------#
  def runApp(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.runApp is an abstract method')

  # -------------------------------------------------------------- #
  # start
  # -------------------------------------------------------------- #
  def start(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.start is an abstract method')

  # -------------------------------------------------------------- #
  # stop
  # -------------------------------------------------------------- #
  def stop(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.stop is an abstract method')

# -------------------------------------------------------------- #
# AppDirector
# ---------------------------------------------------------------#
class AppDirector(AppActor):
  def __init__(self, actorId):
    super().__init__(actorId)

  # ------------------------------------------------------------ #
  # thread / process callable method
  # -------------------------------------------------------------#
  def __call__(self, *args, **kwargs):
    try:
      self.apply(*args, **kwargs)
    except FutCancelledError:
      logger.info(f'{self.name} actor {self.actorId} future is cancelled')
    except Exception as ex:
      logger.error('actor error',exc_info=True)
    finally:
      return self.state

  # -------------------------------------------------------------- #
  # runApp
  # ---------------------------------------------------------------#
  def runApp(self, *args, signal=None, **kwargs):

    state = self.state
    if state.inTransition and signal is not None:
      logger.info(f'{self.name} received signal : {signal}')
      state.advance(signal)
      if state.inTransition:
        self.state = state
        logger.info('transition incomplete, multiple signals required ...')
        # in this case multiple signals are required to resolve the state transition
        # until all signals are received state.hasNext must remain False
        return
      logger.info('state transition resolved by signal : ' + str(signal))
    while state.hasNext: # complete?
      logger.info(f'{self.name} resolving state : {state.current}')
      state = self.resolve[state.current](*args, **kwargs)
      if state.inTransition:
        break
      state.advance()
    self.state = state

# -------------------------------------------------------------- #
# AppCooperator
# ---------------------------------------------------------------#
class AppCooperator(AppActor):
  def __init__(self, actorId):
    super().__init__(actorId)

  # ------------------------------------------------------------ #
  # thread / process callable method
  # -------------------------------------------------------------#
  async def __call__(self, *args, **kwargs):
    try:
      await self.apply(*args, **kwargs)
    except CancelledError:
      logger.info(f'{self.name} actor {self.actorId} future is cancelled')
    except Exception as ex:
      logger.error('actor error',exc_info=True)
    finally:
      return self.state

  # ------------------------------------------------------------ #
  # apply
  # -------------------------------------------------------------#
  async def apply(self, *args, **kwargs):
    try:
      if self.state.status == 'STOPPED':
        self.state.status = 'STARTING'
        self.start(*args, **kwargs)
        self.state.status = 'STARTED'
      elif self.state.status == 'STARTED':
        await self.runApp(*args, **kwargs)
    except Exception as ex:
      self.state.failed = True
      self.state.complete = True
      self.onError(ex)

  # -------------------------------------------------------------- #
  # runApp
  # ---------------------------------------------------------------#
  async def runApp(self, *args, signal=None, **kwargs):

    state = self.state
    if state.inTransition and signal is not None:
      logger.info(f'{self.name} received signal : {signal}')
      state.advance(signal)
      if state.inTransition:
        self.state = state
        logger.info('transition incomplete, multiple signals required ...')
        # in this case multiple signals are required to resolve the state transition
        # until all signals are received state.hasNext must remain False
        return
      logger.info('state transition resolved by signal : ' + str(signal))
    while state.hasNext: # complete?
      logger.info(f'{self.name} resolving state : {state.current}')
      state = await self.resolve[state.current](*args, **kwargs)
      if state.inTransition:
        break
      state.advance()
    self.state = state

# -------------------------------------------------------------- #
# AppState
# ---------------------------------------------------------------#
class AppState:

  def __init__(self, actorId=None):
    self.actorId = actorId
    self.canceled = False
    self.complete = False
    self.current = 'NULL'
    self.distributed = False
    self.failed = False
    self.hasNext = False
    self.hasOutput = False
    self.hasSignal = False
    self.inTransition = False
    self.next = 'NULL'
    self.signal = 0
    self.signalFrom = []
    self.status = 'STOPPED'

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    raise KeyError(f'{key} is not found')

  @property
  def running(self):
    return self.status == 'STARTED'

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self):
    self.complete = True
    self.failed = True
    self.status = 'FAILED'

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, delta):
    self.__dict__.update(delta)

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#
  def advance(self, signal=None):
    # signal = the http status code of the companion actor method
    if signal:
      self.hasSignal = False
      if signal != 201:
        logMsg = f'state transition {self.current} failed, got error signal : {signal}'
        raise Exception(logMsg)
      self.inTransition = False
      if self.complete:
        logger.info(f'{self.current} is resolved and program is complete ...')
        return
      logger.info(f'{self.current} is resolved, advancing ...')
      self.hasNext = True
    if self.hasNext:
      self.current = self.next

# -------------------------------------------------------------- #
# Hashable
# ---------------------------------------------------------------#
class Hashable:

  @property
  def name(self):
    return self.__class__.__qualname__

  def __getitem__(self, key):
    try:
      return getattr(self, key)
    except AttributeError:
      if key in self.__dict__:
        return self.__dict__[key]
      raise AttributeError(f'{key} is not a {self.name} attribute')

  def __setitem__(self, key, value):
    setattr(self, key, value)

  def __delitem__(self, key):
    delattr(self, key)

  def __contains__(self, key):
      return hasattr(key)

  def __len__(self):
      return len(self.__dict__)

# -------------------------------------------------------------- #
# AppResolvar
# ---------------------------------------------------------------#
class AppResolvar(Hashable, Terminal):
  def __init__(self, jobId):
    self._leveldb = HardhashContext.connector(contextId=jobId)

  # -------------------------------------------------------------- #
  # destroy
  # -------------------------------------------------------------- #
  def destroy(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # query
  # ---------------------------------------------------------------#
  def query(self, packet, render='Note'):
    result = self.select(packet)
    if render == 'Note':
      return Note(result)
    elif render == 'Article':
      return Article(result)
    else:
      return result

  # -------------------------------------------------------------- #
  # select
  # ---------------------------------------------------------------#
  def select(self, packet):
    try:      
      result = self._leveldb[packet.eventKey]
      try:
        packet.itemKey
        logger.info('### query item key : ' + packet.itemKey)
        return result[packet.itemKey]
      except AttributeError:
        return result
    except KeyError:
      logger.error(f'saas meta item {packet.eventKey} not found')
      raise Exception
    

