__all__ = ['ZmqDatasource']

# The MIT License
#
# Copyright (c) 2019 Peter A McGill
#
from apibase import Article, AbstractDatasource
from threading import Event 
import asyncio
import logging
import zmq.asyncio
import zmq

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# ZmqDatasource
#----------------------------------------------------------------#		
class ZmqDatasource(AbstractDatasource):
  context = None
  hostAddr = None

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  #----------------------------------------------------------------#
  # start - this is intended to be an abstract class method
  # - by extending, the descendent class owns the context exclusively
  #----------------------------------------------------------------#
  @classmethod
  def __start__(cls, hostAddr):
    if not cls.context:
      cls.context = zmq.asyncio.Context()
      cls.hostAddr = hostAddr #'tcp://127.0.0.1'

  @classmethod
  def destroy(cls):
    logger.info(f'{cls.__name__}, destroying zmq context ...')
    if cls.context:
      cls.context.destroy()

  #----------------------------------------------------------------#
  # makes and returns a broker
  #----------------------------------------------------------------#
  @classmethod
  def makeBroker(cls, brokerId, frontPort=None):
    return ZmqMessageBroker.make(brokerId, frontPort)

  #----------------------------------------------------------------#
  # makes and returns a socket
  # NOTE : socket method must by lock protected by the descendent class
  # See MicroserviceContext.add for an example
  #----------------------------------------------------------------#
  def socket(self, socktype, sockopt={}):
    socket = self.context.socket(socktype)
    for option, value in sockopt.items():
      try:
        socket.setsockopt_string(option, value)
      except zmq.error.ZMQError:
        logger.warn(f'invalid socket option or value : {option}, {str(value)}')
    return socket

#----------------------------------------------------------------#
# ZmqMessageBroker
#----------------------------------------------------------------#		
class ZmqMessageBroker(ZmqDatasource):
  def __init__(self, brokerId, sockets, requestAddr, responseAddr):
    self.brokerId = brokerId
    self.sockets = sockets
    self.requestAddr = requestAddr
    self.responseAddr = responseAddr
    self.active = Event()
    self._future = None

  #----------------------------------------------------------------#
  # title
  #----------------------------------------------------------------#		
  @property
  def title(self):
    return f'{self.__class__.__name__}-{self.brokerId}'

  @classmethod
  def make(cls, brokerId, frontPort=None):
    sockets = []
    sockAddr = Article()
    frontend = cls.context.socket(zmq.ROUTER)
    sockets.append(frontend)
    requestAddr = cls.bind(frontend,cls.hostAddr,frontPort)
    logger.info(f'broker, requestAddr : {requestAddr}')
    backend = cls.context.socket(zmq.ROUTER)
    sockets.append(backend)    
    responseAddr = cls.bind(backend,cls.hostAddr)
    logger.info(f'broker, responseAddr : {responseAddr}')
    return ZmqMessageBroker(brokerId, sockets, requestAddr, responseAddr)

  @classmethod
  def bind(cls, socket, hostAddr, port=None):
    if port:
      sockAddr = f'{hostAddr}:{port}'
      socket.bind(sockAddr)
    else:
      port = socket.bind_to_random_port(hostAddr)
      sockAddr = f'{hostAddr}:{port}'
    return sockAddr

  #----------------------------------------------------------------#
  # backserve
  #----------------------------------------------------------------#
  async def backserve(self):
    logger.info(f'{self.title}, broker backend is starting ...')
    try:
      frontend, backend = self.sockets
      while self.active.is_set():
        packet = await backend.recv_multipart()
        await frontend.send_multipart(packet)
      logger.info(f'{self.title}, broker backend is complete')
    except zmq.ContextTerminated as ex:
      logger.info(f'{self.title}, context terminated, backend closing ...')
    except asyncio.CancelledError:
      logger.info(f'{self.title}, broker backend is canceled')
    except Exception:
      logger.info(f'{self.title}, unexpected exception caught',exc_info=True)
    finally:
      self.close(backend)

  #----------------------------------------------------------------#
  # frontserve
  #----------------------------------------------------------------#
  async def frontserve(self):
    logger.info(f'{self.title}, broker frontend is starting ...')
    try:
      frontend, backend = self.sockets
      while self.active.is_set():
        packet = await frontend.recv_multipart()
        await backend.send_multipart(packet)
      logger.info(f'{self.title}, broker frontend is complete')
    except zmq.ContextTerminated as ex:
      logger.info(f'{self.title}, context terminated, frontend closing ...')
    except asyncio.CancelledError:
      logger.info(f'{self.title}, broker frontend is canceled')
    except Exception:
      logger.info(f'{self.title}, unexpected exception caught',exc_info=True)
    finally:
      self.close(frontend)

  #----------------------------------------------------------------#
  # __call__
  #----------------------------------------------------------------#
  def __call__(self):
    logger.info(f'{self.title}, broker starting ...')
    self.active.set()
    self._future = asyncio.gather(self.backserve(), self.frontserve())

  #----------------------------------------------------------------#
  # shutdown
  #----------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.title}, broker is closing ...')
    if self._future:
      self._future.cancel()
    logger.info(f'{self.title}, broker is closed')

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#
  def close(self):
    # zmq asyncio sockets provide full treatment safe closure 
    frontend, backend = self.sockets
    self._close(frontend)
    self._close(backend)

  def _close(self, socket):
    if socket and not socket.closed:
      socket.close(linger=0)

  def destroy(self):
    pass
