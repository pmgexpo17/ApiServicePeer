__all__ = [
  'AbstractTxnHandler',  
  'AbstractTxnHost'
  ]

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
import asyncio
import logging
import zmq

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# AbstractTxnHandler
#----------------------------------------------------------------#		
class AbstractTxnHandler:
  def __init__(self, connector):
    self._conn = connector

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    raise NotImplementedError(f'{self.name}.destroy is an abstract method')

  #------------------------------------------------------------------#
	# recv - returns a future provided by pyzmq 
	#------------------------------------------------------------------#  
  async def recv(self):
    return await self._conn.recv()

  #------------------------------------------------------------------#
	# send - returns a future provided by pyzmq 
	#------------------------------------------------------------------#  
  async def send(self, packet, sender=None):
    await self._conn.send(packet,sender)

#----------------------------------------------------------------#
# AbstractTxnHost
#----------------------------------------------------------------#		
class AbstractTxnHost(AbstractTxnHandler):
  def __init__(self, connector, contextId, runMode=logging.INFO):
    super().__init__(connector)
    self.contextId = contextId
    self.active = asyncio.Event()
    self.runMode = runMode

  @property
  def hostname(self):
    return f'{self.name}-{self.contextId}-{self.cid}'

  @property
  def cid(self):
    return self._conn.cid

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  async def __call__(self):
    logger.info(f'### {self.hostname} is starting ...')
    self.active.set()    
    await self.serve()

  # -------------------------------------------------------------- #
  # serve
  # ---------------------------------------------------------------#
  async def serve(self):
    try:
      while self.active.is_set():
        packet = await self.recv()
        await self.perform(*packet)
      logger.info(f'{self.hostname}, is complete')
    except zmq.ContextTerminated as ex:
      logger.info(f'{self.hostname}, context terminated, closing ...')
    except asyncio.CancelledError:
      logger.info(f'{self.hostname}, is canceled')
    except Exception:
      logger.info(f'{self.hostname}, unexpected exception caught',exc_info=True)

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    del self._conn

  # -------------------------------------------------------------- #
  # perform
  # ---------------------------------------------------------------#
  async def perform(self, packet):
    raise NotImplementedError(f'{self.hostname}.perform is an abstract class method')

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    self._conn.close()

  # -------------------------------------------------------------- #
  # start
  # -- context startup is expected here
  # ---------------------------------------------------------------#
  def start(self):
    raise NotImplementedError(f'{self.hostname}.start is an abstract method')    
