from apibase import AbstractTxnHost, MakeArgs
import asyncio
import logging
import zmq

logger = logging.getLogger('apipeer.hardhash')

#----------------------------------------------------------------#
# ServiceA
#----------------------------------------------------------------#		
class ServiceA(AbstractTxnHost):
  def __init__(self, connector, brokerId, *args, **kwargs):
    super().__init__(connector, brokerId)
    self.__dict__['NOTIFY'] = self._NOTIFY
    self.__dict__['TESTX'] = self._TESTX

  @classmethod
  def make(cls, context, actorId, packet):
    logger.info(f'{cls.__name__}, creating hardhash service, packet : {str(packet)}')
    connector = context.get(packet.taskId)
    if not connector:
      makeArgs = MakeArgs(
          sockType=zmq.DEALER,
          sockKw={zmq.IDENTITY:packet.taskId},
          conn=[context.hhDb])
      connector = context.addConn(packet.taskId, makeArgs)
      logger.info(f'!!! ServiceA, {packet.taskId}, added connector {connector.cid}')
    return cls(connector, context.hhId, actorId)

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    return getattr(self, key)

  # -------------------------------------------------------------- #
  # __call__
  # __call__ returns coroutine serve, which is intended to be final
  # This enables a descendent class to override __call__ with special 
  # options
  # ---------------------------------------------------------------#
  async def __call__(self):
    logger.info(f'### {self.hostname} is starting ...')
    self.active.set()    
    await self.serve()

  #----------------------------------------------------------------#
  # perform
  #----------------------------------------------------------------#
  async def perform(self, request, *args):    
    logger.info(f'{self.hostname}, got request : {request}')
    response = await self[request](*args)
    if response:
      await self.send(response,self.name)

  #----------------------------------------------------------------#
  # _TESTX - end of transmission
  #----------------------------------------------------------------#
  async def _TESTX(self, packet):
    logger.info(f'{self.hostname}, TESTX called : {str(packet)} ...')
    await asyncio.sleep(0.1)
    return [201,{'message':'TESTX is successful'}]
    
  #----------------------------------------------------------------#
  # _NOTIFY - end of transmission
  #----------------------------------------------------------------#
  async def _NOTIFY(self, taskId):
    logger.info(f'{self.hostname}, notified ...')
    await asyncio.sleep(0.1)
    self.active.clear()
