from apibase import AbstractTxnHost, Connware, TaskError
import asyncio
import logging
import zmq

logger = logging.getLogger('asyncio.microservice')

#----------------------------------------------------------------#
# ServiceA
#----------------------------------------------------------------#		
class ServiceA(AbstractTxnHost):
  def __init__(self, connector, hhId, actorId, *args, **kwargs):
    super().__init__(connector, hhId)
    self.actorId = actorId
    self.__dict__['NOTIFY'] = self._NOTIFY

  #----------------------------------------------------------------#
  # sockAddr
  #----------------------------------------------------------------#		
  @property
  def sockAddr(self):
    return self._conn.sockAddr

  def __getitem__(self, request):
    try:
      return self._conn[request]
    except KeyError:
      return getattr(self, request)

  @classmethod
  def make(cls, context, actorId, packet):
    logger.info(f'{cls.__name__}, creating hardhash service, packet : {packet.body}')
    connector = context.get(packet.taskId)
    if not connector:
      connware = Connware(
          sock=[zmq.DEALER],
          sockopt={zmq.IDENTITY:packet.taskId},
          conn=[context.hhDb])
      connector = context.addConn(packet.taskId, connware)
      logger.info(f'!!! ServiceA, {packet.taskId}, added connector {connector.cid}')
    return cls(connector, context.hhId, actorId)

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    pass

  #----------------------------------------------------------------#
  # perform
  #----------------------------------------------------------------#
  async def perform(self, bRequest, *args):    
    request = bRequest.decode()
    #logger.info(f'{self.hostname}, got request : {request}')
    await self[request](*args)

  #----------------------------------------------------------------#
  # _NOTIFY - end of transmission
  #----------------------------------------------------------------#
  async def _NOTIFY(self, bTaskId):
    taskId = bTaskId.decode()
    logger.info(f'{self.hostname}, notified ...')
    assert taskId == self.cid
    self.active.clear()
    await asyncio.sleep(0.01)
