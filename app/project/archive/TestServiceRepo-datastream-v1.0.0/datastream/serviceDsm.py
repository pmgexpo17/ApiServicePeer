from apibase import AbstractTxnHost, Connware, LeveldbHash, TaskError
import asyncio
import logging, os
import zmq

logger = logging.getLogger('asyncio.microservice')

#----------------------------------------------------------------#
# ServiceA
#----------------------------------------------------------------#		
class ServiceA(AbstractTxnHost):
  def __init__(self, connector, contextId, actorId, *args, **kwargs):
    super().__init__(connector, contextId)
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
    logger.info(f'{cls.__name__}, creating datastream service, packet : {packet.body}')
    connector = context.get(packet.taskId)
    if not connector:
      connware = Connware(
          sock=[zmq.DEALER],
          sockopt={zmq.IDENTITY:packet.taskId})
      connector = context.addConn(packet.taskId, connware)
    return cls(connector, context.contextId, actorId)

  #----------------------------------------------------------------#
  # perform
  #----------------------------------------------------------------#
  async def perform(self, request, *args):
    await self[request](*args)
  
  #----------------------------------------------------------------#
  # _NOTIFY - end of transmission
  #----------------------------------------------------------------#
  async def _NOTIFY(self, jobId):
    logger.info(f'{self.hostname}, {jobId} notified ...')
    self.active.clear()
    asyncio.sleep(0.1)
