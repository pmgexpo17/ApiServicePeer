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
    self.__dict__['PREPARE'] = self._PREPARE
    self.__dict__['START'] = self._START

  #----------------------------------------------------------------#
  # sockAddr
  #----------------------------------------------------------------#		
  @property
  def sockAddr(self):
    return self._conn.sockAddr

  def __getitem__(self, request):
    return getattr(self, request)

  #----------------------------------------------------------------#
  # make - replaced zmq.DEALER with zmq.ROUTER to avoid confusion.
  # ie - by convention a binded socket for serving packets to 1 or
  # more clients should be type ROUTER and a connected socket for 
  # receiving packets should be type DEALER.
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, context, actorId, packet):
    logger.info(f'{cls.__name__}, creating datastream service, packet : {packet.body}')
    connector = context.get(packet.taskId)
    if not connector:
      connware = Connware(
          sock=[zmq.ROUTER],
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
    
  #----------------------------------------------------------------#
  # _PREPARE - moved from connectorDsm.DatastreamResponse, to improve 
  # datastream service concept presentation
  #----------------------------------------------------------------#		
  async def _PREPARE(self, jobId, taskId):
    self.jobId = jobId
    logger.info(f'{self.name}, job {jobId}, preparing {taskId} data stream ...')    
    hardhash = HardhashContext.connector(jobId)
    try:
      dbKey = f'{jobId}|workspace'
      workspace = hardhash[dbKey]
      dbKey = f'{jobId}|datastream|infile'
      self.infileName = hardhash[dbKey]
    except KeyError as ex:
      errmsg = f'{jobId}, failed to get job article from datastorage'
      await self._conn.sendReply([500, {'error': errmsg}])
      raise TaskError(errmsg)

    logger.info(f'{self.name}, datastream workspace, infile : {workspace}, {self.infileName}')
    self.infilePath = f'{workspace}/{self.infileName}'
    if not os.path.exists(self.infilePath):
      errmsg = f'source file {self.infileName} does not exist in workspace'
      await self._conn.sendReply([500, {'error': errmsg}])
    else:
      await self._conn.sendReply([200, {'status':'ready','infile':f'{self.infileName}'}])

  #----------------------------------------------------------------#
  # _START - moved from connectorDsm.DatastreamResponse, to improve
  # datastream service concept presentation
  #----------------------------------------------------------------#		
  async def _START(self):
    logger.info(f'{self.name}, job {self.jobId}, now streaming file {self.infileName} ...')
    with open(self.infilePath, "rb") as bfh:
      while True:
        chunk = bfh.read(1024)
        if not chunk:
          await self._conn.sendBytes(b'')
          break
        await self._conn.sendBytes(chunk,flags=zmq.SNDMORE)
