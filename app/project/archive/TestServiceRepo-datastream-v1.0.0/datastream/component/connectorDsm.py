from apibase import AbstractConnector, LeveldbHash
import asyncio
import logging, os
import zmq

logger = logging.getLogger('asyncio.microservice')

#----------------------------------------------------------------#
# DatastreamResponse
#----------------------------------------------------------------#		
class DatastreamResponse(AbstractConnector):
  def __init__(self, sockware, *args, **kwargs):
    super().__init__(sockware.socket, **kwargs)
    self.sockAddr = sockware.address
    self.__dict__['PREPARE'] = self._PREPARE
    self.__dict__['START'] = self._START

  def __getitem__(self, request):
    return self.__dict__[request]

  #----------------------------------------------------------------#
  # recv
  #----------------------------------------------------------------#		
  def recv(self):
    return self.sock.recv_json()

  #----------------------------------------------------------------#
  # _PREPARE
  #----------------------------------------------------------------#		
  async def _PREPARE(self, jobId, taskId):
    self.jobId = jobId
    logger.info(f'{self.name}, job {jobId}, preparing {taskId} data stream ...')    
    try:
      dbKey = f'{jobId}|workspace'
      workspace = LeveldbHash.db[dbKey]
      dbKey = f'{jobId}|datastream|infile'
      self.infileName = LeveldbHash.db[dbKey]
    except KeyError as ex:
      errmsg = f'{jobId}, failed to get job article from datastorage'
      await self.sock.send_json([500, {'error': errmsg}])
      raise TaskError(errmsg)

    logger.info(f'{self.name}, datastream workspace, infile : {workspace}, {self.infileName}')
    self.infilePath = f'{workspace}/{self.infileName}'
    if not os.path.exists(self.infilePath):
      errmsg = f'source file {self.infileName} does not exist in workspace'
      await self.sock.send_json([500, {'error': errmsg}])
    else:
      await self.sock.send_json([200, {'status':'ready','infile':f'{self.infileName}'}])

  #----------------------------------------------------------------#
  # _START
  #----------------------------------------------------------------#		
  async def _START(self):
    logger.info(f'{self.name}, job {self.jobId}, now streaming file {self.infileName} ...')
    with open(self.infilePath, "rb") as bfh:
      while True:
        chunk = bfh.read(1024)
        if not chunk:
          await self.sock.send(b'')
          break
        await self.sock.send(chunk,flags=zmq.SNDMORE)

#----------------------------------------------------------------#
# DatastreamRequest
#----------------------------------------------------------------#		
class DatastreamRequest(AbstractConnector):
  def __init__(self, sockware, *args, **kwargs):
    super().__init__(sockware.socket, **kwargs)

  #----------------------------------------------------------------#
  # send
  #----------------------------------------------------------------#		
  async def send(self, packet, sender=None):
    logger.info(f'!!! {sender}, {self.cid} is sending a message : {packet}')
    await self.sock.send_json(packet)

  #----------------------------------------------------------------#
  # prepare
  #----------------------------------------------------------------#		
  async def prepare(self, jobId, taskNum):
    taskId = f'task{taskNum}'
    await self.send(['PREPARE', jobId, taskId])
    return await self.sock.recv_json()

  #----------------------------------------------------------------#
  # notify
  #----------------------------------------------------------------#		
  async def notify(self, taskId, owner):
    logger.info(f'{self.cid}.{taskId}, task completion notified by owner {owner}')
    await self.send(['NOTIFY', taskId])

  #----------------------------------------------------------------#
  # read
  #----------------------------------------------------------------#		
  async def read(self):
    await self.send(['START'])
    value = await self.sock.recv()
    yield value
    while self.sock.getsockopt(zmq.RCVMORE):
      value = await self.sock.recv(flags=zmq.RCVMORE)
      yield value
