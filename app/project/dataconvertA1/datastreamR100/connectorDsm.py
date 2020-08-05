from apibase import ZmqConnector
from apitools import HardhashContext
import asyncio
import logging, os
import zmq

logger = logging.getLogger('asyncio.microservice')

#----------------------------------------------------------------#
# DatastreamResponse
#----------------------------------------------------------------#		
class DatastreamResponse(ZmqConnector):
  def __init__(self, sockware, *args, **kwargs):
    super().__init__(sockware.socket, **kwargs)
    self.sockAddr = sockware.address

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, sockware, **kwargs):
    return cls(sockware, **kwargs)

  #----------------------------------------------------------------#
  # recv
  #----------------------------------------------------------------#		
  def recv(self):
    return self.sock.recv_json()

#----------------------------------------------------------------#
# DatastreamRequest
#----------------------------------------------------------------#		
class DatastreamRequest(ZmqConnector):
  def __init__(self, sockware, *args, **kwargs):
    super().__init__(sockware.socket, **kwargs)

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, sockware, **kwargs):
    return cls(sockware, **kwargs)

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
