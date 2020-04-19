__all__ = [
  'ApiConnector',
  'ZmqConnector']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AbstractConnector
import logging

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# ZmqConnector
#----------------------------------------------------------------#		
class ZmqConnector(AbstractConnector):
  def __init__(self, socket, runMode=logging.INFO):
    self.sock = socket
    self._id = socket.identity
    self.runMode = runMode

  @property
  def name(self):
    return f'{self.__class__.__name__}.{self.cid}'

  @property
  def cid(self):
    return self._id.decode()

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#		
  @classmethod
  def make(cls, socket, *args, **kwargs):
    return cls(socket, *args, **kwargs)

  #----------------------------------------------------------------#
  # recv
  #----------------------------------------------------------------#		
  def recv(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.recv is an abstract method')

  #----------------------------------------------------------------#
  # send
  #----------------------------------------------------------------#		
  def send(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.send is an abstract method')

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    if self.sock:
      self.sock.close(linger=0)
    logger.info(f'{self.cid} socket is closed')

  #----------------------------------------------------------------#
  # destroy
  #----------------------------------------------------------------#		
  def destroy(self):
    self.close()
    del self.sock

#----------------------------------------------------------------#
# ApiConnector
#----------------------------------------------------------------#		
class ApiConnector(ZmqConnector):
  def __init__(self, sockware, *args, **kwargs):
    super().__init__(sockware.socket, **kwargs)

  #----------------------------------------------------------------#
  # recv
  #----------------------------------------------------------------#		
  async def recv(self):
    return await self.sock.recv_json()

  #----------------------------------------------------------------#
  # send
  #----------------------------------------------------------------#		
  async def send(self, packet, sender=None):
    if not sender:
      sender = self.cid
    logger.info(f'!!! {sender}, {self.cid} is sending a message : {packet}')
    await self.sock.send_json(packet)