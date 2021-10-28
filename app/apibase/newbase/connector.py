import asyncio
import logging

from asyncio import Future, Queue, StreamReader, StreamWriter
from dataclasses import dataclass, field, InitVar
from datetime import datetime
from typing import Any

from .component import Article, Note

logger = logging.getLogger('asyncio')

#-----------------------------------------------------------------#
# create_task
#-----------------------------------------------------------------#
def create_task(aw) -> asyncio.Task:
  loop = asyncio.get_event_loop()
  return loop.create_task(aw)

#================================================================#
# AbcConnector
#===============================================================-#
class AbcConnector:

  @property
  def name(self):
    return self.__class__.__name__

  #----------------------------------------------------------------//
  # close
  #----------------------------------------------------------------//
  def close(self):
    raise NotImplementedError("{}.close is still an abstract method".format(self.name))

  #----------------------------------------------------------------//
  # open
  #----------------------------------------------------------------//
  @classmethod
  def open(cls) -> object:
    raise NotImplementedError("{}.open is still an abstract method".format(cls.__name__))
  
  #----------------------------------------------------------------//
  # read
  #----------------------------------------------------------------//
  def _read(self):
    raise NotImplementedError("{}.read is still an abstract method".format(self.name))

  #----------------------------------------------------------------//
  # write
  #----------------------------------------------------------------//
  def _write(self):
    raise NotImplementedError("{}.close is still an abstract method".format(self.name))

#================================================================#
# QuConnector - for unit testing without socket connection overhead
#===============================================================-#
@dataclass
class QuConnector:
  id: str
  _reader: Queue
  _writer: Queue
  
  #----------------------------------------------------------------//
  # close - simulate asyncio socket closure
  #----------------------------------------------------------------//
  async def close(self):
    if not self._reader.empty():
      for _ in range(self._reader.qsize()):
        await self._reader.get()
        self._reader.task_done()
    if not self._writer.empty():
      for _ in range(self._writer.qsize()):
        await self._writer.get()
        self._writer.task_done()

  #----------------------------------------------------------------//
  # open
  # -- bandDesc set = (DUEL, SINGLE) where SINGLE band would apply 
  # -- for a PUSH / PULL arrangment
  #----------------------------------------------------------------//
  @classmethod
  def open(cls, bandDesc="DUEL", cid="0") -> object:
    if cid == "0":
      cid = datetime.now().strftime('%S%f')
    if bandDesc == "SINGLE":
      conn = Queue()
      return cls(cid, conn, conn)
    return cls(cid, Queue(), Queue())
  
  #----------------------------------------------------------------//
  # _write
  #----------------------------------------------------------------//
  async def _write(self, payload):
    if isinstance(payload, Article):
      payload = payload.reducce()
    await self._writer.put(payload)

  #----------------------------------------------------------------//
  # receive
  #----------------------------------------------------------------//
  async def _read(self) -> object:
    payload = await self._reader.get()
    if isinstance(payload, dict):
      return Article.deducce(payload)
    return payload

  #----------------------------------------------------------------//
  # cloneReversed
  #----------------------------------------------------------------//
  def cloneReversed(self) -> object:
    return QuConnector(self.id, self._writer, self._reader)
  
#================================================================#
# Connector
#===============================================================-#
@dataclass
class Connector:
  id: str
  _reader: StreamReader
  _writer: StreamWriter

  #----------------------------------------------------------------//
  # close
  #----------------------------------------------------------------//
  async def close(self):
    self._writer.close()
    await self._writer.wait_closed()

  #----------------------------------------------------------------//
  # open
  #----------------------------------------------------------------//
  @classmethod
  async def open(cls, hostName: str, port: int, cid="0") -> object:
    try:
      if cid == "0":
        cid = datetime.now().strftime('%S%f')
      reader, writer = await asyncio.open_connection(hostName, port)
      return cls(cid, reader, writer)
    except (IOError, asyncio.TimeoutError):
      errmsg = "Asyncio failed to create a connection @{}:{}"
      logger.error(errmsg.format(hostName, port), exc_info=True)
      raise
    except Exception:
      errmsg = "Unknown error creating connection @{}:{}"
      logger.error(errmsg.format(hostName, port), exc_info=True)
      raise

  #----------------------------------------------------------------//
  # receive
  #----------------------------------------------------------------//
  async def _read(self) -> Article:
    header = await self._reader.readexactly(2)

    hsize = 2
    if isLarge(header[0]):
      hsize = 5
      header += await self._reader.readexactly(3)

    fsize = int.from_bytes(header[1:hsize],'little')

    bpacket = await self._reader.read(fsize)
    return Article.deserialize(bpacket)

  #----------------------------------------------------------------//
  # _write
  #----------------------------------------------------------------//
  async def _write(self, article):
    bpacket = article.serialize()
    header = parseHeader(bpacket, 0)
    self._writer.write(header)
    self._writer.write(bpacket)
    await self._writer.drain()
    
#=================================================================#
# ChannelProps
#=================================================================#
@dataclass
class ChannelProps:
  # future: SafeFuture = field(init=False)
  future: Future = field(init=False)
  timeout: int = field(init=False) 
  retries: int = field(init=False)
  config: InitVar[dict]

  def __post_init__(self, config):
    # self.future = SafeFuture(self.name)
    self.future = None
    self.timeout = config.get("timeout", 0)
    self.retries = config.get("retries", 0)
    
  # -------------------------------------------------------------- #
  # addFuture
  # ---------------------------------------------------------------#
  def addFuture(self, future: Future, caller, iomode: str):
    def onComplete(fut):
      try:
        # self.future.discard(fut)
        # logger.debug(f"discarding {self.name} {iomode} future")
        self.future = None
        fut.result()
      except asyncio.CancelledError:
        logger.info(f"{caller} {iomode} was cancelled")
      except Exception as ex:
        logger.warn(f"{caller} {iomode} errored : {ex}")
    future.add_done_callback(onComplete)
    self.future = future
    # await self.future.put(future, onComplete)

  #-----------------------------------------------------------------#
  # cancel
  #-----------------------------------------------------------------#
  def cancel(self):
    if self.future:
      self.future.cancel()

  # -------------------------------------------------------------- #
  # cancelled
  # ---------------------------------------------------------------#
  def cancelled(self):
    if self.future:
      return self.future.cancelled()

  #-----------------------------------------------------------------#
  # engaged
  #-----------------------------------------------------------------#
  def engaged(self) -> bool:
    return self.future != None
  
  #-----------------------------------------------------------------#
  # setProps
  #-----------------------------------------------------------------#
  def setProps(self, config):
    self.timeout = config.get("timeout", 0)
    self.retries = config.get("retries", 0)

#=================================================================#
# ConnWATC - Connector With Async Timeout Capability
#=================================================================#
@dataclass
class ConnWATC:
  conn: AbcConnector
  config: InitVar[dict]
  blocked: bool = field(init=False)
  rprops: ChannelProps = field(init=False)
  wprops: ChannelProps = field(init=False)
  statusCode: int = field(init=False)
  timedoutMode: str = field(init=False)
  
  def __post_init__(self, config):
    self.blocked = False
    self.rprops = ChannelProps(config.readProps)
    self.wprops = ChannelProps(config.writeProps)
    self.statusCode = 200
    self.timedoutMode = ""

  @property
  def name(self):
    return self.conn.id

  #-----------------------------------------------------------------#
  # cancelAll
  #-----------------------------------------------------------------#
  def cancelAll(self, blocked=True):
    self.blocked = blocked
    self.wprops.cancel()
    self.rprops.cancel()

  #-----------------------------------------------------------------#
  # cancelRecv
  #-----------------------------------------------------------------#
  def cancelRecv(self):
    self.rprops.cancel()

  #-----------------------------------------------------------------#
  # cancelSend
  #-----------------------------------------------------------------#
  def cancelSend(self):
    self.wprops.cancel()

  #-----------------------------------------------------------------#
  # close
  #-----------------------------------------------------------------#
  async def close(self):
    self.cancelAll()
    await self.conn.close()

  #-----------------------------------------------------------------#
  # engaged
  #-----------------------------------------------------------------#
  def engaged(self):
    return self.wprops.engaged() or self.rprops.engaged()

  #-----------------------------------------------------------------#
  # getEngagedMode
  #-----------------------------------------------------------------#
  def getEngagedMode(self):
    if self.wprops.engaged():
      return "write"
    if self.rprops.engaged():
      return "read"
    return "idle"

  #-----------------------------------------------------------------#
  # getTimedoutMode
  #-----------------------------------------------------------------#
  def getTimedoutMode(self):
    return self.timedoutMode
  
  #-----------------------------------------------------------------#
  # receive
  #-----------------------------------------------------------------#
  async def receive(self, timeout=0) -> object:
    if self.blocked:
      logmsg = "{} cannot receive while io activity is blocked ..."
      logger.debug(logmsg.format(self.name))
      return
    # caller can override configured timeout if required
    # in the default case, use the configured timeout
    if timeout == 0:
      timeout = self.rprops.timeout
    # logger.debug("{} is receiving with a {} sec timeout ...".format(self.name, timeout))
    # timeout == 0 means do NOT use a timeout
    # retries is only relevent when a timeout is applied
    retries = 0 if timeout == 0 else self.rprops.retries
    first = 1
    try:
      self.timedoutMode = ""
      while retries > 0 or first:
        try:
          payload = await self.recvWATC(timeout)
          self.statusCode = 200
          return payload
        except asyncio.TimeoutError:
          self.statusCode = 552
          if first:
            first = 0
          else:
            retries -= 1
          logger.warn("{} receiving timed out. Available retries : {}".format(self.name, retries))
      self.timedoutMode = "read"
    except asyncio.CancelledError:
      logger.warn("{} read coroutine was cancelled".format(self.name))
      self.statusCode = 554
    except asyncio.IncompleteReadError:
      logger.warn("{} connnection reset by peer while receiving".format(self.name))
      self.statusCode = 553
    except Exception as ex:
      logger.info("{} asyncio StreamReader error".format(self.name), exc_info=True)
      self.statusCode = 555
    return None

  #----------------------------------------------------------------//
  # recvWATC -- receive with async timeout capability
  # -- allows anytime cancelation by self.future.cancel()
  #----------------------------------------------------------------//
  async def recvWATC(self, timeout) -> object:
    # timeout time unit is seconds
    future = create_task(self.conn._read())
    self.rprops.addFuture(future, self.name, "reading")
    if timeout == 0:
      # logger.debug("!!! {} is receiving without a timeout !!!".format(self.name))
      return await future
    return await asyncio.wait_for(future, timeout)

  #-----------------------------------------------------------------#
  # send
  #-----------------------------------------------------------------#
  async def send(self, payload: object, timeout=0):
    if self.blocked:
      logmsg = "{} cannot send while io activity is blocked ..."
      logger.debug(logmsg.format(self.name))
      return
    # caller can override configured timeout if required
    # in the default case, use the configured timeout
    if timeout == 0:
      timeout = self.wprops.timeout
    # logger.debug("{} is sending with a {} sec timeout ...".format(self.name, timeout))
    # timeout == 0 means do NOT use a timeout
    # retries is only relevent when a timeout is applied
    retries = 0 if timeout == 0 else self.wprops.retries
    first = 1
    try:
      self.timedoutMode = ""
      while retries > 0 or first:
        try:
          await self.sendWATC(payload, timeout)
          self.statusCode = 200
          return
        except asyncio.TimeoutError:
          self.statusCode = 552
          if first:
            first = 0
          else:
            retries -= 1
          logger.warn("{} sending timed out. Available retries : {}".format(self.name, retries))
      self.timedoutMode = "write"
    except asyncio.CancelledError:
      logger.warn("{} send coroutine was cancelled".format(self.name))
      self.statusCode = 554
    except ConnectionResetError:
      logger.warn("{} connnection reset by peer while sending".format(self.name))
      self.statusCode = 553
    except Exception as ex:
      logger.info("{} asyncio StreamWriter error".format(self.name), exc_info=True)
      self.statusCode = 555

  #----------------------------------------------------------------//
  # sendWATC -- send with async timeout capability
  # -- allows anytime cancelation by self.future.cancel()
  #----------------------------------------------------------------//
  async def sendWATC(self, payload, timeout):
    # timeout time unit is seconds
    future = create_task(self.conn._write(payload))
    self.wprops.addFuture(future, self.name, "writing")
    if timeout == 0:
      # logger.debug("!!! {} is sending without a timeout !!!".format(self.name))
      return await future
    await asyncio.wait_for(future, timeout)
  
  #-----------------------------------------------------------------#
  # setId
  #-----------------------------------------------------------------#
  def setId(self, id):
    self.conn.id = id

  #-----------------------------------------------------------------#
  # setProps
  #-----------------------------------------------------------------#
  def setProps(self, config):
    if config.hasAttr("readProps"):
      self.rprops.setProps(config.readProps)
    if config.hasAttr("writeProps"):
      self.wprops.setProps(config.writeProps)
  
#================================================================#
# Bitmasks
#===============================================================-#
LARGE = 0x1
SNDMORE = 0x2

def isLarge(flag: bytes):
  return flag & LARGE == LARGE

def hasMore(cls, flag: bytes):
  return flag & SNDMORE == SNDMORE

#----------------------------------------------------------------//
#  parseHeader
#----------------------------------------------------------------//
def parseHeader(frame, flag):
  #  Long flag
  fsize = len(frame)
  large = fsize > 255
  # logger.debug("header encoding - frame size : {}".format(fsize))
  if large:
    flag ^= LARGE

  # this bytes conversion only works for int <= 255
  header = bytes([flag])
  if large:
    # logger.debug("large packet header encoding ...")
    header += fsize.to_bytes(4,'little')
  else:
    header += bytes([fsize])
  # logger.debug("encoded header : {}".format(header))
  return bytearray(header)

