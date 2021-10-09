import asyncio
import logging

from asyncio import Future, StreamReader, StreamWriter
from dataclasses import dataclass, field, InitVar

from .component import Article, Note
from .routine import create_task

logger = logging.getLogger('asyncio')

#================================================================#
# Connector - new connector to replace the redundant ZmqConnector
#===============================================================-#
@dataclass
class Connector:
  _reader: StreamReader
  _writer: StreamWriter

  #----------------------------------------------------------------//
  # open
  #----------------------------------------------------------------//
  @classmethod
  async def open(cls, hostName: str, port: int) -> object:
    try:
      reader, writer = await asyncio.open_connection(hostName, port)
      return cls(reader, writer)
    except (IOError, asyncio.TimeoutError):
      errmsg = "Asyncio failed to create a connection @{}:{}"
      logger.error(errmsg.format(hostName, port), exc_info=True)
      raise
    except Exception:
      errmsg = "Unknown error creating connection @{}:{}"
      logger.error(errmsg.format(hostName, port), exc_info=True)
      raise

  #----------------------------------------------------------------//
  # close
  #----------------------------------------------------------------//
  async def close(self):
    self._writer.close()
    await self._writer.wait_closed()

  #----------------------------------------------------------------//
  # _write
  #----------------------------------------------------------------//
  def _write(self, article):
    bpacket = article.serialize()
    header = parseHeader(bpacket, 0)
    self._writer.write(header)
    self._writer.write(bpacket)

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

#=================================================================#
# ChannelProps
#=================================================================#
@dataclass
class ChannelProps:
  name: str
  # future: SafeFuture = field(init=False)
  future: Future = field(init=False)
  timeout: int = field(init=False) 
  retries: int = field(init=False)
  config: InitVar[dict]

  def __post_init__(self, config):
    # self.future = SafeFuture(self.name)
    self.future = None
    self.timeout = config.get("timeout",None)
    self.retries = config.get("retries", 0)
    # logger.debug("{} configured connector timeout : {}".format(self.name, self.timeout))
    # logger.debug("{} configured timeout retries : {}".format(self.name, self.retries))
    
  # -------------------------------------------------------------- #
  # addFuture
  # ---------------------------------------------------------------#
  def addFuture(self, future: Future, iomode: str):
    def onComplete(fut):
      try:
        # self.future.discard(fut)
        # logger.debug(f"discarding {self.name} {iomode} future")
        self.future = None
        fut.result()
      except asyncio.CancelledError:
        logger.info(f"{self.name} {iomode} was cancelled")
      except Exception as ex:
        logger.warn(f"{self.name} {iomode} errored : {ex}")
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
  # setName
  #-----------------------------------------------------------------#
  def setName(self, name):
    self.name = name

  #-----------------------------------------------------------------#
  # setProps
  #-----------------------------------------------------------------#
  def setProps(self, config):
    self.timeout = config.get("timeout",None)
    self.retries = config.get("retries", 0)

#=================================================================#
# ConnWATC - Connector With Async Timeout Capability
#=================================================================#
@dataclass
class ConnWATC:
  conn: Connector
  config: InitVar[dict]
  name: str
  blocked: bool = field(init=False)
  rprops: ChannelProps = field(init=False)
  wprops: ChannelProps = field(init=False)
  statusCode: int = field(init=False)
  
  def __post_init__(self, config):
    self.blocked = False
    self.rprops = ChannelProps(self.name, config.readProps)
    self.wprops = ChannelProps(self.name, config.writeProps)
    self.statusCode = 200

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
    await self.conn.close()

  #-----------------------------------------------------------------#
  # closure
  #-----------------------------------------------------------------#
  async def closure(self):
    self.cancelAll()
    await asyncio.sleep(1)
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
    return "none"

  #-----------------------------------------------------------------#
  # receive
  #-----------------------------------------------------------------#
  async def receive(self, timeout=0) -> Article:
    if self.blocked:
      logmsg = "Cannot receive a {} article while io activity is blocked ..."
      logger.debug(logmsg.format(self.name))
      return
    # caller can override configured timeout if required
    # in the default case, use the configured timeout
    if timeout == 0:
      timeout = self.rprops.timeout
    logger.debug("Receiving {} article with timeout {} secs ...".format(self.name, timeout))
    # timeout == None means do NOT use a timeout
    # retries is only relevent when a timeout is applied
    retries = 0 if timeout == None else self.rprops.retries
    first = 1
    try:
      while retries > 0 or first:
        try:
          article = await self.recvWATC(timeout)
          self.statusCode = 200
          return article
        except asyncio.TimeoutError:
          self.statusCode = 552
          if first:
            first = 0
          else:
            retries -= 1
          logger.warn("{} receiving timed out. Available retries : {}".format(self.name, retries))
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
  async def recvWATC(self, timeout) -> Article:
    # timeout time unit is seconds
    future = create_task(self.conn._read())
    self.rprops.addFuture(future, "reading")
    if timeout == None:
      logger.debug("!!! {} is receiving without a timeout !!!".format(self.name))
      return await future
    return await asyncio.wait_for(future, timeout)

  #-----------------------------------------------------------------#
  # send
  #-----------------------------------------------------------------#
  async def send(self, article: Article, timeout=0):
    if self.blocked:
      logmsg = "Cannot send a {} article while io activity is blocked ..."
      logger.debug(logmsg.format(self.name))
      return
    # caller can override configured timeout if required
    # in the default case, use the configured timeout
    if timeout == 0:
      timeout = self.wprops.timeout
    logger.debug("Sending {} article with timeout {} secs ...".format(self.name, timeout))
    # timeout == None means do NOT use a timeout
    # retries is only relevent when a timeout is applied
    retries = 0 if timeout == None else self.wprops.retries
    first = 1
    try:
      while retries > 0 or first:
        try:
          await self.sendWATC(article, timeout)
          self.statusCode = 200
          return
        except asyncio.TimeoutError:
          self.statusCode = 552
          if first:
            first = 0
          else:
            retries -= 1
          logger.warn("{} sending timed out. Available retries : {}".format(self.name, retries))
    except asyncio.CancelledError:
      logger.warn("{} send coroutine was cancelled".format(self.name))
      self.statusCode = 554
    except asyncio.ConnectionResetError:
      logger.warn("{} connnection reset by peer while sending".format(self.name))
      self.statusCode = 553
    except Exception as ex:
      logger.info("{} asyncio StreamWriter error".format(self.name), exc_info=True)
      self.statusCode = 555

  #----------------------------------------------------------------//
  # sendWATC -- send with async timeout capability
  # -- allows anytime cancelation by self.future.cancel()
  #----------------------------------------------------------------//
  async def sendWATC(self, article, timeout):
    # timeout time unit is seconds
    self.conn._write(article)
    future = create_task(self.conn._writer.drain())
    self.wprops.addFuture(future, "writing")
    if timeout == None:
      logger.debug("!!! {} is sending without a timeout !!!".format(self.name))
      return await future
    sender = asyncio.wait_for(future, timeout)
    try:
      asyncio.shield(sender)
    except asyncio.CancelledError:
      logger.debug("shieled sender was canceled")
    return sender
  
  #-----------------------------------------------------------------#
  # setName
  #-----------------------------------------------------------------#
  def setName(self, name):
    self.name = name
    self.rprops.setName(name)
    self.wprops.setName(name)

  #-----------------------------------------------------------------#
  # setProps
  #-----------------------------------------------------------------#
  def setProps(self, config):
    if config.hasAttr("readProps"):
      self.rprops.setProps(config.readProps)
    if config.hasAttr("writeProps"):
      self.wprops.setProps(config.writeProps)

#================================================================#
# MemCache
#===============================================================-#
@dataclass
class MemCache:
  _cache: dict = field(init=False, default_factory=dict)

  #----------------------------------------------------------------//
  # add - value replacement is default
  #----------------------------------------------------------------//
  def add(self, key: str, item: object):
    if item != None:
      self._cache[key] = item

  #----------------------------------------------------------------//
  # added - value replacement is default
  #----------------------------------------------------------------//
  def added(self, key: str, item: object) -> bool:
    if item != None:
      self._cache[key] = item
      return True
    return False

  #----------------------------------------------------------------//
  # empty
  #----------------------------------------------------------------//
  @property
  def empty(self) -> bool:
    return len(self._cache) == 0

  #----------------------------------------------------------------//
  # size
  #----------------------------------------------------------------//
  @property
  def size(self) -> int:
    return len(self._cache)

  #----------------------------------------------------------------//
  # exists
  #----------------------------------------------------------------//
  def exists(self, key) -> bool:
    return key in self._cache and self._cache[key] != None

  #----------------------------------------------------------------//
  # get
  #----------------------------------------------------------------//
  def get(self, key) -> object:
    return self._cache.get(key, None)

  #----------------------------------------------------------------//
  # remove
  #----------------------------------------------------------------//
  def remove(self, key) -> object:
    return self._cache.pop(key, None)

#================================================================#
# ConnProvider
#===============================================================-#
@dataclass
class ConnProvider(MemCache):
  hostName: str
  port: int

  def __init__(self, hostName: str, port: int) -> object:
    logger.debug(f"Making ConnProvider with remote server address {hostName}:{port}")
    self.hostName = hostName
    self.port = port

  #----------------------------------------------------------------//
  # new
  #----------------------------------------------------------------//
  async def new(self, connId: str, config: Note) -> ConnWATC:
    if not isinstance(config, Note) or not config.hasAttr("connWATC"):
      raise Exception("ConnWATC config requires a Note including connWATC attribute")
    baseConn = await Connector.open(self.hostName, self.port)
    return ConnWATC(baseConn, config.connWATC, connId)
  
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

