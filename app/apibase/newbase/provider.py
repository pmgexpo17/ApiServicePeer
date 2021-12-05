import asyncio
import logging

from asyncio import Future, Queue
from datetime import datetime
from dataclasses import dataclass, field, InitVar

from .component import Article, Note
from .connector import create_task, AbcConnector, Connector, ConnWATC, QuConnector

logger = logging.getLogger('asyncio')

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
    self._cache[key] = item

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
  # get
  #----------------------------------------------------------------//
  def get(self, key) -> object:
    return self._cache.get(key, None)

  #----------------------------------------------------------------//
  # hasEntry
  #----------------------------------------------------------------//
  def hasEntry(self, key) -> bool:
    return key in self._cache

  #----------------------------------------------------------------//
  # remove
  #----------------------------------------------------------------//
  def remove(self, key) -> object:
    return self._cache.pop(key, None)

#================================================================#
# QuClient
#===============================================================-#
@dataclass
class QuClient:
  qchannel: ConnWATC

  @property
  def name(self):
    return "QuClient-" + self.qchannel.conn.id

  #-----------------------------------------------------------------#
  # close
  #-----------------------------------------------------------------#
  async def close(self):
    await self.qchannel.close()

  #-----------------------------------------------------------------#
  # disconnected
  #-----------------------------------------------------------------#
  @property
  def disconnected(self) -> bool:
    if self.qchannel.statusCode == 554:
      logger.info(f"{self.name} is cancelled")
    elif self.qchannel.statusCode >= 300:
      logger.info(f"{self.name} io transport errored")
    else:
      return False
    return True

  #-----------------------------------------------------------------#
  # engaged
  #-----------------------------------------------------------------#
  @property
  def engaged(self) -> bool:
    logger.debug(f"{self.name} is parsing current status ...")
    return not self.disengaged

  #-----------------------------------------------------------------#
  # serve_forever
  #-----------------------------------------------------------------#
  async def open(self, cid="0"):
    if cid == "0":
      cid = datetime.now().strftime('%S%f')
    logger.debug("{} is opening a new QuConnector with id : {}".format(self.name, cid))
    await self.qchannel.send(cid)
    if self.disconnected:
      return None
    qconn = await self.qchannel.receive()
    if self.disconnected:
      return None
    return qconn

#=================================================================#
# QuServer - Data Mining Service Provider
#=================================================================#
@dataclass
class QuServer:
  qchannel: ConnWATC
  acceptor: object
  status: str = field(init=False)
  
  def __post_init__(self):
    self.status = "INIT"

  async def __aenter__(self):
    return self
  
  async def __aexit__(self, *exc):
    self.status = "CLOSED"

  @property
  def name(self):
    return "QuServer-" + self.qchannel.conn.id

  #-----------------------------------------------------------------#
  # disconnected
  #-----------------------------------------------------------------#
  @property
  def disconnected(self) -> bool:
    if self.qchannel.statusCode == 554:
      logger.info(f"{self.name} is cancelled")
    elif self.qchannel.statusCode >= 300:
      logger.info(f"{self.name} io transport errored")
    else:
      return False
    return True

  #-----------------------------------------------------------------#
  # engaged
  #-----------------------------------------------------------------#
  @property
  def engaged(self) -> bool:
    # logger.debug(f"{self.name} is parsing current status ...")
    return not self.disconnected

  #-----------------------------------------------------------------#
  # serve_forever
  #-----------------------------------------------------------------#
  async def serve_forever(self):
    self.status = "RUNNING"
    while self.engaged:
      cid = await self.qchannel.receive()
      if self.disconnected:
        return
      # logger.debug(f"{self.name} is creating a new connection with id : {cid}")
      qconnA = QuConnector.open(cid=cid)
      # need to swap reader and writer at one end for correct dual-band connector arrangement
      qconnB = qconnA.cloneReversed()
      self.acceptor(qconnB._reader, qconnB._writer)
      await self.qchannel.send(qconnA)

  #-----------------------------------------------------------------#
  # shutdown
  #-----------------------------------------------------------------#
  async def shutdown(self):
    logger.debug(f"{self.name} is shutting down ...")
    await self.qchannel.close()
    self.status = "CLOSED"

#================================================================#
# ConnProvider
#===============================================================-#
@dataclass
class ConnProvider:
  tptMode: str
  hostName: str
  port: int
  qclient: object = field(init=False, default_factory=object)
  connWATC: Note = field(init=False)
  config: InitVar[Note]
  
  def __post_init__(self, config: Note) -> object:
    logger.debug(f"ConnProvider constructing with transportMode : {self.tptMode}")
    self.connWATC = config.connWATC
    qconn = QuConnector.open(cid="quChannel")
    qchannel = ConnWATC(qconn, config.connWATC)
    self.qclient = QuClient(qchannel)

  #-----------------------------------------------------------------#
  # close
  #-----------------------------------------------------------------#
  async def close(self):
    if self.qclient:
      await self.qclient.close()

  #----------------------------------------------------------------//
  # newQuConn
  #----------------------------------------------------------------//
  def newQuConn(self, bandDesc, cid="0") -> QuConnector:
    return QuConnector.open(bandDesc, cid)

  #----------------------------------------------------------------//
  # newSockConn
  #----------------------------------------------------------------//
  async def newSockConn(self, cid: str) -> Connector:
    return await Connector.open(self.hostName, self.port, cid)

  #-----------------------------------------------------------------#
  # newQuServer
  #-----------------------------------------------------------------#
  def newQuServer(self, acceptor_s):
    # queue that emulates a TCP binded socket listener and peer endpoint comms channel
    qconn = self.qclient.qchannel.conn.cloneReversed()
    qchannel = ConnWATC(qconn, self.connWATC)
    return QuServer(qchannel, acceptor_s)

  #----------------------------------------------------------------//
  # new
  #----------------------------------------------------------------//
  async def new(self, cid="0") -> AbcConnector:
    if self.tptMode == "SOCKET":
      conn = await self.newSockConn(cid)
    else:
      conn = await self.qclient.open(cid)
    if not conn:
      raise Exception("{} errored opening a new {} connection".format(self.qclient.name, self.tptMode))
    return conn

  #----------------------------------------------------------------//
  # newWATC
  #----------------------------------------------------------------//
  async def newWATC(self, config: Note, cid="0") -> ConnWATC:
    if not isinstance(config, Note) or not config.hasAttr("connWATC"):
      raise Exception("ConnWATC config requires a Note including connWATC attribute")
    baseConn = self.new(cid)
    return ConnWATC(baseConn, config.connWATC)
