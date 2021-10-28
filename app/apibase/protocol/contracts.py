import asyncio
import logging

from asyncio import Task
from dataclasses import dataclass, field, InitVar
from scraperski.component import Article, AbcConnector, ConnWATC, Note
from scraperski.utils import create_task
from .contract import Contract

logger = None

#=================================================================#
# ContractA - Data mining contract routine for peer A
#=================================================================#
@dataclass
class ContractA(Contract):
  config: Note
  
  def __post_init__(self, logRef: str):
    super().__post_init__(logRef)
    global logger
    logger = logging.getLogger(logRef)
    required = ("brokerPolicy","broker_connWATC","maxTurns")
    if not self.config.hasAttr(*required):
      errmsg = f"One or more required config params are missing\nRequired : {required}"
      raise Exception(errmsg)
    self.policy = self.config.annote("brokerPolicy")
    self.turns = self.config.maxTurns

  #-----------------------------------------------------------------#
  # _running
  # -- handshaking with the remote MiningDealer
  #-----------------------------------------------------------------#
  def _running(self, article: Article):
    pass

  #-----------------------------------------------------------------#
  # quickening -- receiving is passive because we are waiting for the
  # peer to send a response. So quickening means any non-receiving
  # behaviour
  #-----------------------------------------------------------------#
  def quickening(self):
    return self.status  == "STARTED"

  #-----------------------------------------------------------------#
  # start
  #-----------------------------------------------------------------#
  def start(self, baseConn: AbcConnector) -> Task:
    logger.debug(f"{self.id} is starting ...")
    baseConn.id = self.id
    config = self.config.annote("broker_connWATC")
    self.conn = ConnWATC(baseConn, config)
    coro = self.run()
    future = create_task(coro)
    self.addFuture(future, "START")
    return future
    
  #-----------------------------------------------------------------#
  # _started
  # -- handshaking with the remote MiningDealer
  #-----------------------------------------------------------------#
  def _started(self, article: Article):
    # acknowledge handshake receipt by sending an ok record
    logger.debug(f"{self.id} is acknowledging new session {self.sessionId} request ...")
    article = Article({
      "peerKey": self.id,
      "scope": "handshake",
      "sessionId": self.sessionId,
      "value": "ok"})
    self.send(article)

  #-----------------------------------------------------------------#
  # _starting
  # -- implements handshaking protocol A1
  #-----------------------------------------------------------------#
  def _starting(self, article: Article):
    required = ("peerKey","scope","sessionId")
    errmsg = "{} remote host handshake failed\nReason : {}"
    if article.hasAttr(*required):
      peerKey, scope, sessionId = article.tell(*required)
      if article.scope == "handshake":
        self.sessionId = sessionId
        logger.debug(f"{self.id} sessionId {sessionId} retrieval from {peerKey} succeeded")
      else:
        reason = f"Unexpected scope parameter: {scope}"
        raise Exception(errmsg.format(self.id, reason))
    else:
      reason = f"One or more required params are missing. Required : {required}"
      raise Exception(errmsg.format(self.id, reason))

#=================================================================#
# ContractB - Data mining contract routine for peer kind B
#=================================================================#
@dataclass
class ContractB(Contract):
  config: Note
  
  def __post_init__(self, logRef: str):
    super().__post_init__(logRef)
    global logger
    logger = logging.getLogger(logRef)
    required = ("dealer_connWATC","dealerPolicy","maxTurns")
    if not self.config.hasAttr(*required):
      errmsg = f"One or more required config params are missing\nRequired : {required}"
      raise Exception(errmsg)
    self.policy = self.config.annote("dealerPolicy")
    self.turns = self.config.maxTurns

  #-----------------------------------------------------------------#
  # _running
  # -- handshaking with the remote MiningDealer
  #-----------------------------------------------------------------#
  def _running(self, article: Article):
    pass

  #-----------------------------------------------------------------#
  # quickening -- receiving is passive because we are waiting for the
  # peer to send a response. So quickening means any non-receiving
  # behaviour
  #-----------------------------------------------------------------#
  def quickening(self):
    return self.status == "STARTING"

  #----------------------------------------------------------------//
  # start
  #----------------------------------------------------------------//
  def start(self, baseConn: AbcConnector, sessionId: str) -> Task:
    logger.debug(f"{self.id} is starting a new session : {sessionId} ...")
    self.sessionId = sessionId
    baseConn.id = self.id
    config = self.config.annote("dealer_connWATC")
    self.conn = ConnWATC(baseConn, config)
    coro = self.run()
    future = create_task(coro)
    self.addFuture(future, "START")
    return future

  #-----------------------------------------------------------------#
  # _started
  #-----------------------------------------------------------------#
  def _started(self, article: Article):
    required = ("peerKey","value")
    errmsg = "{} remote host handshake failed\nReason : {}"
    if article.hasAttr(*required):
      peerKey, result = article.tell(*required)
      if result != "ok":
        reason = "Remote host handshake result is not ok"
        raise Exception(errmsg.format(self.id, reason))
      logger.debug(f"{self.id} sessionId handshake succeeded with {peerKey}")
    else:
      reason = f"One or more required params are missing\nRequired : {required}"
      raise Exception(errmsg.format(self.id, reason))

  #-----------------------------------------------------------------#
  # _starting
  # -- implements handshaking protocol A1
  #-----------------------------------------------------------------#
  def _starting(self, article: Article):
    article = Article({
      "sessionId": self.sessionId,
      "peerKey": self.id,
      "scope": "handshake"})
    logger.debug(f"{self.id} is sending the sessionId {self.sessionId} as handshake token")
    self.send(article)
