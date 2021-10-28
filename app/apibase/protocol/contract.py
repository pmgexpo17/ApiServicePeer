import asyncio
import logging

from asyncio import Task
from dataclasses import dataclass, field, InitVar
from typing import Set
from scraperski.component import Article, ConnWATC, create_task, Note
from .protocol import Protocol

logger = None

#=================================================================#
# Contract - Generic data mining contract routine
#=================================================================#
@dataclass
class Contract(Protocol):
  id: str
  isDaemon: bool = field(init=False, default_factory=bool)
  pendingTasks: Set = field(init=False, default_factory=set)
  policy: Note = field(init=False, default_factory=Note)
  status: str = field(init=False)
  statusCode: int = field(init=False)
  stopping: bool = field(init=False)
  turns: int = field(init=False, default_factory=int)
  logRef: InitVar[str]
  
  def __post_init__(self, logRef: str):
    global logger
    logger = logging.getLogger(logRef)
    self.isDaemon = False
    self.method = {
      "STARTING": self._starting,
      "STARTED": self._started,
      "RUNNING": self._running,
    }
    self.status = "INIT"
    self.statusCode = 200
    self.stopping = False

  @property
  def name(self):
    return self.id

  # -------------------------------------------------------------- #
  # addFuture
  # ---------------------------------------------------------------#
  def addFuture(self, future: Task, futRef: str):
    def onComplete(fut):
      try:
        self.pendingTasks.discard(fut)
        fut.result()
      except asyncio.CancelledError:
        logger.info(f"{self.id} {futRef} was cancelled")
      except Exception:
        kwArgs = {}
        if self.policy.logdetail == "VERBOSE":
          kwArgs["exc_info"] = True
        logger.error(f"{self.id} {futRef} errored", **kwArgs)
    future.add_done_callback(onComplete)
    self.pendingTasks.add(future)

  #-----------------------------------------------------------------#
  # close
  #-----------------------------------------------------------------#
  async def close(self):
    if self.status == "CLOSED":
      return
    logger.debug(f"{self.id} is shutting down ...")
    for task in self.pendingTasks:
      task.cancel()
    await self.conn.close()
    self.status = "CLOSED"
    
  #-----------------------------------------------------------------#
  # disconnected
  #-----------------------------------------------------------------#
  @property
  def disconnected(self) -> bool:
    if self.conn.statusCode == 554:
      logger.info(f"{self.id} io transport was cancelled, closing ...")
    elif self.conn.statusCode >= 300:
      logger.info(f"{self.id} io transport errored, closing ...")
    else:
      return False
    return True

  #-----------------------------------------------------------------#
  # disengaged
  #-----------------------------------------------------------------#
  def disengaged(self, article: Article) -> bool:
    self.stopping = True
    if self.disconnected:
      pass
    elif article and article.hasAttr("statusCode") and article.statusCode >= 300:
      logger.info(f"{self.id} session peer has errored, closing ...")
    elif article and article.hasAttr("complete") and article.complete:
      logger.info(f"{self.id} session is now complete, closing ...")
    else:
      # logger.debug(f"Article parsed ok. {self.id} is still engaged ...")
      self.stopping = False
    return self.stopping
 
  #-----------------------------------------------------------------#
  # engaged
  #-----------------------------------------------------------------#
  @property
  def engaged(self) -> bool:
    # logger.debug(f"{self.id} is parsing current status ...")
    if self.stopping or self.disconnected:
      logger.info(f"{self.id} is stopping ...")
    elif self.statusCode >= 300:
      logger.info(f"{self.id} has errored with status code : {self.statusCode}")
    elif self.turns == 0:
      logger.error("Hit maximum allowed turns limit, closing ...")
    else:
      self.stateTransition()
      if self.status == "COMPLETE":
        logger.info(f"{self.id} is now complete, closing ...")
      else:
        self.statusCode = 200
        return True
    return False

  #-----------------------------------------------------------------#
  # perform
  #-----------------------------------------------------------------#
  async def perform(self):
    state = self.status
    if state not in self.method:
      raise Exception(f"Current state {state} does not have a registered method")
    
    if self.quickening():
      # Use this option to send asynchronously. That is, there is no
      # response expected. Eg sending a closure notice.
      # Return an article if needing to transition from the current async
      # state to the next sync state or to a final COMPLETE status
      article = self.method[state](None)
      if not article:
        return
    else:
      article = await self.receive()
      if self.disengaged(article):
        return
    
    # logger.info("Running next method for state : {}".format(self.status)) 
    self.method[state](article)
    self.turns -= 1

  #-----------------------------------------------------------------#
  # quickening -- receiving is passive because we are waiting for the
  # peer to send a response. So quickening means any non-receiving
  # behaviour. Descendents override this method for state specific
  # quickening
  #-----------------------------------------------------------------#
  def quickening(self):
    return False

  #-----------------------------------------------------------------#
  # _running
  # -- handshaking with the remote MiningDealer
  #-----------------------------------------------------------------#
  def _running(self, article: Article):
    pass

  #-----------------------------------------------------------------#
  # send
  #-----------------------------------------------------------------#
  def send(self, article: Article, futRef=""):
    coro = self.conn.send(article)
    future = create_task(coro)
    if not futRef:
      futRef = self.status
    self.addFuture(future, futRef)
    

  #-----------------------------------------------------------------#
  # start
  #-----------------------------------------------------------------#
  def start(self, *args):
    pass
  
  #-----------------------------------------------------------------#
  # _started
  #-----------------------------------------------------------------#
  def _started(self):
    pass
  
  #-----------------------------------------------------------------#
  # _starting
  #-----------------------------------------------------------------#
  def _starting(self, article: Article):
    pass

  #-----------------------------------------------------------------#
  # stateTransition
  #-----------------------------------------------------------------#
  def stateTransition(self):
    # update the process status / state
    status = self.status
    if self.status == "INIT":
      self.status = "STARTING"
    elif self.status == "STARTING":
      self.status = "STARTED"
    elif self.status == "STARTED":
      self.status = "RUNNING"
    elif self.status == "RUNNING" and not self.isDaemon:
      self.status = "COMPLETE"