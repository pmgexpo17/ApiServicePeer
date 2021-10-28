import asyncio
import logging
import re

from asyncio import Lock, Future
from dataclasses import dataclass, field, InitVar
from typing import List, Union

from .routine import create_task

logger = logging.getLogger('asyncio')

@dataclass
class SafeFuture:
  owner: str
  future: Future = field(init=False)
  _lock: Lock = field(init=False)
  
  def __post_init__(self):
    self.future = None
    self._lock = Lock()

  def cancel(self, msg=None):
    async def run(): 
      async with self._lock:
        if not self.future:
          return
        logger.debug(f"Cancelling [{self.owner}] future")
        if not self.future.cancelled():
          # msg arg is not available < python 3.9
          # self.future.cancel(msg=msg)
          self.future.cancel()
    create_task(run())
    
  def empty(self) -> bool:
    return self.future == None
  
  def put(self, value: Future, onDoneCb: object):
    async def run():
      async with self._lock:
        logger.debug(f"Setting [{self.owner}] future with doneCallback attached")
        value.add_done_callback(onDoneCb)
        self.future = value
    create_task(run())
        
  def discard(self, fut: Future):
    async def run():
      async with self._lock:
        if self.future == fut:
          logger.debug(f"Discarding [{self.owner}] future")
          self.future = None
    create_task(run())
      
  def get(self) -> Future:
    logger.debug(f"Getting [{self.owner}] future")
    return self.future
