__all__ = ('BaseExecutor','AdhocExecutor','ServiceExecutor','MicroserviceExecutor')

from apibase import Article
from concurrent.futures import ThreadPoolExecutor, FIRST_EXCEPTION
from functools import partial
import asyncio
import inspect
import logging
import os, sys

# default task logger
logger = logging.getLogger('asyncio.server')

# -------------------------------------------------------------- #
# BaseExecutor
# ---------------------------------------------------------------#
class BaseExecutor:
  _eventloop = None  

  def __init__(self, demandFactor=10):
    poolsize = len(os.sched_getaffinity(0)) * demandFactor
    self.executor = ThreadPoolExecutor(poolsize)

  # -------------------------------------------------------------- #
  # name
  # ---------------------------------------------------------------#
  @property
  def name(self):
    return f'{self.__class__.__name__}'

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  @classmethod
  def get(cls):
    return cls._instance

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  @staticmethod
  def __start__():
    BaseExecutor._eventloop = asyncio.get_event_loop()

  # -------------------------------------------------------------- #
  # getFuture
  # ---------------------------------------------------------------#
  def getFuture(self, actor, *args, **kwargs):

    if asyncio.iscoroutinefunction(actor.__call__):
      logmsg = f'{self.name}, running coroutine {actor}'
      logger.info(f'{logmsg}, args, kwargs : {args}, {kwargs}')   
      return asyncio.ensure_future(actor(*args, **kwargs))
    coro_or_future = actor.__call__(*args, **kwargs)
    if asyncio.futures.isfuture(coro_or_future):
      return coro_or_future
    elif asyncio.iscoroutinefunction(coro_or_future):
      return asyncio.ensure_future(actor(*args, **kwargs))
    else:
      logmsg = f'{self.name}, running {actor} in an executor future'
      logger.info(f'{logmsg}, args, kwargs : {args}, {kwargs}')   
      return self.eventloop.run_in_executor(None, partial(actor, *args, **kwargs))

# -------------------------------------------------------------- #
# AdhocExecutor
# ---------------------------------------------------------------#
class AdhocExecutor(BaseExecutor):
  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.submit = self.getFuture

  # -------------------------------------------------------------- #
  # xmap 
  # -- execute a task list exclusive of the eventloop
  # -- handles zmq resource shutdown independent of aiohttp shudown
  # ---------------------------------------------------------------#
  def xmap(self, task, xargs, timeout):
    self.executor.map(task, xargs, timeout=timeout)

# -------------------------------------------------------------- #
# ServiceExecutor
# ---------------------------------------------------------------#
class ServiceExecutor(AdhocExecutor):

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self._actor = {}
    self.cache = JobCache()

  def __getitem__(self, actorId):
    if actorId not in self._actor:
      raise KeyError(f'{self.name}, actor {actorId} is not a registered actorId')
    return self._actor[actorId]

  def __setitem__(self, actorId, actor):
    self._actor[actorId] = actor

  def __delitem__(self, actorId):
    del self._actor[actorId]

  # -------------------------------------------------------------- #
  # addActor
  # ---------------------------------------------------------------#
  def addActor(self, actor):
    self._actor[actor.actorId] = actor

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self, actorId, packet):
    actor = self[actorId]
    logger.info(f'{self.name}, about to run {packet.taskKey} ...')
    return self.getFuture(actor, *packet.args, **packet.kwargs)

  # -------------------------------------------------------------- #
  # runNext
  # ---------------------------------------------------------------#
  async def runNext(self, actorId, packet=None):
    if self.cache.hasNext(actorId, packet):
      packet = self.cache.next(actorId, packet.taskKey)
      await self.run(packet)

  # -------------------------------------------------------------- #
  # company
  # ---------------------------------------------------------------#
  @property
  def company(self):
    return [actor for actor in self._actor.values() if actor is not None]

  # -------------------------------------------------------------- #
  # stop
  # ---------------------------------------------------------------#
  async def start(self, actorId, *args, **kwargs):
    await self[actorId](*args, **kwargs)

  # -------------------------------------------------------------- #
  # stop
  # ---------------------------------------------------------------#
  def stop(self, actorId=None):
    if actorId:
      self[actorId].stop()
    else:
      [actor.stop() for actor in self.company]  

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    [actor.destroy() for actor in self.company]    

# -------------------------------------------------------------- #
# MicroserviceExecutor
# ---------------------------------------------------------------#
class MicroserviceExecutor(AdhocExecutor):

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    pass

  # -------------------------------------------------------------- #
  # getFuture
  # ---------------------------------------------------------------#
  def getTask(self, actor, packet, taskNum):
    return self.getFuture(actor, packet.jobId, taskNum, *packet.args, **packet.kwargs)

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  async def run(self, actorGroup, packet, **kwargs):
    logger.info(f'### MicroserviceExecutor, about to run {packet.taskKey} ...')

    result = Article({'complete':True,'failed':False,'signal':201})
    futures = {self.getTask(actor, packet, taskNum): 
                      taskNum for taskNum, actor in actorGroup.ordActors}

    try:
      done, pending = await asyncio.wait(futures.keys(), return_when=FIRST_EXCEPTION)
    except asyncio.CancelledError:
      logger.exception(f'{packet.taskKey}, actorGroup is cancelled')
      result.failed = True
      return result

    # if there are pending tasks is because there was an exception
    # cancel any pending tasks
    for pendingJob in pending:
        pendingJob.cancel()

    # process the done tasks
    for doneJob in done:
      # if an exception is raised one of the Tasks will raise
      taskNum = futures[doneJob]
      try:
        actorId, actorName = actorGroup.tell(taskNum)
        doneJob.result()
        logger.info(f'{actorName} actor {actorId} is complete')
      except Exception as ex:
        logger.exception(f'{actorName} actor {actorId} errored', exc_info=True)
        result.merge({'taskNum':taskNum,'failed':True})
    return result

# -------------------------------------------------------------- #
# JobCache
# ---------------------------------------------------------------#
class JobCache:
  def __init__(self):
    self.cache = {}
    self.activeJob = {}
    
  # -------------------------------------------------------------- #
  # hasNext
  # ---------------------------------------------------------------#
  def hasNext(self, actorId, packet):
    if packet: 
      # if active, append to the cache otherwise add the new job 
      if actorId in self.activeJob:
        logger.info(f'{packet.taskKey}, caching job packet ...')
        self.cache[actorId].append(packet)
        return False
      else:
        self.activeJob[actorId] = packet
        self.cache[actorId] = [packet]
        return True
    # empty packet means test if there are cached jobs ready to execute
    try:
      if len(self.cache[actorId]) > 0:
        logger.info(f'{packet.taskKey}, running cached job ...')
        return True
    except KeyError:
      logger.info(f'{packet.taskKey}, delisting ...')
      self.activeJob.pop(actorId, None)
      return False

  # -------------------------------------------------------------- #
  # next
  # ---------------------------------------------------------------#
  def next(self, actorId, taskKey):
    logger.info(f'{taskKey}, running next job ...')
    packet = self.cache[actorId].pop(0)
    self.activeJob[actorId] = packet
    return packet
