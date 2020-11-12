from apibase import ApiRequest, Article, Note, ServiceExecutor, TaskComplete, TaskError
from .handler import ActorCache, TaskHandler
import asyncio
import copy
import importlib
import logging
import os, sys
import uuid

logger = logging.getLogger('asyncio.microservice')

#------------------------------------------------------------------#
# ServiceHB
# -- Handler for backend services to compliment client/frontend microservices
# -- clients
#------------------------------------------------------------------#
class ServiceHB(TaskHandler):
  executor = None

  def __init__(self, jobId, context):
    super().__init__(jobId)
    self.context = context
    self._request = None
    self.monitor = None

  #------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#  
  @classmethod
  def make(cls, *args, **kwargs):
    raise NotImplementedError('{cls.__name__}.make is an abstract method')

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, actorKey, jobMeta):
    logger.info(f'{self.name}, applying {actorKey} jobMeta : ' + str(jobMeta['assembly']))
    self.__dict__[actorKey] = ActorCache(actorKey, jobMeta['assembly'])

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def arrange(self, connector, *args, **kwargs):
    self._request = connector
    if not self.executor:
      self.executor = ServiceExecutor()

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    self.executor.destroy()

  # -------------------------------------------------------------- #
  # getActor
  # ---------------------------------------------------------------#
  def getActor(self, packet):
    taskId = packet.taskId
    actorKey, article = self[packet.actor].tell(taskId)
    if article:
      # state machine is promoted
      logger.info(f'{packet.taskKey}, actor is already loaded, {article.body} ...')
      return article
    moduleName, className = self[actorKey].classToken.split(':')
    module = sys.modules.get(moduleName)
    if not module:
      module = importlib.import_module(moduleName)
    actorId = str(uuid.uuid4())
    actor = getattr(module, className).make(self.context, actorId, packet)
    article = Article({'actorId':actorId,'sockAddr':actor.sockAddr})
    logger.info(f'{packet.taskKey}, loading new actor, {article.body} ...')    
    self[actorKey].add(taskId, article)
    self.executor[actorId] = actor
    return article

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self, packet):
    if Monitor.status == 'ENGAGED':
      errmsg = f'monitor is currently engaged'
      return self.respond(400, packet, mixin={'error':errmsg})  
    logger.info(f'{self.name} preparing to monitor next {packet.typeKey} lifecycle')
    self.monitor = Monitor(packet)
    return self.respond(201, packet)

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet):
    logger.info(f'{self.name}, making new {packet.typeKey} service {packet.taskId} ...')
    if self.monitor.hasRunner(packet.taskId):
      logger.warn(f'ATTN. {packet.taskId} service is already started')
      article = self[packet.actor].get(packet.taskId)
      return self.respond(400, packet, mixin=article.copy())
    article = self.getActor(packet)
    return self.promovere(article, packet)

  # -------------------------------------------------------------- #
  # promovere - execute task
  # ---------------------------------------------------------------#
  def promovere(self, article, packet):
    coro = self.runActor(article.actorId, packet)
    future = asyncio.ensure_future(coro)
    self.addFuture(future, packet.taskKey)
    return self.respond(201, packet, mixin=article.copy())

  # -------------------------------------------------------------- #
  # restart
  # ---------------------------------------------------------------#
  def restart(self, packet):
    logger.info(f'{self.name}, restarting {packet.typeKey} service {packet.taskId} ...')
    if self.monitor.hasRunner(packet.taskId):
      logger.warn(f'ATTN. {packet.taskId} service is already started')
      article = self[packet.actor].get(packet.taskId)
      return self.respond(200, packet, mixin=article.copy())
    article = self.getActor(packet)
    return self.promovere(article, packet)

  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, actorId, packet):
    try:
      self.monitor.aquire(packet.taskId)
      logger.info(f'### about to run {packet.taskId} actor, actorId : {actorId}')
      await self.executor.run(actorId, packet)
      self.monitor.release(packet.taskId)
    except TaskComplete:
      await self.resume(201)
    except asyncio.CancelledError:
      logger.info(f'ATTN. {packet.taskKey}, service task is canceled')
      self.executor.stop(actorId)
    except Exception as ex:
      logger.error(f'{packet.taskKey}, service task errored', exc_info=True)
      await self.resume(500)

  # -------------------------------------------------------------- #
  # resume
  # ---------------------------------------------------------------#
  async def resume(self, signal):
    jpacket = self.monitor.jpacket
    caller = Note(jpacket.caller)
    logmsg = f'resuming {caller.typeKey}.{caller.actor} with signal {signal} ...'
    logger.info(f'{self.name}, {self.jobId}, {logmsg}')
    rpacket = {
      'jobId': caller.jobId,
      'typeKey': caller.typeKey,
      'caller': jpacket.actor,
      'actor': caller.actor,
      'fromKey': jpacket.typeKey,
      'synchronous': False,
      'signal': signal
    }
    connector = ApiRequest.connector(caller.jobId)
    await self.request('resume', rpacket, connector)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down ...')
    super().shutdown()
    self.context.close()

#----------------------------------------------------------------#
# Monitor
#----------------------------------------------------------------#		
class Monitor:
  status = 'READY'
  
  def __init__(self, jpacket):
    self.status = 'ENGAGED'
    self.jpacket = jpacket
    self._runners = []
    self._taskRange = len(jpacket.taskRange)

  def hasRunner(self, taskId):
    return taskId in self._runners

  def aquire(self, taskId):
    if taskId in self._runners:
      logger.warn(f'monitored {taskId} is not unique')
      return
    logger.info(f'aquiring monitored task {taskId}')
    self._runners.append(taskId)
    self._taskRange -= 1
    
  def release(self, taskId):
    logger.info(f'releasing monitored task {taskId}')      
    self._runners.remove(taskId)
    if not self._runners and not self._taskRange:
      logger.info('### Monitor, backend service is complete')
      raise TaskComplete()
      self.status = 'READY'
