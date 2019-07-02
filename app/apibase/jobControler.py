from abc import ABCMeta, abstractmethod
from functools import partial
from apibase import ApiRequest, JobExecutor, JobGenerator, JobMeta, ServiceRegistry
from apitools.hardhash import HHProvider
from threading import RLock
from aiohttp import ClientSession, web
import asyncio
import copy
import datetime, time
import logging
import simplejson as json
import sys
import uuid

logger = logging.getLogger('apipeer.server')
slogger = logging.getLogger('apipeer.smart')

class AdhocTaskError(Exception):
  pass
  
# -------------------------------------------------------------- #
# JobControler
# ---------------------------------------------------------------#
class JobControler:
  executor = None
  registry = None
  request = None

  def __init__(self, leveldb, jobId):
    self._leveldb = leveldb
    self.jobId = jobId
    self.hhId = None
    self.history = []
    self.signalFrom = []
    self.lock = RLock()
    self.jmeta = {}
    self.pendingFuture = set()

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    elif key in self.jmeta:
      return self.jmeta[key]
    elif not hasattr(self, key):
      raise AttributeError(f'{key} is not a JobControler attribute')
    member = getattr(self, key)
    self.__dict__[key] = member
    return member

  def __getattr__(self, key):
    if key in self.jmeta:
      return self.jmeta[key]

  @staticmethod
  def start(leveldb, register, apiBase):
    JobGenerator.start(apiBase)
    JobControler.executor = JobExecutor()
    JobControler.request = ClientSession()
    registry = ServiceRegistry()
    for serviceName, serviceRef in register.items():
      registry.loadModules(serviceName, serviceRef)
    JobControler.registry = registry

  @staticmethod
  async def make(leveldb, packet):
    try:
      jobId = packet.jobId
      controler = JobControler(leveldb, jobId)
      jobGenerator = JobGenerator(leveldb, jobId)
      await controler.runAdhoc(jobGenerator.make, packet)
      jpacket = {'typeKey':'JOB','itemKey':jobId}    
      jobMeta = controler.runQuery(JobMeta(jpacket),metaItem=packet.eventKey)
      logger.info('### job meta : ' + str(jobMeta))
      controler.loadModules(jobMeta)
      return controler
    except asyncio.CancelledError:
      logger.exception(f'{jobId}, job generator task was canceled')
    except Exception as ex:
      logger.exception(f'{jobId}, controler task errored', exc_info=True)

  # -------------------------------------------------------------- #
  # getModuleRef
  # ---------------------------------------------------------------#
  def getModuleRef(self, actor):
    if self[actor].role != 'microservice':
      moduleName, className = self[actor].className.split(':')
      return {'name':f'apiservice.{moduleName}','fromList':[className]}
    fromList = []
    for msActor in self[actor].actors:
      actorKey = f'{actor}:{msActor}'
      moduleName, className = self[actorKey].className.split(':')
      fromList.append(className)
    # remove the now redundant microservice meta dict
    self.jmeta.pop(actor)
    return {'name':f'apiservice.{moduleName}','fromList':fromList}

  # -------------------------------------------------------------- #
  # loadModules
  # ---------------------------------------------------------------#
  def loadModules(self, jobMeta):
    self.loadMeta(jobMeta)
    modules = [self.getModuleRef(actor) for actor in self['actors']]
    logger.info(f'loading modules for job {self.jobId} : {str(modules)}')
    self.registry.loadModules(self['releaseTag'], modules)
    
  # -------------------------------------------------------------- #
  # loadMeta
  # ---------------------------------------------------------------#
  def loadMeta(self, jmeta):
    self.jmeta = {}
    self.jmeta.update(jmeta)
    actors = jmeta.pop('actors')
    for key, item in jmeta.items():
      if not isinstance(item, dict):
        continue
      if key in actors:
        self.loadActor(key, jmeta)
      else:
        self.jmeta[key] = JobMeta(jmeta[key])

  # -------------------------------------------------------------- #
  # loadActor
  # ---------------------------------------------------------------#
  def loadActor(self, actorName, jmeta):
    self.jmeta[actorName] = ActorMeta(jmeta[actorName])
    if self.jmeta[actorName].role == 'microservice':
      for microName, item in self.jmeta[actorName].actors.items():
        microName = f'{actorName}:{microName}'
        item['role'] = 'microservice'
        self.jmeta[microName] = ActorMeta(item)
        logger.info(f'actor {microName} meta : ' + str(self.jmeta[microName].__dict__))
    else:
      if actorName == self['first']:
        self.jmeta['first'] = self[actorName]
      logger.info(f'actor {actorName} meta : ' + str(self.jmeta[actorName].__dict__))
      self.jmeta[actorName].name = actorName

  # -------------------------------------------------------------- #
  # addFuture
  # ---------------------------------------------------------------#
  def addFuture(self, coro):
    def onComplete(future):
      self.pendingFuture.discard(future)

    f = asyncio.ensure_future(coro)
    f.add_done_callback(onComplete)
    self.pendingFuture.add(f)

  # -------------------------------------------------------------- #
  # addActor
  # ---------------------------------------------------------------#
  def addActor(self, packet):
    actorId = str(uuid.uuid4())
    service = self[packet.actor].className
    module, className = self.registry.getClassName(service)
    # must be an AppDirector derivative, leveldb and actorId packet are fixed by protocol
    actor = getattr(module, className)(self._leveldb, actorId)
    # start the actor, this should not be a long running method
    actor(self.jobId, self[packet.actor])
    self[packet.actor].actorId = actorId    
    self[packet.actor].serviceName = className
    self.executor[actorId] = actor
    return actorId, className

  # -------------------------------------------------------------- #
  # mergeSignal(self, packet, signal)
  # ---------------------------------------------------------------#
  def mergeSignal(self, packet, signal):
    if 'kwargs' in packet:
      packet['kwargs'].update({'signal':signal})
    else:
      packet['kwargs'] = {'signal':signal}

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  async def quicken(self, state, actorId, signal):
    logger.info('quicken state transition %s ... ' % state.current)
    packet, apiUrl = self.executor[actorId][state.current]
    if signal:
      self.mergeSignal(packet, signal)
    logger.info(f'promote args : {str(packet)}, {apiUrl}')
    async with self.request.post(apiUrl,json={'job':packet}) as response:
      rdata = await response.json()
      logger.info(f'api response {dict(rdata)}')
      if 'error' in rdata:
        logMsg = 'service controler failed to terminate'
        raise Exception(f'{self.jobId}, {logMsg}\n{rdata["error"]}')
    
  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, packet):
    try:
      logger.info(f'runActor : {packet.__dict__}')
      actorId, actorName = self[packet.actor].tell()
      future = self.executor.runJob('director', actorId, packet)
      await future
      state = future.result()
      if state.complete:        
        signal = 201
        logMsg = 'actor is complete, removing it now ...'
        if state.failed:
          signal = 500
          logMsg = 'actor failed, removing it now ...'
        if state.hasSignal:
          await self.quicken(state, actorId, signal=signal)
        logger.info(f'{actorName}, {logMsg}, {actorId}')
        del(self.executor[actorId])
        self.onComplete(packet)
      elif state.inTransition:
        signal = 201 if state.hasSignal else None
        await self.quicken(state, actorId, signal)
    except asyncio.CancelledError:
      logMsg = f'{packet.jobKey}, {actorName}, controler task was canceled'
      logger.exception(f'{logMsg}, {actorName}, {actorId}')
    except Exception as ex:
      logMsg = f'{packet.jobKey}, {actorName}, controler task errored'
      logger.exception(f'{logMsg}, {actorName}, {actorId}', exc_info=True)

  # -------------------------------------------------------------- #
  # runTask
  # ---------------------------------------------------------------#
  def runTask(self, packet):
    raise NotImplementedError('for enterprise project enquires, contact pmg7670@gmail.com')
    
  # -------------------------------------------------------------- #
  # onComplete
  # ---------------------------------------------------------------#
  def onComplete(self, packet):
    if self['first'].name == packet.actor:
      if self.hhId:
        coro = self.runAdhoc(HHProvider.delete,self.jobId)
        self.addFuture(coro)
        coro = self.closeHardHashService()
        self.addFuture(coro)
      coro = self.terminate(packet.actor)
      self.addFuture(coro)

  # -------------------------------------------------------------- #
  # resume
  # ---------------------------------------------------------------#
  def resume(self, actorId, actorKey, signal):
    packet = {'jobId':self.jobId,'caller':'controler','actor':actorKey}
    packet['kwargs'] = {'signal':signal}
    self.runTask(JobPacket(packet))

  # -------------------------------------------------------------- #
  # runGroup
  # ---------------------------------------------------------------#
  async def runGroup(self, klass, actorGroup, packet):
    try:
      actorId, actorName = self[packet.caller].tell()
      if True in [True for typeName in klass.__mro__ if 'HardHashActor' == typeName.__name__]:
        taskGroup = actorGroup.taskIds
        logger.info(f'### hardhash task group : {taskGroup}')
        if not self.hhId:
          groupRange = range(1, packet.kwargs['hhGroupSize'] + 1)
          clientGroup = [f'task{clientId:02}' for clientId in groupRange]
          del packet.kwargs['hhGroupSize']
          logger.info(f'### hardhash client pool : {clientGroup}')
          routerAddr = await self.getHardHashService(clientGroup)
          await self.runAdhoc(HHProvider.create,self.jobId,routerAddr,clientGroup)
          self.hhControl = HHProvider.get(self.jobId,'control')
      result = await self.executor.runJob('delegate',actorGroup, packet)
      if result.failed:
        logMsg = 'actor group failed, resuming caller ...'
        logger.info(f'{actorName}, {logMsg}, {actorId}')
        self.resume(actorId, packet.caller, 500)
      elif self.hhControl.status('complete') == 'True':
        logger.info(f'actor group is complete, resuming caller, {actorName}, {actorId}')
        self.resume(actorId, packet.caller, 201)
      else:
        logger.info(f'actor group is NOT complete, aborting ..., {actorName}, {actorId}')
        self.resume(actorId, packet.caller, 500)
    except asyncio.CancelledError:
      logMsg = f'{packet.jobKey}, {actorName}, controler task was canceled'
      logger.exception(f'{logMsg}, {actorName}, {actorId}')
    except Exception as ex:
      logMsg = f'{packet.jobKey}, {actorName}, controler task errored'
      logger.exception(f'{logMsg}, {actorName}, {actorId}', exc_info=True)
      self.resume(actorId, packet.caller, 500)

  # -------------------------------------------------------------- #
  # multiTask
  # ---------------------------------------------------------------#
  def multiTask(self, actorGroup, packet):
    raise NotImplementedError('for enterprise project enquires, contact pmg7670@gmail.com')

  # -------------------------------------------------------------- #
  # runQuery
  # ---------------------------------------------------------------#
  def runQuery(self, packet, metaItem=None):
    if not metaItem:
      logger.info(f'runQuery, metaKey : {packet.metaKey}')
      actorKey, metaKey = packet.metaKey.split(':')
      metaItem = self[actorKey][metaKey]
    args = ['SAAS',packet.typeKey] + metaItem.split('/')
    dbKey = '|'.join(args)
    try:
      result = self._leveldb[dbKey]
      try:
        packet.itemKey
        logger.info('### xform meta key : ' + packet.itemKey)
        return result[packet.itemKey]
      except AttributeError:
        return result
    except KeyError:
      logMsg = f'saas meta item not found. dbKey : {dbKey}'
      logger.warn(logMsg)
      raise Exception(logMsg)

  # -------------------------------------------------------------- #
  # runAdhoc
  # ---------------------------------------------------------------#
  async def runAdhoc(self, *args, **kwargs):
    await self.executor.runJob('adhoc', *args, **kwargs)

  # -------------------------------------------------------------- #
  # terminate
  # ---------------------------------------------------------------#
  async def terminate(self, first):
    try:
      logger.info(f'{self.jobId}, job controler deletion, calling service controler ...')
      packet = {'jobId':self.jobId,'caller':'controler','actor':'first'}
      apiUrl = 'http://%s/api/v1/smart/delete' % self[first].hostName
      async with self.request.post(apiUrl,json={'job':packet}) as response:
        rdata = await response.json()
        logger.info(f'api response {dict(rdata)}')
        if 'error' in rdata:
          logMsg = 'service controler failed to terminate'
          raise Exception(f'{self.jobId}, {logMsg}\n{rdata["error"]}')
    except asyncio.CancelledError:
      logger.exception(f'{self.jobId}, terminate task was canceled')
    except Exception as ex:
      logger.error(f'{self.jobId}, terminate task errored', exc_info=True)

  # -------------------------------------------------------------- #
  # getHardHashService
  # ---------------------------------------------------------------#
  async def getHardHashService(self, clientGroup):
    try:
      logger.info(f'{self.jobId}, hardhash creation, client pool : {clientGroup}')
      packet = {'hhId':None,'clientGroup':clientGroup}
      apiUrl = 'http://%s%s' % (self['hardhash'].hostName,self['hardhash'].uri)
      async with self.request.post(apiUrl,json={'job':packet}) as response:
        rdata = await response.json()
        logger.info(f'api response {dict(rdata)}')
        if 'error' in rdata:
          raise Exception(f'{self.jobId}, hardhash creation failed\n{rdata["error"]}')
      self.hhId = rdata['hhId']
      logger.info(f'{self.jobId}, hardhash {self.hhId} routerAddr : {rdata["routerAddr"]}')
      return rdata['routerAddr']
    except Exception as ex:
      logger.error(f'{self.jobId}, hardhash creation errored', exc_info=True)
      return None

  # -------------------------------------------------------------- #
  # closeHardHashService
  # ---------------------------------------------------------------#
  async def closeHardHashService(self):
    try:
      logger.info(f'{self.jobId}, hardhash {self.hhId} will now be deleted ...')
      packet = {'hhId':self.hhId}
      apiUrl = 'http://%s%s' % (self['hardhash'].hostName,self['hardhash'].uri)
      async with self.request.delete(apiUrl,json={'job':packet}) as response:
        rdata = await response.json()
        logger.info(f'api response {dict(rdata)}')
      if 'error' in rdata:
        raise Exception(f'{self.jobId}, hardhash deletion failed\n{rdata["error"]}')
      logger.info(f'{self.jobId}, hardhash {self.hhId} is now deleted')
    except asyncio.CancelledError:
      logger.exception(f'{self.jobId}, hardhash deletion, future is cancelled')
    except Exception as ex:
      logger.error(f'{self.jobId}, hardhash deletion failed', exc_info=True)

  # -------------------------------------------------------------- #
  # redo
  # ---------------------------------------------------------------#
  def redo(self, packet):
    args = []
    for item in packet.args:
      if item == 'packet':
        args.append(JobPacket(packet['packet']))
        continue
      args.append(packet[item])
    return self[packet.method](*args, **packet.kwargs)

  # -------------------------------------------------------------- #
  # getActor
  # ---------------------------------------------------------------#
  def getActor(self, serviceName, *args):

    module, className = self.registry.getClassName(serviceName)    
    return getattr(module, className)(self._leveldb, *args)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    [f.cancel() for f in self.pendingFuture if not f.done()]
    self.pendingFuture.clear()

# -------------------------------------------------------------- #
# ActorMeta
# ---------------------------------------------------------------#
class ActorMeta:
  def __init__(self, jmeta):
    self.__dict__.update(jmeta)
    self.actorId = None
    self.actor = None
    self.serviceName = None
        
  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    else:
      raise KeyError(f'{key} is not found')

  def tell(self):
    return self.actorId, self.serviceName

# -------------------------------------------------------------- #
# ActorGroup
# ---------------------------------------------------------------#
class ActorGroup:
  def __init__(self, jobRange):
    self.jobRange = jobRange
    self._actorId = {taskNum:str(uuid.uuid4()) for taskNum in jobRange}
    self._actor = {}

  def __setitem__(self, taskNum, actor):
    self._actor[taskNum] = actor

  def tell(self, taskNum):
    actor = self._actor[taskNum]
    return actor.actorId, actor.__class__.__name__

  @property
  def taskIds(self):
    return [f'task{taskNum:02}' for taskNum in self.jobRange]

  @property
  def ordActors(self):
    return [(taskNum, self._actor[taskNum]) for taskNum in self.jobRange]

  @property
  def ordIds(self):
    return [(taskNum, self._actorId[taskNum]) for taskNum in self.jobRange]

  @property
  def ids(self):
    return [id for taskNum, id in self._actorId.items()]

  @property
  def size(self):
    return len([taskNum for taskNum in self.jobRange])

# -------------------------------------------------------------- #
# JobPacket
# ---------------------------------------------------------------#
class JobPacket:
  def __init__(self, packet):
    if 'args' not in packet:
      self.args = []
    if 'kwargs' not in packet:
      self.kwargs = {}
    if 'jobId' not in packet:
      raise Exception("required job param 'jobId' not found in packet")
    self.__dict__.update(packet)
    if hasattr(self,'actor'):
      self.jobKey = f'{self.jobId}:{self.actor}'

  def dump(self, replace={}):
    clone = copy.deepcopy(self.__dict__)
    clone.update(replace)
    return clone

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    else:
      raise KeyError(f'{key} is not found')
