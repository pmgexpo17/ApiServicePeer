from concurrent.futures import FIRST_EXCEPTION
from functools import partial
from threading import RLock
import asyncio
import logging
import sys
import uuid

logger = logging.getLogger('apipeer.server')
slogger = logging.getLogger('apipeer.smart')

# -------------------------------------------------------------- #
# JobExecutor
# ---------------------------------------------------------------#
class JobExecutor:
  def __init__(self):
    self._exec = {}
    self._exec['director'] = DirectorExecutor()
    self._exec['delegate'] = DelegateExecutor()
    self._exec['adhoc'] = AdhocExecutor()

  def __getitem__(self, actorId):
    return self._exec['director'][actorId]

  def __setitem__(self, actorId, actor):
    self._exec['director'][actorId] = actor

  def __delitem__(self, actorId):
    del self._exec['director'][actorId]

  def runJob(self, actorType, *args, **kwargs):
    return self._exec[actorType].runJob(*args, **kwargs)

  def shutdown(self):
    self._exec['director'].shutdown()
    self._exec['delegate'].shutdown()

# -------------------------------------------------------------- #
# BaseExecutor
# ---------------------------------------------------------------#
class BaseExecutor:
  """
  Runs coroutines conventionally, or functions by the event loop default executor.
  """
  def __init__(self):
    self.eventloop = asyncio.get_event_loop()

  # -------------------------------------------------------------- #
  # getFuture
  # ---------------------------------------------------------------#
  def getFuture(self, actor, *args, **kwargs):

    logger.info(f'running future: actor, args, kwargs : {actor}, {args}, {kwargs}')
    if asyncio.iscoroutine(actor):
      # a task is a future-like object that runs a coroutine
      return asyncio.create_task(actor(*args, **kwargs))
    return self.eventloop.run_in_executor(None, partial(actor, *args, **kwargs))

# -------------------------------------------------------------- #
# AdhocExecutor
# ---------------------------------------------------------------#
class AdhocExecutor(BaseExecutor):
  def __init__(self):
    super().__init__()
    self.runJob = self.getFuture        

# -------------------------------------------------------------- #
# DirectorExecutor
# ---------------------------------------------------------------#
class DirectorExecutor(BaseExecutor):
  def __init__(self):
    super().__init__()
    self._actor = {}
    self.cache = JobCache()

  def __getitem__(self, actorId):
    return self._actor[actorId]

  def __setitem__(self, actorId, actor):
    self._actor[actorId] = actor

  def __delitem__(self, actorId):
    del self._actor[actorId]

  # -------------------------------------------------------------- #
  # runJob
  # ---------------------------------------------------------------#
  async def runJob(self, actorId, packet, **kwargs):
    actor = self[actorId]
    logger.info(f'### executor, about to run actor, {actor.name}, {packet.jobKey}')
    try:
      state = actor.state
      future = self.getFuture(actor, *packet.args, **packet.kwargs)
      await future
      future.result()
      state = actor.state      
      if not state.failed:
        logMsg = f'actor state {state.current} is resolved'      
        logger.info(f'{actor.name}, {logMsg}, {actorId}')
        await self.runNext(actorId)
    except asyncio.CancelledError:
      state.onError()
      logMsg = f'actor state {state.current} is canceled'
      logger.exception(f'{actor.name}, {logMsg}, {actorId}')      
    except Exception as ex:
      state.onError()
      logMsg = f'actor state {state.current} errored'
      logger.exception(f'{actor.name}, {logMsg}, {actorId}', exc_info=True)
    finally:
      return state

  # -------------------------------------------------------------- #
  # runNext
  # ---------------------------------------------------------------#
  async def runNext(self, actorId):
    if self.cache.hasNext(actorId):
      packet = self.cache.next(actorId)
      await self.runJob(packet)

# -------------------------------------------------------------- #
# DelegateExecutor
# ---------------------------------------------------------------#
class DelegateExecutor(BaseExecutor):
  def __init__(self):
    super().__init__()

  # -------------------------------------------------------------- #
  # getFuture
  # ---------------------------------------------------------------#
  def getFuture(self, actor, packet, taskNum):
    return super().getFuture(actor, *packet.args, taskNum, **packet.kwargs)

  # -------------------------------------------------------------- #
  # runJob
  # ---------------------------------------------------------------#
  async def runJob(self, actorGroup, packet, **kwargs):
    logger.info(f'### executor, about to run actor group, {packet.jobKey}')

    result = JobMeta({'complete':True,'failed':False,'taskNum':0})
    futures = {self.getFuture(actor, packet, taskNum): 
                      taskNum for taskNum, actor in actorGroup.ordActors}

    try:
      done, pending = await asyncio.wait(futures.keys(), return_when=FIRST_EXCEPTION)
    except asyncio.CancelledError:
      logger.exception(f'{packet.jobKey}, actorGroup is cancelled')
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
        result.taskNum = taskNum
        result.failed = True
    return result

# -------------------------------------------------------------- #
# JobMeta
# ---------------------------------------------------------------#
class JobMeta:
  def __init__(self, jmeta):
    self.__dict__.update(jmeta)

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    else:
      raise KeyError(f'{key} is not found')

# -------------------------------------------------------------- #
# JobCache
# ---------------------------------------------------------------#
class JobCache:
  def __init__(self):
    self.cache = {}
    self.activeJob = {}
    self.lock = RLock()
    
  # -------------------------------------------------------------- #
  # hasNext
  # ---------------------------------------------------------------#
  def hasNext(self, actorId, packet=None):
    with self.lock:
      if packet: 
        # if active, append to the cache otherwise add the new job 
        if actorId in self.activeJob:
          logger.info('%s, caching job packet ...' % actorId)
          self.cache[actorId].append(packet)
          return False
        else:
          self.activeJob[actorId] = packet
          self.cache[actorId] = [packet]
          return True
      # empty packet means test if there are cached jobs ready to execute
      try:
        if len(self.cache[actorId]) > 0:
          logger.info('%s, running cached job ...' % actorId)    
          return True
      except KeyError:
        logger.info('%s, delisting ...' % actorId)
        self.activeJob.pop(actorId, None)
        return False

  # -------------------------------------------------------------- #
  # next
  # ---------------------------------------------------------------#
  def next(self, actorId):
    with self.lock:
      logger.info('%s, running next job ...' % actorId)
      packet = self.cache[actorId].pop(0)
      self.activeJob[actorId] = packet
      return packet

# -------------------------------------------------------------- #
# ServiceRegistry
# ---------------------------------------------------------------#
class ServiceRegistry():
	
  def __init__(self):
    self._modules = {}
    self._services = {}
    
  # ------------------------------------------------------------ #
  # isLoaded
  # -------------------------------------------------------------#
  def isLoaded(self, serviceName):
    try:
      self._services[serviceName]
    except KeyError:
      return False
    else:
      return True

  # ------------------------------------------------------------ #
  # loadModules
  # -------------------------------------------------------------#
  def loadModules(self, serviceName, serviceRef):
    self._services[serviceName] = {}
    for module in serviceRef:
      self.loadModule(serviceName, module['name'], module['fromList'])
	
  # ------------------------------------------------------------ #
  # _loadModule : wrap load module
  # -------------------------------------------------------------#
  def loadModule(self, serviceName, moduleName, fromList):
    self._services[serviceName][moduleName] = fromList
    try:
      self._modules[moduleName]
    except KeyError:
      self._loadModule(moduleName, fromList)
		
  # ------------------------------------------------------------ #
  # _loadModule : execute load module
  # -------------------------------------------------------------#
  def _loadModule(self, moduleName, fromList):    
    # reduce moduleName to the related fileName for storage
    _module = '.'.join(moduleName.split('.')[-2:])
    logger.info('%s is loaded as : %s' % (moduleName, _module))
    self._modules[_module] = __import__(moduleName, fromlist=[fromList])

  # ------------------------------------------------------------ #
  # reloadHelpModule
  # -------------------------------------------------------------#
  def reloadHelpModule(self, serviceName, helpModule):
    try:
      serviceRef = self._services[serviceName]
    except KeyError as ex:
      return ({'status':400,'errdesc':'KeyError','error':str(ex)}, 400)
    for moduleName in serviceRef:
      #_module = moduleName.split('.')[-1]
      _module = '.'.join(moduleName.split('.')[-2:])
      self._modules[_module] = None
    if helpModule not in sys.modules:
      warnmsg = '### support module %s does not exist in sys.modules'
      logger.warn(warnmsg % helpModule)
    else:
      importlib.reload(sys.modules[helpModule])
    for moduleName, fromList in serviceRef.items():
      self._loadModule(moduleName, fromList)
    logger.info('support module is reloaded : ' + helpModule)
    return {'status': 201,'service': serviceName,'module': helpModule}
	
  # ------------------------------------------------------------ #
  # reloadModule
  # -------------------------------------------------------------#
  def reloadModule(self, serviceName, moduleName):
    try:
      serviceRef = self._services[serviceName]
      fromList = serviceRef[moduleName]
    except KeyError:
      return self.reloadHelpModule(serviceName,moduleName)

    # reduce moduleName to the related fileName for storage
    #_module = moduleName.split('.')[-1]
    _module = '.'.join(moduleName.split('.')[-2:])
    self._modules[_module] = None
    importlib.reload(sys.modules[moduleName])    
    logger.info('%s is reloaded as : %s' % (moduleName, _module))
    self._modules[_module] = __import__(moduleName, fromlist=[fromList])
    return {'status': 201,'service': serviceName,'module': moduleName}
	
  # ------------------------------------------------------------ #
  # getClassName
  # -------------------------------------------------------------#
  def getClassName(self, classRef):

    if ':' not in classRef:
      raise ValueError('Invalid classRef %s, expecting module:className' % classRef)
    moduleName, className = classRef.split(':')

    try:
      module = self._modules[moduleName]
    except KeyError:
      raise Exception('Service module name not found in register : ' + moduleName)

    if not hasattr(module, className):
      raise Exception('Service classname not found in service register : ' + className)
    
    return (module, className)
