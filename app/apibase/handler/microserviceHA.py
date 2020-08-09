from apibase import ApiRequest, Article, MicroserviceExecutor, Note
from .handler import TaskHandler
import asyncio
import importlib
import logging
import os, sys
import uuid

logger = logging.getLogger('asyncio.broker')

# -------------------------------------------------------------- #
# MicroserviceHandler - handler for stateless microservice actors
# - this handler class : 
#   - stores the context of 1 Microservice type actors
#   - runs the related ActorGroup by multitasking future creation
# ---------------------------------------------------------------#
class MicroserviceHandler(TaskHandler):
  executor = None

  def __init__(self, jobId):
    super().__init__(jobId)
    self._request = None

  #------------------------------------------------------------------#
  # make
  #------------------------------------------------------------------#
  @classmethod
  def make(cls, *args, **kwargs):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, actorKey, jobMeta):
    serviceActor = jobMeta.pop('serviceActor')
    assemblyA = jobMeta['assembly']
    self.__dict__[actorKey] = Article(assemblyA)
    for microKey, assemblyB in serviceActor.items():
      subKey = f'{actorKey}:{microKey}'
      assemblyB.update(assemblyA)
      logger.info(f'{self.name}, applying {subKey} jobMeta : {assemblyB}')
      self.__dict__[subKey] = ActorBrief(subKey, assemblyB)

  # -------------------------------------------------------------- #
  # arrange
  # ---------------------------------------------------------------#
  def arrange(self, connector, *args, **kwargs):
    logger.info(f'##### {self.name}, setting ApiRequest connector ...')    
    self._request = connector
    if not self.executor:
      self.executor = MicroserviceExecutor()

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    self.executor.destroy()    

  # -------------------------------------------------------------- #
  # runGroup
  # ---------------------------------------------------------------#
  async def runGroup(self, actorGroup, packet):
    logger.info(f'about to runGroup, {packet.actor} ...')
    result = Article({'complete':True,'failed':True,'signal':500})
    try:
      result = await self.executor.run(actorGroup, packet)
      if result.failed:
        logger.info(f'{packet.taskKey}, microservice failed, aborting ...')
    except asyncio.CancelledError:
      logger.warn(f'{packet.taskKey}, microservice task was canceled')
    except Exception as ex:
      logger.error(f'{packet.taskKey}, microservice task errored', exc_info=True)
      raise
    finally:
      await self.resume(result, packet)

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self, packet):
    logger.info(f'{self.name}, no preparation required')    
    self.respond(200, packet)

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet):
    classToken = self[packet.actor].classToken
    logger.info(f'{self.jobId}, microservice actor : {classToken}')
    actorGroup = ActorGroup(packet.taskRange)
    mixin = actorGroup.arrange(classToken)
    coro = self.submitTask(actorGroup, packet)
    future = asyncio.ensure_future(coro)
    self.addFuture(future, packet.taskKey)    
    return self.respond(201, packet, mixin)

  # -------------------------------------------------------------- #
  # submitTask
  # ---------------------------------------------------------------#
  def submitTask(self):
    raise NotImplementedError(f'{self.__name__}.submitTask is an abstract method')

  # -------------------------------------------------------------- #
  # resume
  # ---------------------------------------------------------------#
  async def resume(self, result, jpacket):
    caller = Note(jpacket.caller)
    logmsg = f'resuming {caller.typeKey}.{caller.actor} with signal {result.signal} ...'
    logger.info(f'{self.name}, {self.jobId}, {logmsg}')
    rpacket = {
      'jobId': caller.jobId,
      'typeKey': caller.typeKey,
      'caller': jpacket.actor,
      'actor': caller.actor,
      'fromKey': jpacket.typeKey,
      'synchronous': False,
      'signal': result.signal
    }
    await self.request('resume', rpacket)

  # -------------------------------------------------------------- #
  # start - protocol expects component futures returned
  # ---------------------------------------------------------------#
  def start(self, *args, **kwargs):
    return []

# -------------------------------------------------------------- #
# MicroserviceHA
# - Microservice Handler for an exclusive data channel for direct
# - connection of client/service apps. eg DatastreamMicroservice
# ---------------------------------------------------------------#
class MicroserviceHA(MicroserviceHandler):
  executor = None

  def __init__(self, jobId, peerNote=None):
    super().__init__(jobId)
    self.peerNote = peerNote
    self._request = None

  # -------------------------------------------------------------- #
  # submitTask
  # ---------------------------------------------------------------#
  async def submitTask(self, actorGroup, jpacket):
    logger.info(f'{jpacket.taskKey}, requesting DatastoreA prepare for microservice job ...')
    ppacket = {
      'jobId': self.peerNote.jobId,
      'typeKey': self.peerNote.typeKey,
      'caller': jpacket.caller,
      'actor': self.peerNote.actor,
      'fromKey': jpacket.typeKey,
      'synchronous': True,
      'taskRange': len(jpacket.taskRange)
    }
    connector = ApiRequest.connector(self.peerNote.jobId)
    response = await self.request('prepare', ppacket, connector)
    logger.info(f'{self.name}, prepare response : {response}')    
    logger.info(f'{jpacket.taskKey}, {jpacket.caller} microservice ...')
    await self.runGroup(actorGroup, jpacket)

# -------------------------------------------------------------- #
# MicroserviceHB
# -- Microservice handler using an embedded hardhash datastore
# ---------------------------------------------------------------#
class MicroserviceHB(MicroserviceHandler):

  def __init__(self, jobId):
    super().__init__(jobId)
    self._request = None

  # -------------------------------------------------------------- #
  # submitTask
  # ---------------------------------------------------------------#
  async def submitTask(self, actorGroup, jpacket):
    logger.info(f'{jpacket.taskKey}, {jpacket.caller} microservice ...')
    await self.runGroup(actorGroup, jpacket)

# -------------------------------------------------------------- #
# ActorGroup
# ---------------------------------------------------------------#
class ActorGroup:
  def __init__(self, taskRange):
    self.actorId = {taskNum:str(uuid.uuid4()) for taskNum in taskRange}
    self.taskRange = taskRange
    self.actor = {}

  # -------------------------------------------------------------- #
  # arrange
  # ---------------------------------------------------------------#
  def arrange(self, classToken):
    moduleName, className = classToken.split(':')
    module = sys.modules.get(moduleName)
    if not module:
      module = importlib.import_module(moduleName)
    actorKlass = getattr(module, className)
    for taskNum, actorId in self.ordset:
      logger.info(f'### job {taskNum}, actor id : {actorId}')
      actor = actorKlass(taskNum, actorId)
      self.actor[taskNum] = actor
    return {'id': self.ids}

  def tell(self, taskNum):
    actor = self.actor[taskNum]
    return actor.actorId, actor.name

  @property
  def ordset(self):
    return [(taskNum, self.actorId[taskNum]) for taskNum in self.taskRange]

  @property
  def taskset(self):
    return [f'task{taskNum:02}' for taskNum in self.taskRange]

  @property
  def ordActors(self):
    return [(taskNum, self.actor[taskNum]) for taskNum in self.taskRange]

  @property
  def ids(self):
    return [id for taskNum, id in self.actorId.items()]

  @property
  def size(self):
    return len([taskNum for taskNum in self.taskRange])

