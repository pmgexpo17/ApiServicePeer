from apibase import ApiRequest, Article, ServiceExecutor, TaskComplete, TaskError
from firebase_admin import credentials
from .handler import ActorBrief, TaskHandler
import asyncio
import copy
import importlib
import logging
import os, sys
import uuid
import firebase_admin

logger = logging.getLogger('asyncio.smart')

# -------------------------------------------------------------- #
# LeveldbHA
# -- Service handler for state-machine actor services
# ---------------------------------------------------------------#
class LeveldbHA(ServiceHA):
  executor = None
  projectBase = None
  fbApp = None

  def __init__(self, jobId):
    super().__init__(jobId)

  @classmethod
  def make(cls, credPath):
    credPath = f'{apiBase}/etc/apipeer-sysadmin-firebase-accountkey.json'
    url = 'https://apipeer-sysadmin.firebaseio.com'
    #cred = credentials.RefreshToken(credPath)
    cred = credentials.Certificate(credPath)
    fbApp = firebase_admin.initialize_app(cred)
    db = fbdb.reference(path=refPath,url=url)
    return cls(credPath, url)

  @classmethod
  def make(cls, projectId, jobId):
    credPath = f'{cls.projectBase}/{projectId}-firebase-credentials.json'
    cred = credentials.RefreshToken(credPath)
    cls.fbApp = firebase_admin.initialize_app(cred)
    return cls(jobId)

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, actorKey, jobMeta):
    logger.info(f'{self.name}, applying {actorKey} jobMeta : ' + str(jobMeta['assembly']))
    self.__dict__[actorKey] = ActorBrief(actorKey, jobMeta['assembly'])

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
    actorKey, actorId, classToken = self[packet.actor].tell()
    logger.info(f'{self.jobId}, service actor : {actorKey},{classToken},{actorId}')
    moduleName, className = classToken.split(':')
    if actorId:
      # state machine is promoted
      logger.info(f'{packet.taskKey}, resuming live actor ...')
      return (actorId, classToken)  
    logger.info(f'{packet.taskKey}, loading new actor ...')
    module = sys.modules.get(moduleName)
    if not module:
      module = importlib.import_module(moduleName)
    actorId = str(uuid.uuid4())
    # must be an AppDirector derivative, parameters are fixed by protocol
    actor = getattr(module, className)(actorId)
    self[packet.actor].actorId = actorId
    self.executor[actorId] = actor
    return (actorId, classToken)

  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, actorId, packet):
    try:
      state = self.executor[actorId].prepare()
      if not state.running:
        # start actor
        await self.executor.start(actorId, self.jobId, self[packet.actor])      
      state = await self.executor.run(actorId, packet)      
      await self.advance(state, packet)
    except asyncio.CancelledError:
      logger.info(f'ATTN. {packet.taskKey}, service task is canceled')
      state.canceled = True
      self.executor.stop(actorId)
    except Exception as ex:
      state.apply({'complete':True,'failed':True,'canceled':False})
      logger.error(f'{packet.taskKey}, service task errored', exc_info=True)

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet):
    actorId, classToken = self.getActor(packet)
    coro = self.runActor(actorId, packet)
    future = asyncio.ensure_future(coro)
    self.addFuture(future, packet.taskKey)
    return self.respond(201, packet)

  # -------------------------------------------------------------- #
  # resume
  # ---------------------------------------------------------------#
  def resume(self, rpacket):
    try:
      logger.info(f'{self.name}, resume signal from handler {rpacket.fromKey}, caller {rpacket.caller}')
      self.resumeHandler.resolve(rpacket)
      return self.respond(201, rpacket)
    except TaskComplete:
      state = self.resumeHandler.state
      rpacket.kwargs.update({'signal':rpacket.signal})
      logger.info(f'{self.name}, {rpacket.actor} {state.current} is complete, resuming ...')
      return self.promote(rpacket)

  # -------------------------------------------------------------- #
  # advance
  # - quicken is called only for these conditions
  # --- 1. actor is complete and actor is not the first / lead actor
  # --- 2. state is inTransition
  # - state transition has 2 cases
  # -- 1. a stateful actor calling another stateful peer
  # -- 2. a stateful actor calling a microservice actor group 
  # -- For case 1, hasSignal == True otherwise False
  # ---------------------------------------------------------------# 
  async def advance(self, state, packet):
    if state.canceled: # no time for termination so return now
      return
    elif state.failed or (state.complete and packet.first):
      await self.terminate(state, packet)
    elif state.inTransition or (state.complete and not packet.first):
      await self.quicken(state)

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  async def quicken(self, state):
    logger.info(f'quicken state transition {state.current} ... ')
    # get the new current state profile
    if state.inTransition:      
      self.resumeHandler = ResumeHandler(state)    
    packet = self.executor[state.actorId].quicken()
    if state.hasSignal:
      packet.update({'kwargs':{'signal':state.signal}})
    logger.info(f'{self.name}, promote args : {packet}')
    await self.request('promote', packet)

  # -------------------------------------------------------------- #
  # terminate
  # ---------------------------------------------------------------#
  async def terminate(self, state, packet):
    if state.failed:
      logger.info(f'ATTN. {packet.taskKey}, service failed, removing it now ...')
    else:
      logger.info(f'ATTN. {packet.taskKey}, service is complete, removing it now ...')
    logger.info(f'{self.name}, requesting {self.jobId} job termination ...')
    tpacket = {
      'jobId': packet.jobId,
      'typeKey': packet.typeKey,
      'caller': packet.actor,
      'actor': 'controler',
      'synchronous': False,
      'context': 'failed' if state.failed else 'complete'
    }
    connector = ApiRequest.connector('control')
    await self.request('terminate', tpacket, connector)

# -------------------------------------------------------------- #
# ResumeHandler
# ---------------------------------------------------------------#
class ResumeHandler:
  def __init__(self, state):
    self.state = state
    try:      
      self._signalFrom = copy.copy(state.signalFrom)
    except AttributeError:
      # state transition does not have a resume condition
      self._signalFrom = []

  def resolve(self, packet):
    if not self._signalFrom:
      logger.info('ResumeHandler, state transition does not have a resume condition')
      raise TaskComplete
    logger.info('ResumeHandler, resolving resume status ...')
    if packet.fromKey not in self._signalFrom:
      raise TaskError(f'resume resolution failed, {packet.fromKey} is not registered for this service')
    if packet.signal != 201:
      errMsg = f'resume resolution failed, {packet.fromKey} actor {packet.caller} failed'
      raise TaskError(f'{errMsg}\nresume packet : {packet.copy()}')
    self._signalFrom.remove(packet.fromKey)
    if not self._signalFrom:
      raise TaskComplete
