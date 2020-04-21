from apibase import ApiRequest, Article, LeveldbHash, Note, Packet, TaskError, TaskHandler, Terminal
from functools import partial
import asyncio
import importlib
import logging
import subprocess
import os, sys

logger = logging.getLogger('asyncio.server')

# -------------------------------------------------------------- #
# JobArrangement
# ---------------------------------------------------------------#
class JobArrangement:
  apiBase = None

  def __init__(self, jobId):
    self.jobId = jobId
    self._handler = {}
    self._leveldb = LeveldbHash.get()
      
  @classmethod
  def __start__(cls, apiBase):
    cls.apiBase = apiBase

  @property
  def name(self):
    return f'{self.__class__.__name__}.{self.jobId}'

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, assembly):
    logger.info(f'{self.name} assembly : {assembly}')
    self.__dict__.update(assembly)

  # -------------------------------------------------------------- #
  # arrange
  # ---------------------------------------------------------------#
  def arrange(self, jpacket):
    for metaItem in jpacket.handlers:
      self.addHandler(Packet(metaItem))

    for actorKey in jpacket.serviceActors:
      self.assemble(actorKey, jpacket[actorKey])

  # -------------------------------------------------------------- #
  # assemble
  # ---------------------------------------------------------------#
  def assemble(self, actorKey, jobMeta):
    logger.info(f'{actorKey} handler assembly ...')
    assembly = Article(jobMeta['assembly'])
    try:
      logger.info(f'{actorKey} type key : {assembly.typeKey}')
      handler = self._handler[assembly.typeKey]
      handler.apply(actorKey, jobMeta)
    except KeyError:
      raise Exception(f'{actorKey} handler typeKey {assembly.typeKey} is invalid')

  # -------------------------------------------------------------- #
  # addHandler
  # ---------------------------------------------------------------#
  def addHandler(self, packet):
    moduleName, className = packet.classToken.split(':')
    logger.info(f'provider, adding handler : {packet.classToken}')
    module = sys.modules.get(moduleName)
    if not module:
      logger.info(f'debug, importing module {moduleName}')
      module = importlib.import_module(moduleName)
    logger.info(f'provider, adding {packet.typeKey} handler {className}, args : {packet.args}')
    self._handler[packet.typeKey] = getattr(module, className).make(self.jobId,*packet.args)

  # -------------------------------------------------------------- #
  # query
  # ---------------------------------------------------------------#
  def query(self, packet, render='Note'):
    result = self.select(packet)
    if render == 'Note':
      return Note(result)
    elif render == 'Article':
      return Article(result)
    return result

  # -------------------------------------------------------------- #
  # select
  # ---------------------------------------------------------------#
  def select(self, packet):
    try:      
      result = self._leveldb[packet.eventKey]
      try:
        packet.itemKey
        logger.info('### query item key : ' + packet.itemKey)
        return result[packet.itemKey]
      except AttributeError:
        return result
    except KeyError:
      logger.error(f'saas meta item {packet.eventKey} not found')
      raise Exception

# -------------------------------------------------------------- #
# JobControler
# ---------------------------------------------------------------#
class JobControler(JobArrangement):

  def __init__(self, jobId):
    super().__init__(jobId)
    self.pendingFutures = set()

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    if key in self._handler:
      return self._handler[key]
    return getattr(self, key)

  # -------------------------------------------------------------- #
  # make
  # ---------------------------------------------------------------#
  @classmethod
  def make(cls, jpacket):
    controler = cls(jpacket.jobId)
    controler.arrange(jpacket)
    controler.start(jpacket)
    return controler

  # -------------------------------------------------------------- #
  # addFuture
  # ---------------------------------------------------------------#
  def addFuture(self, future, taskKey):
    def onComplete(fut):
      try:
        self.pendingFutures.discard(fut)
        fut.result()
      except Exception as ex:
        logger.error(f'{taskKey}, task error', exc_info=True)
    self.pendingFutures.add(future)
    future.add_done_callback(onComplete)

  # -------------------------------------------------------------- #
  # arrange
  # ---------------------------------------------------------------#
  def arrange(self, jpacket):
    logger.info(f'{self.name}, arranging assets ...')
    self.apply(jpacket.assembly)    
    super().arrange(jpacket)
    for handler in self._handler.values():
      connector = ApiRequest.connector(self.jobId)
      handler.arrange(connector, jpacket.assembly)
    self.shutdownH = ShutdownHandler.make(jpacket.shutdownPolicy)
    typeKey = self.shutdownH.typeKey
    self._handler[typeKey] = self.shutdownH

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, jpacket):
    logger.info(f'{self.name}, starting ...')
    for handler in self._handler.values():
      handler.start(jpacket.assembly)

  # -------------------------------------------------------------- #
  # resume
  # ---------------------------------------------------------------#
  def resume(self, packet, *args, **kwargs):
    try:
      logger.info(f'{packet.taskKey}, resuming job ...')
      packet = self.precede(packet)
      return self[packet.typeKey].resume(packet, *args, **kwargs)
    except Exception as ex:
      logger.error(f'{packet.taskKey}, handler error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self, packet, *args, **kwargs):
    try:
      logger.info(f'{packet.taskKey}, preparing task : {packet.body}')
      packet = self.precede(packet)
      return self[packet.typeKey].prepare(packet, *args, **kwargs)
    except Exception as ex:
      logger.error(f'{packet.taskKey}, handler error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet, *args, **kwargs):
    try:
      logger.info(f'{packet.taskKey}, promoting job : {packet.body}')
      packet = self.precede(packet)
      return self[packet.typeKey].promote(packet, *args, **kwargs)
    except Exception as ex:
      logger.error(f'{packet.taskKey}, handler error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # precede
  # ---------------------------------------------------------------#
  def precede(self, packet):
    try:
      if packet.actor == 'first':
        packet.actor = self.first
        logger.info('ATTN. first actor : ' + self.first)
      # set the first flag to enable completion test => first actor called is complete
      packet.first = packet.actor == self.first
      if packet.typeKey not in self._handler:
        raise TaskError(f'{packet.taskKey}, {packet.typeKey} is NOT a registered handler')
      return packet
    except Exception as ex:
      logger.error(f'{packet.taskKey}, handler error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # restart
  # ---------------------------------------------------------------#
  def restart(self, packet, *args, **kwargs):
    try:
      logger.info(f'{packet.taskKey}, restarting service : {packet.body}')
      packet = self.precede(packet)
      return self[packet.typeKey].restart(packet, *args, **kwargs)
    except Exception as ex:
      logger.error(f'{packet.taskKey}, handler error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down, cancel futures ...')
    [handler.shutdown() for handler in self._handler.values()]
    
  # -------------------------------------------------------------- #
  # terminate
  # ---------------------------------------------------------------#
  def terminate(self, packet, *args, **kwargs):
    try:
      response = self._terminateA(packet)
      if packet.synchronous:
        return response
      return None
    except Exception as ex:
      logger.error(f'{self.jobId}, controler terminate error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # _terminateA
  # ---------------------------------------------------------------#
  def _terminateA(self, packet):
    if self.shutdownH.terminateApproved(packet):
      coro = self._terminateB(packet)
      future = asyncio.ensure_future(coro)
      self.addFuture(future, packet.taskKey)
      logger.info(f'{self.name}, ATTN. terminate approved by shutdown policy')
      return [201, {'status':201,'method':'terminate'}]
    logger.info(f'{self.name}, ATTN. terminate ignored by shutdown policy')
    return [417, {'status':417,'message':'terminate ignored by shutdown policy'}]

  # -------------------------------------------------------------- #
  # _terminateB
  # ---------------------------------------------------------------#
  async def _terminateB(self, packet):
    logger.info(f'{self.jobId}, terminate controler resources ...')
    self.shutdown()
    await asyncio.sleep(2)
    [handler.destroy() for handler in self._handler.values()]
    await asyncio.sleep(2)
    if self.shutdownH.deleteApproved(packet):
      coro = self.shutdownH.submit()
      asyncio.ensure_future(coro)

  # -------------------------------------------------------------- #
  # delete
  # ---------------------------------------------------------------#
  def delete(self, packet, *args, **kwargs):
    try:
      logger.info(f'{packet.taskKey}, deleting service module ...\n{packet.body}')
      [f.cancel() for f in self.pendingFutures if not f.done()]
      self.pendingFutures.clear()
      return self[packet.typeKey].delete(packet, *args, **kwargs)
    except Exception as ex:
      logger.error(f'{self.name}, delete error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    try:
      logger.info(f'{self.jobId}, destroy handler resources ...')
      [handler.destroy() for handler in self._handler.values()]        
    except Exception as ex:
      logger.error(f'{self.jobId}, handler destroy error', exc_info=True)
      raise TaskError(ex)

# -------------------------------------------------------------- #
# ShutdownHandler
# ---------------------------------------------------------------#
class ShutdownHandler(TaskHandler, Terminal):
  def __init__(self, policy):
    self.__dict__.update(policy)
    logger.info(f'@@@@@@@@@@@@@@@{self.name}, BODY : {self.__dict__}')

  # ------------------------------------------------------------ #
  # make
  # ------------------------------------------------------------ #
  @classmethod
  def make(cls, shutdownPolicy):
    handler = cls(shutdownPolicy)
    connector = ApiRequest.connector('control')    
    handler.arrange(connector)
    return handler

  # ------------------------------------------------------------ #
  # destroy
  # -------------------------------------------------------------#
  def destroy(self):
    pass

  # ------------------------------------------------------------ #
  # terminateApproved
  # -------------------------------------------------------------#
  def terminateApproved(self, packet):
    if packet.context == 'failed':
      return self.onError in ('delete','terminate')
    elif packet.context == 'complete':
      return self.onComplete in ('delete','terminate')
    return False

  # ------------------------------------------------------------ #
  # deleteApproved
  # -------------------------------------------------------------#
  def deleteApproved(self, packet):
    if packet.context == 'failed':
      return self.onError == 'delete'
    elif packet.context == 'complete':
      return self.onComplete == 'delete'
    return False

  # ------------------------------------------------------------ #
  # submit
  # ------------------------------------------------------------ #
  async def submit(self):
    try:
      packet = {
        'jobId': self.jobId,
        'typeKey': self.typeKey,
        'caller': 'controler',
        'actor': None,
        'synchronous': False
        }
      await self.request('delete', packet)
    except Exception as ex:
      logger.error(f'{self.name}, task errored', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # delete
  # ---------------------------------------------------------------#
  def delete(self, packet, *args, **kwargs):
    try:
      if self.onComplete != 'delete':
        logger.info(f'{self.name}, {self.onComplete} rule does not permit delete action')
        return
      projectBase = f'project/{self.projectId}'
      self.sysCmd(['rm','-rf',self.jobId],cwd=f'{JobControler.apiBase}/{projectBase}')
      logger.info(f'{self.name}, job module {self.jobId} is deleted, base path : {projectBase}')      
    except Exception as ex:
      logger.error(f'{self.jobId}, controler delete error', exc_info=True)
      raise TaskError(ex)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    pass
