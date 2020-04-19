__all__ = ['JobProvider']
from apibase import (AbstractTxnHost, ApiContext, ApiPacket, JobPacket, 
    LeveldbHash, Note, TaskError, Terminal)
from .jobControler import JobControler
import copy
import importlib
import logging
import os, sys

logger = logging.getLogger('asyncio.server')

class RenderError(Exception):
  pass

# -------------------------------------------------------------- #
# AbstractProvider
# ---------------------------------------------------------------#
class AbstractProvider(Terminal):

  # -------------------------------------------------------------- #
  # query
  # ---------------------------------------------------------------#
  def query(self, packet):
    try:
      result = self._leveldb[packet.eventKey]
    except KeyError:
      logger.error(f'datastore key {packet.eventKey} not found')
      raise Exception

    try:
      packet.itemKey
      logger.info('### query item key : ' + packet.itemKey)
      return result[packet.itemKey]
    except AttributeError:
      return result
    except KeyError:
      logger.error(f'item key {packet.itemKey} not found in {packet.eventKey}')
      raise Exception

  # ------------------------------------------------------------ #
  # importModule
  # -------------------------------------------------------------#
  def importModule(self, moduleName=None):
    if not moduleName:
      moduleName = self.moduleName
    logger.info(f'### job module name : {moduleName}') 
    module = sys.modules.get(moduleName)
    if not module:
      importlib.import_module(moduleName)
    else:
      logger.info(f'### reloading job module : {moduleName}')
      importlib.reload(module)

# -------------------------------------------------------------- #
# DependencyError
# ---------------------------------------------------------------#
class DependencyError(Exception):
  pass

# -------------------------------------------------------------- #
# JobProvider
# ---------------------------------------------------------------#
class JobProvider(AbstractProvider):

  def __init__(self, jpacket):
    self.serverId = jpacket.serverId
    self._leveldb = LeveldbHash.get()
    self.depth = 1
    self.dependency = copy.copy(jpacket.dependency)

  @property
  def hasNext(self):
    return self.dependency != []

  # -------------------------------------------------------------- #
  # make
  # ---------------------------------------------------------------#
  @classmethod
  def run(cls, jpacket):
    dealers = {}
    provider = cls.resolve(packet=jpacket)
    while provider.hasNext:
      gpacket = provider.next()
      provider.make(dealers, gpacket)
    return dealers

  # -------------------------------------------------------------- #
  # make
  # ---------------------------------------------------------------#
  def make(self, dealers, gpacket):
    logger.info(f'{gpacket.jobId}, making job dependency {gpacket.eventKey}')
    moduleName, className = gpacket.generator.split(':')
    module = sys.modules.get(moduleName)
    if not module:
      module = importlib.import_module(moduleName)
    genKlass = getattr(module, className)
    logger.info(f'provider, job generation class : {className}')
    jpacket = genKlass()(gpacket)
    logger.info(f'job {jpacket.jobId} assembly : {jpacket.assembly}')
    controler = JobControler.make(jpacket)
    dealers[jpacket.jobId] = JobDealer.make(self.serverId, controler, jpacket)

  # -------------------------------------------------------------- #
  # resolve
  # ---------------------------------------------------------------#
  @classmethod
  def resolve(cls, provider=None, packet=None):
    if not provider:
      provider = cls(packet)
      logger.info(f'root job dependency list : {provider.dependency}')
      if not packet.dependency:
        return provider
    if not provider.extended():
      return provider
    logger.info(f'job dependency list : {provider.dependency}')
    if provider.depth > 10:
      raise DependencyError('dependency depth limit of 10 exceeded')
    return cls.resolve(provider)

  # -------------------------------------------------------------- #
  # extended
  # ---------------------------------------------------------------#
  def extended(self):
    eventKey = self.dependency[-1]
    params = {'eventKey':eventKey,'itemKey':'dependency'}
    nextDep = self.query(Note(params))
    logger.info(f'next dependency : {nextDep}')
    if nextDep:
      self.dependency.extend(nextDep)
      self.depth += 1
    return nextDep != []

  # -------------------------------------------------------------- #
  # next
  # ---------------------------------------------------------------#
  def next(self):
    eventKey = self.dependency.pop()
    params = Note({'eventKey':eventKey})
    jobMeta = self.query(params)
    logger.info(f'next generate meta : {jobMeta}')
    return JobPacket(jobMeta)

#----------------------------------------------------------------#
# JobDealer
#----------------------------------------------------------------#		
class JobDealer(AbstractTxnHost):
  def __init__(self, connector, contextId, controler, runMode=logging.INFO):
    super().__init__(connector, contextId)
    self.controler = controler
    self.runMode = runMode

  #----------------------------------------------------------------#
  # desc
  #----------------------------------------------------------------#
  @property
  def desc(self):
    return self.controler.desc

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, contextId, controler, jpacket):
    connector = ApiContext.connector(jpacket.jobId)
    hostname = f'{cls.__name__}-{contextId}-{connector.cid}'
    logger.info(f'making new {hostname} instance for job {jpacket.jobId} ...')    
    return cls(connector, contextId, controler)

  #----------------------------------------------------------------#
  # perform
  #----------------------------------------------------------------#
  async def perform(self, request, packet):
    try:
      jpacket = self.checkPacket(request, packet)
      response = self.controler[request](jpacket)
      if jpacket.synchronous:
        await self.send(response,jpacket.actor)
    except Exception as ex:
      if jpacket.synchronous:
        await self.send([404,{"error":str(ex)}])

  #----------------------------------------------------------------#
  # checkPacket
  #----------------------------------------------------------------#
  def checkPacket(self, request, packet):
    logmsg = f'{self.hostname}, got request {request}'
    if isinstance(packet, dict):
      logger.info(f'{logmsg}\n{packet}')
      return ApiPacket(packet)
    logger.info(f'{logmsg}\n{packet.body}')
    return packet

  #----------------------------------------------------------------#
  # shutdown
  #----------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.hostname}, shutting down ...')
    self.controler.shutdown()
    self._conn.close()

  #----------------------------------------------------------------#
  # start
  #----------------------------------------------------------------#
  def start(self):
    pass
