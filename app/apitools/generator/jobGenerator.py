from apibase import AbstractDatasource, Article, JobProvider, LeveldbHash, TaskError, Terminal
import importlib
import logging
import os, sys

logger = logging.getLogger('asyncio.server')

class RenderError(Exception):
  pass

# -------------------------------------------------------------- #
# AbstractMakeKit
# -- class level attributes enable companion classes to access
# -- them as a shared resource
# ---------------------------------------------------------------#
class AbstractMakeKit(Terminal):
  _meta = None
  _leveldb = None
  request = None

  def __getattr__(self, key):
    raise NotImplementedError('AbstractMakeKit.__getattr__ is an abstract method that must be implemented')

  def __getitem__(self, key):
    raise NotImplementedError('AbstractMakeKit.__getattr__ is an abstract method that must be implemented')    

  def __setitem__(self, key, value):
    raise NotImplementedError('AbstractMakeKit.__setitem__ is an abstract method that must be implemented')

  def merge(self, merjar):
    raise NotImplementedError('AbstractMakeKit.update is an abstract method that must be implemented')

  def prepare(self, *args):
    raise NotImplementedError('AbstractMakeKit.prepare is an abstract method that must be implemented')

  def select(self, *args):
    raise NotImplementedError('AbstractMakeKit.select is an abstract method that must be implemented')

  @classmethod
  def __start__(cls, genPacket):
    packet = genPacket.copy()
    for key, value in packet.items():
      if isinstance(value,dict):
        packet[key] = Article(value)
    cls._meta = {}
    cls._meta.update(packet)
    cls._leveldb = LeveldbHash.get()
    return packet

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
      module = importlib.import_module(moduleName)
    else:
      logger.info(f'### reloading job module : {moduleName}')
      module = importlib.reload(module)
    return module

# -------------------------------------------------------------- #
# DependencyError
# ---------------------------------------------------------------#
class DependencyError(Exception):
  pass

# -------------------------------------------------------------- #
# AbstractGenerator
# - a descendant class must also inherit a local MakeKit class
# ---------------------------------------------------------------#
class AbstractGenerator:

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  # -------------------------------------------------------------- #
  # assemble
  # ---------------------------------------------------------------#
  def assemble(self):
    params = {'eventKey':self.eventKey}    
    jobMeta = self.query(Article(params))
    self.merge(jobMeta['assembly'])
    self['releaseInfo'] = self.select('gitUser','owner','product','releaseTag')
    gitUser, owner, product, releaseTag = self.releaseInfo
    logger.info(f'release params : {gitUser} {owner} {product} {releaseTag}')
    productBase = f'{owner}-{releaseTag}/{product}'
    logger.info(f'productBase : {productBase}')
    return jobMeta

  # ------------------------------------------------------------ #
  # __call__
  # -------------------------------------------------------------#
  def __call__(self, packet):
    try:
      self.prepare(packet)
      return self.generate()
    except (RenderError, Exception) as ex:      
      logger.error(f'job {packet.jobId} generation error', exc_info=True)
      # TO DO : on error, before removing the servicePack should be archived
      raise TaskError

  # ------------------------------------------------------------ #
  # generate
  # -- make.build flag --
  # -- 0 : no build
  # -- 1 : build only if module base does NOT exist
  # -- 2 : rebuild when module base exists
  # -------------------------------------------------------------#
  def generate(self):
    jobMeta = self.assemble()
    self.setModuleParms()    
    if self.make.build == "nobuild":
      logger.warn(f'make.build condition is negative, aborting {self.moduleName} build ...')
    else:
      self.build(jobMeta)
    self.importModule()
    return Article(jobMeta)

  # ------------------------------------------------------------ #
  # setModuleParms
  # -------------------------------------------------------------#
  def setModuleParms(self):
    raise NotImplementedError(f'{self.name}.setModuleParms is an abstract method')

  # -------------------------------------------------------------- #
  # build
  # ---------------------------------------------------------------#
  def build(self, jobMeta):
    raise NotImplementedError(f'{self.name}.build is an abstract method')

  # -------------------------------------------------------------- #
  # deploy
  # ---------------------------------------------------------------#
  def deploy(self, jobMeta):
    raise NotImplementedError(f'{self.name}.deploy is an abstract method')

  # -------------------------------------------------------------- #
  # render
  # ---------------------------------------------------------------#
  def render(self, jobMeta):
    raise NotImplementedError(f'{self.name}.render is an abstract method')

  # -------------------------------------------------------------- #
  # loadResources
  # ---------------------------------------------------------------#
  def loadResources(self, jobMeta):
    raise NotImplementedError(f'{self.name}.loadResources is an abstract method')

  # -------------------------------------------------------------- #
  # runAdhoc
  # ---------------------------------------------------------------#
  async def runAdhoc(self, *args, **kwargs):
    await self.executor.run(*args, **kwargs)
