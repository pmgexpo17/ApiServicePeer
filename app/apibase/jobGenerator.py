from jinja2 import Environment, PackageLoader, Template
from apibase import ApiRequest, JobMeta, Terminal
import simplejson as json
import os
import logging
import sys

logger = logging.getLogger('apipeer.server')

class RenderError(Exception):
  pass

# -------------------------------------------------------------- #
# JobGenerator
# ---------------------------------------------------------------#
class JobGenerator(Terminal):
  apiBase = None

  def __init__(self, leveldb, jobId):
    self._leveldb = leveldb
    self.jobId = jobId
    self.request = ApiRequest()
    self.env = Environment(loader=PackageLoader('apiservice', 'templates'),trim_blocks=True)

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    elif key in self.jmeta:
      return self.jmeta[key]
    elif not hasattr(self, key):
      raise AttributeError(f'{key} is not a JobGenerator attribute')
    member = getattr(self, key)
    self.__dict__[key] = member
    return member

  @staticmethod
  def start(apiBase):
    JobGenerator.apiBase = apiBase

  # -------------------------------------------------------------- #
  # _filter
  # ---------------------------------------------------------------#
  def _filter(self, *args):
    return [self.jmeta[key] for key in args if key in self.jmeta]

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
  def loadActor(self, key, jmeta):
    self.jmeta[key] = ActorMeta(jmeta[key])
    if self.jmeta[key].role == 'microservice':
      for actorName, item in self.jmeta[key].actors.items():
        subkey = f'{key}:{actorName}'
        self.jmeta[subkey] = ActorMeta(item)
    else:
      self.jmeta[key] = ActorMeta(jmeta[key])

  # ------------------------------------------------------------ #
  # make
  # -------------------------------------------------------------#
  def make(self, packet):
    self.jobBase = f'{self.apiBase}/apiservice/{self.jobId}'    
    if os.path.exists(self.jobBase):
      logger.warn(f'job service module {self.jobId} already exists, aborting job generation ...')
      return
    try:
      jpacket = {'typeKey':'JOB','itemKey':self.jobId}    
      jobMeta = self.runQuery(JobMeta(jpacket),metaItem=packet.eventKey)
      self.loadMeta(jobMeta)
      self.sysCmd(['mkdir', '-p', self.jobBase])
      self.downloadServicePack()
      self.untarArchive()
      self.renderService()
      self.renderInitFw(packet)
    except Exception as ex:
      logger.error(f'{self.jobId} job generation failed, aborting ...')
      self.removeJobModule()
      raise

  # -------------------------------------------------------------- #
  # removeJobModule
  # ---------------------------------------------------------------#
  def removeJobModule(self):
    if os.path.exists(self.jobBase):
      self.sysCmd(['rm','-rf',self.jobBase])

  # -------------------------------------------------------------- #
  # downloadServicePack
  # ---------------------------------------------------------------#
  def downloadServicePack(self):
    raise NotImplementedError('for enterprise project enquires, contact pmg7670@gmail.com')

  # -------------------------------------------------------------- #
  # untarArchive
  # ---------------------------------------------------------------#
  def untarArchive(self):
    logger.info(f'untarArchive ...')
    gitUser, owner, product, releaseTag = self._filter('gitUser','owner','product','releaseTag')
    logger.info(f'github params : {gitUser}, {owner}, {product}, {releaseTag}')
    self.tarFile = f'{releaseTag}.tar.gz'    
    logger.info(f'exploding {self.tarFile} archive  ...')    
    try:
      cmdArgs = ['tar','-zxf',self.tarFile]
      self.sysCmd(cmdArgs,cwd=self.jobBase)
    except Exception as ex:
      errmsg = f'{self.tarFile}, gunzip failed'
      logger.error(errmsg, exc_info=True)
      raise

  # -------------------------------------------------------------- #
  # renderActor
  # ---------------------------------------------------------------#
  def renderActor(self, actorName):
    raise NotImplementedError('for enterprise project enquires, contact pmg7670@gmail.com')
        
  # -------------------------------------------------------------- #
  # getRenderData
  # ---------------------------------------------------------------#
  def getRenderData(self, actorName):
    keys = ['jobId','moduleName','className']
    className = self[actorName].className.split(':')[-1]
    values =  [self.jobId, actorName, className] 
    return JobMeta(dict(zip(keys, values)))

  # -------------------------------------------------------------- #
  # renderService
  # ---------------------------------------------------------------#
  def renderService(self):
    raise NotImplementedError('for enterprise project enquires, contact pmg7670@gmail.com')

  # -------------------------------------------------------------- #
  # actorExists
  # ---------------------------------------------------------------#
  def actorExists(self, actorName):
    if self[actorName].type == 'delegate':
      actorPath = f'{self.jobBase}/{self.productBase}/{actorName}.py'
    else:
      actorName = 'resolvar' + actorName[-1]
      actorPath = f'{self.jobBase}/{self.productBase}/{actorName}.py'
    return os.path.exists(actorPath)

  # -------------------------------------------------------------- #
  # renderInitFw
  # ---------------------------------------------------------------#
  def renderInitFw(self, packet):
    try:
      logger.info(f'renderInitFw ...')
      jpacket = {'typeKey':'JGEN','itemKey':self.jobId}    
      jmeta = self.runQuery(JobMeta(jpacket),metaItem=packet.eventKey)
      template = self.env.get_template('jobInitFw.jinja')
      logger.info(f'rendering __init__.py ...')
      actors = [actor for actor in self['actors'] if self[actor].type == 'director']
      initFile = f'{self.jobBase}/__init__.py'
      template.stream(actors=actors,jmeta=jmeta).dump(initFile)
    except Exception as ex:
      errmsg = 'write failed : __init__.py'
      logger.error(errmsg, exc_info=True)
      raise

  # -------------------------------------------------------------- #
  # runQuery
  # ---------------------------------------------------------------#
  def runQuery(self, packet, metaItem=None):
    if not metaItem:
      logger.info(f'metaKey : {packet.metaKey}')
      actorName, metaKey = packet.metaKey.split(':')
      logger.info(f'actorName, metaKey : {actorName}, {metaKey}')
      metaItem = self[actorName][metaKey]
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
  # storeMeta
  # ---------------------------------------------------------------#
  def storeMeta(self, metaFile, packet):
    try:      
      with open(f'{self.apiBase}/temp/{metaFile}', 'r') as rfh:
        jmeta = json.load(rfh)       

      label = jmeta['job']['label'].upper()
      suffix = '|'.join(packet.eventKey.split('/'))      
      dbKey = f'SAAS|{label}|{suffix}'
      self._leveldb[dbKey] = jmeta['metadoc']
      logger.info(f'SAAS put key : {dbKey}')      
    except Exception as ex:
      logMsg = f'saas meta item not found. dbKey, itemKey : {dbKey}'
      logger.error(logMsg)
      raise Exception(logMsg)
