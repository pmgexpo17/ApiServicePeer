#from github import Github, GithubException
from jinja2 import Environment, PackageLoader, Template
from apibase import Article, FilePacket, Note, Terminal
from apitools import HardhashContext
from . import AbstractMakeKit, AbstractGenerator, RenderError
from copy import copy
import importlib
import logging
import os, sys
import time

logger = logging.getLogger('asyncio.server')

class MakeKit(AbstractMakeKit):

  def __getattr__(self, key):
    if key in self._meta:
      return self._meta[key]

  def __getitem__(self, key):
    if key in self._meta:
      return self._meta[key]
    elif not hasattr(self, key):
      raise AttributeError(f'{key} is not a JobGenerator attribute')
    member = getattr(self, key)
    return member

  def __setitem__(self, key, value):
    self._meta[key] = value

  def __delattr__(self, key):
    if key in self._meta:
      del self._meta[key]
    else:
      delattr(self,key)

  def select(self, *args):
    return [self._meta[key] for key in args if key in self._meta]

  def merge(self, merjar):
    if not isinstance(merjar, dict):
      raise TypeError('dict.merge requires a dict argument')
    self._meta.update(merjar)

  def prepare(self, packet):
    MakeKit.__start__(packet)

# ------------------------------------------------------------ #
# JobGeneratorA1
# -------------------------------------------------------------#
class JobGeneratorA1(MakeKit, AbstractGenerator):

  # ------------------------------------------------------------ #
  # setModuleParms
  # -------------------------------------------------------------#
  def setModuleParms(self):
    gitUser, owner, product, releaseTag = self.releaseInfo
    self['moduleName'] = f'project.{self.projectId}.{self.jobId}'
    self['projectPath'] = f'{self.apiBase}/project/{self.projectId}'
    self['modulePath'] = f'{self.apiBase}/project/{self.projectId}/{self.jobId}'
    self['productPath'] = f'{self.modulePath}/{product}'
    logger.info(f'deploying codes to {self.moduleName} ...')

  # -------------------------------------------------------------- #
  # build
  # ---------------------------------------------------------------#
  def build(self, jobMeta):
    if os.path.exists(self.modulePath):
      if self.make.build == 'build':
        return        
    elif self.make.build == 'nobuild':
      errmsg = f'{self.jobId}, build is disabled but modulePath does not exist'
      raise Exception(f'{errmsg}\nmodule path : {self.modulePath}')
    self.deploy(jobMeta)
    self.render(jobMeta)
    
  # -------------------------------------------------------------- #
  # deploy
  # ---------------------------------------------------------------#
  def deploy(self, jobMeta):
    logger.info(f'deploying framework service pack in project module {self.projectId}')
    CodeDeployment().run(jobMeta)

  # -------------------------------------------------------------- #
  # render
  # ---------------------------------------------------------------#
  def render(self, jobMeta):
    logger.info(f'rendering service pack codes in project module {self.projectId}')
    JobRenderer().run(jobMeta)
  
# ------------------------------------------------------------ #
# JobRenderer
# -------------------------------------------------------------#
class JobRenderer(MakeKit):

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self, jobMeta):
    self['serviceActors'] = copy(jobMeta['serviceActors'])
    self.prepare()
    self.apply(jobMeta)
    self.render(jobMeta)

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self):
    self.renderer = {}
    self.renderer['Service'] = RenderService('service.jinja')  
    self.renderer['Microservice'] = RenderMicroservice('microservice.jinja')
    self.renderer['InitFw'] = RenderInitFw('initFw.jinja')
    moduleName = f'{self.moduleName}.{self.product}'
    logger.info(f'{self.name}, moduleName, resourcePath : {moduleName}, build')
    self['jenviron'] = Environment(loader=PackageLoader(moduleName, 'build'),trim_blocks=True)

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, jobMeta):
    for actorKey in self.serviceActors:
      self.assemble(actorKey, jobMeta[actorKey])
    self.renderer['InitFw'].apply(jobMeta)

  # -------------------------------------------------------------- #
  # assemble
  # ---------------------------------------------------------------#
  def assemble(self, actorKey, metaItem):
    logger.info(f'{actorKey} assembly metaItem : ' + str(metaItem['build']))
    assembly = Article(metaItem['build'])
    renderer = self.getRenderer(assembly.typeKey)
    renderer.apply(actorKey, assembly)
    logger.info(f'renderer {actorKey} is assembled with {renderer.tmpltFile}')
    self.renderer[actorKey] = renderer

  # -------------------------------------------------------------- #
  # getRenderer
  # ---------------------------------------------------------------#
  def getRenderer(self, typeKey):
    try:
      return self.renderer[typeKey]
    except KeyError:
      raise RenderError(f'{actorKey} renderer does not exist for {self.modelName}')

  # -------------------------------------------------------------- #
  # render
  # ---------------------------------------------------------------#
  def render(self, jobMeta):
    try:
      for actorKey in self.serviceActors:
        logger.info(f'rendering service actor {actorKey} ...')
        self.renderer[actorKey].run(actorKey)
      self.renderer['InitFw'].run()
      if self.make.cleanup:
        self.cleanup()
    except RenderError:
      raise 
    except Exception as ex:
      logger.error(f'render service error, {self.jobId}, {self.product}, {actorKey}')
      raise

  # -------------------------------------------------------------- #
  # cleanup
  # ---------------------------------------------------------------#
  def cleanup(self):
    gitUser, owner, product, releaseTag = self.releaseInfo
    logger.info(f'cleanup, removing {self.product} build source folder ...')
    self.sysCmd(['rm','-rf',f'{self.productPath}'])

# -------------------------------------------------------------- #
# RenderActor
# ---------------------------------------------------------------#
class RenderActor(MakeKit, Terminal):

  def __init__(self, tmpltFile):
    self.tmpltFile = tmpltFile
    self.jmeta = {}

  def __getitem__(self, key):
    if key in self.jmeta:
      return self.jmeta[key]

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, actorKey, assembly):
    logger.info(f'render {str(self.__class__)} apply')
    self.jmeta[actorKey] = assembly

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self, actorKey):
    try:
      logger.info(f'rendering {actorKey}.py with {self.tmpltFile} ...')
      self.copyDefaultFiles()
      template = self.getTemplate()
      outfilePath = f'{self.modulePath}/{actorKey}.py'
      rdata = self.getRenderData(actorKey)
      actorName, infilePath = self.getInfilePath(actorKey)
      with open(outfilePath,'w') as wfh:
        template.stream(rdata=rdata).dump(wfh)
        logger.info(f'appending product code {actorName}.py ...')        
        wfh.write('\n\n')
        with open(infilePath, 'r') as rfh:
          try:
            while True:
              wfh.write(next(rfh))
          except StopIteration:
            pass
    except Exception as ex:
      logger.error(f'render failed : {self.modulePath}, {self.product}, {actorKey}')
      raise RenderError

  # -------------------------------------------------------------- #
  # copyDefaultFiles
  # ---------------------------------------------------------------#
  def copyDefaultFiles(self):
    pass

  # -------------------------------------------------------------- #
  # getInfilePath
  # ---------------------------------------------------------------#
  def getInfilePath(self, actorName):
    actorPath = f'{self.productPath}/{actorName}.py'
    logger.info(f'actor path : {actorPath}')
    if not os.path.exists(actorPath):
      errMsg = f'{actorName} script not found in release {self.releaseTag}, aborting ...'
      raise Exception(errMsg)
    return actorName, actorPath

  # -------------------------------------------------------------- #
  # getTemplate
  # ---------------------------------------------------------------#
  def getTemplate(self):
    return self.jenviron.get_template(self.tmpltFile)

  # -------------------------------------------------------------- #
  # getRenderData
  # ---------------------------------------------------------------#
  def getRenderData(self, actorKey):
    pass

# -------------------------------------------------------------- #
# RenderService
# ---------------------------------------------------------------#
class RenderService(RenderActor):

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, actorKey, assembly):
    super().apply(actorKey, assembly)

  # -------------------------------------------------------------- #
  # getInfilePath
  # ---------------------------------------------------------------#
  def getInfilePath(self, actorKey):    
    actorName = 'resolvar' + actorKey[-1]
    logger.info(f'render, actor name : {actorName}')
    return super().getInfilePath(actorName)

  # -------------------------------------------------------------- #
  # getRenderData
  # ---------------------------------------------------------------#
  def getRenderData(self, actorKey):
    keys = ['jobId','projectId','className','moduleName']
    logger.info(f'appActor {actorKey} render data keys : {keys}')
    className = self[actorKey].classToken.split(':')[-1]
    values =  [self.jobId, self.projectId, className, actorKey] 
    return Article(dict(zip(keys, values)))

# -------------------------------------------------------------- #
# RenderMicroservice
# ---------------------------------------------------------------#
class RenderMicroservice(RenderActor):

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, actorKey, assembly):
    super().apply(actorKey, assembly)
    if assembly.libPath:
      self.sysCmd(['mv',f'{self.product}/{assembly.libPath}','.'],cwd=self.modulePath)
      
  # -------------------------------------------------------------- #
  # getRenderData
  # ---------------------------------------------------------------#
  def getRenderData(self, actorKey):
    rdata = {'projectId':self.projectId}
    rdata.update(self[actorKey].dependency)
    return rdata

  # -------------------------------------------------------------- #
  # copyDefaultFiles
  # ---------------------------------------------------------------#
  def copyDefaultFiles(self, actorKey):
    libPath = self[actorKey].libPath
    if libPath:
      self.sysCmd(['mv',f'{self.product}/{libPath}','.'],cwd=self.modulePath)

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self, actorKey):
    try:
      self.copyDefaultFiles(actorKey)
      logger.info(f'copying {actorKey}.py from {self.product} ...')
      self.sysCmd(['cp',f'{self.product}/{actorKey}.py','.'],cwd=self.modulePath)
    except Exception as ex:
      logger.error(f'render failed : {self.modulePath}, {self.product}, {actorKey}')
      raise RenderError

# -------------------------------------------------------------- #
# RenderInitFw
# ---------------------------------------------------------------#
class RenderInitFw(RenderActor):

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, jobMeta):
    for actorKey in self.serviceActors:
      self.assemble(actorKey, jobMeta[actorKey])
      if 'Microservice' in self[actorKey].typeKey:
        continue
      dataKey = f'{actorKey}-promote'
      self.jmeta[dataKey] = jobMeta[dataKey]
      dataKey = f'{actorKey}-iterate'
      self.jmeta[dataKey] = jobMeta[dataKey]

  # -------------------------------------------------------------- #
  # assemble
  # ---------------------------------------------------------------#
  def assemble(self, actorKey, metaItem):
    logger.info(f'{self.name}, initFw {actorKey} assembly : ' + str(metaItem['build']))
    assembly = Article(metaItem['build'])
    self.jmeta[actorKey] = assembly

  # -------------------------------------------------------------- #
  # getRenderData
  # ---------------------------------------------------------------#
  def getRenderData(self, actorKey):
    if 'Microservice' in self[actorKey].typeKey:
      return {}
    keys = [f'{actorKey}-promote',f'{actorKey}-iterate']
    logger.info(f'appActor {actorKey} render data keys : {keys}')
    values =  [self[f'{actorKey}-promote'],self[f'{actorKey}-iterate']] 
    return dict(zip(keys, values))

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self):
    try:
      logger.info(f'rendering __init__.py ...')
      rdata = {}
      for actorKey in self.serviceActors:
        rdata.update(self.getRenderData(actorKey))
      template = self.jenviron.get_template('initFw.jinja')
      outFile = f'{self.modulePath}/__init__.py'
      serviceActors = copy(self.serviceActors)
      template.stream(actors=serviceActors,rdata=rdata).dump(outFile)
    except Exception as ex:
      logger.error('write failed : __init__.py')
      raise

# -------------------------------------------------------------- #
# CodeDeployment
# ---------------------------------------------------------------#
class CodeDeployment(MakeKit):

  # -------------------------------------------------------------- #
  # downloadServicePack
  # ---------------------------------------------------------------#
  def downloadServicePack(self):
    try:
      logger.info(f'downloadServicePack ...')
      gitUser, owner, product, releaseTag = self.releaseInfo
      logger.info(f'release params : {gitUser} {owner} {product} {releaseTag}')
      self.tarFile = f'{releaseTag}.tar.gz'
      archFilePath = f'{self.apiBase}/project/archive/{self.tarFile}'
      tarFilePath = f'{self.modulePath}/{self.tarFile}'
      if os.path.exists(archFilePath):
        logger.info(f'{releaseTag} is already archived, skipping download ...')
        self.sysCmd(['cp',archFilePath,tarFilePath])
        return
      logger.info(f'tar file : {tarFilePath}')      
      headers = {'Authorization': f'token {self.token}'}
      apiUrl = f'https://github.com/{gitUser}/{owner}/archive/{releaseTag}.tar.gz'
      logger.info(f'github url : {apiUrl}')
      # NOTE: stream=True parameter
      dstream = self.request.get(apiUrl,stream=True,headers=headers)
      with open(tarFilePath, 'wb') as fhwb:
        for chunk in dstream.iter_content(chunk_size=1024): 
          if chunk: # filter out keep-alive new chunks
            fhwb.write(chunk)
      logger.info('download complete!')
    except Exception as ex:
      logger.error(f'downloadServicePack failed : {self.tarFile}')
      raise

  # -------------------------------------------------------------- #
  # extractDeploy
  # ---------------------------------------------------------------#
  def extractDeploy(self):
    logger.info(f'extractDeploy ...')
    gitUser, owner, product, releaseTag = self.releaseInfo
    logger.info(f'release params : {gitUser}, {owner}, {product}, {releaseTag}')
    self.tarFile = f'{releaseTag}.tar.gz'    
    logger.info(f'extracting {self.tarFile} service pack tar archive  ...')
    try:
      extractPath = f'{self.apiBase}/project/archive/temp'
      self.sysCmd(['tar','-zxf',self.tarFile],cwd=extractPath)
      archivePath = f'{self.apiBase}/project/archive'
      self.sysCmd(['mv',f'temp/{owner}-{releaseTag}','.'],cwd=archivePath)
      self.sysCmd(['mkdir','-p',self.modulePath])
      productPath = f'{archivePath}/{owner}-{releaseTag}/{product}'
      self.sysCmd(['cp','-rf',productPath,self.modulePath])
    except Exception as ex:
      logger.error(f'{self.tarFile}, gunzip failed')
      raise

  # -------------------------------------------------------------- #
  # deploy
  # ---------------------------------------------------------------#
  def deploy(self):
    if self.method == 'download':
      self.downloadServicePack()
      self.extractDeploy()
    else:
      self.copyDeploy()

  # ------------------------------------------------------------ #
  # loadMeta
  # -------------------------------------------------------------#
  def loadMeta(self, jobMeta):    
    hardhash = HardhashContext.connector(jobMeta.jobId)
    hardhash['apiBase'] = self.apiBase
    subPath = self.assets['subPath']  
    assetPath = f'{self.productPath}/{subPath}'
    logger.info(f'loading {jobMeta.jobId} meta assets in {assetPath}')
    for metaFile in self.assets['metaFiles']:
      metaPath = f'{assetPath}/{metaFile}'
      packet = FilePacket.open(metaPath)
      logger.info(f'{self.name}, loading item : {packet.eventKey}')
      hardhash[packet.eventKey] = packet.metaDoc

  # -------------------------------------------------------------- #
  # copyDeploy
  # ---------------------------------------------------------------#
  def copyDeploy(self):
    logger.info(f'deploying {self.releaseTag} service pack ...')      
    projectPath = f'{self.apiBase}/project/{self.projectId}'
    if not os.path.exists(projectPath):
      self.sysCmd(['mkdir','-p',projectPath])
      self.sysCmd(['touch','__init__.py'],cwd=projectPath)
    if not os.path.exists(self.modulePath):
      self.sysCmd(['mkdir','-p',self.modulePath])
      self.sysCmd(['touch','__init__.py'],cwd=self.modulePath)
    else:
      if self.make.build == "build":
        # build only if base module does NOT exist, but rebuild always builds
        return
      logger.info(f'{self.jobId}, rebuilding module {self.moduleName} ...')
      if self.make.copy == 'replace':
        logger.info(f'replace mode, deleting current {self.releaseTag} module content ... ')
        self.sysCmd(f'rm -rf {self.modulePath}/*',shell=True)
        self.sysCmd(['touch','__init__.py'],cwd=self.modulePath)      

    if self.make.copy != 'nocopy':
      gitUser, owner, product, releaseTag = self.releaseInfo      
      archivePath = f'{self.apiBase}/project/archive'
      sourcePath = f'{archivePath}/{owner}-{releaseTag}/{product}'
      self.sysCmd(['cp','-rf',sourcePath,'.'],cwd=self.modulePath)

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self, jobMeta):
    gitUser, owner, product, releaseTag = self.releaseInfo
    logger.info(f'Code deployment, using release package : {gitUser} {owner} {product} {releaseTag}')
    archivePath = f'{self.apiBase}/project/archive/{owner}-{releaseTag}'
    self.method = 'copy' if os.path.exists(archivePath) else 'extract'
    self.deploy()
    self.loadMeta(Note(jobMeta))