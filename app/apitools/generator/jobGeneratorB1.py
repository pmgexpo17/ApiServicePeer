#from github import Github, GithubException
from jinja2 import Environment, PackageLoader, Template
from apitools.datastore import HardhashContext
from . import AbstractMakeKit, AbstractGenerator, RenderError
import copy
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
# JobGeneratorB1
# -------------------------------------------------------------#
class JobGeneratorB1(MakeKit, AbstractGenerator):

  # ------------------------------------------------------------ #
  # setModuleParms
  # -------------------------------------------------------------#
  def setModuleParms(self):
    gitUser, owner, product, releaseTag = self.releaseInfo
    self['moduleName'] = f'project.{self.projectId}.{self.jobId}'
    self['projectPath'] = f'{self.apiBase}/project/{self.projectId}'
    self['modulePath'] = f'{self.projectPath}/{self.jobId}'
    self['productPath'] = f'{self.modulePath}/{product}'    

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
    logger.info(f'{self.name}, build is complete')
  # -------------------------------------------------------------- #
  # deploy
  # ---------------------------------------------------------------#
  def deploy(self, jobMeta):
    logger.info(f'deploying framework service pack in project module {self.projectId}')
    CodeDeployment().run()

  # ------------------------------------------------------------ #
  # loadResources
  # -------------------------------------------------------------#
  def loadResources(self, jobMeta):
    pass
  
  # -------------------------------------------------------------- #
  # render
  # ---------------------------------------------------------------#
  def render(self, jobMeta):
    logger.info(f'this service pack does not require rendering')

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

  # -------------------------------------------------------------- #
  # copyDeploy
  # ---------------------------------------------------------------#
  def copyDeploy(self):
    logger.info(f'deploying {self.releaseTag} service pack ...')      
    projectPath = f'{self.apiBase}/project/{self.projectId}'
    logger.info(f'{self.name}, project path : {projectPath}')
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
      if self.make.copy == 'replace':
        logger.info(f'replace mode, deleting current {self.releaseTag} module content ... ')
        self.sysCmd(f'rm -rf {self.modulePath}/*',shell=True)
        self.sysCmd(['touch','__init__.py'],cwd=self.modulePath)

    if self.make.copy != 'nocopy':
      gitUser, owner, product, releaseTag = self.releaseInfo
      archivePath = f'{self.apiBase}/project/archive'
      sourcePath = f'{archivePath}/{owner}-{releaseTag}/{product}/*'
      self.sysCmd([f'cp -rf {sourcePath} .'],cwd=self.modulePath,shell=True)
      #self.sysCmd(['touch','__init__.py'],cwd=f'{self.modulePath}/test')

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self):
    gitUser, owner, product, releaseTag = self.releaseInfo
    logger.info(f'Code deployment, using release package : {gitUser} {owner} {product} {releaseTag}')
    archivePath = f'{self.apiBase}/project/archive/{owner}-{releaseTag}'
    self.method = 'copy' if os.path.exists(archivePath) else 'extract'
    self.deploy()
