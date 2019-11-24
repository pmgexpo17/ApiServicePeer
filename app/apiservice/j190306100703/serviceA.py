# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AppCooperator, TaskError
from . import promote, iterate
import logging

logger = logging.getLogger('asyncio.smart')

# -------------------------------------------------------------- #
# ServiceA
# ---------------------------------------------------------------#
class ServiceA(AppCooperator):
  def __init__(self, actorId):
    super().__init__(actorId)
    self.state.hasNext = True
    self.resolve = None

  # -------------------------------------------------------------- #
  # __getitem__
  # ---------------------------------------------------------------#
  def __getitem__(self, key):
    if key in self._quicken:
      return self._quicken[key]
    if key in self.__dict__:
      return self.__dict__[key]
    raise TypeError(f'{key} is not a valid {self.name} attribute')

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  @promote('serviceA')
  def start(self, jobId, jobMeta, **kwargs):
    logger.info(f'{self.name} is starting ...')
    self.jobId = jobId
    self.resolve = Resolvar()
    self.resolve.state = self.state    
    self.resolve.start(jobId, jobMeta)
     
  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    state = self.state
    actorId, actorName = self.tell()
    if state.status != 'STARTED':
      logMsg = f'actor error, job was {state.status}, aborting ...'
    else:
      logMsg = f'actor error, job {self.jobId} was {state.status}, aborting ...'

    logMsg = f'{logMsg}\nActor, {actorName}, {actorId},'
    logger.error(logMsg, exc_info=True)

  # -------------------------------------------------------------- #
  # destroy
  # -------------------------------------------------------------- #
  def destroy(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # stop
  # -------------------------------------------------------------- #
  def stop(self, *args, **kwargs):
    pass

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AppResolvar, Note, TaskError
from datetime import datetime
from .component import activate
import os

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, jobId, jobMeta):
    if jobMeta.firstState:
      self.state.current = jobMeta.firstState
    else:
      self.state.current = 'NORMALISE_CSV'
    logger.info(f'{self.name}, first state : {self.state.current}')
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    logger.info(f'{self.name}, activating the microservices component module ...')
    activate()
    logger.info(f'{self.name}, starting job {self.jobId} ...')

  # -------------------------------------------------------------- #
  # NORMALISE_CSV
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def NORMALISE_CSV(self):
    self.evalSysStatus()
    #self.putXformMeta()

  # -------------------------------------------------------------- #
  # COMPILE_JSON
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def COMPILE_JSON(self):
    self.putJsonFileMeta()

  # -------------------------------------------------------------- #
  # FINAL_HANDSHAKE
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def FINAL_HANDSHAKE(self):
    self.compressFile()

  # -------------------------------------------------------------- #
  # REMOVE_WORKSPACE
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def REMOVE_WORKSPACE(self):
    self.removeWorkSpace()

  # -------------------------------------------------------------- #
  # evalSysStatus
  # ---------------------------------------------------------------#
  def evalSysStatus(self):
    jpacket = {'eventKey':f'REPO|{self.jobId}','itemKey':'csvToJson'}
    repo = self.query(Note(jpacket))
    
    apiBase = self._leveldb['apiBase']
    sysPath = f'{apiBase}/{repo.sysPath}'
    if not os.path.exists(sysPath):
      errmsg = f'xform input path does not exist : {sysPath}'
      raise TaskError(errmsg)
    
    catPath = self.jmeta.category
    if catPath not in repo.consumerCategories:
      errmsg = 'consumer category branch %s does not exist under %s' \
                                % (catPath, str(repo.consumerCategories))
      raise TaskError(errmsg)
  
    repoPath = f'{sysPath}/{catPath}'
    logger.info('input zipfile repo path : ' + repoPath)

    inputZipFile = f'{self.jobId}.{self.jmeta.fileExt}'
    logger.info('input zipfile : ' + inputZipFile)

    zipFilePath = f'{repoPath}/{inputZipFile}'
    if not os.path.exists(zipFilePath):
      errmsg = 'xform input zipfile does not exist in source repo'
      raise TaskError(errmsg)

    workbase = f'{apiBase}/{self.jmeta.workspace}'
    if not os.path.exists(workbase):
      errmsg = f'xform workspace path does not exist : {workbase}'
      raise TaskError(errmsg)

    tsXref = datetime.now().strftime('%y%m%d%H%M%S')

    workspace = f'{workbase}/{tsXref}'
    logger.info('session workspace : ' + workspace)
    logger.info('creating session workspace ... ')

    try:
      cmdArgs = ['mkdir','-p',workspace]      
      self.sysCmd(cmdArgs)
    except TaskError as ex:
      logger.error(f'{self.jobId}, workspace creation failed')
      raise

    try:
      self.sysCmd(['cp',zipFilePath,workspace])
    except TaskError as ex:
      logger.error(f'zipfile copy to workspace failed : {zipFilePath}')
      raise

    try:
      cmdArgs = ['tar','-xzf',inputZipFile]
      self.sysCmd(cmdArgs,cwd=workspace)
    except TaskError as ex:
      logger.error(f'{inputZipFile}, gunzip tar extract command failed')
      raise

    # put workspace path in storage for micro-service access
    dbKey = f'{self.jobId}|workspace'
    self._leveldb[dbKey] = workspace
    self.workspace = workspace

  # -------------------------------------------------------------- #
  # putJsonFileMeta
  # ---------------------------------------------------------------#
  def putJsonFileMeta(self):
    jsonFile = self.jobId + '.json'
    dbKey = f'{self.jobId}|output|jsonFile'
    self._leveldb[dbKey] = jsonFile

  # -------------------------------------------------------------- #
  # compressFile
  # ---------------------------------------------------------------#
  def compressFile(self):
    logger.info(f'{self.name}, gziping {self.jobId}.json ...')

    dbKey = f'{self.jobId}|workspace'
    workspace = self._leveldb[dbKey]

    jsonFile = self.jobId + '.json'
    self.sysCmd(['gzip',jsonFile],cwd=workspace)
    dbKey = f'{self.jobId}|datastream|infile'
    self._leveldb[dbKey] = jsonFile + '.gz'
    dbKey = f'{self.jobId}|datastream|workspace'
    self._leveldb[dbKey] = workspace

  # -------------------------------------------------------------- #
  # removeWorkSpace
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    logger.info(f'ATTN. NOT removing {self.jobId} workspace ...')
    try:
      return
      dbKey = f'{self.jobId}|workspace'
      workspace = self._leveldb[dbKey]
      self.sysCmd(['rm','-rf',workspace])
      logger.info(f'ATTN. {self.jobId} workspace is now removed : {workspace}')
    except TaskError as ex:
      logger.error(f'workspace {self.jobId} removal failed')
      raise
