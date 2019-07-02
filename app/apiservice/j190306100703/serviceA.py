# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AppDirector, ApiRequest, AppResolvar, JobMeta
from apiservice.j190306100703 import promote, iterate
import logging

logger = logging.getLogger('apipeer.smart')

# -------------------------------------------------------------- #
# ServiceA
# ---------------------------------------------------------------#
class ServiceA(AppDirector):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = Resolvar(leveldb)
    self.resolve.state = self.state
    self.request = ApiRequest()

  # -------------------------------------------------------------- #
  # __getitem__
  # ---------------------------------------------------------------#
  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    elif key in self._quicken:
      return self._quicken[key]
    raise TypeError(f'{self.name}, {key} does not exist in obj properties')

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  @promote('serviceA')
  def start(self, jobId, jobMeta, **kwargs):
    logger.info(f'{self.name}.start ...')
    self.jobId = jobId
    self.hostName = jobMeta['hostName']
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

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apitools.csvxform import XformMetaPrvdr
import datetime
import os, sys
import simplejson as json

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.request = ApiRequest()

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, jobId, jobMeta):
    self.state.current = 'EVAL_XFORM_META'
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    logger.info(f'{self.name}, starting job {self.jobId} ...')

  # -------------------------------------------------------------- #
  # NORMALISE_CSV
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def EVAL_XFORM_META(self):
    self.evalXformMeta()

  # -------------------------------------------------------------- #
  # NORMALISE_CSV
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def NORMALISE_CSV(self):
    self.evalSysStatus()
    self.putXformMeta()

  # -------------------------------------------------------------- #
  # COMPILE_JSON
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def COMPILE_JSON(self):
    pass

  # -------------------------------------------------------------- #
  # COMPOSE_JSFILE
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def COMPOSE_JSFILE(self):
    self.putJsonFileMeta()

  # -------------------------------------------------------------- #
  # FINAL_HANDSHAKE
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def FINAL_HANDSHAKE(self):
    pass

  # -------------------------------------------------------------- #
  # REMOVE_WORKSPACE
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def REMOVE_WORKSPACE(self):
    self.removeWorkSpace()

  # -------------------------------------------------------------- #
  # evalXformMeta -
  # ---------------------------------------------------------------#
  def evalXformMeta(self):
    jpacket = {'metaKey':'xformMeta','typeKey':'XFORM','itemKey':'csvToJson'}
    repoMeta = self.runQuery(JobMeta(jpacket))

    metaFile = repoMeta['repoName'] + '/' + repoMeta['xformMeta']
    logger.info('xform meta file : ' + metaFile)
    if not os.path.exists(metaFile):
      errmsg = 'xform meta file does not exist : %s' % metaFile
      raise Exception(errmsg)

    try:
      xformMeta = XformMetaPrvdr()
      xformMeta.load(metaFile)
      xformMeta.validate('csvToJson')
    except Exception as ex:
      errmsg = '%s, xformMeta validation failed\nError : %s' % (repoMeta['xformMeta'], str(ex))
      raise Exception(errmsg)
    self.rootName = xformMeta.getRootName()
    dbKey = '%s|XFORM|rootname' % self.jobId
    self._leveldb[dbKey] = self.rootName
    self.xformMeta = xformMeta

  # -------------------------------------------------------------- #
  # evalSysStatus
  # ---------------------------------------------------------------#
  def evalSysStatus(self):
    jpacket = {'metaKey':'repoMeta','typeKey':'REPO'}
    repoMeta = self.runQuery(JobMeta(jpacket))
    
    if not os.path.exists(repoMeta['sysPath']):
      errmsg = 'xform input path does not exist : ' + repoMeta['sysPath']
      raise Exception(errmsg)
    
    catPath = self.jmeta.category
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'consumer category branch %s does not exist under %s' \
                                % (catPath, str(repoMeta['consumer categories']))
      raise Exception(errmsg)
  
    repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('input zipfile repo path : ' + repoPath)

    #inputZipFile = self.jobId + '.tar.gz'
    inputZipFile = '%s.%s' % (self.jobId, self.jmeta.fileExt)
    logger.info('input zipfile : ' + inputZipFile)

    zipFilePath = '%s/%s' % (repoPath, inputZipFile)
    if not os.path.exists(zipFilePath):
      errmsg = 'xform input zipfile does not exist in source repo'
      raise Exception(errmsg)

    if not os.path.exists(self.jmeta.workSpace):
      errmsg = 'xform workspace path does not exist : ' + self.jmeta.workSpace
      raise Exception(errmsg)

    tsXref = datetime.datetime.now().strftime('%y%m%d%H%M%S')

    workSpace = '%s/%s' % (self.jmeta.workSpace, tsXref)
    logger.info('session workspace : ' + workSpace)
    logger.info('creating session workspace ... ')

    try:
      cmdArgs = ['mkdir','-p',workSpace]      
      self.sysCmd(cmdArgs)
    except Exception as ex:
      logger.error('%s, workspace creation failed' % self.jobId)
      raise

    try:
      cmdArgs = ['cp',zipFilePath,workSpace]
      self.sysCmd(cmdArgs)
    except Exception as ex:
      logger.error('%s, copy to workspace failed' % inputZipFile)
      raise

    try:
      cmdArgs = ['tar','-xzf',inputZipFile]
      self.sysCmd(cmdArgs,cwd=workSpace)
    except Exception as ex:
      logger.error('%s, gunzip tar extract command failed' % inputZipFile)
      raise

    # put workspace path in storage for subprocess access
    dbKey = '%s|workspace' % self.jobId
    self._leveldb[dbKey] = workSpace
    self.workSpace = workSpace

  # -------------------------------------------------------------- #
  # putXformMeta
  # ---------------------------------------------------------------#
  def putXformMeta(self):
    # put the json schema metainfo to storage for retrieval by workers
    for metaIndex, nodeName in enumerate(self.xformMeta.nodeNames):
      metaIndex += 1
      csvMeta = self.xformMeta.get(nodeName)
      dbKey = '%s|XFORM|META|%d' % (self.jobId, metaIndex)
      self._leveldb[dbKey] = csvMeta
      dbKey = '%s|XFORM|META|%s' % (self.jobId, nodeName)
      self._leveldb[dbKey] = csvMeta
      logger.info('csvToJson %s meta index : %d' % (nodeName, metaIndex))
      logger.info('%s meta item : %s ' % (nodeName, str(csvMeta)))
    self.jobRange = metaIndex

  # -------------------------------------------------------------- #
  # putJsonFileMeta
  # ---------------------------------------------------------------#
  def putJsonFileMeta(self):
    jsonFile = self.jobId + '.json'
    dbKey = '%s|OUTPUT|jsonFile' % self.jobId
    self._leveldb[dbKey] = jsonFile

  # -------------------------------------------------------------- #
  # makeZipFile
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    logger.info('removing %s workspace ...' % self.jobId)
    dbKey = '%s|workspace' % self.jobId
    self.workSpace = self._leveldb[dbKey]
    cmdArgs = ['rm','-rf',self.workSpace]
    try:
      self.sysCmd(cmdArgs)
      logger.info('%s workspace is removed' % self.jobId)      
    except Exception as ex:
      logger.error('%s, workspace removal failed' % self.jobId)
      raise
