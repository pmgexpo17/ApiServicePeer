# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AppResolvar, Note
from datetime import datetime
from .component import activate
import os
import math

# -------------------------------------------------------------- #
# getLineCount
# ---------------------------------------------------------------#
def getLineCount(fname):
  with open(fname) as f:
    for i, l in enumerate(f):
      pass
  return i + 1

# -------------------------------------------------------------- #
# getSplitFileTag
# ---------------------------------------------------------------#
def getSplitFileTag(taskNum):
  tagOrd = int((taskNum-1) / 26)
  tagChr1 = chr(ord('a') + tagOrd)
  tagOrd = int((taskNum-1) % 26)
  tagChr2 = chr(ord('a') + tagOrd)
  return tagChr1 + tagChr2

# -------------------------------------------------------------- #
# Resolvar
# - Framework added properties : _leveldb
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
      2. request : Requests session connection for making api calls
  '''
  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, jobId, jobMeta):
    if jobMeta.firstState:
      self.state.current = jobMeta.firstState
    else:
      self.state.current = 'NORMALISE_XML'
    logger.info(f'{self.name}, first state : {self.state.current}')
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    logger.info(f'{self.name}, activating the microservices component module ...')
    activate()
    logger.info(f'{self.name}, starting job {self.jobId} ...')

  # -------------------------------------------------------------- #
  # NORMALISE_XML
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def NORMALISE_XML(self):
    self.evalSysStatus()

  # -------------------------------------------------------------- #
  # COMPOSE_CSV_FILES
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def COMPOSE_CSV_FILES(self):
    pass

  # -------------------------------------------------------------- #
  # MAKE_ZIP_FILE
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def MAKE_ZIPFILE(self):
    self.makeGZipFile()

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
  # evalSysStatus
  # ---------------------------------------------------------------#
  def evalSysStatus(self):
    jpacket = {'eventKey':f'REPO|{self.jobId}','itemKey':'xmlToCsv'}
    repo = self.query(Note(jpacket))
    
    apiBase = self._leveldb['apiBase']
    sysPath = f'{apiBase}/{repo.sysPath}'
    if not os.path.exists(sysPath):
      errmsg = f'xform input path does not exist : {sysPath}'
      raise TaskError(errmsg)
    
    catPath = self.jmeta.category
    if catPath not in repo.consumerCategories:
      errmsg = 'consumer category branch %s does not exist in %s' \
                                % (catPath, str(repo.consumerCategories))
      raise TaskError(errmsg)
  
    repoPath = f'{sysPath}/{catPath}'
    logger.info('xml input file repo path : ' + repoPath)

    inputXmlFile = f'{self.jobId}.{self.jmeta.fileExt}'
    logger.info('xml input file : ' + inputXmlFile)

    xmlFilePath = f'{repoPath}/{inputXmlFile}'
    if not os.path.exists(xmlFilePath):
      errmsg = 'xform xml input file does not exist in source repo'
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
      self.sysCmd(['mkdir','-p',workspace])
    except TaskError as ex:
      logger.error(f'{self.jobId}, workspace creation failed')
      raise

    try:
      cmdArgs = ['cp',xmlFilePath,workspace]
      self.sysCmd(cmdArgs)
    except TaskError as ex:
      logger.error(f'copy to workspace failed : {inputXmlFile}')
      raise
    
    xmlFilePath = f'{workspace}/{inputXmlFile}'
    lineCount = getLineCount(xmlFilePath)    
    if lineCount <= 2000:
      logMsg = f'file split not required, line count : {lineCount} < 2000'
      logger.info(f'{self.jobId}, {logMsg}')
      self.jobRange = 1
      dbKey = f'{self.jobId}|XFORM|input|1|xmlFile'
      self._leveldb[dbKey] = inputXmlFile
    else:
      self.jobRange = 2
      splitSize = int(math.ceil(lineCount / self.jobRange))
      # round up to the nearest 50
      #splitSize = int(math.ceil(splitSize / 50.0)) * 50
      logger.info(f'{self.jobId}, line count, split size : {lineCount}, {splitSize}')
      try:
        splitFileName = self.jobId
        cmdArgs = ['split','-l',str(splitSize),inputXmlFile,splitFileName]
        self.sysCmd(cmdArgs,cwd=workspace)
      except TaskError as ex:
        logger.error(f'{inputXmlFile}, split command failed')
        raise

      for i in range(1, self.jobRange+1):
        self.putSplitFilename(i)

    # put workspace path in storage for micro-service access
    dbKey = f'{self.jobId}|workspace'
    self._leveldb[dbKey] = workspace
    self.workspace = workspace

  # -------------------------------------------------------------- #
  # putSplitFilename
  # ---------------------------------------------------------------#
  def putSplitFilename(self, taskNum):
    fileTag = getSplitFileTag(taskNum)
    splitFileName = self.jobId + fileTag
    dbKey = f'{self.jobId}|XFORM|input|{taskNum}|xmlFile'
    self._leveldb[dbKey] = splitFileName

  # -------------------------------------------------------------- #
  # makeGZipFile
  # ---------------------------------------------------------------#
  def makeGZipFile(self):
    dbKey = f'{self.jobId}|workspace'
    workspace = self._leveldb[dbKey]
    gzipFile = f'{self.jobId}.tar.gz'
    logger.info(f'making tar gzipfile {gzipFile} ...')

    cmd = f'tar -czf {gzipFile} *.csv'
    try:
      self.sysCmd(cmd,cwd=workspace,shell=True)
      dbKey = f'{self.jobId}|datastream|infile'
      self._leveldb[dbKey] = gzipFile
    except TaskError as ex:
      errmsg = f'{gzipFile}, tar gzip failed'
      logger.error(errmsg)
      raise
    dbKey = f'{self.jobId}|datastream|infile'
    self._leveldb[dbKey] = gzipFile
    dbKey = f'{self.jobId}|datastream|workspace'
    self._leveldb[dbKey] = workspace

  # -------------------------------------------------------------- #
  # removeWorkSpace
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    logger.info(f'removing {self.jobId} workspace ...')
    try:
      dbKey = f'{self.jobId}|workspace'
      workspace = self._leveldb[dbKey]
      self.sysCmd(['rm','-rf',workspace])
      logger.info(f'ATTN. {self.jobId} workspace is now removed : {workspace}')
    except TaskError as ex:
      logger.error(f'{self.jobId}, workspace removal failed')
      raise
