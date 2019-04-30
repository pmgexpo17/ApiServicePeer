# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
from apibase import AppDirector, ApiRequest, AppResolvar
from apitools.jsonxform import XformMetaPrvdr
import datetime
import logging
import math
import os, sys, time
import simplejson as json
import uuid

logger = logging.getLogger('apipeer.smart')

# -------------------------------------------------------------- #
# JsonToCsvSaas
# ---------------------------------------------------------------#
class JsonToCsvSaas(AppDirector):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = Resolvar(leveldb)
    self.resolve.state = self.state
    self.request = ApiRequest()

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, jobId, jobMeta, **kwargs):
    logger.info('%s._start' % self.__class__.__name__)
    self.jobId = jobId
    self.hostName = jobMeta.hostName
    self.resolve._start(jobId, jobMeta)

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    state = self.state
    # signal = the http status code of the companion actor method
    if signal:
      state.hasSignal = False
      if signal != 201:
        logMsg = f'state transition {state.current} failed, got error signal : {signal}'
        raise Exception(logMsg)
      logger.info(f'{state.current} is resolved, advancing ...')
      state.inTransition = False
      state.hasNext = True
    if state.hasNext:
      state.current = state.next
    return state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):
    if self.state.hasSignal:
      if self.state.current in ('NORMALISE_JSON','COMPOSE_CSV_FILES','FINAL_HANDSHAKE'):
        self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    state = self.state
    logger.info('!!! putApiRequest, %s' % state.current)
    if self.state.current == 'FINAL_HANDSHAKE':
      packet = {'jobId':self.jobId,'caller':'service','actor':'client'}
      packet['args'] = [self.jobId]
      apiUrl = 'http://%s/api/v1/smart' % self.hostName
    elif state.current == 'NORMALISE_JSON':
      packet = {'jobId':self.jobId,'caller':'service','actor':'normalise'}
      packet['args'] = [self.jobId]
      packet['kwargs'] = {'hhGroupSize':self.resolve.csvRange}
      apiUrl = 'http://%s/api/v1/multi/%d' \
                        % (self.hostName, self.resolve.jobRange)
    elif self.state.current == 'COMPOSE_CSV_FILES':
      packet = {'jobId':self.jobId,'caller':'service','actor':'compose'}
      packet['args'] = [self.jobId, self.resolve.jobRange]
      apiUrl = 'http://%s/api/v1/multi/%d' \
                        % (self.hostName, self.resolve.csvRange)
    else:
      logger.warn(f'{state.current} transition is not implemented')
      return
    response = self.request.post(apiUrl,json={'job':packet})
    logger.info('%s, api response : %s' % (self.name, response.text))

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    actorId, actorName = self.tell()
    state = self.state
    if state.status != 'STARTED':
      logMsg = f'actor error, job was {state.status}, aborting ...'
    else:
      logMsg = f'actor error, job {self.jobId} was {state.status}, aborting ...'

    logMsg = f'{logMsg}\nActor, {actorName}, {actorId},'
    logger.error(logMsg, exc_info=True)

# -------------------------------------------------------------- #
# getLineCount
# ---------------------------------------------------------------#
def getLineCount(fname):
  with open(fname) as f:
    for i, l in enumerate(f):
      pass
  return i

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
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.request = ApiRequest()
    self.__dict__['EVAL_XFORM_META'] = self.EVAL_XFORM_META
    self.__dict__['NORMALISE_JSON'] = self.NORMALISE_JSON
    self.__dict__['COMPOSE_CSV_FILES'] = self.COMPOSE_CSV_FILES
    self.__dict__['MAKE_ZIP_FILE'] = self.MAKE_ZIP_FILE
    self.__dict__['FINAL_HANDSHAKE'] = self.FINAL_HANDSHAKE
    self.__dict__['REMOVE_WORKSPACE'] = self.REMOVE_WORKSPACE

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, jobId, jobMeta):
    self.state.current = 'EVAL_XFORM_META'
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    msg = '%s, starting job %s ...'
    logger.info(msg % (self.name, self.jobId))

  # -------------------------------------------------------------- #
  # getSaasMeta
  # - generic method to lookup and return xform meta
  # ---------------------------------------------------------------#
  def getSaasMeta(self, metaKey, typeKey, itemKey=None):
    packet = {'jobId': self.jobId,'metaKey': metaKey,'typeKey': typeKey,'itemKey': itemKey}
    apiUrl = 'http://%s/api/v1/saas/meta' % self.jmeta.hostName
    logger.info(f'### getSaasMeta : {apiUrl}')
    response = self.request.get(apiUrl,json={'job':packet})
    result = json.loads(response.text)
    if 'error' in result:
      raise Exception(result['error'])
    return result

	# -------------------------------------------------------------- #
	# EVAL_CVS_META
	# ---------------------------------------------------------------#
  def EVAL_XFORM_META(self):
    self.evalXformMeta()
    state = self.state
    state.next = 'NORMALISE_JSON'
    state.hasNext = True
    return state

  # -------------------------------------------------------------- #
  # NORMALISE_JSON
  # ---------------------------------------------------------------#
  def NORMALISE_JSON(self):
    self.evalSysStatus()
    self.putXformMeta()
    state = self.state
    state.inTransition = True
    state.hasSignal = True
    state.next = 'COMPOSE_CSV_FILES'
    state.hasNext = False
    return state

  # -------------------------------------------------------------- #
  # COMPOSE_CSV_FILES
  # ---------------------------------------------------------------#
  def COMPOSE_CSV_FILES(self):
    state = self.state
    state.inTransition = True
    state.hasSignal = True
    state.next = 'MAKE_ZIP_FILE'
    state.hasNext = False
    return state

  # -------------------------------------------------------------- #
  # MAKE_ZIP_FILE
  # ---------------------------------------------------------------#
  def MAKE_ZIP_FILE(self):
    self.makeGZipFile()
    state = self.state
    state.inTransition = False
    state.hasSignal = False
    state.next = 'FINAL_HANDSHAKE'
    state.hasNext = True
    return state

  # -------------------------------------------------------------- #
  # FINAL_HANDSHAKE
  # ---------------------------------------------------------------#
  def FINAL_HANDSHAKE(self):
    state = self.state
    state.inTransition = True
    state.hasSignal = True
    state.next = 'REMOVE_WORKSPACE'
    state.hasNext = False
    return state

  # -------------------------------------------------------------- #
  # REMOVE_WORKSPACE
  # ---------------------------------------------------------------#
  def REMOVE_WORKSPACE(self):
    self.removeWorkSpace()
    state = self.state
    state.hasNext = False
    state.complete = True
    return state

  # -------------------------------------------------------------- #
  # evalXformMeta -
  # ---------------------------------------------------------------#
  def evalXformMeta(self):
    repoMeta = self.getSaasMeta('service:xformMeta','XFORM',itemKey='jsonToCsv')
    metaFile = repoMeta['repoName'] + '/' + repoMeta['xformMeta']
    logger.info('xform meta file : ' + metaFile)
    if not os.path.exists(metaFile):
      errmsg = 'xform meta file does not exist : %s' % metaFile
      raise Exception(errmsg)

    try:
      xformMeta = XformMetaPrvdr()
      xformMeta.load(metaFile)
      xformMeta.validate('jsonToCsv')
    except Exception as ex:
      errmsg = '%s, xformMeta validation failed\nError : %s' % (repoMeta['xformMeta'], str(ex))
      raise Exception(errmsg)
    self.rootName = xformMeta.getRootName()
    dbKey = '%s|XFORM|rootname' % self.jobId
    self._leveldb[dbKey] = self.rootName
    self.xformMeta = xformMeta
    self.csvRange = len(xformMeta.tableNames)

  # -------------------------------------------------------------- #
  # evalSysStatus
  # ---------------------------------------------------------------#
  def evalSysStatus(self):
    repoMeta = self.getSaasMeta('service:repoMeta','REPO')
    if not os.path.exists(repoMeta['sysPath']):
      errmsg = 'xform input path does not exist : ' + repoMeta['sysPath']
      raise Exception(errmsg)

    catPath = self.jmeta.category
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'consumer category branch %s does not exist under : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      raise Exception(errmsg)
  
    repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('input json file repo path : ' + repoPath)

    inputJsonFile = '%s.%s' % (self.jobId, self.jmeta.fileExt)
    logger.info('input json file : ' + inputJsonFile)

    jsonFilePath = '%s/%s' % (repoPath, inputJsonFile)
    if not os.path.exists(jsonFilePath):
      errmsg = 'xform input zip file does not exist in source repo'
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
      cmdArgs = ['cp',jsonFilePath,workSpace]
      self.sysCmd(cmdArgs)
    except Exception as ex:
      logger.error('%s, copy to workspace failed' % inputJsonFile)
      raise
    
    self.jsonFileName = self.jobId

    jsonFilePath = workSpace + '/' + inputJsonFile
    lineCount = getLineCount(jsonFilePath)
    self.jobRange = 2
    splitSize = int(math.ceil(lineCount / self.jobRange))
    # round up to the nearest 50
    #splitSize = int(math.ceil(splitSize / 50.0)) * 50
    try:
      cmdArgs = ['split','-l',str(splitSize),inputJsonFile,self.jsonFileName]
      self.sysCmd(cmdArgs,cwd=workSpace)
    except Exception as ex:
      logger.error('%s, split command failed' % inputJsonFile)
      raise

    for i in range(1, self.jobRange+1):
      self.putSplitFilename(i)

    dbKey = '%s|workspace' % self.jobId
    self._leveldb[dbKey] = workSpace
    self.workSpace = workSpace

  # -------------------------------------------------------------- #
  # putSplitFilename
  # ---------------------------------------------------------------#
  def putSplitFilename(self, taskNum):
    fileTag = getSplitFileTag(taskNum)
    splitFileName = self.jobId + fileTag
    dbKey = '%s|INPUT|%d|jsonFile' % (self.jobId, taskNum)
    self._leveldb[dbKey] = splitFileName

  # -------------------------------------------------------------- #
  # putXformMeta
  # ---------------------------------------------------------------#
  def putXformMeta(self):
    # tableName has a one to many relation to nodeName
    for tableIndex, tableName in enumerate(self.xformMeta.tableNames):
      tableIndex += 1
      csvPath = '%s/%s.csv' % (self.workSpace, tableName)
      dbKey = '%s|XFORM|TABLENAME|%d' % (self.jobId, tableIndex)
      self._leveldb[dbKey] = tableName
      dbKey = '%s|XFORM|CSVPATH|%d' % (self.jobId, tableIndex)
      self._leveldb[dbKey] = csvPath
      nodeList = self.xformMeta.get(tableName=tableName)
      dbKey = '%s|XFORM|NODEMAP|%d' % (self.jobId, tableIndex)
      self._leveldb[dbKey] = nodeList
    # put the json schema metainfo to storage for retrieval by workers
    for metaIndex, nodeName in enumerate(self.xformMeta.nodeNames):
      metaIndex += 1
      csvMeta = self.xformMeta.get(nodeName)
      dbKey = '%s|XFORM|META|%d' % (self.jobId, metaIndex)
      self._leveldb[dbKey] = csvMeta
      dbKey = '%s|XFORM|META|%s' % (self.jobId, nodeName)
      self._leveldb[dbKey] = csvMeta
      logger.info('jsonToCsv %s meta index : %d' % (nodeName, metaIndex))
      logger.info('%s meta item : %s ' % (nodeName, str(csvMeta)))
    return metaIndex

  # -------------------------------------------------------------- #
  # makeGZipFile
  # ---------------------------------------------------------------#
  def makeGZipFile(self):
    gzipFile = '%s.tar.gz' % self.jobId
    logger.info('making tar gzipfile %s ...' % gzipFile)

    cmd = 'tar -czf %s *.csv' % gzipFile
    try:
      self.sysCmd(cmd,cwd=self.workSpace,shell=True)
    except Exception as ex:
      errmsg = '%s, tar gzip failed' % gzipFile
      logger.error(errmsg)
      raise

  # -------------------------------------------------------------- #
  # removeWorkSpace
  # ---------------------------------------------------------------#
  def removeWorkSpace(self):
    logger.info('removing %s workspace ...' % self.jobId)
    cmdArgs = ['rm','-rf',self.workSpace]
    try:
      self.sysCmd(cmdArgs)
      logger.info('%s workspace is removed' % self.jobId)      
    except Exception as ex:
      logger.error('%s, workspace removal failed' % self.jobId)
      raise
