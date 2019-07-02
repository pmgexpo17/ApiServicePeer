# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AppDirector, ApiRequest, AppResolvar, JobMeta
from apiservice.j190306100703 import promote, iterate
import logging

logger = logging.getLogger('apipeer.smart')

# -------------------------------------------------------------- #
# ClientB
# ---------------------------------------------------------------#
class ClientB(AppDirector):
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
  @promote('clientB')
  def start(self, jobId, jobMeta, **kwargs):
    logger.info(f'{self.name}.start ...')
    self.jobId = jobId
    self.hostName = jobMeta['hostName']
    self.resolve.start(jobId, jobMeta)
     
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

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
import simplejson as json
import os, sys

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
    self.state.current = 'DOWNLOAD_ZIPFILE'
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    logger.info(f'{self.name}, starting job {self.jobId} ...')

  # -------------------------------------------------------------- #
  # DOWNLOAD_ZIPFILE
  # -------------------------------------------------------------- #  
  @iterate('clientB')
  def DOWNLOAD_ZIPFILE(self):
    self.downloadJsonFile()
    self.uncompressFile()

  # -------------------------------------------------------------- #
  # downloadJsonFile
  # ---------------------------------------------------------------#
  def downloadJsonFile(self):
    jpacket = {'metaKey':'repoMeta','typeKey':'REPO'}
    repoMeta = self.runQuery(JobMeta(jpacket))

    if not os.path.exists(repoMeta['sysPath']):
      errmsg = 'xform output path does not exist : ' + repoMeta['sysPath']
      raise Exception(errmsg)

    catPath = self.jmeta['category']
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'consumer category branch %s does not exist under : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      raise Exception(errmsg)
  
    self.repoPath = '%s/%s' % (repoMeta['sysPath'], catPath)
    logger.info('output json gzipfile repo path : ' + self.repoPath)

    self.jsonZipFile = '%s.%s' % (self.jobId, self.jmeta['fileExt'])
    logger.info('output json gzipfile : ' + self.jsonZipFile)

    zipFilePath = '%s/%s' % (self.repoPath, self.jsonZipFile)
    dstream = self.getFileStream()

    try:
      with open(zipFilePath, 'wb') as fhwb:
        for chunk in dstream.iter_content(chunk_size=1024): 
          if chunk: # filter out keep-alive new chunks
            fhwb.write(chunk)
    except Exception as ex:
      errmsg = '%s write failed : ' + self.jsonZipFile
      logger.error(errmsg)
      raise

  # -------------------------------------------------------------- #
  # getFileStream
  # ---------------------------------------------------------------#
  def getFileStream(self):
    try:
      dstream = self._getFileStream()
    except Exception as ex:
      errmsg = 'json gzip file stream api request failed'
      logger.error(errmsg)
      raise
    if dstream.status_code != 201:
      errmsg = 'json gzip file stream api request failed\nError : %s' % dstream.text
      raise Exception(errmsg)
    return dstream

  # -------------------------------------------------------------- #
  # _getFileStream
  # ---------------------------------------------------------------#
  def _getFileStream(self):
    packet = {'jobId':self.jobId,'service':'datatxn.dataTxnPrvdr:BinryFileStreamPrvdr'}
    packet['responseType'] = 'stream'
    packet['args'] = [self.jobId, self.jsonZipFile]
    apiUrl = 'http://%s/api/v1/sync' % self.hostName
    # NOTE: stream=True parameter
    return self.request.post(apiUrl,stream=True,json={'job':packet})

  # -------------------------------------------------------------- #
  # uncompressFile
  # ---------------------------------------------------------------#
  def uncompressFile(self):
    logger.info('%s, gunzip ...' % self.jsonZipFile)
    try:
      cmdArgs = ['gunzip','-f',self.jsonZipFile]
      self.sysCmd(cmdArgs,cwd=self.repoPath)
    except Exception as ex:
      errmsg = '%s, gunzip failed' % self.jsonZipFile
      logger.error(errmsg)
      raise

