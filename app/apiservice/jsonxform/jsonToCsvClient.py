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
from __future__ import division
from abc import ABCMeta, abstractmethod
from apibase import ApiRequest, AppDirector, AppState, AppResolvar
from apitools.jsonxform import XformMetaPrvdr
from threading import RLock
import logging
import simplejson as json
import os, sys, time

logger = logging.getLogger('apipeer.smart')

# -------------------------------------------------------------- #
# JsonToCsv
# ---------------------------------------------------------------#
class JsonToCsv(AppDirector):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = Resolvar()
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
  # -------------------------------------------------------------- #
  def advance(self, signal=None):
    state = self.state
    # signal = the http status code of the companion actor method
    if signal:
      state.hasSignal = False
      if signal != 201:
        errmsg = 'actor %s error, %s failed, returned error signal : %d'
        raise Exception(errmsg % (self.actorId, state.transition, signal))
    if state.hasNext:
      state.current = state.next
    return state
    
  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):
    if self.state.hasSignal:
      self.putApiRequest()
    
  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    state = self.state
    if state.complete:
      logger.info('handshake response to service ...')
      packet = {'jobId':self.jobId,'caller':'client','actor':'service'}
      packet['kwargs'] = {'signal':201}
      apiUrl = 'http://%s/api/v1/smart' % self.hostName
    else:
      logger.warn(f'{state.current} transition is not implemented')
      return
    response = self.request.post(apiUrl,json={'job':packet})
    logger.info('%s, api response : %s' % (self.name, response.text))

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    if self.state.status != 'STARTED':
      errmsg = 'actor %s error, job was %s, aborting ... ' \
        % (self.actorId, self.state.status)
    else:
      errmsg = 'actor %s error, job %s was STARTED, aborting ... ' \
       % (self.actorId, self.jobId)

    logger.error(errmsg, exc_info=True)

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):
  
  def __init__(self):
    self.request = ApiRequest()
    self.__dict__['DOWNLOAD_ZIPFILE'] = self.DOWNLOAD_ZIPFILE

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, jobId, jobMeta):
    self.state.current = 'DOWNLOAD_ZIPFILE'
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    msg = '%s, starting job %s ...'
    logger.info(msg % (self.__class__.__name__,self.jobId))

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
  # DOWNLOAD_ZIPFILE
  # ---------------------------------------------------------------#
  def DOWNLOAD_ZIPFILE(self):
    self.downloadZipFile()
    self.uncompressFile()
    state = self.state
    state.hasSignal = True
    state.hasNext = False
    state.complete = True
    return state

  # -------------------------------------------------------------- #
  # downloadZipFile
  # ---------------------------------------------------------------#
  def downloadZipFile(self):
    repoMeta = self.getSaasMeta('client:repoMeta','REPO')
    if not os.path.exists(repoMeta['sysPath']):
      errmsg = 'xform output path does not exist : ' + repoMeta['sysPath']
      raise Exception(errmsg)

    catPath = self.jmeta['category']
    if catPath not in repoMeta['consumer categories']:
      errmsg = 'consumer category %s does not exist in : %s ' \
                                          % (catPath, repoMeta['sysPath'])
      raise Exception(errmsg)
  
    self.repoPath = '%s/%s/%s' % (repoMeta['sysPath'], catPath, self.jobId)
    logger.info('making output csv zipfile repo path, ' + self.repoPath)

    try:
      cmdArgs = ['mkdir','-p',self.repoPath]
      self.sysCmd(cmdArgs)
    except Exception as ex:
      logger.error('output repo path creation failed')
      raise

    self.csvGZipFile = '%s.%s' % (self.jobId, self.jmeta['fileExt'])
    logger.info('output csv tar gzipfile : ' + self.csvGZipFile)

    csvGZipPath = '%s/%s' % (self.repoPath, self.csvGZipFile)
    dstream = self.getFileStream()

    try:
      with open(csvGZipPath, 'wb') as fhwb:
        for chunk in dstream.iter_content(chunk_size=1024): 
          if chunk: # filter out keep-alive new chunks
            fhwb.write(chunk)
    except Exception as ex:
      errmsg = '%s write failed ' % self.csvGZipFile
      logger.error(errmsg)
      raise

  # -------------------------------------------------------------- #
  # getFileStream
  # ---------------------------------------------------------------#
  def getFileStream(self):
    try:
      dstream = self._getFileStream()
    except Exception as ex:
      errmsg = 'csv tar gzip file stream api request failed'
      logger.error(errmsg)
      raise
    if dstream.status_code != 201:
      errmsg = 'csv tar gzip file stream api request failed\nError : %s' % dstream.text
      raise Exception(errmsg)
    return dstream

  # -------------------------------------------------------------- #
  # _getFileStream
  # ---------------------------------------------------------------#
  def _getFileStream(self):
    packet = {'jobId':self.jobId,'service':'datatxn.dataTxnPrvdr:BinryFileStreamPrvdr'}
    packet['responseType'] = 'stream'
    packet['args'] = [self.jobId, self.csvGZipFile]
    apiUrl = 'http://%s/api/v1/sync' % self.hostName
    # NOTE: stream=True parameter
    return self.request.post(apiUrl,stream=True,json={'job':packet})

  # -------------------------------------------------------------- #
  # uncompressFile
  # ---------------------------------------------------------------#
  def uncompressFile(self):
    logger.info('%s, tar extract gunzip ...' % self.csvGZipFile)
    try:
      cmdArgs = ['tar','-xzf',self.csvGZipFile]
      self.sysCmd(cmdArgs,cwd=self.repoPath)
    except Exception as ex:
      errmsg = '%s, tar extract gunzip failed' % self.csvGZipFile
      logger.error(errmsg)
      raise
