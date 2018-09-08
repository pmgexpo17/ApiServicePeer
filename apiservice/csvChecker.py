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
from apiservice import AppDirector, AppResolvar, AppListener, SasScriptPrvdr, SysCmdUnit, logger
from apitools import CcEmailPrvdr
import os, sys, time
import json
import requests
import uuid

# -------------------------------------------------------------- #
# CsvChecker
# ---------------------------------------------------------------#
class CsvChecker(AppDirector):

  def __init__(self, leveldb, jobId, caller):
    super(CsvChecker,self).__init__(leveldb, jobId)
    self.caller = caller
    self._type = 'delegate'
    self.state.hasNext = True
    self.resolve = CcResolvar(leveldb)
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # runApp
  # - override to get the callee jobId sent from NormalizeXml partner
  # ---------------------------------------------------------------#
  def runApp(self, signal=None):
    logger.info('csvChecker.CsvChecker.runApp')
    super(CsvChecker, self).runApp(signal=signal)

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, tblKey):
    logger.info('csvChecker.CsvChecker._start, caller : ' + self.caller)
    self.resolve.tblKey = tblKey
    scriptPrvdr = CcScriptPrvdr(self._leveldb,self.jobId,self.caller)
    scriptPrvdr.tblKey = tblKey
    tsXref, pmeta = scriptPrvdr()
    self.resolve._start(tsXref, pmeta)
    CcEmailPrvdr.subscribe('CsvChecker')

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    if self.state.transition == 'APPLY_ORACLE_NOWAIT':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'ApplyOracleBySgmt failed, returned error signal : %d' % signal
        raise Exception(errmsg)
    self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'APPLY_ORACLE_NOWAIT':
      self.putApiRequest(201)
    elif self.state.complete:
      self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    if self.state.transition == 'APPLY_ORACLE_NOWAIT':
      classRef = 'csvChecker:ApplyOracleBySgmt'
      pdata = (classRef,self.jobId,self.resolve.tblKey)
      params = '{"type":"delegate","service":"%s","id":"%s","args":["%s"]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/async/%s' % self.resolve.sgmtRange
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.complete:
      classRef = 'ccService:CcDirector'
      pdata = (self.caller,classRef,json.dumps({'signal':signal}))
      params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/smart'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    # if error is due to delegate failure then don't post an email
    if self.state.inTransition:
      self.putApiRequest(500)
      return
    # if CcResolvar has caught an exception an error mail is ready to be sent
    if not CcEmailPrvdr.hasMailReady('CsvChecker'):
      method = 'csvChecker.CcResolvar.' + self.state.current
      errdesc = 'unmanaged error'
      self.sendMail('ERR1',method,errdesc,str(ex))
      return
    CcEmailPrvdr.sendMail('CsvChecker')

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self,*args):      
    CcEmailPrvdr.sendMail('CsvChecker',*args)

# -------------------------------------------------------------- #
# CcResolveUnit
# ---------------------------------------------------------------#
class CcResolvar(AppResolvar):
  
  def __init__(self, leveldb):
    self.__dict__['EVAL_CSV_SGMT_COUNT'] = self.EVAL_CSV_SGMT_COUNT
    self.__dict__['APPLY_ORACLE_REPEAT'] = self.APPLY_ORACLE_REPEAT
    self.__dict__['MAKE_ORACLE_DS'] = self.MAKE_ORACLE_DS
    self.__dict__['EVAL_CSV_CHECKER'] = self.EVAL_CSV_CHECKER
    self._leveldb = leveldb
    self.callee = None
    self.method = ''
    self.pmeta = None
    
  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, tsXref, pmeta):
    self.tsXref = tsXref
    self.pmeta = pmeta
    self.state.current = 'EVAL_CSV_SGMT_COUNT'
    self.sgmtCount = 0
    self.sgmtStart = 1
    self.sgmtEnd = 0

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    CcEmailPrvdr.newMail('CsvChecker',bodyKey,self.method,*args)
    
	# -------------------------------------------------------------- #
	# EVAL_CSV_SGMT_COUNT
	# ---------------------------------------------------------------#
  def EVAL_CSV_SGMT_COUNT(self):
    try:
      self.evalCsvSgmtCount()
    except Exception:
      self.newMail('ERR2',self.tblKey,'evalCsvSgmtCount')
      raise
    self.state.next = 'APPLY_ORACLE_REPEAT'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # evalCsvSgmtCount
  # ---------------------------------------------------------------#
  def evalCsvSgmtCount(self):
    self.method = 'csvChecker.CsvChecker.evalCsvSgmtCount'    
    sasPrgm = 'evalCsvSgmtCount.sas'
    logfile = 'log/%s/evalCsvSgmtCount.log' % self.tblKey
    sysArgs = ['sas','-sysin',sasPrgm,'-set','tname',self.tblKey]
    sysArgs += ['-altlog',logfile]

    logger.info('run importAppCsv.sas in subprocess ...')
    cwd = self.pmeta['progLib']
    stdout = self.runProcess(sysArgs,cwd=cwd)
    self.sgmtCount = int(stdout);
    logger.info('%s segment count : %d' % (self.tblKey, self.sgmtCount))

  # -------------------------------------------------------------- #
  # APPLY_ORACLE_REPEAT
  # ---------------------------------------------------------------#
  def APPLY_ORACLE_REPEAT(self):
    if self.sgmtEnd == self.sgmtCount:
      self.state.next = 'MAKE_ORACLE_DS'
      self.state.hasNext = True
    else:
      self.state.transition = 'APPLY_ORACLE_NOWAIT'
      self.state.inTransition = True
      self.state.next = 'APPLY_ORACLE_REPEAT'
      self.state.hasNext = True
      sgmtDiff = self.sgmtCount - self.sgmtStart
      sgmtDiff = 6 if sgmtDiff > 6 else sgmtDiff
      self.sgmtEnd = sgmtEnd = self.sgmtStart + sgmtDiff
      if self.sgmtEnd == self.sgmtCount:
        sgmtEnd += 1
      self.sgmtRange = '%d-%d' % (self.sgmtStart, sgmtEnd)
      # advance sgmtStart for next iteration
      self.sgmtStart += sgmtDiff
      logger.info('next %s segment range : %s' % (self.tblKey, self.sgmtRange))
    return self.state

  # -------------------------------------------------------------- #
  # MAKE_ORACLE_DS
  # ---------------------------------------------------------------#
  def MAKE_ORACLE_DS(self):
    try:
      self.makeOracleDs()
    except Exception:
      self.newMail('ERR2',self.tblKey,'makeOracleBySgmt')
      raise
    self.state.hasNext = True
    self.state.next = 'EVAL_CSV_CHECKER'
    return self.state

  # -------------------------------------------------------------- #
  # makeOracleDs
  # ---------------------------------------------------------------#
  def makeOracleDs(self):
    self.method = 'csvChecker.CsvChecker.makeOracleDs'
    sasPrgm = 'makeOracleBySgmt.sas'
    logfile = 'log/%s/makeOracleBySgmt.log' % self.tblKey
    sysArgs = ['sas','-sysin',sasPrgm,'-set','tname',self.tblKey,'-set','sgmtCount',str(self.sgmtCount)]
    sysArgs += ['-altlog',logfile]
    logger.info('run makeOracleBySgmt.sas in subprocess ...')
    cwd = self.pmeta['progLib']
    self.runProcess(sysArgs,cwd=cwd)

  # -------------------------------------------------------------- #
  # EVAL_CSV_CHECKER
  # ---------------------------------------------------------------#
  def EVAL_CSV_CHECKER(self):
    try:
      self.evalCsvChecker()
    except Exception:
      self.newMail('ERR2',self.tblKey,'csvChecker')
      raise
    self.state.hasNext = False
    self.state.complete = True
    self.state.hasSignal = True
    return self.state

  # -------------------------------------------------------------- #
  # evalCsvChecker
  # ---------------------------------------------------------------#
  def evalCsvChecker(self):
    self.method = 'csvChecker.CsvChecker.evalCsvChecker'
    sasPrgm = 'csvChecker.sas'
    logfile = 'log/%s/csvChecker.log' % self.tblKey
    sysArgs = ['sas','-sysin',sasPrgm,'-set','tname',self.tblKey]
    sysArgs += ['-altlog',logfile]
    logger.info('run evalCsvChecker.sas in subprocess ...')
    cwd = self.pmeta['progLib']
    self.runProcess(sysArgs,cwd=cwd)

# -------------------------------------------------------------- #
# ApplyOracleBySgmt
# ---------------------------------------------------------------#
class ApplyOracleBySgmt(SysCmdUnit):

  def __init__(self, leveldb, caller):
    self._leveldb = leveldb
    self.caller = caller
    
  # -------------------------------------------------------------- #
  # runEmltnBySgmt
  # ---------------------------------------------------------------#
  def __call__(self, tblKey, sgmtNum):
    self.method = 'csvChecker.ApplyOracleBySgmt.__call__'
    dbKey = 'PMETA|PROGLIB|' + self.caller
    try:
      progLib = self._leveldb.Get(dbKey)
      logger.info('csvChecker.ApplyOracleBySgmt progLib : ' + progLib)
    except KeyError:
      raise Exception('EEOWW! pmeta proglib param not found : ' + dbKey)
    try:
      sasPrgm = 'applyOracleBySgmt.sas'
      logfile = 'log/%s/applyOracleBySgmt_%d.log' % (tblKey, sgmtNum)
      sysArgs = ['sas','-sysin',sasPrgm,'-set','tname',tblKey,'-set','sgmtNum',str(sgmtNum)]
      sysArgs += ['-altlog',logfile]
      logger.info('run ApplyOracleBySgmt.sas for sgmtNum %d in subprocess ...' % sgmtNum)
      self.runProcess(sysArgs,cwd=progLib)
    except Exception:
      self.sendMail('applyOracleBySgmt')
      raise
    
  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):
    CcEmailPrvdr.sendMail('CsvChecker','ERR2',self.method,*args)

# -------------------------------------------------------------- #
# CcListener
# ---------------------------------------------------------------#
class CcListener(AppListener):

  def __init__(self, leveldb, caller):
    super(CcListener, self).__init__(leveldb, caller)
    self.state = None
    self.jobIdList = []

  def __call__(self, event):    
    if not event.exception and self.state.transition == 'APPLY_ORACLE_NOWAIT':
      if event.job_id in self.jobIdList:
        with self.state.lock:
          self.jobIdList.remove(event.job_id)
          if not self.jobIdList:
            self.putApiRequest(201)
    elif event.exception:
      self.putApiRequest(500)

  # -------------------------------------------------------------- #
  # register - add a list of live job ids
  # ---------------------------------------------------------------#
  def register(self, jobRange=None):
    logger.info('csvChecker.CcListener register : ' + str(jobRange))
    self.jobIdList = jobIds = []
    for _ in jobRange:
      jobIds += [str(uuid.uuid4())]
    return jobIds

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'ccService:CsvChecker'
    pdata = (self.caller,classRef, json.dumps({'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/smart'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# CcScriptPrvdr
# ---------------------------------------------------------------#
class CcScriptPrvdr(SasScriptPrvdr, SysCmdUnit):

  def __init__(self, leveldb, jobId, caller):
    super(CcScriptPrvdr, self).__init__(leveldb)
    self.jobId = jobId
    self.caller = caller
    self.pmeta = None
      
  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):
    self.pmeta = self.getProgramMeta()
    try:
      self.compileScript('evalCsvSgmtCount.sas')
      self.compileScript('applyOracleBySgmt.sas')
      self.compileScript('makeOracleBySgmt.sas',incItems=['ccautoLib','workSpace'])
      self.compileScript('csvChecker.sas',incItems=['ccautoLib','workSpace'])
      dbKey = 'TSXREF|' + self.caller
      try:
        tsXref = self._leveldb.Get(dbKey)
      except KeyError:
        errmsg = 'EEOWW! tsXref param not found : ' + dbKey
        self.newMail('ERR1','leveldb lookup failed',errmsg)
        raise Exception(errmsg)
      return (tsXref, self.pmeta)
    except Exception as ex:
      self.method = 'csvChecker.CcScriptPrvdr.__call__'
      self.newMail('ERR1','compile script error',str(ex))
      raise

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    CcEmailPrvdr.newMail('CsvChecker',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    self.method = 'csvChecker.CcScriptPrvdr.getProgramMeta'
    dbKey1 = 'PMETA|' + self.caller
    dbKey2 = 'PMETA|%s|%s' % (self.caller, self.tblKey)
    try:
      pmetadoc = self._leveldb.Get(dbKey1)
      tableInfo = self._leveldb.Get(dbKey2)
    except KeyError:
      errmsg = 'EEOWW! either pmeta json document not found : %s, %s'
      errmsg % (dbKey1, dbKey2)
      self.newMail('ERR1','leveldb lookup failed',errmsg)
      raise Exception(errmsg)
      
    try:
      logger.info('CcScriptPrvdr tableInfo : ' + tableInfo)
      pmetaAll = json.loads(pmetadoc)
      pmeta = pmetaAll['CsvChecker']
      del(pmeta['globals'])
      pmeta.update(pmetaAll['Global'])
  
      # make a separate workspace per table
      workSpace = pmeta['workSpace'] + '/' + self.tblKey
      logDir = '%s/log/%s' % (pmeta['progLib'], self.tblKey)
      self.sysCmd(['mkdir',workSpace])
      self.sysCmd(['mkdir',logDir])
  
      gipeTblName, csvFilename = tableInfo.split(':')
      pmeta['gotn'] = gipeTblName
      pmeta['csvFilename'] = csvFilename
      # put the progLib to storage for lookup by delegates
      dbKey = 'PMETA|PROGLIB|' + self.jobId
      self._leveldb.Put(dbKey,pmeta['progLib'])
      return pmeta
    except Exception as ex:
      self.newMail('ERR1','compile pmeta failed',str(ex))
      raise
