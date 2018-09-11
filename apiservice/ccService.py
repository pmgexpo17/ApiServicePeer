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
from apiservice import (AppDirector, AppState, AppResolvar, AppListener,
  SasScriptPrvdr, SysCmdUnit, logger)
from apitools import CcEmailPrvdr
import datetime
from threading import RLock
import logging
import json
import os, sys, time
import requests
import uuid

# -------------------------------------------------------------- #
# CcDirector
# ---------------------------------------------------------------#
class CcDirector(AppDirector):

  def __init__(self, leveldb, jobId):
    super(CcDirector, self).__init__(leveldb, jobId)
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = CcResolvar(leveldb, jobId)
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    logger.info('ccService.CcDirector._start')
    scriptPrvdr = CcScriptPrvdr(self._leveldb, self.jobId)
    pmeta = scriptPrvdr()
    CcEmailPrvdr._start('CcDirector',pmeta)    
    self.resolve._start(pmeta)
    
  # -------------------------------------------------------------- #
  # advance
  # -------------------------------------------------------------- #
  def advance(self, signal=None):
    if self.state.transition == 'EVAL_CC_WAIT':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'CsvChecker failed, returned error signal : %d' % signal
        logger.error(errmsg)
        raise Exception(errmsg)
    self.state.current = self.state.next
    return self.state
    
  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'EVAL_CC_WAIT':
      self.putApiRequest()
    elif self.state.complete:
      self.onComplete()
      
  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    if self.state.transition == 'EVAL_CC_WAIT':
      classRef = 'csvChecker:CsvChecker'
      listener = 'csvChecker:CcListener'
      pdata = (classRef,listener,self.resolve.tblKey,self.jobId)
      params = '{"type":"director","id":null,"service":"%s","listener":"%s","args":["%s"],"caller":"%s"}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/smart'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onComplete
  # ---------------------------------------------------------------#
  def onComplete(self):
    logger.info('sending completion email to user ...')
    CcEmailPrvdr.newMail('CcDirector','EOP1')
    CcEmailPrvdr.sendMail('CcDirector')

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    # if error is due to delegate failure then don't post an email
    if self.state.inTransition:
      return
    # if CcResolvar has caught an exception an error mail is ready to be sent
    if not CcEmailPrvdr.hasMailReady('CcDirector'):
      method = 'ccService.CcResolvar.' + self.state.current
      errdesc = 'system error'
      self.sendMail('ERR1',method,errdesc,str(ex))
      return
    CcEmailPrvdr.sendMail('CcDirector')

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):      
    CcEmailPrvdr.sendMail('CcDirector',*args)
    
# -------------------------------------------------------------- #
# CcResolvar
# ---------------------------------------------------------------#
class CcResolvar(AppResolvar):
  
  def __init__(self, leveldb, jobId):
    self.__dict__['CHECK_CC_PARAMS'] = self.CHECK_CC_PARAMS
    self.__dict__['EVAL_CC_REPEAT'] = self.EVAL_CC_REPEAT
    self.__dict__['EVAL_CC_REPORT'] = self.EVAL_CC_REPORT
    self._leveldb = leveldb
    self.jobId = jobId
    self.method = ''
    self.pmeta = None
    self.ccCount = 0
    self.ccNum = 0  

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, pmeta):
    self.method = 'ccService.CcResolvar._start'
    self.state.current = 'CHECK_CC_PARAMS'
    self.pmeta = pmeta

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    CcEmailPrvdr.newMail('CcDirector',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # CHECK_CC_PARAMS
  # - state.current = CHECK_PARAMS
  # - state.next = EVAL_CC_JOBS
  # ---------------------------------------------------------------#
  def CHECK_CC_PARAMS(self):
    self.checkCcParams()
    self.state.next = 'EVAL_CC_REPEAT'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # getTblInfoName -
  # ---------------------------------------------------------------#
  def getTblInfoName(self):
    bizType = self.pmeta['bizType'].upper()
    productType = self.pmeta['productType'].upper()
    if productType == 'HOME':
      return bizType + 'HomeTableInfo.json'
    return bizType + 'MotorTableInfo.json'

  # -------------------------------------------------------------- #
  # checkCcParams -
  # ---------------------------------------------------------------#
  def checkCcParams(self):
    self.method = 'ccService.CcResolvar.checkCcParams'
    if self.pmeta['bizType'].upper() not in ['PI','CI','CR']:
      errmsg = 'business type %s in not valid, must be either PI CI or CR'
      errmsg = errmsg % self.pmeta['bizType']
      self.newMail('ERR1','user error',errmsg)
      raise Exception(errmsg)
    
    if self.pmeta['productType'].upper() not in ('HOME','MOTOR'):
      errmsg = 'product type %s in not valid, must be either home or motor'
      errmsg = errmsg % self.pmeta['productType']
      self.newMail('ERR1','user error',errmsg)
      raise Exception(errmsg)

    try:
      tblInfoName = self.getTblInfoName()
    except Exception as ex:
      self.method = 'ccService.CcResolvar.getTblInfoName'
      self.newMail('ERR1','user error',str(ex))
      raise

    assetFile = self.pmeta['assetLib'] + '/' + tblInfoName
    with open(assetFile,'r') as fhr:
      try:
        self.ccTableInfo = json.load(fhr)
      except ValueError as ex:
        errdesc = 'json load error, %s' % assetFile
        self.newMail('ERR1',errdesc,str(ex))
        raise
    
    self.ccTables = self.pmeta['tableList']
    
    for tblKey in self.ccTables:
      try:
        self.ccTableInfo[tblKey]
      except KeyError:
        errmsg = 'table key %s is NOT found in assets/%s'
        errmsg = errmsg % (tblKey, tblInfoName)
        self.newMail('ERR1','user error',errmsg)
        raise Exception(errmsg)
        
    self.ccCount = len(self.ccTables)
    self.ccNum = 0
    
  # -------------------------------------------------------------- #
  # TXN_REPEAT - evalTxnRepeat
  # - state.current = 'TXN_REPEAT'
  # - state.next = 'TXN_SGMT_REPEAT'
  # ---------------------------------------------------------------#
  def EVAL_CC_REPEAT(self):
    if self.ccNum == self.ccCount:
      self.state.next = 'EVAL_CC_REPORT'
      self.state.hasNext = True
    else:
      self.putCcTableInfo()
      self.ccNum += 1      
      self.state.transition = 'EVAL_CC_WAIT'
      self.state.inTransition = True
      self.state.next = 'EVAL_CC_REPEAT'
      self.state.hasNext = True
    return self.state
 
  # -------------------------------------------------------------- #
  # putCcTableInfo -
  # ---------------------------------------------------------------#
  def putCcTableInfo(self):
    self.method = 'ccService.CcResolvar.getTblInfoName'
    try:
      self.tblKey = self.ccTables[self.ccNum]
      oracleTblMeta = self.ccTableInfo[self.tblKey]
      dbKey = 'PMETA|%s|%s' % (self.jobId,self.tblKey)
      logger.info('csvchecker %s tableInfo : %s' % (self.tblKey, oracleTblMeta))
      self._leveldb.Put(dbKey, oracleTblMeta)
    except Exception as ex:
      self.newMail('ERR1','leveldb storage failed',str(ex))
      raise

  # -------------------------------------------------------------- #
  # EVAL_CC_REPORT
  # - state.next = 'EOP'
  # ---------------------------------------------------------------#
  def EVAL_CC_REPORT(self):
    try:
      self.evalCcReport()
    except Exception:
      self.newMail('ERR3','evalCheckReport')
      raise
    self.state.hasNext = False
    self.state.complete = True
    self.state.hasSignal = True
    return self.state

  # -------------------------------------------------------------- #
  # evalCcReport
  # ---------------------------------------------------------------#
  def evalCcReport(self):
    self.method = 'ccService.CcResolvar.evalCcReport'
    sasPrgm = 'evalCheckReport.sas'
    logfile = 'log/evalCheckReport.log'
    sysArgs = ['sas','-sysin',sasPrgm,'-altlog',logfile]
    logger.info('run evalCheckReport.sas in subprocess ...')
    cwd = self.pmeta['progLib']
    self.runProcess(sysArgs,cwd=cwd)

# -------------------------------------------------------------- #
# CcScriptPrvdr
# ---------------------------------------------------------------#
class CcScriptPrvdr(SasScriptPrvdr, SysCmdUnit):
  
  def __init__(self, leveldb, jobId):
    super(CcScriptPrvdr, self).__init__(leveldb)
    self.jobId = jobId
    self.method = ''
    
  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):
    self.putCredentials()
    pmeta = self.getProgramMeta()
    self.method = 'ccService.CcScriptPrvdr.__call__'    
    try:
      self.pmeta = pmeta
      ccTables = pmeta['tableList'].split(' ')
      ccTables = list(filter(lambda x: x!='', ccTables))
      # for compiling sas leave tableList as a string of table keys
      self.pmeta['tableCount'] = len(ccTables)
      _incItems = ['ccautoLib','tableList','tableCount','workSpace']
      self.compileScript('evalCheckReport.sas',incItems=_incItems)
      # for resolve class member, set tableList as a python list
      pmeta['tableList'] = ccTables 
      return pmeta
    except Exception as ex:
      self.newMail('ERR1','compile script error',str(ex))
      raise

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    CcEmailPrvdr.newMail('CcDirector',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    self.method = 'ccService.CcScriptPrvdr.getProgramMeta'
    dbKey = 'TSXREF|' + self.jobId
    tsXref = datetime.datetime.now().strftime('%y%m%d%H%M%S')
    self._leveldb.Put(dbKey, tsXref)

    dbKey = 'PMETA|' + self.jobId
    try:
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      errmsg = 'EEOWW! pmeta json document not found : ' + dbKey
      self.newMail('ERR1','leveldb lookup failed',errmsg)
      raise Exception(errmsg)

    try:
      pmetaAll = json.loads(pmetadoc)
      pmeta = pmetaAll['Global']

      workSpace = pmeta['workSpace']
      session = '/CC' + tsXref
      workSpace += session;
      self.sysCmd(['mkdir','-p',workSpace])
      progLib = workSpace + '/saslib'
      logDir = workSpace + '/saslib/log'
      self.sysCmd(['mkdir',progLib])
      self.sysCmd(['mkdir',logDir])
      workSpace += '/ssnwork'    
      self.sysCmd(['mkdir',workSpace])
      pmetaAll['Global']['workSpace'] = workSpace
      pmetaAll['Global']['progLib'] = progLib
      # write the pmeta session dict back for delegates to use
    
      self._leveldb.Put(dbKey, json.dumps(pmetaAll))
      dbKey = 'PMETA|PROGLIB|' + self.jobId
      self._leveldb.Put(dbKey, progLib)

      pmeta = pmetaAll['CsvChecker']
      pmeta.update(pmetaAll['Global'])
      return pmeta
    except Exception as ex:
      self.newMail('ERR1','compile pmeta failed',str(ex))
      raise

  # -------------------------------------------------------------- #
  # putCredentials -
  # ---------------------------------------------------------------#
  def putCredentials(self):
    self.method = 'ccService.CcScriptPrvdr.putCredentials'
    credentials = os.environ['HOME'] + '/.landrive'
    username = None
    password = None
    try:
      with open(credentials,'r') as fhr:
        for credItem in fhr:
          credItem = credItem.strip() 
          if 'username' in credItem:
            username = credItem.split('=')[1]
          elif 'password' in credItem:
            password = credItem.split('=')[1]
    except Exception as ex:
      self.newMail('ERR1','failed to parse credentials file',str(ex))
      raise
     
    if not username or not password:
      errmsg = 'user landrive credentials are not valid'
      self.newMail('ERR1','system error',errmsg)
      raise Exception(errmsg)

    dbKey = 'CRED|' + self.jobId
    credentials = username + ':' + password
    self._leveldb.Put(dbKey,credentials)
    self.uid = username
