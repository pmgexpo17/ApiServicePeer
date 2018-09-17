from apibase import (AppDirector, AppState, AppResolvar, AppListener,
  SasScriptPrvdr, SysCmdUnit, logger)
from apitools.wcemltn import WcEmailPrvdr
import datetime
from threading import RLock
import logging
import json
import os, sys, time
import requests
import uuid

# -------------------------------------------------------------- #
# WcDirector
# ---------------------------------------------------------------#
class WcDirector(AppDirector):

  def __init__(self, leveldb, jobId):
    super(WcDirector, self).__init__(leveldb, jobId)
    self._type = 'director'
    self.state.hasNext = True
    self.resolve = WcResolvar()
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    logger.info('wcService.WcDirector._start')
    WcEmailPrvdr.init('WcDirector')
    scriptPrvdr = WcScriptPrvdr(self._leveldb, self.jobId)
    pmeta = scriptPrvdr()
    WcEmailPrvdr.start('WcDirector',pmeta)
    self.resolve._start(pmeta)

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    if self.state.transition == 'XML_TO_SAS':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'WcEmltnInputPrvdr failed, returned error signal : %d' % signal
        logger.error(errmsg)
        raise Exception(errmsg)
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      # signal = the http status code of the listener promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        errmsg = 'WcEmltnBySgmt failed, returned error signal : %d' % signal
        logger.error(errmsg)
        raise Exception(errmsg)
    self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'XML_TO_SAS':
      self.putApiRequest()
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      self.putApiRequest()
    elif self.state.complete and not self.state.failed:
      self.onComplete()

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    
    if self.state.transition == 'XML_TO_SAS':
      classRef = 'wcInputPrvdr:WcInputPrvdr'
      pdata = (classRef,self.jobId)
      params = '{"type":"director","id":null,"service":"%s","args":[],"caller":"%s"}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/smart'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':      
      classRef = 'wcService:WcEmltnBySgmt'
      pdata = (self.jobId,classRef, self.resolve.txnNum)
      params = '{"type":"delegate","id":"%s","service":"%s","args":[%d]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/async/%d' % self.resolve.sgmtCount
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onComplete
  # ---------------------------------------------------------------#
  def onComplete(self):
    logger.info('sending completion email to user ...')
    WcEmailPrvdr.newMail('WcDirector','EOP1')
    WcEmailPrvdr.sendMail('WcDirector')

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    # if error is due to delegate failure then don't post an email
    if self.state.inTransition:
      return
    # if WcResolvar has caught an exception an error mail is ready to be sent
    if not WcEmailPrvdr.hasMailReady('WcDirector'):
      method = 'wcService.WcResolvar.' + self.state.current
      errdesc = 'system error'
      self.sendMail('ERR1',method,errdesc,str(ex))
      return
    WcEmailPrvdr.sendMail('WcDirector')

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):      
    WcEmailPrvdr.sendMail('WcDirector',*args)

# -------------------------------------------------------------- #
# WcResolvar
# ---------------------------------------------------------------#
class WcResolvar(AppResolvar):
  
  def __init__(self):
    self.__dict__['XML_TO_SAS'] = self.XML_TO_SAS
    self.__dict__['GET_TXN_COUNT'] = self.GET_TXN_COUNT
    self.__dict__['TXN_REPEAT'] = self.TXN_REPEAT
    self.__dict__['TXN_SGMT_REPEAT'] = self.TXN_SGMT_REPEAT
    self.__dict__['TXN_SGMT_RESTACK'] = self.TXN_SGMT_RESTACK
    self.__dict__['TXN_RESTACK'] = self.TXN_RESTACK
    self.pmeta = None
    self.sgmtCount = 0
    self.txnNum = 0  
    self.txnCount = 0

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    WcEmailPrvdr.newMail('WcDirector',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # XML_TO_SAS - evalTxnCount
  # - state.current = 'XML_TO_SAS'
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def XML_TO_SAS(self):
    self.state.transition = 'XML_TO_SAS'
    self.state.inTransition = True
    self.state.next = 'GET_TXN_COUNT'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self, pmeta):
    logger.info('wcService.WcResolvar._start')    
    self.state.current = 'XML_TO_SAS'
    self.pmeta = pmeta

  # -------------------------------------------------------------- #
  # getTxnCount -
  # ---------------------------------------------------------------#
  def getTxnCount(self):
    sasPrgm = 'batchTxnScheduleWC.sas'
    logfile = 'log/batchTxnScheduleWC.log'
    sysArgs = ['sas','-sysin',sasPrgm,'-altlog',logfile]
    
    logger.info('run batchTxnScheduleWC.sas in subprocess ...')
    progLib = self.pmeta['progLib']
    try:
      stdout = self.runProcess(sysArgs,cwd=progLib)
    except Exception:
      self.newMail('ERR2','restackTxnOutputWC',logfile,progLib)
      raise
      
    logger.info('program output : %s' % stdout)
    
    txnPacket = json.loads(stdout)
    self.txnCount = txnPacket['count']
    self.txnNum = 0

  # -------------------------------------------------------------- #
  # GET_TXN_COUNT
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def GET_TXN_COUNT(self):
    self.getTxnCount()
    self.state.next = 'TXN_REPEAT'
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # TXN_REPEAT - evalTxnRepeat
  # - state.current = 'TXN_REPEAT'
  # - state.next = 'TXN_SGMT_REPEAT'
  # ---------------------------------------------------------------#
  def TXN_REPEAT(self):
    if self.txnNum == self.txnCount:
      self.state.next = 'TXN_RESTACK'
      self.state.hasNext = True
    else:
      self.txnNum += 1
      self.state.next = 'TXN_SGMT_REPEAT'
      self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # TXN_RESTACK
  # - state.next = 'EOP'
  # ---------------------------------------------------------------#
  def TXN_RESTACK(self):
    self.restackTxnOutputAll()
    self.state.hasNext = False
    self.state.complete = True
    return self.state

  # -------------------------------------------------------------- #
  # restackTxnOutputAll
  # ---------------------------------------------------------------#
  def restackTxnOutputAll(self):
    sasPrgm = 'restackTxnOutputWC.sas'
    logfile = 'log/restackTxnOutputWC.log'
    txnCount = str(self.txnCount)
    sysArgs = ['sas','-sysin',sasPrgm,'-set','txnCount',txnCount,'-altlog',logfile]

    logger.info('run restackTxnOutputWC.sas in subprocess ...')
    progLib = self.pmeta['progLib']
    try:
      self.runProcess(sysArgs,cwd=progLib)
    except Exception:
      self.newMail('ERR2','restackTxnOutputWC',logfile,progLib)
      raise

  # -------------------------------------------------------------- #
  # TXN_SGMT_REPEAT - evalTxnSgmtRepeat
  # - state.current = 'TXN_SGMT_REPEAT'
  # - state.next = 'TXN_SGMT_RESTACK'
  # ---------------------------------------------------------------#
  def TXN_SGMT_REPEAT(self):
    self.state.transition = 'EMLTN_BYSGMT_NOWAIT'
    self.state.inTransition = True
    self.state.next = 'TXN_SGMT_RESTACK'
    self.state.hasNext = True
    self.getTxnSgmtCount()
    return self.state

  # -------------------------------------------------------------- #
  # getTxnSgmtCount
  # ---------------------------------------------------------------#
  def getTxnSgmtCount(self):
    sasPrgm = 'batchScheduleWC.sas'
    logfile = 'log/batchScheduleWC.log'
    txnIndex = str(self.txnNum)
    sysArgs = ['sas','-sysin',sasPrgm,'-set','txnIndex',txnIndex,'-altlog',logfile]
    
    logger.info('run batchScheduleWC.sas in subprocess, txn[%d] ...' % self.txnNum)
    progLib = self.pmeta['progLib']
    try:
      stdout = self.runProcess(sysArgs,cwd=progLib)
    except Exception:
      self.newMail('ERR2','batchScheduleWC',logfile,progLib)
      raise

    logger.info('program output : %s' % stdout)
    
    sgmtPacket = json.loads(stdout)
    self.sgmtCount = sgmtPacket['count']

  # -------------------------------------------------------------- #
  # TXN_SGMT_RESTACK - evalRestackSgmtAll
  # - state.current = 'TXN_SGMT_RESTACK'
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def TXN_SGMT_RESTACK(self):
    self.state.next = 'TXN_REPEAT'
    self.restackSgmtOutputAll()
    self.sgmtCount = 0
    return self.state

  # -------------------------------------------------------------- #
  # restackSgmtOutput
  # ---------------------------------------------------------------#
  def restackSgmtOutputAll(self):
    sasPrgm = 'restackSgmtOutputWC.sas'
    logfile = 'log/restackSgmtOutputWC_txn%d.log' % self.txnNum
    txnIndex = str(self.txnNum)
    sgmtCount = str(self.sgmtCount)
    sysArgs = ['sas','-sysin',sasPrgm,'-set','txnIndex',txnIndex]
    sysArgs += ['-set','sgmtCount',sgmtCount,'-altlog',logfile]
    
    logger.info('run restackSgmtOutputWC.sas in subprocess txn[%d] ...' % self.txnNum)
    progLib = self.pmeta['progLib']
    try:
      self.runProcess(sysArgs,cwd=progLib)
    except Exception:
      self.newMail('ERR2','restackSgmtOutputWC',logfile,progLib)
      raise

# -------------------------------------------------------------- #
# WcEmltnBySgmt
# ---------------------------------------------------------------#
class WcEmltnBySgmt(SysCmdUnit):

  def __init__(self, leveldb, caller):
    self._leveldb = leveldb
    self.caller = caller
    
  # -------------------------------------------------------------- #
  # WcEmltnBySgmt
  # ---------------------------------------------------------------#
  def __call__(self, txnNum, sgmtNum):
    self.method = 'wcService.WcEmltnBySgmt.__call__'
    dbKey = 'PMETA|PROGLIB|' + self.caller
    try:
      progLib = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! pmeta proglib param not found : ' + dbKey)
    try:
      sasPrgm = 'batchEmulatorWC_txn%d_s%d.sas' % (txnNum, sgmtNum)
      logfile = 'log/batchEmulatorWC_txn%d_s%d.log' % (txnNum, sgmtNum)
      sysArgs = ['sas','-sysin',sasPrgm,'-altlog',logfile]
  
      logMsg = 'run batchEmulatorWC_txn%d_s%d.sas in subprocess ...'
      logger.info(logMsg % (txnNum, sgmtNum))
      self.runProcess(sysArgs,cwd=progLib)
    except Exception:
      self.sendMail(sasPrgm, logfile, progLib)
      raise
    
  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def sendMail(self, *args):
    WcEmailPrvdr.sendMail('WcDirector','ERR2',self.method,*args)

# -------------------------------------------------------------- #
# WcListener
# ---------------------------------------------------------------#
class WcListener(AppListener):

  def __init__(self, leveldb, caller):
    super(WcListener, self).__init__(leveldb, caller)
    self.state = None
    self.jobIdList = []

  def __call__(self, event):    
    if not event.exception and self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
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

    self.jobIdList = jobIds = []
    for jobNum in jobRange:
      jobIds += [str(uuid.uuid4())]
    return jobIds

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'wcService:WcDirector'
    pdata = (self.caller,classRef, json.dumps({'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/smart'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcScriptPrvdr
# ---------------------------------------------------------------#
class WcScriptPrvdr(SasScriptPrvdr, SysCmdUnit):
  
  def __init__(self, leveldb, jobId):
    super(WcScriptPrvdr, self).__init__(leveldb)
    self.jobId = jobId
    
  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):
    self.putCredentials()
    self.pmeta = self.getProgramMeta()
    self.method = 'wcService.WcScriptPrvdr.__call__'    
    try:
      self.compileScript('batchTxnScheduleWC.sas')    
      self.compileScript('batchScheduleWC.sas')
      incItems = ['ciwork','macroLib']
      self.compileScript('restackSgmtOutputWC.sas',incItems=incItems)
      incItems = ['ciwork','macroLib','progLib','userEmail']
      self.compileScript('restackTxnOutputWC.sas',incItems=incItems)
      cwd = self.pmeta['assetLib']
      self.sysCmd(['cp','batchEmulatorWC.inc',self.pmeta['progLib']],cwd=cwd)
      return self.pmeta
    except Exception as ex:
      self.newMail('ERR1','compile script error',str(ex))
      raise

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    WcEmailPrvdr.newMail('WcDirector',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    self.method = 'wcService.WcScriptPrvdr.getProgramMeta'
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
      ciwork = pmetaAll['Global']['ciwork']
      session = '/WC' + tsXref
      ciwork += session
      self.sysCmd(['mkdir','-p',ciwork])
      proglib = ciwork + '/saslib'
      logdir = ciwork + '/saslib/log'
      self.sysCmd(['mkdir',proglib])
      self.sysCmd(['mkdir',logdir])
      ciwork += '/ssnwork'
      self.sysCmd(['mkdir',ciwork])
      pmetaAll['Global']['ciwork'] = ciwork
      pmetaAll['Global']['progLib'] = proglib
      # write the pmeta session dict back for delegates to use
      self._leveldb.Put(dbKey, json.dumps(pmetaAll))
      dbKey = 'PMETA|PROGLIB|' + self.jobId
      self._leveldb.Put(dbKey, proglib)
      
      pmeta = pmetaAll['WcDirector']
      _globals = pmeta['globals'] # append global vars in this list
      del(pmeta['globals'])
      if _globals[0] == '*':
        pmeta.update(pmetaAll['Global'])
      else:
        for item in _globals:
          pmeta[item] = pmetaAll['Global'][item]
      return pmeta
    except Exception as ex:
      self.newMail('ERR1','compile pmeta failed',str(ex))
      raise

  # -------------------------------------------------------------- #
  # putCredentials -
  # ---------------------------------------------------------------#
  def putCredentials(self):
    self.method = 'wcService.WcScriptPrvdr.putCredentials'
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
