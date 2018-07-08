from apiservice import SysCmdUnit, AppDirector, AppState, AppResolveUnit, AppListener, SasScriptPrvdr, logger
import datetime
from email.mime.text import MIMEText
from threading import RLock
import logging
import json
import os, sys, time
import requests
import smtplib
import uuid

# -------------------------------------------------------------- #
# WcEmltnDirector
# ---------------------------------------------------------------#
class WcEmltnDirector(AppDirector):

  def __init__(self, leveldb, jobId):
    super(WcEmltnDirector, self).__init__(leveldb, jobId)
    self.appType = 'director'
    self.state.hasNext = True
    self.resolve = WcResolveUnit()
    self.resolve.state = self.state
    self.mailer = WcEmailUnit()
    self.mailer.state = self.state

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    logger.debug('[START] loadProgramMeta')
    scriptPrvdr = WcScriptPrvdr(self._leveldb, self.jobId)
    pmeta = scriptPrvdr()
    self.resolve._start(pmeta)
    self.mailer._start(pmeta)

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
        raise Exception('WcEmltnInputPrvdr server process failed, rc : %d' % signal)
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      # signal = the http status code of the listener promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        raise Exception('WcEmltnBySgmt server process failed, rc : %d' % signal)
    self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, errorMsg):
    self.mailer[self.state.current]('ERROR')

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'XML_TO_SAS':
      self.putApiRequest()
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      self.putApiRequest()

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    
    if self.state.transition == 'XML_TO_SAS':
      classRef = 'wcEmltnInputPrvdr:WcEmltnInputPrvdr'
      pdata = (classRef,self.jobId)
      params = '{"type":"director","id":null,"service":"%s","args":[],"caller":"%s"}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/smart'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':      
      classRef = 'wcEmltnService:WcEmltnBySgmt'
      pdata = (self.jobId,classRef, self.resolve.txnNum)
      params = '{"type":"delegate","id":"%s","service":"%s","args":[%d]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/async/%d' % self.resolve.sgmtCount
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcResolveUnit
# ---------------------------------------------------------------#
class WcResolveUnit(AppResolveUnit):
  
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
    logger.info('[START] landriveMount')    
    self.state.current = 'XML_TO_SAS'
    self.pmeta = pmeta
    self.mountPath = 'webapi/wcemltn/session'
    logger.info('landrive mount path : ' + self.mountPath)
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if not unMounted:
      logger.info('landrive is already mounted : ' + self.mountPath)
      return

    logger.info('wcemltn service base : ' + self.pmeta['localBase'])
    cifsEnv = os.environ['HOME'] + '/.wcemltn/cifs_env'
    with open(cifsEnv,'w') as fhw:
      fhw.write('ADUSER=%s\n' % os.environ['USER'])
      fhw.write("DFSPATH='%s'\n" % self.pmeta['localBase'])
      fhw.write('MOUNTINST=%s\n' % self.mountPath)
    
    credentials = os.environ['HOME'] + '/.landrive'
    runDir = os.environ['HOME'] + '/.wcemltn'
    sysArgs = ['sudo','landrive.sh','--mountcifs','--cifsenv','cifs_env','--credentials',credentials]
    self.sysCmd(sysArgs,cwd=runDir)

    sysArgs = ['grep','-w',self.mountPath,'/etc/mtab']
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if unMounted:
      errmsg = 'failed to mount landrive : ' + self.mountPath
      logger.error(errmsg)
      raise Exception(errmsg)

    linkPath = self.pmeta['linkBase']
    logger.info('landrive symlink : ' + linkPath)
    self.landrive = '/lan/%s/%s' % (os.environ['USER'], self.mountPath)
    logger.info('landrive : ' + self.landrive)
    if not os.path.exists(linkPath):
      self.sysCmd(['ln','-s', self.landrive, linkPath])

  # -------------------------------------------------------------- #
  # getTxnCount -
  # ---------------------------------------------------------------#
  def getTxnCount(self):
    sasPrgm = 'batchTxnScheduleWC.sas'
    logfile = 'log/batchTxnScheduleWC.log'
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    
    logger.info('run batchTxnScheduleWC.sas in subprocess ...')
    cwd = self.pmeta['progLib']
    stdout = self.runProcess(sysArgs,cwd=cwd)
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
    sysArgs = ['sas','-sysin',sasPrgm,'-set','txnCount',txnCount,'-log',logfile,'-logparm','open=replace']

    logger.info('run restackTxnOutputWC.sas in subprocess ...')
    cwd = self.pmeta['progLib']
    self.runProcess(sysArgs,cwd=cwd)

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
    sysArgs = ['sas','-sysin',sasPrgm,'-set','txnIndex',txnIndex,'-log',logfile,'-logparm','open=replace']
    
    logger.info('run batchScheduleWC.sas in subprocess, txn[%d] ...' % self.txnNum)
    cwd = self.pmeta['progLib']
    stdout = self.runProcess(sysArgs,cwd=cwd)
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
    sysArgs = ['sas','-sysin',sasPrgm,'-set','txnIndex',txnIndex,'-set','sgmtCount',sgmtCount]
    sysArgs += ['-log',logfile,'-logparm','open=replace']
    
    logger.info('run restackSgmtOutputWC.sas in subprocess txn[%d] ...' % self.txnNum)
    cwd = self.pmeta['progLib']
    self.runProcess(sysArgs,cwd=cwd)

# -------------------------------------------------------------- #
# WcEmltnBySgmt
# ---------------------------------------------------------------#
class WcEmltnBySgmt(SysCmdUnit):

  def __init__(self, leveldb, caller):
    self._leveldb = leveldb
    self.caller = caller
    
  # -------------------------------------------------------------- #
  # runEmltnBySgmt
  # ---------------------------------------------------------------#
  def __call__(self, txnNum, sgmtNum):
    dbKey = 'PMETA|PROGLIB|' + self.caller
    try:
      progLib = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! pmeta proglib param not found : ' + dbKey)
    sasPrgm = 'batchEmulatorWC_txn%d_s%d.sas' % (txnNum, sgmtNum)
    logfile = 'log/batchEmulatorWC_txn%d_s%d.log' % (txnNum, sgmtNum)
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']

    logger.info('run batchEmulatorWC_txn%d_s%d.sas in subprocess ...' % (txnNum, sgmtNum))
    self.runProcess(sysArgs,cwd=progLib)

# -------------------------------------------------------------- #
# WcEmltnListener
# ---------------------------------------------------------------#
class WcEmltnListener(AppListener):

  def __init__(self, leveldb, caller):
    super(WcEmltnListener, self).__init__(leveldb, caller)
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
    classRef = 'wcEmltnService:WcEmltnDirector'
    pdata = (self.caller,classRef, json.dumps({'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/smart'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcEmailUnit
# ---------------------------------------------------------------#
class WcEmailUnit(AppResolveUnit):
  
  def __init__(self):
    self.__dict__['XML_TO_SAS'] = self.XML_TO_SAS
    self.__dict__['TXN_REPEAT'] = self.TXN_REPEAT
    self.__dict__['TXN_SGMT_REPEAT'] = self.TXN_SGMT_REPEAT
    self.__dict__['TXN_SGMT_RESTACK'] = self.TXN_SGMT_RESTACK
    self._from = 'Pricing-Implementation-CI-Fasttracks@suncorp.com.au'
    self.pmeta = None
    self.state = None

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#  
  def _start(self, pmeta):
    self.pmeta = pmeta
    self._to = self.pmeta['userEmail']
    
  # -------------------------------------------------------------- #
  # TXN_SGMT_RESTACK
  # ---------------------------------------------------------------#  
  def sendMail(self, subject, body):

    msg = MIMEText(body, 'plain')
    msg['Subject'] = subject
    msg['From'] = self._from
    msg['To'] = self._to

    smtp = smtplib.SMTP('smlsmtp')
    smtp.sendmail(self._from, [self._to], msg.as_string())
    smtp.quit()

  # -------------------------------------------------------------- #
  # XML_TO_SAS
  # ---------------------------------------------------------------#
  def XML_TO_SAS(self, context):
    
    subject, body = self.getEmailBody(context)
    script = 'wcInputXml2Sas'
    if context == 'ERROR':
      body = body % (script, script, self.pmeta['progLib'])
    self.sendMail(subject, body)

  # -------------------------------------------------------------- #
  # TXN_REPEAT
  # ---------------------------------------------------------------#
  def TXN_REPEAT(self, context):

    if self.state.next == 'XML_TO_SAS':
      script = 'wcInputXmlToSas'
    elif self.state.next == 'TXN_REPEAT':
      script = 'batchTxnScheduleWC'
    elif self.state.next == 'COMPLETE':
      script = 'restackTxnOutputWC'
    subject, body = self.getEmailBody(context)
    if context == 'ERROR':
      body = body % (script, script, self.pmeta['progLib'])
    self.sendMail(subject, body)
    
  # -------------------------------------------------------------- #
  # TXN_SGMT_REPEAT
  # ---------------------------------------------------------------#
  def TXN_SGMT_REPEAT(self, context):
    
    subject, body = self.getEmailBody(context)
    script = 'batchScheduleWC'
    if context == 'ERROR':
      body = body % (script, script, self.pmeta['progLib'])
    self.sendMail(subject, body)

  # -------------------------------------------------------------- #
  # TXN_SGMT_RESTACK
  # ---------------------------------------------------------------#
  def TXN_SGMT_RESTACK(self, context):

    subject, body = self.getEmailBody(context)
    script = 'restackSgmtOutputWC'
    if context == 'ERROR':
      body = body % (script, script, self.pmeta['progLib'])
    self.sendMail(subject, body)

  # -------------------------------------------------------------- #
  # getEmailBody
  # ---------------------------------------------------------------#
  def getEmailBody(self, context, errMsg=None):

#    errNote = '\nsystem message : %s\n' % errMsg if errMsg else ''
    errBody = '''
  Hi CI workerscomp team,
    CI workerscomp %s.sas has errored :<
    Please inspect %s.log to get the error details
    Your workerscomp emulation log region is : %s/log
    Then contact us for further investigation
  Thanks and Regards,
  CI workerscomp emulation team
'''
    if context == 'ERROR':
      subject = 'ci workerscomp emulation has errored :<'
      return (subject, errBody)
    return ('','')

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
    self.pmeta = self.getProgramMeta()
    self.compileScript('batchTxnScheduleWC.sas')    
    self.compileScript('batchScheduleWC.sas')
    incItems = ['ciwork','macroLib']
    self.compileScript('restackSgmtOutputWC.sas',incItems=incItems)
    incItems = ['ciwork','macroLib','progLib','userEmail']
    self.compileScript('restackTxnOutputWC.sas',incItems=incItems)
    cwd = self.pmeta['assetLib']
    self.sysCmd(['cp','batchEmulatorWC.inc',self.pmeta['progLib']],cwd=cwd)
    self.putCredentials()
    return self.pmeta

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):

    dbKey = 'TSXREF|' + self.jobId
    tsXref = datetime.datetime.now().strftime('%y%m%d%H%M%S')
    self._leveldb.Put(dbKey, tsXref)

    dbKey = 'PMETA|' + self.jobId
    try:
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! pmeta json document not found : ' + dbKey)

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
    
    pmeta = pmetaAll['WcEmltnDirector']
    _globals = pmeta['globals'] # append global vars in this list
    del(pmeta['globals'])
    if _globals[0] == '*':
      pmeta.update(pmetaAll['Global'])
    else:
      for item in _globals:
        pmeta[item] = pmetaAll['Global'][item]
    return pmeta

  # -------------------------------------------------------------- #
  # putCredentials -
  # ---------------------------------------------------------------#
  def putCredentials(self):

    credentials = os.environ['HOME'] + '/.landrive'
    username = None
    password = None
    with open(credentials,'r') as fhr:
      for credItem in fhr:
        credItem = credItem.strip() 
        if 'username' in credItem:
          username = credItem.split('=')[1]
        elif 'password' in credItem:
          password = credItem.split('=')[1]
    if not username or not password:
      raise Exception('user landrive credentials are not valid')
    dbKey = 'CRED|' + self.jobId
    credentials = username + ':' + password
    self._leveldb.Put(dbKey,credentials)
