from apiservice import AppDirector, AppState, AppResolveUnit, AppListener, AppDelegate, logger
from email.mime.text import MIMEText
from threading import RLock
from subprocess import Popen, PIPE
import logging
import json
import os, sys, time
import requests, re
import smtplib
import uuid

# -------------------------------------------------------------- #
# WcEmltnDirector
# ---------------------------------------------------------------#
class WcEmltnDirector(AppDirector):

  def __init__(self, leveldb, jobId):
    super(WcEmltnDirector, self).__init__(leveldb, jobId)
    self.serviceName = 'WcEmltnDirector'
    self.state.hasNext = True
    self.resolve = WcResolveUnit()
    self.resolve.state = self.state
    self.mailer = WcEmailUnit()
    self.mailer.state = self.state
    
  # -------------------------------------------------------------- #
  # getServiceName
  # ---------------------------------------------------------------#                                                                                                          
  def getServiceName(self):
    return self.serviceName

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _START(self):
    logger.debug('[START] loadProgramMeta')
    _pmeta = self.getProgramMeta()
    pmeta = _pmeta['WcEmltnDirector']
    _globals = pmeta['globals'] # append global vars in this list
    del(pmeta['globals'])
    if _globals[0] == '*':
      pmeta.update(_pmeta['Global'])
    else:
      for item in _globals:
        pmeta[item] = _pmeta['Global'][item]    
    self.resolve.pmeta = pmeta
    self.resolve._START()
    self.mailer.pmeta = pmeta
    self.mailer._START()

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#                                          
  def getProgramMeta(self):

    dbKey = 'PMETA|' + self.state.jobId
    try:
      _pmeta = self._leveldb.Get(dbKey)
    except KeyError:
      raise BaseException('EEEOWWW! pmeta json document not found : ' + dbKey)
    return json.loads(_pmeta)

  # -------------------------------------------------------------- #
  # iterate
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    if self.state.transition == 'EMLTN_INPUT_PRVDR':
      # signal = the http status code of the listener promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        raise BaseException('WcEmltnInputPrvdr server process failed, rc : %d' % signal)
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      # signal = the http status code of the listener promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        raise BaseException('WcEmltnBySgmt server process failed, rc : %d' % signal)
    self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'EMLTN_INPUT_PRVDR':
      self.resolve.putApiRequest()
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      self.resolve.putApiRequest()

# -------------------------------------------------------------- #
# WcResolveUnit
# ---------------------------------------------------------------#
class WcResolveUnit(AppResolveUnit):
  def __init__(self):
    self.__dict__['INIT'] = self.INIT
    self.__dict__['TXN_REPEAT'] = self.TXN_REPEAT
    self.__dict__['TXN_SGMT_REPEAT'] = self.TXN_SGMT_REPEAT
    self.__dict__['TXN_SGMT_RESTACK'] = self.TXN_SGMT_RESTACK
    self.pmeta = None
    self.sgmtCount = 0
    self.txnNum = 0    
    self.txnCount = 0

  # -------------------------------------------------------------- #
  # _START
  # ---------------------------------------------------------------#  
  def _START(self):
    pass    
  # -------------------------------------------------------------- #
  # INIT - evalTxnCount
  # - state.current = 'INIT'
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def INIT(self):
    self.state.transition = 'EMLTN_INPUT_PRVDR'
    self.state.inTransition = True
    self.state.next = 'TXN_REPEAT'      
    self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # getTxnCount -
  # ---------------------------------------------------------------#
  def getTxnCount(self):
    sasPrgm = '%s/batchTxnScheduleWC.sas' % self.pmeta['progLib']
    logfile = '%s/log/batchTxnScheduleWC.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    logger.info('run batchTxnScheduleWC.sas in subprocess ...')

    try :
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
        logger.error('please investigate the sas error reported in batchTxnScheduleWC.log')
        raise BaseException('batchTxnScheduleWC.sas failed, stderr : ' + stderr)
      logger.info('program output : %s' % stdout)
    except OSError as ex:
      raise BaseException('WcResolveUnit failed, OSError : ' + str(ex))

    txnPacket = json.loads(stdout)
    self.txnCount = txnPacket['count']
    self.txnNum = 0

  # -------------------------------------------------------------- #
  # TXN_REPEAT - evalTxnRepeat
  # - state.current = 'TXN_REPEAT'
  # - state.next = 'TXN_SGMT_REPEAT'
  # ---------------------------------------------------------------#
  def TXN_REPEAT(self):
    if self.txnCount == 0:
      self.compileSessionVars('batchTxnScheduleWC.sas')
      self.state.next = 'TXN_REPEAT'
      self.getTxnCount()
      self.state.hasNext = True
      return self.state
    elif self.txnNum == self.txnCount:
      incItems = ['ciwork','macroLib','progLib','userEmail']
      self.compileSessionVars('restackTxnOutputWC.sas',incItems=incItems)
      self.state.next = 'COMPLETE'
      self.restackTxnOutputAll()
      self.state.complete = True
      self.state.hasNext = False
    else:
      self.txnNum += 1
      self.state.next = 'TXN_SGMT_REPEAT'
      self.state.hasNext = True
    return self.state

  # -------------------------------------------------------------- #
  # restackTxnOutputAll
  # ---------------------------------------------------------------#
  def restackTxnOutputAll(self):
    sasPrgm = '%s/restackTxnOutputWC.sas' % self.pmeta['progLib']
    logfile = '%s/log/restackTxnOutputWC.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    logger.info('run restackTxnOutputWC.sas in subprocess ...')
    try :
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
        logger.error('please investigate the sas error reported in restackTxnOutputWC.log')
        raise BaseException('restackTxnOutputWC.sas failed, stderr : ' + stderr)
    except OSError as ex:
      raise BaseException('WcResolveUnit failed, OSError : ' + str(ex))

  # -------------------------------------------------------------- #
  # TXN_SGMT_REPEAT - evalTxnSgmtRepeat
  # - state.current = 'TXN_SGMT_REPEAT'
  # - state.next = 'TXN_SGMT_RESTACK'
  # ---------------------------------------------------------------#
  def TXN_SGMT_REPEAT(self):
    self.compileSessionVars('batchScheduleWC.sas')
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
    sasPrgm = '%s/batchScheduleWC.sas' % self.pmeta['progLib']
    logfile = '%s/log/batchScheduleWC.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    logger.info('run batchScheduleWC.sas in subprocess, txn[%d] ...' % self.txnNum)
    try :
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
        logger.error('please investigate the sas error reported in batchScheduleWC.log')
        raise BaseException('batchScheduleWC.sas failed, stderr : ' + stderr)
      logger.info('program output : %s' % stdout)
    except OSError as ex:
      raise BaseException('WcResolveUnit failed, OSError : ' + str(ex))

    sgmtPacket = json.loads(stdout)
    self.sgmtCount = sgmtPacket['count']

  # -------------------------------------------------------------- #
  # TXN_SGMT_RESTACK - evalRestackSgmtAll
  # - state.current = 'TXN_SGMT_RESTACK'
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def TXN_SGMT_RESTACK(self):
    incItems = ['ciwork','macroLib']
    self.compileSessionVars('restackSgmtOutputWC.sas',incItems=incItems)
    self.state.next = 'TXN_REPEAT'
    self.restackSgmtOutputAll()
    self.sgmtCount = 0
    return self.state

  # -------------------------------------------------------------- #
  # restackSgmtOutput
  # ---------------------------------------------------------------#
  def restackSgmtOutputAll(self):
    sasPrgm = '%s/restackSgmtOutputWC.sas' % self.pmeta['progLib']
    logfile = '%s/log/restackSgmtOutputWC_txn%d.log' % (self.pmeta['progLib'], self.txnNum)
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    logger.info('run restackSgmtOutputWC.sas in subprocess txn[%d] ...' % self.txnNum)
    try :
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
        logger.error('please investigate the sas error reported in restackSgmtOutputWC.log')
        raise BaseException('restackSgmtOutputWC.sas failed, stderr : ' + stderr)
    except OSError as ex:
      raise BaseException('WcResolveUnit failed, OSError : ' + str(ex))

  # -------------------------------------------------------------- #
  # compileSessionVars
  # ---------------------------------------------------------------#
  def compileSessionVars(self, sasfile, incItems=None):
    logger.debug('[START] compileSessionVars')
    logger.debug('progLib : ' + self.pmeta['progLib'])
    tmpltName = '%s/tmplt/%s' % (self.pmeta['progLib'], sasfile)
    sasFile = '%s/%s' % (self.pmeta['progLib'], sasfile)
    fhr = open(tmpltName,'r')
    _puttext = '  %let {} = {};\n'
    with open(sasFile,'w') as fhw :
      for line in fhr:
        if re.search(r'<insert>', line):
          self.putMetaItems(_puttext, fhw, incItems)
        else:
          fhw.write(line)
    fhr.close()

  # -------------------------------------------------------------- #
  # putMetaItems
  # ---------------------------------------------------------------#                                          
  def putMetaItems(self, puttext, fhw, incItems):
    for itemKey, metaItem in self.pmeta.items():
      if not incItems or itemKey in incItems:
        fhw.write(puttext.format(itemKey, metaItem))

    if self.txnNum > 0:
      fhw.write(puttext.format('txnIndex', self.txnNum))
      fhw.write(puttext.format('txnCount', self.txnCount))
    if self.sgmtCount > 0:
      fhw.write(puttext.format('sgmtCount', self.sgmtCount))

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    
    if self.state.transition == 'EMLTN_INPUT_PRVDR':
      classRef = 'wcEmltnInputPrvdr:WcEmltnInputPrvdr'
      pdata = (self.state.jobId,classRef)
      params = '{"type":"delegate","id":"%s","responder":"self","service":"%s","args":[]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/job/1'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.transition == 'EMLTN_BYSGMT_NOWAIT':      
      classRef = 'wcEmltnService:WcEmltnBySgmt'
      args = [self.pmeta['progLib'], self.txnNum]
      pdata = (self.state.jobId,classRef, json.dumps(args))
      params = '{"type":"delegate","id":"%s","responder":"listener","service":"%s","args":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/job/%d' % self.sgmtCount
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcEmltnBySgmt
# ---------------------------------------------------------------#
class WcEmltnBySgmt(AppDelegate):

  def __init__(self, leveldb):
    super(WcEmltnBySgmt, self).__init__(leveldb)
    
  # -------------------------------------------------------------- #
  # runEmltnBySgmt
  # ---------------------------------------------------------------#
  def __call__(self, progLib, txnNum, sgmtNum):
    sasPrgm = '%s/batchEmulatorWC_txn%d_s%d.sas' % (progLib, txnNum, sgmtNum)
    logfile = '%s/log/batchEmulatorWC_txn%d_s%d.log' % (progLib, txnNum, sgmtNum)
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    logger.info('run batchEmulatorWC_txn%d_s%d.sas in subprocess ...' % (txnNum, sgmtNum))                                  
    try :
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
    except BaseException as ex:
      logger.error('failed to create subprocess : %s' % str(ex))
    if prcss.returncode:
      logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
      logger.error('please investigate the sas error reported in batchEmulatorWC_txn%d_s%d.log' % (txnNum, sgmtNum))

# -------------------------------------------------------------- #
# WcEmltnListener
# ---------------------------------------------------------------#
class WcEmltnListener(AppListener):

  def __init__(self, leveldb):
    super(WcEmltnListener, self).__init__(leveldb)
    self.state = None
    self.jobIdList = []

  def __call__(self, event):    
    if not event.exception and self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      if event.job_id in self.jobIdList:
        with self.state.lock:
          self.jobIdList.remove(event.job_id)
          if not self.jobIdList:
            self.putApiRequest()

  # -------------------------------------------------------------- #
  # addJob - add a live job id
  # ---------------------------------------------------------------#
  def addJob(self, jobId):
    self.jobIdList = [jobId]

  # -------------------------------------------------------------- #
  # addJobs - add a list of live job ids
  # ---------------------------------------------------------------#
  def addJobs(self, jobRange):

    self.jobIdList = jobIds = []
    for jobNum in jobRange:
      jobId = str(uuid.uuid4())
      jobIds.append(jobId)
    return jobIds

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    classRef = 'wcEmltnService:WcEmltnDirector'
    pdata = (self.state.jobId,classRef, json.dumps({'signal':201}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/job/1'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcEmailUnit
# ---------------------------------------------------------------#
class WcEmailUnit(AppResolveUnit):
  def __init__(self):
    self.__dict__['INIT'] = self.INIT
    self.__dict__['TXN_REPEAT'] = self.TXN_REPEAT
    self.__dict__['TXN_SGMT_REPEAT'] = self.TXN_SGMT_REPEAT
    self.__dict__['TXN_SGMT_RESTACK'] = self.TXN_SGMT_RESTACK
    self._from = 'CI workerscomp emulation team <Pricing-Implementation-CI-Fasttracks@suncorp.com.au>'
    self.pmeta = None
    self.state = None

  # -------------------------------------------------------------- #
  # _START
  # ---------------------------------------------------------------#  
  def _START(self):
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
  # INIT
  # ---------------------------------------------------------------#
  def INIT(self, context):
    
    subject, body = self.getEmailBody(context)
    script = 'wcInputXml2Sas'
    if context == 'ERROR':
      body = body % (script, script, self.pmeta['progLib'])
    self.sendMail(subject, body)

  # -------------------------------------------------------------- #
  # TXN_REPEAT
  # ---------------------------------------------------------------#
  def TXN_REPEAT(self, context):

    if self.state.next == 'TXN_REPEAT':
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
  def getEmailBody(self, context):

    errorBody = '''
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
      return (subject, errorBody)
    return ('','')


