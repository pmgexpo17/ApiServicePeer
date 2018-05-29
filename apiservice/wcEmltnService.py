import os, sys, time
import logging
import json
import requests, re
import uuid
from apiservice import AppDirector, AppState, AppResolveUnit, AppListener, AppDelegate, logger
from threading import RLock
from subprocess import Popen, PIPE

# -------------------------------------------------------------- #
# WcEmltnDirector
# ---------------------------------------------------------------#
class WcEmltnDirector(AppDirector):

  def __init__(self, leveldb, jobId):
    super(WcEmltnDirector, self).__init__()
    self._leveldb = leveldb
    self.serviceName = 'WCEMLTN'
    self.state.hasNext = True
    self.state.jobId = jobId
    self.resolve = WcResolveUnit()
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # getServiceName
  # ---------------------------------------------------------------#                                                                                                          
  def getServiceName(self):
    return self.serviceName

  # -------------------------------------------------------------- #
  # iterate
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    if self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
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
    if self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      logger.info('state transition : ' + self.state.transition)
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
  # INIT - evalTxnCount
  # - state.current = 'INIT'
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def INIT(self):
    self.compileSessionVars('batchTxnScheduleWC.sas')
    self.getTxnCount()
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
        raise
      logger.info('program output : %s' % stdout)
    except BaseException as ex:
      logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
      logger.error('please investigate the sas error reported in batchTxnScheduleWC.log')
      raise

    txnPacket = json.loads(stdout)
    self.txnCount = txnPacket['count']
    self.txnNum = 0

  # -------------------------------------------------------------- #
  # TXN_REPEAT - evalTxnRepeat
  # - state.current = 'TXN_REPEAT'
  # - state.next = 'TXN_SGMT_REPEAT'
  # ---------------------------------------------------------------#
  def TXN_REPEAT(self):
    if self.txnNum == self.txnCount:
      incItems = ['ciwork','macroLib','progLib','userEmail']
      self.compileSessionVars('restackTxnOutputWC.sas',incItems=incItems)
      self.restackTxnOutputAll()
      self.state.next = 'COMPLETE'
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
        raise
    except BaseException as ex:
      logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
      logger.error('please investigate the sas error reported in restackTxnOutputWC.log')

  # -------------------------------------------------------------- #
  # TXN_SGMT_REPEAT - evalTxnSgmtRepeat
  # - state.current = 'TXN_SGMT_REPEAT'
  # - state.next = 'TXN_SGMT_RESTACK'
  # ---------------------------------------------------------------#
  def TXN_SGMT_REPEAT(self):
    self.compileSessionVars('batchScheduleWC.sas')
    self.getTxnSgmtCount()
    self.state.transition = 'EMLTN_BYSGMT_NOWAIT'
    self.state.inTransition = True
    self.state.next = 'TXN_SGMT_RESTACK'      
    self.state.hasNext = True
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
        raise

      logger.info('program output : %s' % stdout)
    except BaseException as ex:
      logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
      logger.error('please investigate the sas error reported in batchScheduleWC.log')
      raise

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
    self.restackSgmtOutputAll()
    self.sgmtCount = 0
    self.state.next = 'TXN_REPEAT'
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
        raise
    except BaseException as ex:
      logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
      logger.error('please investigate the sas error reported in restackSgmtOutputWC.log')

  # -------------------------------------------------------------- #
  # loadProgramMeta
  # ---------------------------------------------------------------#                                          
  def loadProgramMeta(self):
    logger.debug('[START] loadProgramMeta')
    with open('./wcEmulation.json') as fhr:
      pmeta = json.load(fhr)
      self.pmeta = pmeta['Session']

  # -------------------------------------------------------------- #
  # compileSessionVars
  # ---------------------------------------------------------------#
  def compileSessionVars(self, sasfile, incItems=None):
    logger.debug('[START] compileSessionVars')
    if not self.pmeta:
      self.loadProgramMeta()
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

    if self.txnNum:
      fhw.write(puttext.format('txnIndex', self.txnNum))
      fhw.write(puttext.format('txnCount', self.txnCount))
    if self.sgmtCount:
      fhw.write(puttext.format('sgmtCount', self.sgmtCount))

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    classRef = 'wcEmltnService:WcEmltnBySgmt'
    args = [self.pmeta['progLib'], self.txnNum]
    pdata = (self.state.jobId,classRef, json.dumps(args))
    params = '{"type":"delegate","id":"%s","service":"%s","args":%s}' % pdata
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



