import os, sys, time
import logging
import json
import requests, re
from apiservice import AppDirector, AppState
from threading import RLock
from subprocess import Popen, PIPE

# -------------------------------------------------------------- #
# WcEmltnDirector
# ---------------------------------------------------------------#
class WcEmltnDirector(AppDirector):

  def __init__(self, jobstore, jobId):
    super(WcEmltnDirector, self).__init__()
    self._jobstore = jobstore
    self.jobId = jobId
    self.resolve = WcResolveUnit()
    self.resolve.state = self.state
    self.serviceName = 'WCEMLTN'                            
    self.logger = logging.getLogger('apscheduler.wcemltn')

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
      status = json.loads(signal)
      if status['rc'] == 0:
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        raise BaseException('WcEmltnBySgmt server process failed, rc : %d' % status['rc'])
    self.state.current = self.state.next

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):
      
    if self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      self.logger.info('running emltn service[EMLTN_BYSGMT_NOWAIT]')
      apiUrl, data = self.resolve.getRequestArgs()
      self.resolve.putApiRequest()

# -------------------------------------------------------------- #
# WcResolveUnit
# ---------------------------------------------------------------#
class WcResolveUnit(object):
  def __init__(self):
    self.__dict__['INIT'] = self.INIT
    self.__dict__['TXN_REPEAT'] = self.TXN_REPEAT
    self.__dict__['TXN_SGMT_REPEAT'] = self.TXN_SGMT_REPEAT
    self.__dict__['TXN_SGMT_RESTACK'] = self.TXN_SGMT_REPEAT
    self.pmeta = None
    self.logger = logging.getLogger('apscheduler.wcemltn')

  def __getitem__(self, key):
      return self.__dict__[key]

  def __setitem__(self, key, value):
      self.__dict__[key] = value

  def __delitem__(self, key):
      del self.__dict__[key]

  def __contains__(self, key):
      return key in self.__dict__

  def __len__(self):
      return len(self.__dict__)

  def __repr__(self):
      return repr(self.__dict__)

  # -------------------------------------------------------------- #
  # INIT - evalTxnCount
  # - state.current = 'INIT'
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def INIT(self):
    self.logger.debug('[start] evalTxnCount')
    #self.compileSessionVars('batchTxnScheduleWC.sas')
    self.getTxnCount()
    self.state.next = 'TXN_REPEAT'
    self.hasNext = True
    return state

  # -------------------------------------------------------------- #
  # getTxnCount -
  # ---------------------------------------------------------------#
  def getTxnCount(self):
    #self.logger.debug('[start] getTxnCount')
    #sasPrgm = '%s/batchTxnScheduleWC.sas' % self.pmeta['progLib']
    #logfile = '%s/log/batchTxnScheduleWC.log' % self.pmeta['progLib']
    #sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    #self.logger.info('run batchTxnScheduleWC.sas in subprocess ...')

    #try :
    #  prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
    #  (stdout, stderr) = prcss.communicate()
    #  if prcss.returncode:
    #    raise
    #  self.logger.info('program output : %s' % stdout)
    #except BaseException as ex:
    #  self.logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
    #  self.logger.error('please investigate the sas error reported in batchTxnScheduleWC.log')
    #  raise

    #txnPacket = json.loads(stdout)
    #self.txnCount = txnPacket['count']
    time.sleep(2)
    self.txnCount = 1
    self.txnNum = 0

  # -------------------------------------------------------------- #
  # TXN_REPEAT - evalTxnRepeat
  # - state.current = 'TXN_REPEAT'
  # - state.next = 'TXN_SGMT_REPEAT'
  # ---------------------------------------------------------------#
  def TXN_REPEAT(self):
    if self.txnNum == self.txnCount:
      incItems = ['macroLib','ciwork','userEmail']
      #self.compileSessionVars('restackTxnOutputWC.sas',incItems=incItems)
      self.restackTxnOutputAll(self)
      self.state.next = 'DONE'
      self.hasNext = False
    else:
      self.txnNum += 1
      self.state.next = 'TXN_SGMT_REPEAT'
      self.hasNext = True

  # -------------------------------------------------------------- #
  # restackTxnOutputAll
  # ---------------------------------------------------------------#
  def restackTxnOutputAll(self):
    #sasPrgm = '%s/restackTxnOutputWC.sas' % self.pmeta['progLib']
    #sysArgs = ['sas','-sysin',sasPrgm]
    #self.logger.info('running wrkcmp emulation transaction output restack by txn')
    #try :
    #  prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
    #  (stdout, stderr) = prcss.communicate()
    #  if prcss.returncode:
    #    raise
    #except BaseException as ex:
    #  self.logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
    #  self.logger.error('please investigate the sas error reported in restackTxnOutputWC.log')
    time.sleep(3)

  # -------------------------------------------------------------- #
  # TXN_SGMT_REPEAT - evalTxnSgmtRepeat
  # - state.current = 'TXN_SGMT_REPEAT'
  # - state.next = 'TXN_SGMT_RESTACK'
  # ---------------------------------------------------------------#
  def TXN_SGMT_REPEAT(self):
    #self.compileSessionVars('batchScheduleWC.sas')
    self.getTxnSgmtCount()
    self.state.transition = 'EMLTN_BYSGMT_NOWAIT'
    self.state.inTransition = True
    self.state.next = 'TXN_SGMT_RESTACK'      
    self.hasNext = True
    return state

  # -------------------------------------------------------------- #
  # getTxnSgmtCount
  # ---------------------------------------------------------------#
  def getTxnSgmtCount(self):
    #sasPrgm = '%s/batchScheduleWC.sas' % self.pmeta['progLib']
    #logfile = '%s/log/batchScheduleWC.log' % self.pmeta['progLib']
    #sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    #self.logger.info('run batchScheduleWC.sas in subprocess, txn[%d] ...' % self.txnNum)
    #try :
    #  prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
    #  (stdout, stderr) = prcss.communicate()
    #  if prcss.returncode:
    #    raise

    #  self.logger.info('program output : %s' % stdout)
    #except BaseException as ex:
    #  self.logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
    #  self.logger.error('please investigate the sas error reported in batchScheduleWC.log')
    #  raise

    #sgmtPacket = json.loads(stdout)
    #self.sgmtCount = sgmtPacket['count']
    time.sleep(3)
    self.sgmtCount = 2

  # -------------------------------------------------------------- #
  # TXN_SGMT_RESTACK - evalRestackSgmtAll
  # - state.current = 'TXN_SGMT_RESTACK'
  # - state.next = 'TXN_REPEAT'
  # ---------------------------------------------------------------#
  def TXN_SGMT_RESTACK(self):
    incItems = ['macroLib','ciwork']
    #self.compileSessionVars('restackSgmtOutputWC.sas',incItems=incItems)
    self.restackSgmtOutputAll(self)
    self.sgmtCount = None
    self.state.next = 'TXN_REPEAT'
    return state

  # -------------------------------------------------------------- #
  # restackSgmtOutput
  # ---------------------------------------------------------------#
  def restackSgmtOutputAll(self):
    #sasPrgm = '%s/restackSgmtOutputWC.sas' % self.progLib
    #sysArgs = ['sas','-sysin',sasPrgm]
    #self.logger.info('running wrkcmp emulation transaction[%d] output restack by segment' % txnNum)
    #try :
    #  prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
    #  (stdout, stderr) = prcss.communicate()
    #  if prcss.returncode:
    #    raise
    #except BaseException as ex:
    #  self.logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
    #  self.logger.error('please investigate the sas error reported in restackSgmtOutputWC.log')
    time.sleep(2)

  # -------------------------------------------------------------- #
  # loadProgramMeta
  # ---------------------------------------------------------------#                                          
  def loadProgramMeta(self):
    self.logger.debug('[start] loadProgramMeta')
    with open('./wcEmulation.json') as fhr:
      pmeta = json.load(fhr)
      self.pmeta = pmeta['Session']

  # -------------------------------------------------------------- #
  # compileSessionVars
  # ---------------------------------------------------------------#
  def compileSessionVars(self, sasfile, incItems=None):
    self.logger.debug('[start] compileSessionVar')
    if not self.pmeta:
      self.loadProgramMeta()
    self.logger.debug('progLib : ' + self.pmeta['progLib'])
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

    fhw.write(puttext.format('txnIndex', self.txnNum))
    if self.sgmtCount:
      fhw.write(puttext.format('sgmtCount', self.sgmtCount))

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    classRef = 'wcEmltnService:WcEmltnBySgmt'
    args = [self.pmeta['progLib'], self.txnNum]
    jobParams = '{"service":"%s","args":%s}' % (classRef, json.dumps(args))
    data = [('job',jobParams)]
    apiUrl = 'http://localhost:5000/api/v1/job/%d' % self.sgmtCount
    response = requests.post(apiUrl,data=data)
    self.logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcEmltnBySgmt
# ---------------------------------------------------------------#
class WcEmltnBySgmt(object):

  # -------------------------------------------------------------- #
  # runEmltnBySgmt
  # ---------------------------------------------------------------#                                        
  def __call__(self, progLib, txnNum, sgmtNum):
    #sasPrgm = '%s/batchEmulatorWC_txn%d_s%d.sas' % (progLib, txnNum, sgmtNum)
    #logfile = '%s/log/batchEmulatorWC_txn%d_s%d.log' % (progLib, txnNum, sgmtNum)
    #sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    #self.logger.info('running batchEmulatorWC_txn%d_s%d.sas in subprocess ...' % (txnNum, sgmtNum))                                  
    #try :
    #  prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
    #  (stdout, stderr) = prcss.communicate()
    #except BaseException as ex:
    #  self.logger.error('failed to create subprocess : %s' % str(ex))
    #if prcss.returncode:
    #  self.logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
    #  self.logger.error('please investigate the sas error reported in batchEmulatorWC_txn%d_s%d.log' % (txnNum, sgmtNum))
    time.sleep(15)

# -------------------------------------------------------------- #
# WcEmltnListener
# ---------------------------------------------------------------#
class WcEmltnListener(object):

  def __init__(self, jobstore, jobId):
    self._jobstore = jobstore
    self.jobId = jobId
    self.state = None
    self.jobIdList = []

  def __call__(self, event):    
    if not event.exception and self.state.transition == 'EMLTN_BYSGMT_NOWAIT':
      if event.job_id in self.jobIdList:
        with self.state.lock:
          self.jobIdList.remove(event.job_id)
          if not self.jobIdList:
            self.putApiRequest()

  def addJob(self, jobId):
    self.jobIdList = [jobId]

  def addJobs(self, jobRange):

    self.jobIdList = jobIds = []
    for jobNum in jobRange:
      jobIds.append(uuid.uuid4())
    return jobIds

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self):
    classRef = 'wcEmltnService:WcEmltnDirector'
    jobParams = '{"service":"%s","kwargs":%s,"args":[]}' % (classRef, json.dumps({'signal':0}))
    data = [('job',jobParams)]
    apiUrl = 'http://localhost:5000/api/v1/job/1'
    response = requests.post(apiUrl,data=data)
    self.logger.info('api response ' + response.text)
    

