import os, sys, time
import logging
import json
import requests, re
from apiservice import AppDelegate, logger
from subprocess import Popen, PIPE

# -------------------------------------------------------------- #
# WcEmltnInputPrvdr
# ---------------------------------------------------------------#
class WcEmltnInputPrvdr(AppDelegate):

  def __init__(self, leveldb, jobId):
    super(WcEmltnInputPrvdr,self).__init__(leveldb, jobId=jobId)
    self.pmeta = None

	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def __call__(self):

    try:
      self.runApp()
    except Exception as ex:
      self.putApiRequest(500)
      
	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def runApp(self):

    self.loadProgramMeta()
    self.getPmovDataFromS3()    
    self.landriveMount()
    self.checkInputFile()
    self.compileSessionVars('wcInputXml2Sas.sas')
    sasPrgm = '%s/wcInputXml2Sas.sas' % self.pmeta['progLib']
    logfile = '%s/log/wcInputXml2Sas.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']

    logger.info('run wcInputXml2Sas.sas in subprocess ...')
    self.runProcess(sysArgs)
    self.putApiRequest(201)

	# -------------------------------------------------------------- #
	# convertXmlToSas
	# ---------------------------------------------------------------#
  def convertXmlToSas(self):

    env = Environment(loader=PackageLoader('apiservice', 'xmlToSas'),trim_blocks=True)
    sasPrgm = '%s/get.sas' % self.pmeta['progLib']
    logfile = '%s/log/wcInputXml2Sas.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    
	# -------------------------------------------------------------- #
	# getPmovDataFromS3
	# ---------------------------------------------------------------#
  def getPmovDataFromS3(self):

    pmovDir = 'pmov'
    if self.pmeta['ciwork'][-1] != '/':
      pmovDir = '/pmov'
    pmovLib = self.pmeta['ciwork'] + pmovDir
    self.pmeta['pmovLib'] = pmovLib
    
    if self.pmeta["inputTxnFilter"] != 'LAST_INF_BY_PCID':
      return
    try:      
      pmovDsId = datetime.datetime.strptime(self.pmeta['pmovDsId'],'%y%m')
    except ValueError:
      raise Exception('pmov dataset id not valid : ' + self.pmeta['pmovDsId'])

    if not os.path.exists(pmovLib):
      self.runProcess(['mkdir','-p',pmovLib])
    else:
      pmovDsName = 'pmov%s_combined.sas7bdat' % self.pmeta['pmovDsId']
      pmovDsPath = '%s/%s' % (pmovLib, pmovDsName)
      if os.path.exists(pmovDsPath):        
        lastPutStamp = os.path.getmtime(pmovDsPath)
        lastPut = datetime.datetime.fromtimestamp(lastPutStamp)
        today = datetime.datetime.today()
        if lastPut.day == today.day and lastPut.month == today.month and lastPut.year == today.year:
          logger.info(pmovDsName + ' has been transfered already today')
          logger.info('Note : wcEmltnService S3 transfer min period = daily')
          return

    pmovS3Dir = pmovDsId.strftime('%Y%m')
    if self.pmeta['pmovS3Base'][-1] != '/':
      pmovS3Dir = '/' + pmovS3Dir
    self.pmeta['pmovS3Repo'] += pmovS3Dir
    incItems = ['macroLib','pmovLib','pmovS3Repo','pmovDsId']
    self.compileSessionVars('getPmovFromS3.sas',incItems=incItems)
    
    sasPrgm = '%s/getPmovFromS3.sas' % self.pmeta['progLib']
    logfile = '%s/log/getPmovFromS3.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
        
    logger.info('run getPmovFromS3.sas in subprocess ...')
    self.runProcess(sysArgs)

  # -------------------------------------------------------------- #
  # landriveMount
  # ---------------------------------------------------------------#                                          
  def landriveMount(self):
    
    logger.info('[START] landriveMount')
    self.mountPath = 'webapi/wcemltn/session'
    logger.info('landrive mount path : ' + self.mountPath)
    unMounted = self.runProcess(['grep','-w',self.mountPath,'/etc/mtab'],returnRc=True)
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
    self.runProcess(sysArgs,cwd=runDir)

    sysArgs = ['grep','-w',self.mountPath,'/etc/mtab']
    unMounted = self.runProcess(['grep','-w',self.mountPath,'/etc/mtab'],returnRc=True)
    if unMounted:
      errmsg = 'failed to mount landrive : ' + self.mountPath
      logger.error(errmsg)
      raise Exception(errmsg)

  # -------------------------------------------------------------- #
  # checkInputFile
  # ---------------------------------------------------------------#                                          
  def checkInputFile(self):
    logger.info('[START] checkInputFile')

    linkPath = self.pmeta['linkBase'] + '/session'
    logger.info('landrive symlink : ' + linkPath)
    self.landrive = '/lan/%s/%s' % (os.environ['USER'], self.mountPath)
    logger.info('landrive : ' + self.landrive)
    if not os.path.exists(linkPath):
      self.runProcess(['ln','-s', self.landrive, linkPath])
    self.pmeta['inputXmlBase'] = linkPath + '/inputXml'

    inputXmlPath = self.pmeta['inputXmlBase'] + '/' + self.pmeta['inputXmlFile']
    try:
      self.runProcess(['ls', inputXmlPath])
    except Exception:
      errmsg = 'wcEmulation inputXmlFile is not found : ' +  inputXmlPath
      logger.error(errmsg)
      raise Exception(errmsg)
    else:
      logger.info('wcEmulation inputXmlFile : ' + inputXmlPath)
    logger.info('[END] checkInputFile')
    
  # -------------------------------------------------------------- #
  # loadProgramMeta
  # ---------------------------------------------------------------#                                          
  def loadProgramMeta(self):
    logger.debug('[START] loadProgramMeta')
    dbKey = 'PMETA|' + self.jobId
    try:
      _pmeta = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEEOWWW! pmeta json document not found : ' + dbKey)
    logger.info('dbKey : ' + dbKey)
    pmeta = json.loads(_pmeta)
    self.pmeta = pmeta['WcInputXml2Sas']
    _globals = self.pmeta['globals']
    del(self.pmeta['globals'])
    print('globals : ' + _globals[0])
    if _globals[0] == '*':
      self.pmeta.update(pmeta['Global'])
    else:
      for item in _globals:
        self.pmeta[item] = pmeta['Global'][item]

  # -------------------------------------------------------------- #
  # compileSessionVars
  # ---------------------------------------------------------------#
  def compileSessionVars(self, sasfile, incItems=None):
    logger.debug('[START] compileSessionVars')
    dbKey = 'TSXREF|' + self.jobId
    try:
      timestamp = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('timestamp job xref id not found for job : %s' % self.jobId) 
    self.pmeta['ciwork'] += '/WC%s' % timestamp
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
    for itemKey in sorted(self.pmeta.keys()):
      if not incItems or itemKey in incItems:
        fhw.write(puttext.format(itemKey, self.pmeta[itemKey]))

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'wcEmltnService:WcEmltnDirector'
    pdata = (self.jobId,classRef,json.dumps({'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/job/1'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)
