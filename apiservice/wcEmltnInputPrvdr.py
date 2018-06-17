import os, sys, time
import logging
import json
import requests, re
from apiservice import AppDelegate, logger

# -------------------------------------------------------------- #
# WcEmltnInputPrvdr
# ---------------------------------------------------------------#
class WcEmltnInputPrvdr(AppDirector):

  def __init__(self, leveldb, jobId):
    super(WcEmltnInputPrvdr,self).__init__(leveldb, jobId)
    self.appType = 'delegate'
    self.state.hasNext = True
    self.resolve = WcResolveUnit(leveldb)
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    scriptPrvdr = WcScriptPrvdr(self._leveldb,jobId=self.jobId)
    self.resolve.pmeta = scriptPrvdr()
    self.resolve._start()

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    if self.state.transition == 'NORMALISE_XML':
      # signal = the http status code of the companion promote method
      if signal == 201:
        logger.info('state transition is resolved, advancing ...')
        self.state.transition = 'NA'
        self.state.inTransition = False
      else:
        raise Exception('WcEmltnInputPrvdr server process failed, rc : %d' % signal)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#                                          
  def getProgramMeta(self):
    logger.debug('[START] getProgramMeta')
    dbKey = 'PMETA|' + self.jobId
    try:
      _pmeta = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEEOWWW! pmeta json document not found : ' + dbKey)

  # -------------------------------------------------------------- #
  # onComplete
  # ---------------------------------------------------------------#
  def onComplete(self):
    self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, errorMsg):
    self.putApiRequest(500)

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'NORMALISE_XML':
      self.resolve.putApiRequest()

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    if self.state.transition == 'STREAM_TO_SAS':
      classRef = 'xmlToStream:XmlToStream'
      pdata = (self.state.jobId,classRef)
      params = '{"type":"director","id":"%s","responder":"self","service":"%s","args":[]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/job/1'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.complete:
      classRef = 'wcEmltnService:WcEmltnDirector'
      pdata = (self.jobId,classRef,json.dumps({'signal':signal}))
      params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/job/1'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcResolveUnit
# ---------------------------------------------------------------#
class WcResolveUnit(AppResolveUnit):
  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.__dict__['NORMALISE_XML'] = self.NORMALISE_XML
    self.__dict__['IMPORT_TO_SAS'] = self.IMPORT_TO_SAS
    self.__dict__['GET_PMOV_DATA'] = self.GET_PMOV_DATA
    self.__dict__['TRANSFORM_SAS'] = self.TRANSFORM_SAS
    self.pmeta = None

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self):
    dbKey = 'CRED|' + self.jobId
    credentials = self._leveldb.Get(dbKey)
    stashPath = '/%s/%s' % (self.pmeta['stashHost'],self.pmeta['stashBase'])
    uriPath = '%s/%s/%s?raw' % (stashPath,self.pmeta['schemaRepo'],self.pmeta['xmlSchema'])
    xmlSchema = '%s/%s' %  (self.pmeta['ciwork'],self.pmeta['xmlSchema'])
    self.sysCmd(['curl','-u',credentials,uriPath,'-o',xmlSchema])
    self.importXmlSchema(xmlSchema)
    
  # -------------------------------------------------------------- #
  # importXmlSchema
  # ---------------------------------------------------------------#
  def importXmlSchema(self, xmlSchema):
    
    with open(xmlSchema,'r') as fhr:
      schemaDoc = json.load(fhr)
    tableDefn = schemaDoc['tableDefn']
    columnOrder = schemaDoc['columnOrder']
    columnDefn = schemaDoc['columnDefn']

    sasDefn = {}
    for tableName in sorted(tableDefn):
      _sasDefn = []
      for nodeKey in columnOrder[tableName]
        for defnItem in columnDefn[nodeKey]:
          del(defnItem[1])
          _sasDefn += self.getSasFormat(defnItem)
      sasDefn[tableName] = _sasDefn
    self.sasDefn = sasDefn

  # -------------------------------------------------------------- #
  # getSasFormat
  # ---------------------------------------------------------------#
  def getSasFormat(self, fmtItem):
    fmtItem += '$' if fmtItem[1][0] == '$' else ''
    return [fmtItem]
    
	# -------------------------------------------------------------- #
	# NORMALISE_XML
	# ---------------------------------------------------------------#
  def NORMALISE_XML(self):
    self.state.transition = 'NORMALISE_XML'
    self.state.inTransition = True
    self.state.next = 'IMPORT_TO_SAS'
    self.state.hasNext = True
    return self.state

	# -------------------------------------------------------------- #
	# IMPORT_TO_SAS
	# ---------------------------------------------------------------#
  def IMPORT_TO_SAS(self):
    env = Environment(loader=PackageLoader('apiservice', 'xmlToSas'),trim_blocks=True)
    template = env.get_template('streamToSas.jinja')    
    for tableName in sorted(self.sasDefn):
      sasDefn = self.sasDefn[tableName]
      sasPrgm = '%s/%s.sas' % (self.pmeta['progLib'],tableName)
      template.stream(jobId=self.jobId,tableName=tableName,sasDefn=sasDefn).dump(sasPrgm)

      logfile = '%s/log/%s.log' % (self.pmeta['progLib'],tableName)
      sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
      self.runProcess(sysArgs)
    self.state.next = 'GET_PMOV_DATA'
    self.state.hasNext = True
    return self.state

	# -------------------------------------------------------------- #
	# GET_PMOV_DATA
	# ---------------------------------------------------------------#
  def GET_PMOV_DATA(self):

    self.state.next = 'TRANSFORM_SAS'
    self.state.hasNext = True
    if self.pmeta["inputTxnFilter"] != 'LAST_INF_BY_PCID':
      return self.state

    pmovDir = 'pmov'
    if self.pmeta['ciwork'][-1] != '/':
      pmovDir = '/pmov'
    pmovLib = self.pmeta['ciwork'] + pmovDir
    self.pmeta['pmovLib'] = pmovLib

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
          return self.state

    S3Dir = pmovDsId.strftime('%Y%m')
    if self.pmeta['S3Path'][-1] != '/':
      S3Dir = '/' + S3Dir
    self.pmeta['S3Path'] += S3Dir
    incItems = ['macroLib','pmovLib','S3Path','pmovDsId']
    self.compileSessionVars('getPmovFromS3.sas',incItems=incItems)
    
    sasPrgm = '%s/getPmovFromS3.sas' % self.pmeta['progLib']
    logfile = '%s/log/getPmovFromS3.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
        
    logger.info('run getPmovFromS3.sas in subprocess ...')
    self.runProcess(sysArgs)
    return self.state

	# -------------------------------------------------------------- #
	# TRANSFORM_SAS
	# ---------------------------------------------------------------#
  def TRANSFORM_SAS(self):

    sasPrgm = '%s/wcInputXml2Sas.sas' % self.pmeta['progLib']
    logfile = '%s/log/wcInputXml2Sas.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']

    logger.info('run wcInputXml2Sas.sas in subprocess ...')
    self.runProcess(sysArgs)
    self.state.hasNext = False
    self.state.complete = True
    
# -------------------------------------------------------------- #
# WcScriptPrvdr
# ---------------------------------------------------------------#
class WcScriptPrvdr(SasScriptPrvdr):

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):

    pmeta = self.getProgramMeta()
    dbKey = 'TSXREF|' + self.jobId
    try:
      tsXref = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! tsXref param not found : ' + dbKey)
    self.compileSessionVars('wcInputXml2Sas.sas')

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    dbKey = 'PMETA|' + self.state.jobId
    try:
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! pmeta json document not found : ' + dbKey)

    _pmeta = json.loads(pmetadoc)
    pmeta = _pmeta['WcInputXmlToSas']
    _globals = pmeta['globals'] # append global vars in this list
    del(pmeta['globals'])
    if _globals[0] == '*':
      pmeta.update(_pmeta['Global'])
    else:
      for item in _globals:
        pmeta[item] = _pmeta['Global'][item]
    return pmeta
