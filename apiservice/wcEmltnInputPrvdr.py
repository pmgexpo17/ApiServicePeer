from apiservice import AppDirector, AppResolveUnit, SasScriptPrvdr, logger
from jinja2 import Environment, PackageLoader
import os, sys, time
import json
import requests

# -------------------------------------------------------------- #
# WcEmltnInputPrvdr
# ---------------------------------------------------------------#
class WcEmltnInputPrvdr(AppDirector):

  def __init__(self, leveldb, jobId, caller):
    super(WcEmltnInputPrvdr,self).__init__(leveldb, jobId)
    self.caller = caller
    self.appType = 'delegate'
    self.state.hasNext = True
    self.resolve = WcResolveUnit(leveldb)
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # runApp
  # - override to get the callee jobId sent from NormalizeXml partner
  # ---------------------------------------------------------------#
  def runApp(self, signal=None, callee=None):
    logger.info('[START] WcEmltnInputPrvdr.runApp, callee : ' + str(callee))
    self.resolve.callee = callee    
    super(WcEmltnInputPrvdr, self).runApp(signal)

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    logger.info('[START] WcEmltnInputPrvdr._start, caller : ' + str(self.caller))
    scriptPrvdr = WcScriptPrvdr(self._leveldb,self.jobId,self.caller)
    tsXref, pmeta = scriptPrvdr()
    self.resolve._start(tsXref, pmeta)

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
    self.state.current = self.state.next
    return self.state

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
      self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    if self.state.transition == 'NORMALISE_XML':
      classRef = 'normalizeXml:NormalizeXml'
      pdata = (classRef,json.dumps([self.caller,self.jobId]))
      params = '{"type":"director","id":null,"responder":"self","service":"%s","args":[],"caller":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/smart'
      response = requests.put(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.complete:
      classRef = 'wcEmltnService:WcEmltnDirector'
      pdata = (self.caller,classRef,json.dumps({'signal':signal}))
      params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/async/1'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# WcResolveUnit
# ---------------------------------------------------------------#
class WcResolveUnit(AppResolveUnit):
  
  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.callee = None
    self.pmeta = None
    self.__dict__['NORMALISE_XML'] = self.NORMALISE_XML
    self.__dict__['IMPORT_TO_SAS'] = self.IMPORT_TO_SAS
    self.__dict__['GET_PMOV_DATA'] = self.GET_PMOV_DATA
    self.__dict__['TRANSFORM_SAS'] = self.TRANSFORM_SAS
    
  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, tsXref, pmeta):
    logger.info('[START] WcResolveUnit._start')
    self.tsXref = tsXref
    self.pmeta = pmeta
    #self.state.current = 'NORMALISE_XML'
    self.state.current = 'IMPORT_TO_SAS'
    trnswrk = self.pmeta['ciwork'] + '/ssnwork/trnswrk'
    cnvtwrk = self.pmeta['ciwork'] + '/ssnwork/cnvtwrk'
    assets = self.pmeta['ciwork'] + '/assets'
    self.sysCmd(['mkdir','-p',trnswrk])
    self.sysCmd(['mkdir',cnvtwrk])
    self.sysCmd(['mkdir',assets])    
    xmlSchema = '%s/%s' % (self.pmeta['assetLib'], self.pmeta['xmlSchema'])
    self.sysCmd(['cp',xmlSchema,assets]) # copy to here for normalizer to use
    try:
      self.makeSasFormat(xmlSchema)
    except Exception as ex:
      logger.info('error making sas format dict : ' + str(ex))
      raise
    
  # -------------------------------------------------------------- #
  # makeSasFormat
  # ---------------------------------------------------------------#
  def makeSasFormat(self, xmlSchema):
    logger.info('[START] WcResolveUnit.makeSasFormat')
    with open(xmlSchema,'r') as fhr:
      schemaDoc = json.load(fhr)
    ukeyOrder = schemaDoc['uKeyOrder']
    tableDefn = schemaDoc['tableDefn']
    columnOrder = schemaDoc['columnOrder']
    columnDefn = schemaDoc['columnDefn']

    for tableName in ukeyOrder:
      self.putUKeySasDefn(tableName, tableDefn[tableName], columnDefn)
      
    sasDefn = {}
    for tableName, detail in tableDefn.items():
      if not detail['keep']:
        continue
      _sasDefn = self.getFKeySasDefn(detail['fkeyRef'])
      for nodeKey in columnOrder[tableName]:
        for defnItem in columnDefn[nodeKey]:
          del(defnItem[1])
          _sasDefn += self.getSasFormat(defnItem)
      sasDefn[tableName] = _sasDefn
    self.sasDefn = sasDefn

  # -------------------------------------------------------------- #
  # putUKeySasDefn
  # ---------------------------------------------------------------#
  def putUKeySasDefn(self, tableName, detail, columnDefn):
    if not detail['keep']:
      return
    logger.info('table : ' + tableName)
    ukeyRef = detail['ukeyDefn'].pop(0)
    ukeyDefn = detail['ukeyDefn']
    fkeyRef = detail['fkeyRef']

    sasDefn = []
    if fkeyRef:
      dbKey = '%s|%s|SASFMT' % (self.tsXref, fkeyRef)
      sasDefn = self.retrieve(dbKey)
    for index in ukeyDefn:
      defnItem = list(columnDefn[ukeyRef][index])
      del(defnItem[1])
      sasDefn += self.getSasFormat(defnItem)
    dbKey = '%s|%s|SASFMT' % (self.tsXref, tableName)
    logger.info('sas format : ' + str(sasDefn))
    self._leveldb.Put(dbKey,json.dumps(sasDefn))

  # -------------------------------------------------------------- #
  # getFKeySasDefn
  # ---------------------------------------------------------------#
  def getFKeySasDefn(self, fkeyRef):
    if not fkeyRef:
      return []
    dbKey = '%s|%s|SASFMT' % (self.tsXref, fkeyRef)
    return self.retrieve(dbKey)

  # -------------------------------------------------------------- #
  # retrieve
  # ---------------------------------------------------------------#
  def retrieve(self, dbKey, noValue=None):
    try:
      record = self._leveldb.Get(dbKey)
      return json.loads(record)
    except KeyError:
      logger.error('key not found in key storage : ' + dbKey)
      return noValue

  # -------------------------------------------------------------- #
  # getSasFormat
  # ---------------------------------------------------------------#
  def getSasFormat(self, fmtItem):
    fmtItem += '$' if fmtItem[1][0] == '$' else ''
    return [fmtItem]

	# -------------------------------------------------------------- #
	# getDataStream
	# ---------------------------------------------------------------#
  def getDataStream(self, tableName=None):
    pdata = ['<>']
    pdata.append('%s|%s|ROW1' % (self.tsXref, tableName))
    pdata.append('%s|%s|ROW:' % (self.tsXref, tableName))
    params = '{"service":"dataTxnPrvdr:DlmrStreamPrvdr","args":%s}' % json.dumps(pdata)
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/sync'
    return requests.post(apiUrl,data=data)
    
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
    template = env.get_template('streamToSas.sas')
    trnswrk = self.pmeta['ciwork'] + '/ssnwork/trnswrk'
    params = {'trnswrk': trnswrk, 'reclen': self.pmeta['reclen']}
    for tableName in sorted(self.sasDefn):
      logger.info('IMPORT_TO SAS : ' + tableName)
      params['tableName'] = tableName
      logger.info('sas import params : ' + str(params))
      sasDefn = self.sasDefn[tableName]
      sasPrgm = '%s/%s.sas' % (self.pmeta['progLib'],tableName)
      template.stream(params=params,sasDefn=sasDefn).dump(sasPrgm)
      logfile = '%s/log/%s.log' % (self.pmeta['progLib'],tableName)
      sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
      dstream = self.getDataStream(tableName)
      if dstream.status_code != 201:
        raise Exception('data stream api request failed[%d] : %s' % (dstream.status_code,dstream.text))
      logger.info('run %s in subprocess ...' % sasPrgm)
      self.runProcess(sysArgs,stdin=dstream.text)
      break
    self.state.hasNext = False
    self.state.complete = True
    #self.purgeKeyData(purge=True)
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
    return self.state
    
# -------------------------------------------------------------- #
# WcScriptPrvdr
# ---------------------------------------------------------------#
class WcScriptPrvdr(SasScriptPrvdr):

  def __init__(self, leveldb, jobId, caller):
    super(WcScriptPrvdr, self).__init__(leveldb)
    self.jobId = jobId
    self.caller = caller
    self.pmeta = None
      
  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):
    self.pmeta = self.getProgramMeta()
    #dbKey = 'TSXREF|' + self.caller
    #try:
    #  tsXref = self._leveldb.Get(dbKey)
    #except KeyError:
    #  raise Exception('EEOWW! tsXref param not found : ' + dbKey)
    tsXref = 180703074802
    self.compileScript('wcInputXml2Sas.sas')
    return (tsXref, self.pmeta)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    dbKey = 'PMETA|' + self.caller
    try:
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! pmeta json document not found : ' + dbKey)
    pmetaAll = json.loads(pmetadoc)
    pmeta = pmetaAll['WcInputXml2Sas']
    _globals = pmeta['globals'] # append global vars in this list
    del(pmeta['globals'])
    if _globals[0] == '*':
      pmeta.update(pmetaAll['Global'])
    else:
      for item in _globals:
        pmeta[item] = pmetaAll['Global'][item]
    return pmeta
