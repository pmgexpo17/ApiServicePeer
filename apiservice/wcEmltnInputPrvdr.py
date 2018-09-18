from apibase import AppDirector, AppResolvar, SasScriptPrvdr, logger
from apitools.wcemltn import WcEmailPrvdr
from jinja2 import Environment, PackageLoader
import os, sys, time
import json
import requests

# -------------------------------------------------------------- #
# WcInputPrvdr
# ---------------------------------------------------------------#
class WcInputPrvdr(AppDirector):

  def __init__(self, leveldb, jobId, caller):
    super(WcInputPrvdr,self).__init__(leveldb, jobId)
    self.caller = caller
    self._type = 'delegate'
    self.state.hasNext = True
    self.resolve = WcResolvar(leveldb)
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # runApp
  # - override to get the callee jobId sent from NormalizeXml partner
  # ---------------------------------------------------------------#
  def runApp(self, signal=None, callee=None):
    logger.info('wcInputPrvdr.WcInputPrvdr.runApp, callee : ' + str(callee))
    self.resolve.callee = callee    
    super(WcInputPrvdr, self).runApp(signal)

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    logger.info('wcInputPrvdr.WcInputPrvdr._start, caller : ' + str(self.caller))
    WcEmailPrvdr.subscribe('WcInputPrvdr')
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
      	self.state.transition += ':FAILED'
        raise Exception('NORMALISE_XML failed, rc : %d' % signal)
    self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'NORMALISE_XML':
      self.putApiRequest(201)
    elif self.state.complete:
      self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    if self.state.transition == 'NORMALISE_XML':
      classRef = 'normalizeXml:NormalizeXml'
      pdata = (classRef,json.dumps([self.caller,self.jobId]))
      params = '{"type":"director","id":null,"service":"%s","args":[],"caller":%s}' % pdata
      data = [('job',params)]
      apiUrl = 'http://localhost:5000/api/v1/smart'
      response = requests.post(apiUrl,data=data)
      logger.info('api response ' + response.text)
    elif self.state.complete:
      classRef = 'wcEmltnService:WcEmltnDirector'
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
    if 'FAILED' in self.state.transition:
      self.putApiRequest(500)
      return
    # if WcResolvar has caught an exception an error mail is ready to be sent
    if not WcEmailPrvdr.hasMailReady('WcInputPrvdr'):
      method = 'wcInputPrvdr.WcResolvar.' + self.state.current
      errdesc = 'system error'
      self.sendMail('ERR1',method,errdesc,str(ex))
    else:
      WcEmailPrvdr.sendMail('WcInputPrvdr')
    self.putApiRequest(500)

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self,*args):      
    WcEmailPrvdr.sendMail('WcInputPrvdr',*args)

# -------------------------------------------------------------- #
# WcResolvar
# ---------------------------------------------------------------#
class WcResolvar(AppResolvar):
  
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
    self.method = 'wcInputPrvdr.WcResolvar._start'
    logger.info('wcInputPrvdr.WcResolvar._start')
    try:
      self.tsXref = tsXref
      self.pmeta = pmeta
      self.state.current = 'NORMALISE_XML'
      trnswrk = self.pmeta['ciwork'] + '/trnswrk'
      cnvtwrk = self.pmeta['ciwork'] + '/cnvtwrk'
      assets = self.pmeta['ciwork'] + '/assets'
      self.sysCmd(['mkdir','-p',trnswrk])
      self.sysCmd(['mkdir',cnvtwrk])
      self.sysCmd(['mkdir',assets])    
      xmlSchema = '%s/%s' % (self.pmeta['assetLib'], self.pmeta['xmlSchema'])
      self.sysCmd(['cp',xmlSchema,assets]) # copy to here for normalizer to use
    except Exception as ex:
      errmsg = 'error making sas format dict : ' + str(ex)
      self.newMail('ERR1','compile pmeta failed',errmsg)
      raise
    try:
      self.makeSasFormat(xmlSchema)
    except Exception as ex:
      self.newMail('ERR1','system error',str(ex))
      raise

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    WcEmailPrvdr.newMail('WcInputPrvdr',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # makeSasFormat
  # ---------------------------------------------------------------#
  def makeSasFormat(self, xmlSchema):
    self.method = 'wcInputPrvdr.WcResolvar.makeSasFormat'
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
    try:
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
    except Exception as ex:
      self.method = 'wcInputPrvdr.WcResolvar.putUKeySasDefn'
      raise
      
  # -------------------------------------------------------------- #
  # getFKeySasDefn
  # ---------------------------------------------------------------#
  def getFKeySasDefn(self, fkeyRef):
    if not fkeyRef:
      return []
    dbKey = '%s|%s|SASFMT' % (self.tsXref, fkeyRef)
    try:
      return self.retrieve(dbKey)
    except Exception as ex:
      self.method = 'wcInputPrvdr.WcResolvar.getFKeySasDefn'
      raise

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
	# purgeStreamData
	# ---------------------------------------------------------------#
  def purgeDataStream(self):
    pdata = []
    startKey = '%s|/' % self.tsXref
    endKey = '%s||' % self.tsXref
    classRef = 'dataTxnPrvdr:DelDataUnit'
    pdata = (classRef,json.dumps([startKey,endKey]))
    params = '{"type":"delegate","id":null,"service":"%s","args":%s}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/async/1'
    return requests.post(apiUrl,data=data)
    
	# -------------------------------------------------------------- #
	# evalStreamConsumer
	# ---------------------------------------------------------------#
  def evalStreamConsumer(self, template, params, tableName):
    self.method = 'wcInputPrvdr.WcResolvar.evalStreamConsumer'
    params['tableName'] = tableName
    logger.info('sas import params : ' + str(params))
    sasDefn = self.sasDefn[tableName]
    sasPrgm = '%s/%s.sas' % (self.pmeta['progLib'],tableName)
    template.stream(params=params,sasDefn=sasDefn).dump(sasPrgm)
    dstream = self.getDataStream(tableName)
    if dstream.status_code != 201:
      self.method = 'wcInputPrvdr.WcResolvar.getDataStream'
      errmsg = 'data stream api request failed[%d] : %s'
      raise Exception(errmsg % (dstream.status_code,dstream.text))
    sasPrgm = tableName + '.sas'
    logfile = 'log/%s.log' % tableName
    sysArgs = ['sas','-sysin',sasPrgm,'-altlog',logfile]
    logger.info('run %s in subprocess ...' % sasPrgm)
    cwd = self.pmeta['progLib']    
    self.runProcess(sysArgs,stdin=dstream.text,cwd=cwd)
    
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
    try:
      self.importToSas()
      return self.state
    except Exception as ex:
      self.newMail('ERR1','data stream error',str(ex))
      raise

	# -------------------------------------------------------------- #
	# importToSas
	# ---------------------------------------------------------------#
  def importToSas(self):
    self.method = 'wcInputPrvdr.WcResolvar.importToSas'
    env = Environment(loader=PackageLoader('apitools', 'wcemltn'),trim_blocks=True)
    template = env.get_template('streamToSas.sas')
    trnswrk = self.pmeta['ciwork'] + '/trnswrk'
    params = {'trnswrk': trnswrk, 'reclen': self.pmeta['reclen']}
    for tableName in sorted(self.sasDefn):
      logger.info('IMPORT_TO SAS : ' + tableName)
      self.evalStreamConsumer(template, params, tableName)
    try:
      response = self.purgeDataStream()
    except Exception as ex:
      self.method = 'wcInputPrvdr.WcResolvar.purgeDataStream'
      raise      
    logger.info('api response ' + response.text)
    self.state.next = 'GET_PMOV_DATA'
    self.state.hasNext = True

	# -------------------------------------------------------------- #
	# GET_PMOV_DATA
	# ---------------------------------------------------------------#
  def GET_PMOV_DATA(self):

    self.state.next = 'TRANSFORM_SAS'
    self.state.hasNext = True
    if self.pmeta["inputTxnFilter"] != 'LAST_INF_BY_PCID':
      return self.state

    pmovLib = self.pmeta['ciwork'] + '/pmov'
    self.pmeta['pmovLib'] = pmovLib

    try:      
      pmovDsId = datetime.datetime.strptime(self.pmeta['pmovDsId'],'%y%m')
    except ValueError:
      raise Exception('pmov dataset id not valid : ' + self.pmeta['pmovDsId'])

    if not os.path.exists(pmovLib):
      self.sysCmd(['mkdir','-p',pmovLib])
    else:
      pmovDsName = 'pmov%s_combined.sas7bdat' % self.pmeta['pmovDsId']
      pmovDsPath = '%s/%s' % (pmovLib, pmovDsName)
      if os.path.exists(pmovDsPath):
        lastPutStamp = os.path.getmtime(pmovDsPath)
        lastPut = datetime.datetime.fromtimestamp(lastPutStamp)
        today = datetime.datetime.today()
        if lastPut.day == today.day and lastPut.month == today.month and lastPut.year == today.year:
          logger.info(pmovDsName + ' has been transfered already today')
          logger.info('!! NOTE : wcEmltnService S3 transfer min period = daily !!')
          return self.state

    S3Dir = pmovDsId.strftime('%Y%m')
    if self.pmeta['S3Path'][-1] != '/':
      S3Dir = '/' + S3Dir
    self.pmeta['S3Path'] += S3Dir
    incItems = ['macroLib','pmovLib','S3Path','pmovDsId']
    self.compileScript('getPmovFromS3.sas',incItems=incItems)
    
    sasPrgm = 'getPmovFromS3.sas'
    logfile = 'log/getPmovFromS3.log'
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']
    logger.info('run getPmovFromS3.sas in subprocess ...')
    cwd = self.pmeta['progLib']
    self.runProcess(sysArgs,cwd=cwd)
    return self.state

	# -------------------------------------------------------------- #
	# TRANSFORM_SAS
	# ---------------------------------------------------------------#
  def TRANSFORM_SAS(self):
    self.transformSas()
    return self.state

	# -------------------------------------------------------------- #
	# transformSas
	# ---------------------------------------------------------------#
  def transformSas(self):  
    self.method = 'wcInputPrvdr.WcResolvar.transformSas'  
    sasPrgm = 'wcInputXml2Sas.sas'
    logfile = 'log/wcInputXml2Sas.log'
    progLib = self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-altlog',logfile]

    logger.info('run wcInputXml2Sas.sas in subprocess ...')
    try:
      self.runProcess(sysArgs,cwd=progLib)
    except Exception as ex:
      self.newMail('ERR2',sasPrgm,logfile,progLib)
      raise
    self.state.hasNext = False
    self.state.complete = True
    self.state.hasSignal = True
    
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
    self.method = 'wcInputPrvdr.WcScriptPrvdr.__call__'
    try:
      self.compileScript('wcInputXml2Sas.sas')
    except Exception as ex:
      self.newMail('ERR1','compile script error',str(ex))
      raise
    try:
      dbKey = 'TSXREF|' + self.caller
      tsXref = self._leveldb.Get(dbKey)
      return (tsXref, self.pmeta)      
    except KeyError:
      errmsg = 'EEOWW! tsXref param not found : ' + dbKey
      self.newMail('ERR1','leveldb lookup failed',errmsg)
      raise Exception(errmsg)

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    WcEmailPrvdr.newMail('WcInputPrvdr',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    self.method = 'wcInputPrvdr.WcScriptPrvdr.getProgramMeta'
    try:
      dbKey = 'PMETA|' + self.caller
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      errmsg = 'EEOWW! pmeta json document not found : ' + dbKey
      self.newMail('ERR1','leveldb lookup failed',errmsg)
      raise Exception(errmsg)

    try:
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
    except Exception as ex:
      self.newMail('ERR1','compile pmeta failed',str(ex))
      raise
