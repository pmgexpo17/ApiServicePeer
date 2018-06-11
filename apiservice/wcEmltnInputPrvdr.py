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

  def __init__(self, leveldb, **kwargs):
    super(WcEmltnInputPrvdr,self).__init__(leveldb, **kwargs)
    self.pmeta = None

	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def __call__(self):

    try:
      self.runApp()
      self.cleanout();
    except (BaseException, Exception):
      self.cleanout();
      self.putApiRequest(500)
      
	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def runApp(self):

    self.loadProgramMeta()
    self.putXmlFileInbound()
    self.compileSessionVars('wcInputXml2Sas.sas')
    sasPrgm = '%s/wcInputXml2Sas.sas' % self.pmeta['progLib']
    logfile = '%s/log/wcInputXml2Sas.log' % self.pmeta['progLib']
    sysArgs = ['sas','-sysin',sasPrgm,'-log',logfile,'-logparm','open=replace']

    logger.info('run wcInputXml2Sas.sas in subprocess ...')
    try :
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        logger.error('returncode: %d, stderr: %s' % (prcss.returncode, stderr))
        logger.error('please investigate the sas error reported in wcInputXml2Sas.log')
        raise BaseException('wcInputXml2Sas.sas failed, stderr : ' + stderr)
    except OSError as ex:
      raise BaseException('WcEmltnInputPrvdr failed, OSError : ' + str(ex))
    self.putApiRequest(201)

	# -------------------------------------------------------------- #
	# cleanout
	# ---------------------------------------------------------------#
  def cleanout(self):
    
    sysArgs = ['rm', '-rf', self.pmeta['inputXmlBase']]
    prcss1 = Popen(sysArgs, stderr=PIPE)
    prcss1.communicate()    
    
  # -------------------------------------------------------------- #
  # putXmlFileInbound
  # ---------------------------------------------------------------#                                          
  def putXmlFileInbound(self):
    
    prcss1 = Popen(['mount'], stdout=PIPE)
    sysArgs = ['grep', self.pmeta['mountPath']]
    prcss2 = Popen(sysArgs, stdin=prcss1.stdout,stdout=PIPE,stderr=PIPE)
    prcss1.stdout.close()
    (stdout, stderr) = prcss2.communicate()
    if stdout:
      logger.info('wcEmulation service local base is mounted : ' + self.pmeta['mountPath'])
    else:
      errmsg = 'wcEmulation service local base is not mounted : ' + self.pmeta['mountPath']
      raise BaseException(errmsg)
    inputXmlFile = self.pmeta['inputXmlPath'].split('/')[-1]
    self.srcXmlPath = '%s/%s.txt' % (self.pmeta['mountPath'], self.pmeta['inputXmlPath'])
    sysArgs = ['ls', self.srcXmlPath]
    prcss1 = Popen(sysArgs, stdout=PIPE,stderr=PIPE)
    (stdout, stderr) = prcss1.communicate()
    if stdout:
      logger.info( 'wcEmulation inputXmlPath is valid')
    else:
      errmsg = 'wcEmulation inputXmlPath is not valid : ' +  self.srcXmlPath
      raise BaseException(errmsg)
    #inputXmlPath = '%s/%s.txt.gz' %  (self.pmeta['inputXmlBase'], inputXmlFile)
    #gzipXmlFh = open(inputXmlPath,'w')
    #sysArgs = ['gzip', '-c', self.inputXmlPath]
    #prcss1 = Popen(sysArgs, stdout=gzipXmlFh, stderr=PIPE)
    inputXmlBase = '%s/%s' % (self.pmeta['inputXmlBase'], self.jobId)
    sysArgs = ['mkdir', inputXmlBase]
    prcss1 = Popen(sysArgs, stderr=PIPE)
    (stdout, stderr) = prcss1.communicate()    
    if prcss1.returncode:
      errmsg = 'failed to make input xml storage : ' + inputXmlBase
      raise BaseException(errmsg)
    inputXmlPath = '%s/%s.txt' %  (inputXmlBase, inputXmlFile)
    sysArgs = ['cp', self.srcXmlPath, inputXmlPath]
    prcss1 = Popen(sysArgs, stderr=PIPE)
    (stdout, stderr) = prcss1.communicate()    
    if prcss1.returncode:
      errmsg = 'failed to copy inputXmlFile to inbound : ' + stderr
      raise BaseException(errmsg)
    #gzipXmlFh.close()
    sysArgs = ['chmod','664',inputXmlPath]
    prcss1 = Popen(sysArgs, stdout=PIPE,stderr=PIPE)
    (stdout, stderr) = prcss1.communicate()
    if prcss1.returncode:
      errmsg = 'failed to chmod 664 on inbound/inputXmlFile : ' + stderr
      raise BaseException(errmsg)
    self.pmeta['inputXmlFile'] = inputXmlFile
    self.pmeta['inputXmlBase'] = inputXmlBase

  # -------------------------------------------------------------- #
  # mountLocalBase
  # ---------------------------------------------------------------#                                          
  def mountLocalBase(self):
    
    cifsMeta = os.environ['HOME'] + '/.cifsenv_wcemltn'
    with open(cifsMeta,'w') as fhw:
      fhw.write('ADUSER='+ os.environ['USER'])
      fhw.write("DFSPATH='%s'" % self.pmeta['localBase'])
      fhw.write("MOUNTINST='/data/wcEmltnService'")
    
    sysArgs = ['sudo','/usr/local/bin/landrive.sh','--mountcifs','--cifsenv','./cifsenv_wcemltn','--credentials','.landrive']    
    prcss = Popen(sysArgs, stdout=PIPE,stderr=PIPE,cwd=os.environ['HOME'])		
    (stdout, stderr) = prcss.communicate()
    
  # -------------------------------------------------------------- #
  # loadProgramMeta
  # ---------------------------------------------------------------#                                          
  def loadProgramMeta(self):
    logger.debug('[START] loadProgramMeta')
    dbKey = 'PMETA|' + self.jobId
    try:
      _pmeta = self._leveldb.Get(dbKey)
    except KeyError:
      raise BaseException('EEEOWWW! pmeta json document not found : ' + dbKey)
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
    try :
      self.pmeta['inputXmlFile']
    except KeyError:
      inputXmlFile = self.pmeta['inputXmlPath'].split('/')[-1]
      self.pmeta['inputXmlFile'] = inputXmlFile
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
