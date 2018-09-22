# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
import json
import logging
import virtualenv
import pip
import os, sys
import platform
import requests
from optparse import OptionParser

# -------------------------------------------------------------- #
# getCredentials -
# ---------------------------------------------------------------#
def getCredentials():
  credentials = os.environ['HOME'] + '/.landrive'
  global uid
  global pwd

  try:
    with open(credentials,'r') as fhr:
      for credItem in fhr:
        credItem = credItem.strip() 
        if 'username' in credItem:
          uid = credItem.split('=')[1]
        elif 'password' in credItem:
          pwd = credItem.split('=')[1]
  except Exception as ex:
    logger.error('failed to parse credentials file : ' + str(ex))

# -------------------------------------------------------------- #
# ApiInstaller
# ---------------------------------------------------------------#
class ApiInstaller(object):
  
  def __init__(self, repoPath, serviceName):
    self.repoPath = repoPath
    self.serviceName = serviceName

  def getMetaFile(self):
    pass
    
  def importMeta(self):
    pass
    
  def installFiles(self):
    pass

  # -------------------------------------------------------------- #
  # createPath
  # ---------------------------------------------------------------#
  def createPath(self, sysPath):
    logger.info('making system path : ' + sysPath)
    os.system('mkdir -p ' + sysPath)

	# -------------------------------------------------------------- #
	# isRunning
	# ---------------------------------------------------------------#
  def isRunning(self, domain):

    try:
      response = requests.get('http://%s/api/v1/ping' % domain)
      result = json.loads(response.text)
      if result['status'] == 200:
        logmsg = 'webapi service is running, pid : %d' % result['pid']
        logger.info(logmsg)
      else:
        logmsg = 'webapi service is not available, status : %d' % result['status']
        logger.info(logmsg)
    except requests.exceptions.RequestException as ex:
      if 'Errno 111' in ex.__repr__():
        return False
    return True        
    
  # -------------------------------------------------------------- #
  # makeVirtualEnv
  # ---------------------------------------------------------------#
  def makeVirtualEnv(self, makeMeta):
  
    logger.info('making virtual enviroment ...')      
    virtualenv.create_environment(self.apiRoot)
    logger.info('activating virtual enviroment ...')
    activate_this = self.apiRoot + '/bin/activate_this.py'
    execfile(activate_this, dict(__file__=activate_this))
  
    # pip install a package using the venv as a prefix
    for envItem in makeMeta['PipEnv']:
      pipKey, pipVal = envItem.split('|')
      os.environ[pipKey] = pipVal
    pipRequire = self.apiRoot + '/requirements.txt'
    logger.info('install requirements by pip ...')
    pip.main(["install", "--prefix", self.apiRoot, "-r", pipRequire])
    os.system('chgrp -R actuser %s/' % self.apiRoot)

  # -------------------------------------------------------------- #
  # importMeta
  # ---------------------------------------------------------------#
  def importMeta(self):
    self.apiMetaFile = self.getMetaFile()
    with open(self.apiMetaFile,'r') as fhr:
      makeMeta = json.load(fhr)
    makeMeta['PipEnv']
    makeMeta['ApiRoot']
    makeMeta['ApiCore']
    makeMeta['ApiPlan']
    makeMeta['RepoName']
    makeMeta['Domain']
    makeMeta['Installed']
    makeMeta['Services']
    makeMeta[self.serviceName]
    return makeMeta

  # -------------------------------------------------------------- #
  # resolveApiPlan
  # - ApiPlan - sysdir list of all api service modules
  # - SysPlan - sysdir list of all saslib or asset repos
  # ---------------------------------------------------------------#
  def resolveApiPlan(self, serviceName, makeMeta, make=False):

    logger.info('resolving %s ApiPlan ...' % serviceName)   
    # isApiPlan : distinguish api specific sysdir or otherwise
    isApiPlan = True 
    if serviceName == 'ApiCore':
      apiPlan = makeMeta['ApiPlan']
    else:
      isApiPlan = 'ApiPlan' in makeMeta[serviceName]
      if isApiPlan:
        apiPlan = makeMeta[serviceName]['ApiPlan']
      elif 'SysPlan' in makeMeta[serviceName]:
        apiPlan = makeMeta[serviceName]['SysPlan']
      else:
        return
    self.evalApiPlan(apiPlan, isApiPlan, make)
    # service may has both an ApiPlan as well as a SysPlan
    if isApiPlan and 'SysPlan' in makeMeta[serviceName]:
      apiPlan = makeMeta[serviceName]['SysPlan']
      self.evalApiPlan(apiPlan, False, make)
        
  # -------------------------------------------------------------- #
  # evalApiPlan
  # ---------------------------------------------------------------#
  def evalApiPlan(self, apiPlan, isApiPlan, make=False):
    for makePath in apiPlan:      
      repoKey, sysPath = makePath.split('|')
      _sysRoot = self.apiRoot if isApiPlan else self.sysRoot
      _sysPath = sysPath if sysPath == 'root' else _sysRoot + '/' + sysPath
      self.apiPlan[repoKey] = _sysPath
      if not make and not os.path.exists(_sysPath):
        logmsg = 'EEOWW! expected api path %s does not exist, making now ...'
        logger.warn(logmsg % _sysPath)
        make = True
      if make:
        self.createPath(_sysPath)

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self):
    self.serverName = platform.node()
    logger.info('### repoPath : %s ' % self.repoPath)
    logger.info('### serverName : %s ' % self.serverName)
    logger.info('### starting api installation ...  ###')
    
    try:
      makeMeta = self.importMeta()
    except ValueError as ex:
      errmsg = 'apiInstaller.json markup is not valid : ' + str(ex)
      logger.error(errmsg)
      raise Exception(errmsg)
    except KeyError as ex:
      errmsg = 'apiInstaller.json is missing required install key ' + str(ex)
      logger.error(errmsg)
      raise Exception(errmsg)

    apiKey = 'default'
    if self.serverName in makeMeta['ApiRoot']:
      apiKey = self.serverName
    self.apiRoot = makeMeta['ApiRoot'][apiKey]
    logger.info('api root : ' + self.apiRoot)

    apiKey = 'default'
    if self.serverName in makeMeta['SysPlan']:
      apiKey = self.serverName
    self.sysRoot = makeMeta['SysPlan'][apiKey]
    logger.info('api sysplan root : ' + self.sysRoot)
    
    self.installLog = []
    if self.serverName in makeMeta['Installed']:
      self.installLog = makeMeta['Installed'][self.serverName]
    logger.info('api install log : ' + str(self.installLog))

    if self.serviceName == 'ApiCore':
      domains = list(set(makeMeta['Domain'].values()))
      for domain in domains:
        if self.isRunning(domain):
          errmsg = 'cant update ApiCore while %s domain is running'
          logger.error(errmsg)
          raise Exception(errmsg)
    else:
      serviceDomain = makeMeta['Domain'][self.serviceName]
      logger.info('api domain : ' + serviceDomain)
      if self.serviceName in self.installLog:
        if self.isRunning(serviceDomain):
          errmsg = self.serviceName + ' is aleady installed, and the host is running\n'
          errmsg += 'use the apiAgent reloadModule function instead'
          logger.error(errmsg)
          raise Exception(errmsg)

      if self.serviceName not in makeMeta['RepoName']:
        errmsg = '%s does not exist in ApiInstaller.json' % self.serviceName
        logger.error(errmsg)
        raise Exception(errmsg)

    if self.serviceName in self.installLog:
      if not self.updateParts and not self.forcedWrite:
        errmsg = 'service is aleady installed, so the -f option must be used'
        logger.error(errmsg)
        raise Exception(errmsg)
        
    installed = self.installFiles(makeMeta)
    self.installLog += installed
    self.updateMeta(makeMeta)
    if 'ApiCore' in installed:
      self.makeVirtualEnv(makeMeta)

  # -------------------------------------------------------------- #
  # updateMeta
  # ---------------------------------------------------------------#
  def updateMeta(self, makeMeta):

    if self.serviceName != 'ApiCore':
      self.exportApiMeta(makeMeta)
    
    makeMeta['Installed'][self.serverName] = list(self.installLog)
    logger.info('rewriting apiInstaller.json with new installed status ...')
    _apiMetaFile = self.apiMetaFile.split('.')[0] + '_new.json'
    with open(_apiMetaFile,'w') as fhw:
      fhw.write(json.dumps(makeMeta, indent=2, sort_keys=True))
    os.system('mv %s %s' % (_apiMetaFile, self.apiMetaFile))

  # -------------------------------------------------------------- #
  # exportApiMeta
  # ---------------------------------------------------------------#
  def exportApiMeta(self, makeMeta):
    
    if not os.path.exists('/apps/etc'):
      os.system('mkdir -p /apps/etc')
    with open('/apps/etc/apimeta.txt','w') as fhw:
      for serviceName, domain in makeMeta['Domain'].items():
        fhw.write('apiDomain=%s|%s\n' % (serviceName,domain))
      fhw.write('apiRoot=%s\n' % self.apiRoot)
      fhw.write('sysRoot=%s\n' % self.sysRoot)

# -------------------------------------------------------------- #
# StashApiInstaller
# ---------------------------------------------------------------#
class StashApiInstaller(ApiInstaller):
  
  def __init__(self, repoPath, serviceName):
    super(StashApiInstaller, self).__init__(repoPath, serviceName)

  # -------------------------------------------------------------- #
  # downloadFile
  # ---------------------------------------------------------------#
  def downloadFile(self, item, repoPath, apiPath):
    stashUri = '%s/%s/%s' % (self.repoPath, repoPath, item)
    outFile = '%s/%s' % (apiPath, item)
    logger.info('downloading %s/%s' % (repoPath, item))
    logger.info('downloading into %s ...' % outFile)
    os.system('curl -u %s:%s %s?raw -o %s' % (uid, pwd, stashUri, outFile))
    return outFile
    
  # -------------------------------------------------------------- #
  # downloadFiles
  # ---------------------------------------------------------------#
  def downloadFiles(self, repoItems, apiPath):
    logger.info('installing codes in ' + apiPath)
    repoPath = repoItems[0]
    for item in repoItems[1:]:
      self.downloadFile(item, repoPath, apiPath)

  # -------------------------------------------------------------- #
  # getMetaFile
  # ---------------------------------------------------------------#
  def getMetaFile(self):
    return self.downloadFile('apiInstaller.json','installer',localDir)
    
  # -------------------------------------------------------------- #
  # installFiles
  # ---------------------------------------------------------------#
  def installFiles(self, makeMeta):  
  
    logger.info('StashApiInstaller - installing files ...')
    # create and activate the virtual environment
    logger.info('api root : ' + self.apiRoot)
    for makePath in makeMeta['SysPlan']:
      repoKey, sysPath = makePath.split('|')
      sysPath = self.createPath(sysPath, True)
      if repoKey in makeMeta['RepoMap']:
        self.downloadFiles(makeMeta['RepoMap'][repoKey], sysPath)
    for makePath in makeMeta['SysPlan']:
      repoKey, sysPath = makePath.split('|')
      self.createPath(sysPath, False)
      if repoKey in makeMeta['RepoMap']:
        self.downloadFiles(makeMeta['RepoMap'][repoKey], sysPath)

# -------------------------------------------------------------- #
# FileSysApiInstaller
# ---------------------------------------------------------------#
class FileSysApiInstaller(ApiInstaller):
  
  def __init__(self, repoPath, serviceName):
    super(FileSysApiInstaller, self).__init__(repoPath, serviceName)

  # -------------------------------------------------------------- #
  # getMetaFile
  # ---------------------------------------------------------------#
  def getMetaFile(self):
    return self.repoPath + '/ApiInstaller/apiInstaller.json'

  # -------------------------------------------------------------- #
  # installFiles
  # ---------------------------------------------------------------#
  def installFiles(self, makeMeta):  

    self.apiPlan = {}
    installed = []
    
    if 'ApiCore' not in self.installLog:
      if os.path.exists(self.apiRoot):
        errmsg = 'ApiCore install failed. api root already exists : '
        errmsg += self.apiRoot
        logger.error(errmsg)
        raise Exception(errmsg)
      # resolve and make the ApiCore sysdir list
      self.resolveApiPlan('ApiCore',makeMeta,make=True)
      self._installFiles('ApiCore', makeMeta)
      installed.append('ApiCore')
    else:
      # always include ApiCore sysPath info for service installations
      # don't make the sysdir list but resolve for copy files
      self.resolveApiPlan('ApiCore',makeMeta,make=False)
    # resolve and make the service sysdir list
    makeFlag = self.serviceName not in self.installLog
    self.resolveApiPlan(self.serviceName,makeMeta,make=makeFlag)
    self._installFiles(self.serviceName, makeMeta)
    if self.serviceName not in self.installLog:
      installed.append(serviceName)
    return installed
      
  # -------------------------------------------------------------- #
  # installFiles
  # ---------------------------------------------------------------#
  def _installFiles(self, serviceName, makeMetaAll):
  
    logger.info('FileSysApiInstaller - installing %s ...' % serviceName)
    repoName = makeMetaAll['RepoName'][serviceName]
    makeMeta = makeMetaAll[serviceName]
    for repoKey, repoItems in makeMeta['RepoMap'].items():
      if not self.updateParts or repoKey in self.updateParts:
        self.copyFiles(repoKey, repoName, repoItems)

  # -------------------------------------------------------------- #
  # copyFiles
  # ---------------------------------------------------------------#
  def copyFiles(self, repoKey, repoName, repoItems):
    sysPath = self.apiPlan[repoKey]
    logger.info('installing codes in ' + sysPath)
    repoPath = '%s/%s/%s' % (self.repoPath, repoName, repoItems[0])
    if not os.path.exists(repoPath):
      errmsg = repoPath + ' does NOT exist, aborting ...'
      logger.error(errmsg)
      raise Exception(errmsg)
    logger.info('repo source : ' + repoPath)
    self.evalCopy(repoPath,repoItems[1:],sysPath)
    os.system('chgrp -R actuser %s/' % sysPath)
    
  # -------------------------------------------------------------- #
  # evalCopy
  # ---------------------------------------------------------------#
  def evalCopy(self, repoPath, repoItems, sysPath):
    
    # default bash profile makes landrive copied file with 600 umask
    # by touching the dest file first, the umask is 664
    if repoItems[0] == '*.*':
      repoItems = [f for f in os.listdir(repoPath) if os.path.isfile(repoPath + '/' + f)]
    for item in repoItems:
      if '*' in item:
        errmsg = 'wildcard file spec is limited to *.* for any repoPath'
        logger.error(errmsg)
        raise Exception(errmsg)
      else:
        os.system('touch %s/%s' % (sysPath,item))
        os.system('cp %s/%s %s/%s' % (repoPath,item,sysPath,item))
  
if __name__ == '__main__':

  global localDir
  localDir = os.path.dirname(os.path.realpath(__file__))
  logger = logging.getLogger('apiinstaller')
  logFormat = '%(levelname)s:%(asctime)s %(message)s'
  logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
  logfile = localDir + '/apiInstaller.log'
  fileHandler = logging.FileHandler(logfile)
  fileHandler.setFormatter(logFormatter)
  logger.addHandler(fileHandler)
  
  consoleHandler = logging.StreamHandler(sys.stdout)
  consoleHandler.setFormatter(logFormatter)
  logger.addHandler(consoleHandler)
  logger.setLevel(logging.INFO)

  parser = OptionParser()
  parser.add_option("-f", "--force", action="store_true", dest="forcedWrite",
                  help="force overwrite existing service package", default=False)
  parser.add_option("-u", "--update", dest="updateParts",
                  help="colon separated repo parts list for update", default='')
  (options, args) = parser.parse_args()

  if len(args) != 2:
    errmsg = 'wrong args count. apiInstaller.py expects 2 arguments : '
    errmsg += '#1 code source repoPath, #2 serviceName'
    logger.error(errmsg)
    raise Exception(errmsg)
  
  repoPath, serviceName = args
  try:
    if repoPath[:7] == 'file://':
      installer = FileSysApiInstaller(repoPath[7:], serviceName)
    else:
      getCredentials()
      installer = StashApiInstaller(repoPath, serviceName)
    installer.forcedWrite = options.forcedWrite
    installer.updateParts = options.updateParts
    installer.run()
  except (Exception, OSError) as ex:
    logger.error('caught exception : ' + str(ex))
    raise
