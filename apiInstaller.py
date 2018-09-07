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
  
  def __init__(self, apiBase, repoPath):
    self.apiBase = apiBase
    self.repoPath = repoPath

  # -------------------------------------------------------------- #
  # downloadFile
  # ---------------------------------------------------------------#
  def downloadFile(self, item, repoPath, apiPath):
    stashUri = '%s/%s/%s' % (self.repoPath, repoPath, item)
    outFile = '%s/%s' % (apiPath, item)
    logger.info('downloading %s/%s' % (repoPath, item))
    logger.info('downloading into %s ...' % outFile)
    os.system('curl -u %s:%s %s?raw -o %s' % (uid, pwd, stashUri, outFile))
    
  # -------------------------------------------------------------- #
  # downloadFiles
  # ---------------------------------------------------------------#
  def downloadFiles(self, repoItems, apiPath):
    logger.info('installing codes in ' + apiPath)
    repoPath = repoItems.pop(0)
    for item in repoItems:
      self.downloadFile(item, repoPath, apiPath)

  # -------------------------------------------------------------- #
  # importMeta
  # ---------------------------------------------------------------#
  def importMeta(self):
    self.downloadFile('apiInstaller.json','installer',localDir)
    with open('apiInstaller.json','r') as fhr:
      makeMeta = json.load(fhr)
    makeMeta['PipEnv']
    makeMeta['ApiAsset']
    makeMeta['OtherAsset']
    makeMeta['RepoMap']
    return makeMeta

  # -------------------------------------------------------------- #
  # createPath
  # ---------------------------------------------------------------#
  def createPath(self, sysPath, isApiPath):
    if isApiPath:
      sysPath = sysPath % self.apiBase
    logger.info('making system path : ' + sysPath)
    os.system('mkdir -p ' + sysPath)
    return sysPath

  # -------------------------------------------------------------- #
  # installFiles
  # ---------------------------------------------------------------#
  def installFiles(self, makeMeta):  
  
    # create and activate the virtual environment
    self.apiRoot = makeMeta['ApiAsset'][0].split('|')[-1] % self.apiBase
    logger.info('api root : ' + self.apiRoot)
    if os.path.exists(self.apiRoot):
      raise Exception('EEOWW! %s apienv already exists' % self.apiRoot)
    for makePath in makeMeta['ApiAsset']:
      repoKey, sysPath = makePath.split('|')
      sysPath = self.createPath(sysPath, True)
      if repoKey in makeMeta['RepoMap']:
        self.downloadFiles(makeMeta['RepoMap'][repoKey], sysPath)
    for makePath in makeMeta['OtherAsset']:
      repoKey, sysPath = makePath.split('|')
      self.createPath(sysPath, False)
      if repoKey in makeMeta['RepoMap']:
        self.downloadFiles(makeMeta['RepoMap'][repoKey], sysPath)
  
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
    os.system('chgrp -R actuser /apps/webapi/')

  # -------------------------------------------------------------- #
  # run
  # ---------------------------------------------------------------#
  def run(self):
    logger.info('### repoPath : %s ' % self.repoPath)
    logger.info('### starting api installation ...  ###')

    try:
      makeMeta = self.importMeta()
    except ValueError as ex:
      errmsg = 'apiInstaller.json markup is not valid : ' + str(ex)
      logger.error(errmsg)
      raise
    except KeyError as ex:
      errmsg = 'apiInstaller.json is missing required install key ' + str(ex)
      logger.error(errmsg)
      raise
    self.installFiles(makeMeta)
    self.makeVirtualEnv(makeMeta)

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

  stashBase = 'https://bitbucket.int.corp.sun/projects/PI/repos/ci/browse/PiDevApps/CsvCheckerApi'
  
  parser = OptionParser()
  parser.add_option("-r", "--repoPath", dest="repoPath",
                  help="provide source repo base url", default=stashBase)
  (options, args) = parser.parse_args()

  if len(args) != 0:
    errmsg = 'wrong args count. apiInstaller.py expects 1 option argument : code source repoPath'
    logger.error(errmsg)
    raise Exception(errmsg)
  
  try:
    getCredentials()
    installer = ApiInstaller('webapi', options.repoPath)
    installer.run()
  except (Exception, OSError) as ex:
    logger.error('caught exception : ' + str(ex))
    raise
