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
import os

apiBase = os.path.dirname(os.path.realpath(__file__))
activate_this = '%s/bin/activate_this.py' % apiBase
execfile(activate_this, dict(__file__=activate_this))

import logging
import sys
import json
import requests
from apibase import ApiPeer
from optparse import OptionParser
from subprocess import call as subcall
from threading import RLock

logger = logging.getLogger('apiagent')
logFormat = '%(levelname)s:%(asctime)s %(message)s'
logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
logfile = '%s/log/apiAgent.log' % apiBase
fileHandler = logging.FileHandler(logfile)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)

# -------------------------------------------------------------- #
# ApiAgent
# ---------------------------------------------------------------#
class ApiAgent(object):
  _lock = RLock()

	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def run(self):
    
    self.start()
    self._isRunning = self.isRunning()
    self._run()

	# -------------------------------------------------------------- #
	# start
	# ---------------------------------------------------------------#
  def start(self):
    
    parser = OptionParser()
    parser.add_option("-s", "--submit-job", dest="jobPath",
                  help="submit a webapi smart job", default='')
    parser.add_option("-r", "--reload-module", dest="moduleName",
                  help="reload a service module", default='')
    (options, args) = parser.parse_args()      

    if len(args) != 1:
      errmsg = 'wrong args count. apiAgent expects 1 argument : service name'
      logger.error(errmsg)
      raise Exception(errmsg)
  
    try:
      regFile = apiBase + '/apiservices.json'
      with open(regFile,'r') as fhr:
        register = json.load(fhr)
    except (IOError, ValueError) as ex:
      logger.error('failed loading service register : ' + str(ex))
      raise
      
    serviceName = args[0]
      
    try:
      register[serviceName]
    except KeyError:
      errmsg = 'service name is not registered : ' + serviceName
      logger.error(errmsg)
      raise Exception(errmsg)

    self.jobPath = options.jobPath
    self.moduleName = options.moduleName
    self.service = register[serviceName]
    self.domain = register['domain'][serviceName]
    self.smartJob = register['smartJob'][serviceName]
    self.serviceName = serviceName

	# -------------------------------------------------------------- #
	# isRunning
	# ---------------------------------------------------------------#
  def isRunning(self):

    try:
      response = requests.get('http://%s/api/v1/ping' % self.domain)
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
	# isLoaded
	# ---------------------------------------------------------------#
  def isLoaded(self):
    
    apiUrl = 'http://%s/api/v1/service/%s'
    response = requests.get(apiUrl % (self.domain, self.serviceName))
    return json.loads(response.text)['loaded']

	# -------------------------------------------------------------- #
	# loadService
	# ---------------------------------------------------------------#
  def loadService(self):
    
    apiUrl = 'http://%s/api/v1/service/%s'
    apiUrl = apiUrl % (self.domain, self.serviceName)
    data = [('service',json.dumps(self.service))]
    response = requests.put(apiUrl,data=data)
    logger.info('api response ' + response.text)

	# -------------------------------------------------------------- #
	# reloadModule
	# ---------------------------------------------------------------#
  def reloadModule(self):
    
    apiUrl = 'http://%s/api/v1/service/%s'
    apiUrl = apiUrl % (self.domain, self.serviceName)
    data = [('module',self.moduleName)]
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)
      
	# -------------------------------------------------------------- #
	# _run
	# ---------------------------------------------------------------#
  def _run(self):
    
    if self._isRunning:
      if self.jobPath:
        if not self.isLoaded():
          logger.info('### registering service : %s ###' % self.serviceName)
          self.loadService()
        else:
          logger.info('service is loaded : ' + self.serviceName)
        logger.info('### submitting %s job ###' % self.serviceName)
        self.submitJob()
      elif self.moduleName:
        logmsg = '### reloading service module : %s, %s ###'
        logger.info(logmsg % (self.serviceName, self.moduleName))
        self.reloadModule()
    else:
      if self.jobPath:
        errmsg = 'job submission failed, api host is not started'
        logger.error(errmsg)
        raise Exception(errmsg)
      elif self.moduleName:
        errmsg = 'module reload failed, api host is not started'
        logger.error(errmsg)
        raise Exception(errmsg)

      logger.info('### starting %s api service ###' % self.serviceName)    
      try:
        ApiPeer._make(apiBase, self.serviceName, self.service)
        apiPeer = ApiPeer._start(self.domain)
        apiPeer.start()
      except KeyboardInterrupt:
        apiPeer.stop()

	# -------------------------------------------------------------- #
	# submitJob
	# ---------------------------------------------------------------#
  def submitJob(self):
    smartJob, pmetaJson = self.smartJob
    with open(self.jobPath + '/' + pmetaJson) as pmeta:
      _pmeta = pmeta.read()
    data = [('job',json.dumps(smartJob)),('pmeta',_pmeta)]
    apiUrl = 'http://%s/api/v1/smart' % self.domain
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)
  
if __name__ == '__main__':

  with ApiAgent._lock:

    ApiAgent = ApiAgent()
    ApiAgent.run()
