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
import logging
import os, sys, time
import json
from optparse import OptionParser
from subprocess import call as subcall
from threading import RLock

apiBase = os.path.dirname(os.path.realpath(__file__))

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
  def run(self, serviceReg, jobPath):
    
    with ApiAgent._lock:
      if jobPath:
        self.submitJob(serviceReg, jobPath)
      else:
        self._run(serviceReg)

	# -------------------------------------------------------------- #
	# isRunning
	# ---------------------------------------------------------------#
  def isRunning(self, serviceReg):

    try:
      response = requests.get('http://%s/api/v1/ping' % serviceReg['domain'])
      result = json.loads(response.text)
      if result['status'] == 200:
        logmsg = 'csvChecker api service is already running, pid : %d' % result['pid']
        logger.info(logmsg)
      else:
        logmsg = 'csvChecker api service is not available, status : %d' % result['status']
        logger.info(logmsg)
    except requests.exceptions.RequestException as ex:
      if 'Errno 111' in ex.__repr__():
        return False
    return True        
      
	# -------------------------------------------------------------- #
	# _run
	# ---------------------------------------------------------------#
  def _run(self, serviceReg):

    from apibase import ApiPeer
    
    if self.isRunning(serviceReg):
      ApiPeer.appPrvdr.register(serviceReg['service'])
      return 

    logger.info('### starting %s api service ###' % serviceName)    
    try:
      ApiPeer._make(apiBase, serviceReg['service'])
      apiPeer = ApiPeer._start(serviceReg['domain'])
      apiPeer.start()
    except KeyboardInterrupt:
      apiPeer.stop()


	# -------------------------------------------------------------- #
	# submitJob
	# ---------------------------------------------------------------#
  def submitJob(self, serviceReg, jobPath):
    if not self.isRunning(serviceReg):
      logger.error('job submission requires the api be running aleady')
      return
      
    with open(jobPath + '/ccPmeta.json') as pmeta:
      _pmeta = pmeta.read()
    data = [('job',serviceReg['smartJob']),('pmeta',_pmeta)]
    apiUrl = 'http://%s/api/v1/smart' % serviceReg['domain']
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)
    
# -------------------------------------------------------------- #
# _start_
# ---------------------------------------------------------------#
def _start_():

  parser = OptionParser()
  parser.add_option("-s", "--submit-job", dest="jobPath",
                  help="submit a csvchecker job", default=None)
  (options, args) = parser.parse_args()
  
  if len(args) != 1:
    errmsg = 'wrong args count. apiAgent expects 1 argument : service name'
    logger.error(errmsg)
    raise Exception(errmsg)

  try:
    regFile = apiBase + '/apiservices.json'
    with open(regFile,'r') as fhr:
      register = json.load(fhr)
  except IOError as ex:
    logger('failed loading service register : ' + str(ex))
    raise
    
  global serviceName
  serviceName = args[0].lower()
    
  try:
    register[serviceName]
  except KeyError:
    errmsg = 'service name is not registered : ' + serviceName
    logger.error(errmsg)
    raise Exception(errmsg)

  _register = {}
  _register['service'] = register[serviceName]
  _register['domain'] = register['domain'][serviceName]
  _register['smartJob'] = register['smartJob'][serviceName]
  
  return (_register, options)
  
if __name__ == '__main__':

  serviceReg, options = _start_()
  
  activate_this = '%s/bin/activate_this.py' % apiBase
  execfile(activate_this, dict(__file__=activate_this))
    
  import requests

  ApiAgent = ApiAgent()
  ApiAgent.run(serviceReg, options.jobPath)
  
