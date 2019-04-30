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
import asyncio
import logging
import os, sys
import requests
import simplejson as json
from aiohttp import web
from apibase import ApiConnectError, addHandler, ApiPeer, ApiRequest
from optparse import OptionParser
from threading import RLock

apiBase = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger('apiagent')
logfile = '%s/log/apiAgent.log' % apiBase
addHandler(logger, logfile=logfile)
addHandler(logger)
logger.setLevel(logging.INFO)

# -------------------------------------------------------------- #
# ApiAgent
# ---------------------------------------------------------------#
class ApiAgent:
  def __init__(self):
    self.request = ApiRequest()
    self.runner = None

	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  async def run(self, port):
    
    try:
      self.start()
      self._isRunning = self.isRunning(port)
      if not self._isRunning:
        logger.info('!!! ready to start')
    except Exception as ex:
      logger.error('api http service is not available\nError : ' + str(ex))
    else:
      await self._run(port)

	# -------------------------------------------------------------- #
	# start
	# ---------------------------------------------------------------#
  def start(self):
    
    parser = OptionParser()
    parser.add_option("-r", "--reload-module", dest="moduleName", default=None,
                  help="reload a service module, where service name matches apiservices.json")
    (options, args) = parser.parse_args()      

    if len(args) > 1:
      errmsg = 'usage : apiAgent [serviceName -r moduleName]'
      errmsg += '\t no args = start the http api, loading all services in apiservice.json'
      errmsg += '\t-r = reload module referenced by serviceName'
      logger.error(errmsg)
      raise Exception(errmsg)

    self.starting = len(args) == 0

    try:
      regFile = apiBase + '/apiservices.json'
      with open(regFile,'r') as fhr:
        register = json.load(fhr)
        self.register = register['services']
    except (IOError, ValueError) as ex:
      logger.error('failed loading service register : ' + str(ex))
      raise
    except KeyError:
      errmsg = "services.json root node 'services' does not exist"
      logger.error(errmsg)
      raise Exception(errmsg)

    self.moduleName = None
    if len(args) == 1 and options.moduleName:
      serviceName = args[0]
      try:
        self.service = self.register[serviceName]
        self.moduleName = options.moduleName
        self.serviceName = serviceName
      except KeyError:
        errmsg = 'service name is not registered : ' + serviceName
        logger.error(errmsg)
        raise Exception(errmsg)

	# -------------------------------------------------------------- #
	# isRunning
	# ---------------------------------------------------------------#
  def isRunning(self, port):

    try:
      response = self.request.get(f'http://localhost:{port}/api/v1/ping')
      result = json.loads(response.text)
      if result['status'] == 200:
        logmsg = 'webapi service is running, pid : %d' % result['pid']
        logger.info(logmsg)
      else:
        errmsg = 'webapi service is not available, status : %d' % result['status']
        raise Exception(errmsg)
    except ApiConnectError as ex:      
      return False
    return True

	# -------------------------------------------------------------- #
	# isLoaded
	# ---------------------------------------------------------------#
  def isLoaded(self):
    
    apiUrl = 'http://localhost:5000/api/v1/service/%s'
    response = requests.get(apiUrl % self.serviceName)
    return json.loads(response.text)['loaded']

	# -------------------------------------------------------------- #
	# loadService
	# ---------------------------------------------------------------#
  def loadService(self):
    
    apiUrl = 'http://localhost:5000/api/v1/service/%s'
    apiUrl = apiUrl % self.serviceName
    data = [('service',json.dumps(self.service))]
    response = requests.put(apiUrl,data=data)
    logger.info('api response ' + response.text)

	# -------------------------------------------------------------- #
	# reloadModule
	# ---------------------------------------------------------------#
  def reloadModule(self):
    
    apiUrl = 'http://localhost:5000/api/v1/service/%s'
    apiUrl = apiUrl % self.serviceName
    data = [('module',self.moduleName)]
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)
      
	# -------------------------------------------------------------- #
	# _run
	# ---------------------------------------------------------------#
  async def _run(self, port):
    
    if self._isRunning:
      if self.starting:
        logger.warn('start action ignored, api is already started ...')
      elif self.moduleName:
        logmsg = '### reloading service module : %s, %s ###'
        logger.info(logmsg % (self.serviceName, self.moduleName))
        self.reloadModule()
    else:
      if self.moduleName:
        logger.warn('reload module action ignored, api is not started')
      elif self.starting:
        self.request.close()
        logger.info('### starting apiservicepeer ... ###')
        self.runner = await ApiPeer.start(apiBase, self.register, port)
        return self.runner
      else:
        raise Exception('a valid option was not provided')

	# -------------------------------------------------------------- #
	# stop
	# ---------------------------------------------------------------#
  async def stop(self):
    if self.runner:
      await self.runner.cleanup()

if __name__ == '__main__':

  try:
      apiAgent = ApiAgent()
      loop = asyncio.get_event_loop()
      loop.run_until_complete(apiAgent.run(5000))
      loop.run_forever()
  except Exception as ex:
    logger.error('Error : ' + str(ex))
  except KeyboardInterrupt:
    pass
  finally:
    if loop:
      loop.run_until_complete(apiAgent.stop())
