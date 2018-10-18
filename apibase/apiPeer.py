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
import os, sys
import json
import uuid

from flask import Flask, g
from flask_restful import reqparse, abort, Resource, Api
from apibase import AppProvider

def setPrvdr():
  if 'prvdr' not in g:
    g.prvdr = ApiPeer.appPrvdr

parser = reqparse.RequestParser()
parser.add_argument('job')
parser.add_argument('pmeta')
parser.add_argument('service')
parser.add_argument('module')

# promotes a smart, ie, stateful long running job
class SmartJob(Resource):

  def post(self):
    setPrvdr()
    args = parser.parse_args()
    if not args:
      return {'status':400,'error':'empty form args'}, 400
    try:
      if args['pmeta']:
        jobId = str(uuid.uuid4())
        dbKey = 'PMETA|' + jobId
        g.prvdr.db.Put(dbKey, args['pmeta'])
        try:
          params = json.loads(args['job'])
        except ValueError as ex:
          return {'status':400,'error':str(ex),'context':'json parse'}, 400
        logger.info('job args : ' + str(params))
        g.prvdr.promote(params,jobId=jobId)
        return {'status':201,'job_id':jobId}, 201
      else:
        logger.info('pmeta not in request')
      if args['job']:
        try:
          params = json.loads(args['job'])
        except ValueError as ex:
          return {'status':400,'error':str(ex),'context':'json parse'}, 400
        logger.info('job args : ' + str(params))
        jobId = g.prvdr.promote(params)
        return {'status':201,'job_id':jobId}, 201
      return {'status':400,'error':"form parameter 'job' not found"}, 400
    except Exception as ex:
      return {'status':500,'error':str(ex)}, 500
    else:
      return {'status':201,'job_id':jobId}, 201

# promotes one or more asynchonous jobs
class AsyncJob(Resource):

  def post(self, jobRange):
    setPrvdr()
    args = parser.parse_args()
    if args['job']:
      try:
        params = json.loads(args['job'])
      except ValueError as ex:
        return {'status':400,'error':str(ex),'context':'json parse'}, 400
      logger.info('job args : ' + str(params))
      try:
        jobList = g.prvdr.promote(params,jobRange=jobRange)
      except Exception as ex:
        return {'status':500,'error':str(ex)}, 500
      else:
        return {'status':201,'job_ids':jobList}, 201
    else:
      return {'status':400,'error':"form parameter 'job' not found"}, 400

# adds a new program job item, and runs it (TO DO: at the datetime specified)
class SyncJob(Resource):

  def post(self):
    setPrvdr()
    args = parser.parse_args()
    logger.info('args : ' + str(args))
    if args['job']:
      try:
        params = json.loads(args['job'])
      except ValueError as ex:
        return {'status':400,'error':str(ex),'context':'json parse'}, 400
      logger.info('job args : ' + str(params))
      try:      
        response = g.prvdr.resolve(params)
      except Exception as ex:
        return {'status':500,'error':str(ex)}, 500
      else:
        return response
    else:
      return {'status':400,'error':"form parameter 'job' not found"}, 400

# update a service module
class ServiceManager(Resource):

  # GET: get the service load status
  def get(self, serviceName):
    setPrvdr()
    try:
      return g.prvdr.getLoadStatus(serviceName), 200
    except Exception as ex:
      return {'status':500,'error':str(ex)}, 500

  # POST: reload a service module
  def post(self, serviceName):
    setPrvdr()
    args = parser.parse_args()
    logger.info('args : ' + str(args))
    if args['module']:
      moduleName = args['module']
      logger.info('service, module : %s, %s' % (serviceName, moduleName))
      try:      
        response, rcode = g.prvdr.reloadModule(serviceName, moduleName)
      except Exception as ex:
        return {'status':500,'error':str(ex)}, 500
      else:
        return response, rcode
    else:
      return {'status':400,'error':"form parameter 'module' not found"}, 400
    
  # PUT : load all service modules
  def put(self, serviceName):
    setPrvdr()
    args = parser.parse_args()
    logger.info('args : ' + str(args))
    if args['service']:
      try:
        serviceRef = json.loads(args['service'])
      except ValueError as ex:
        return {'status':400,'error':str(ex),'context':'json parse'}, 400
      logger.info('service : ' + str(serviceRef))
      try:      
        g.prvdr.loadService(serviceName, serviceRef)
      except Exception as ex:
        return {'status':500,'error':str(ex)}, 500
      else:
        return {'status':201,'service':serviceName}, 201
    else:
      return {'status':400,'error':"form parameter 'service' not found"}, 400

# ping to test if server is up
class Ping(Resource):

  def get(self):
    logger.info('ping request ...')
    setPrvdr()
    return {'status':200,'pid':os.getpid()}, 200

# create_app style which supports the Flask factory pattern
class ApiPeer(object):
  appPrvdr = None
  
  @staticmethod
  def _make(apiBase, serviceName, serviceRef):
    
    global logger
    logger = logging.getLogger('apscheduler')
    logFormat = '%(levelname)s:%(asctime)s %(message)s'
    logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
    logfile = '%s/log/apiPeer.log' % apiBase
    fileHandler = logging.FileHandler(logfile)
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)
    logger.setLevel(logging.INFO)

    dbPath = apiBase + '/leveldb'
    ApiPeer.appPrvdr = AppProvider.connect(dbPath)    
    ApiPeer.appPrvdr.loadService(serviceName, serviceRef)
    
  @staticmethod
  def _start(domain, app_config=None):
    
    flask = Flask(__name__)
    if app_config:
      flask.config.from_pyfile(app_config)

    ##
    ## Actually setup the Api resource routing here
    ##
    flaskApi = Api(flask)
    flaskApi.add_resource(SmartJob, '/api/v1/smart')
    flaskApi.add_resource(AsyncJob, '/api/v1/async/<jobRange>')
    flaskApi.add_resource(SyncJob, '/api/v1/sync')
    flaskApi.add_resource(ServiceManager, '/api/v1/service/<serviceName>')
    flaskApi.add_resource(Ping, '/api/v1/ping')

    from cheroot.wsgi import PathInfoDispatcher
    from cheroot.wsgi import Server as wsgiserver
    
    hostName, port = domain.split(':')
    wsgiapp = PathInfoDispatcher({'/': flask})
    return wsgiserver((hostName, int(port)), wsgiapp)
