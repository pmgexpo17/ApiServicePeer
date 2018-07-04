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
import sys
logger = logging.getLogger('apscheduler')
logFormatter = logging.Formatter('%(levelname)s:%(asctime)s %(message)s', datefmt='%d-%m-%Y %I:%M:%S %p')
fileHandler = logging.FileHandler('./apiPeer.log')
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)

from apibase import appPrvdr
from flask import Flask, g
from flask_restful import reqparse, abort, Resource, Api
import json
import uuid

def setPrvdr():
  if 'prvdr' not in g:
    g.prvdr = appPrvdr

parser = reqparse.RequestParser()
parser.add_argument('job')
parser.add_argument('pmeta')

# promotes a smart, ie, stateful long running job
class SmartJob(Resource):

  def put(self):
    setPrvdr()
    args = parser.parse_args()
    if not args:
      return {'status':500,'error':'empty form args'}, 500
    try:
      jobId = str(uuid.uuid4())      
      if args['pmeta']:
        dbKey = 'PMETA|' + jobId
        g.prvdr.db.Put(dbKey, args['pmeta'])
        params = json.loads(args['job'])
        logger.info('job args : ' + str(params))
        g.prvdr.promote(params,jobId=jobId)
        return {'status':201,'job_id':jobId}, 201
      else:
        logger.info('pmeta not in request')
      if args['job']:
        params = json.loads(args['job'])
        logger.info('job args : ' + str(params))
        jobId = g.prvdr.promote(params,jobId=jobId)
        return {'status':201,'job_id':jobId}, 201
      return {'status':500,'error':"form parameter 'job' not found"}, 500
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
      params = json.loads(args['job'])
      logger.info('job args : ' + str(params))
      try:
        jobList = g.prvdr.promote(params,jobRange=int(jobRange))
      except Exception as ex:
        return {'status':500,'error':str(ex)}, 500
      else:
        return {'status':201,'job_ids':jobList}, 201
    else:
      return {'status':500,'error':"form parameter 'job' not found"}, 500

# adds a new program job item, and runs it (TO DO: at the datetime specified)
class SyncJob(Resource):

  def post(self):
    setPrvdr()
    args = parser.parse_args()
    logger.info('args : ' + str(args))
    if args['job']:
      params = json.loads(args['job'])
      logger.info('job args : ' + str(params))
      try:      
        response = g.prvdr.resolve(params)
      except Exception as ex:
        return {'status':500,'error':str(ex)}, 500
      else:
        return response
    else:
      return {'status':500,'error':"'job' form parameter not found"}, 500

##
## Actually setup the Api resource routing here
##

# create_app style which supports the Flask factory pattern
def create_app(app_config=None):

  flask = Flask(__name__)
  if app_config:
    flask.config.from_pyfile(app_config)
  return flask

flask = create_app()
flaskApi = Api(flask)

flaskApi.add_resource(SmartJob, '/api/v1/smart')
flaskApi.add_resource(AsyncJob, '/api/v1/async/<jobRange>')
flaskApi.add_resource(SyncJob, '/api/v1/sync')

from cheroot.wsgi import PathInfoDispatcher
from cheroot.wsgi import Server as wsgiserver

wsgiapp = PathInfoDispatcher({'/': flask})
server = wsgiserver(('0.0.0.0', 5000), wsgiapp)

if __name__ == '__main__':
   try:
      server.start()
   except KeyboardInterrupt:
      server.stop()
