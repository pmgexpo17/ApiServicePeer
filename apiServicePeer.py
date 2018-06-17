# Copyright (c) 2018 Peter A McGill
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. 
#
from flask import Flask, g
from flask_restful import reqparse, abort, Resource, Api
from apibase import appPrvdr
import json
import logging
import sys
import uuid

# create_app style which supports the Flask factory pattern
def create_app(app_config=None):

  flask = Flask(__name__)
  if app_config:
    flask.config.from_pyfile(app_config)
  return flask

flask = create_app()
flaskApi = Api(flask)

def setPrvdr():
  if 'prvdr' not in g:
    g.prvdr = appPrvdr

parser = reqparse.RequestParser()
parser.add_argument('job')
parser.add_argument('pmeta')

# adds a new program job item, and runs it (TO DO: at the datetime specified)
class PutJob(Resource):

  def put(self):
    setPrvdr()
    args = parser.parse_args()
    if not args:
      return {'status':500,'error':'empty form args'}, 500
    try:
      if 'pmeta' in args:
        jobId = str(uuid.uuid4())
        dbKey = 'PMETA|' + jobId
        g.prvdr.db.Put(dbKey, args['pmeta'])
        params = json.loads(args['job'])
        logger.info('job args : ' + str(params))
        g.prvdr.promote(params,jobId=jobId)
        return {'status':201,'job_id':jobId}, 201
      else:
        logger.info('pmeta not in request')
      if 'job' in args:
        params = json.loads(args['job'])
        logger.info('job args : ' + str(params))
        jobId = g.prvdr.promote(params)
        return {'status':201,'job_id':jobId}, 201
      return {'status':500,'error':"form parameter 'job' not found"}, 500
    except Exception as ex:
      return {'status':404,'error':str(ex)}, 404
    else:
      return {'status':201,'job_id':jobId}, 201

# promotes a live program job item
class PostJob(Resource):

  def post(self, jobCount):
    setPrvdr()
    args = parser.parse_args()
    if args and 'job' in args:
      params = json.loads(args['job'])
      logger.info('job args : ' + str(params))
      try:
        jobList = g.prvdr.promote(params,jobCount=int(jobCount))
      except Exception as ex:
        return {'status':404,'error':str(ex)}, 404
      else:
        return {'status':201,'job_ids':jobList}, 201
    else:
      return {'status':500,'error':"form parameter 'job' not found"}, 500

parser.add_argument('jobId')
parser.add_argument('dataKey')

# adds a new program job item, and runs it (TO DO: at the datetime specified)
class DataJob(Resource):

  def get(self):
    setPrvdr()
    args = parser.parse_args()
    if args and 'id' in args:
      params = json.loads(args)
      logger.info('job args : ' + str(params))
      try:      
        streamGen = g.prvdr.getStreamGen(params)
      except Exception as ex:
        return {'status':500,'error':str(ex)}, 500
      else:
        return streamGen, 201
    else:
      return {'status':500,'error':"form parameter 'job' not found"}, 500

##
## Actually setup the Api resource routing here
##

flaskApi.add_resource(PutJob, '/api/v1/job')
flaskApi.add_resource(PostJob, '/api/v1/job/<jobCount>')
flaskApi.add_resource(DataJob, '/api/v1/data')

if __name__ == '__main__':

  logger = logging.getLogger('apscheduler')
  logFormatter = logging.Formatter('%(levelname)s:%(asctime)s %(message)s', datefmt='%d-%m-%Y %I:%M:%S %p')
  fileHandler = logging.FileHandler('./apiServicePeer.log')
  fileHandler.setFormatter(logFormatter)
  logger.addHandler(fileHandler)

  consoleHandler = logging.StreamHandler(sys.stdout)
  consoleHandler.setFormatter(logFormatter)
  logger.addHandler(consoleHandler)
  logger.setLevel(logging.INFO)

  flask.run(debug=True,use_reloader=False)
