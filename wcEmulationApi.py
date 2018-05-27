from flask import Flask, g
from flask_restful import reqparse, abort, Resource, Api
from apibase import appPrvdr
import json
import logging
import sys

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
parser.add_argument('params')

# adds a new program job item, and runs it (TO DO: at the datetime specified)
class PutJob(Resource):

  def put(self):
    setPrvdr()
    args = parser.parse_args()
    if args and 'job' in args:
      params = json.loads(args['job'])
      logger.info('job args : ' + str(params))
      try:
        g.prvdr.promote(params)
      except BaseException as ex:
        return {'status':404,'error':str(ex)}
      else:
        return {'status':201,'job_id':jobId}, 201
    else:
      return {'status':500,'error':"form parameter 'job' not found"}, 500

    def parseArgs(self) :

      parser.parse_args()

# promotes a live program job item
class PostJob(Resource):

  def post(self, jobCount):
    setPrvdr()
    args = parser.parse_args()
    if args and 'job' in args:
      params = json.loads(args['job'])
      logger.info('job args : ' + str(params))
      try:
        g.prvdr.promote(params,jobCount=jobCount)
      except BaseException as ex:
        return {'status':404,'error':str(ex)}
      else:
        return {'status':201,'job_id':jobId}, 201
    else:
      return {'status':500,'error':"form parameter 'job' not found"}, 500

##
## Actually setup the Api resource routing here
##

flaskApi.add_resource(PutJob, '/api/v1/job')
flaskApi.add_resource(PostJob, '/api/v1/job/<jobCount>')

if __name__ == '__main__':

  logger = logging.getLogger('apscheduler')
  logFormatter = logging.Formatter('%(levelname)s:%(asctime)s %(message)s', datefmt='%d-%m-%Y %I:%M:%S %p')
  fileHandler = logging.FileHandler('./wcEmulationApi.log')
  fileHandler.setFormatter(logFormatter)
  logger.addHandler(fileHandler)

  consoleHandler = logging.StreamHandler(sys.stdout)
  consoleHandler.setFormatter(logFormatter)
  logger.addHandler(consoleHandler)
  logger.setLevel(logging.INFO)

  flask.run(debug=True,use_reloader=False)

 