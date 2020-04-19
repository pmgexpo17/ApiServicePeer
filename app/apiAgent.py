# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import addHandler, ApiLoader, ApiServer, ApiPeer
import asyncio
import logging
import os, sys

apiBase = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger('asyncio.apiagent')
logfile = f'{apiBase}/log/apiAgent.log'
addHandler(logger, logfile=logfile)
addHandler(logger)
logger.setLevel(logging.INFO)

# -------------------------------------------------------------- #
# ApiAgent
# ---------------------------------------------------------------#
class ApiAgent:

  def __init__(self):
    self.loader = None

	# -------------------------------------------------------------- #
	# join
	# ---------------------------------------------------------------#
  def join(self, resourceList):
    f = asyncio.Future()
    try:
      logger.info('calling ApiLoader for resource loading and running ...')
      loader = ApiLoader.make(apiBase)
      loader.run(resourceList)
    except Exception as ex:
      f.set_exception(ex)
    finally:
      return f

	# -------------------------------------------------------------- #
	# test
	# ---------------------------------------------------------------#
  def test(self, resourceList):
    f = asyncio.Future()
    try:
      logger.info('calling ApiLoader for resource loading and running ...')
      loader = ApiLoader.make(apiBase)
      ApiDatastore.start(apiBase)
      loader.run(resourceList)
    except Exception as ex:
      f.set_exception(ex)
    finally:
      return f

	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def run(self, port, hostAddr='tcp://127.0.0.1'):

    logger.info('### starting Api Service Peer ... ###')
    #Generator.start(apiBase)
    ApiPeer.make(apiBase)

    return ApiServer.make(hostAddr, port)

if __name__ == '__main__':

  loop = server = None
  try:
      apiAgent = ApiAgent()
      loop = asyncio.get_event_loop()
      server = apiAgent.run(5550)
      future = asyncio.gather(server.start(), server())
      joinery = ['apiResources.json','apiServices.json']
      loop.run_until_complete(apiAgent.join(joinery))
      loop.run_forever()
  except Exception as ex:
    logger.error('apiPeer startup error', exc_info=True)    
  except KeyboardInterrupt:
    pass
  finally:
    if loop and server:
      loop.run_until_complete(server.terminate())
