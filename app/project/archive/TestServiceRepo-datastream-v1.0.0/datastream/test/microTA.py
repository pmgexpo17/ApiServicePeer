# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import Article
import logging

logger = logging.getLogger('apipeer.microservice')

# -------------------------------------------------------------- #
# UnitTestA
# ---------------------------------------------------------------#
class UnitTestA(Microservice):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
  '''
  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, jobId, taskNum, *args, **kwargs):
    try:
      logger.info(f'### {self.name} {taskNum} is called ... ###')
      connector = await Hardhash.connector(taskNum, self.name)
      logger.info('!!!! running TESTX request !!!!')
      packet = Hardhash.dsProfile.clone()
      await connector.send(['TESTX',packet])
      response = await connector.recv()
      logger.info(f'!!!! got ServiceA response : {response}')
      logger.info(f'!!!! sending NOTIFY signal')      
      await connector.notify(taskNum, self.name)
    except Exception as ex:
      logger.error(f'actor {self.actorId} error', exc_info=True)
      raise BaseException(ex)

