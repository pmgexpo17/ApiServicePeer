from project.dataconvertA1.jdatastreamR100 import Microservice

Datastream = Microservice.subscriber()

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError, Terminal
import logging
import os

logger = logging.getLogger('asyncio.microservice')

#------------------------------------------------------------------#
# DatastreamReader
#------------------------------------------------------------------#
class DatastreamReader(Microservice, Terminal):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
  '''
  #------------------------------------------------------------------#
	# runActor
	#------------------------------------------------------------------#
  async def runActor(self, jobId, taskNum, **kwargs):
    try:
      logger.info(f'### {self.name} is called ... ###')
      dbKey = f'{jobId}|datastream|workspace'
      self.workspace = self._leveldb[dbKey]
      dbKey = f'{jobId}|datastream|outfile'
      self.outfileName = self._leveldb[dbKey]
    except KeyError as ex:
      errmsg = f'{jobId} workspace or filename info not found'
      logger.error(errmsg)
      raise TaskError(errmsg)
    
    await self.readFile(jobId, taskNum)
    self.uncompressFile()

  # -------------------------------------------------------------- #
  # readFile
  # ---------------------------------------------------------------#
  async def readFile(self, jobId, taskNum):
    if not os.path.exists(self.workspace):
      errmsg = f'workspace {self.workspace} does not exist'
      raise TaskError(errmsg)

    outfilePath = f'{self.workspace}/{self.outfileName}'
    connector = await Datastream.connector(taskNum, self.name)
    status, response = await connector.prepare(jobId, taskNum)
    if status not in (200,201):
      raise TaskError(f'{self.name}, datastream preparation failed : {response}')
    try:
      logger.info('{self.name}, about to read {self.outfileName} by datastream ...')
      with open(outfilePath, 'wb') as fhwb:
        async for chunk in connector.read():
          fhwb.write(chunk)
    except Exception as ex:
      errmsg = f'failed writing outfile {outfilePath}'
      logger.error(errmsg)
      raise TaskError(errmsg)

  # -------------------------------------------------------------- #
  # uncompressFile
  # ---------------------------------------------------------------#
  def uncompressFile(self):
    logger.info(f'{self.name}, extract by gunzip, {self.outfileName} ...')
    try:
      cmdArgs = ['gunzip',self.outfileName]
      self.sysCmd(cmdArgs,cwd=self.workspace)
    except Exception as ex:
      errmsg = f'{self.name}, extract by gunzip failed, {self.outfileName}'
      logger.error(errmsg)
      raise TaskError(errmsg)
