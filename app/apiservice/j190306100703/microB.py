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
    logger.info(f'### {self.name} is called ... ###')
    await self.readFile(jobId, taskNum)

  # -------------------------------------------------------------- #
  # readFile
  # ---------------------------------------------------------------#
  async def readFile(self, jobId, taskNum):
    try:
      dbkey = f'{jobId}|datastream|workspace'
      workspace = self._leveldb[dbkey]
      dbkey = f'{jobId}|datastream|outfile'
      outfileName = self._leveldb[dbkey]
    except KeyError as ex:
      errmsg = f'{jobId} workspace or filename info not found'
      logger.error(errmsg)
      raise TaskError(errmsg)

    if not os.path.exists(workspace):
      errmsg = f'workspace {workspace} does not exist'
      raise TaskError(errmsg)

    outfilePath = f'{workspace}/{outfileName}'
    connector = await Datastream.connector(taskNum, self.name)
    status, response = await connector.prepare(jobId, taskNum)
    if status not in (200,201):
      raise TaskError(f'{self.name}, datastream preparation failed : {response}')
    try:
      logger.info(f'{self.name}, about to read {outfileName} by datastream ...')
      with open(outfilePath, 'wb') as fhwb:
        async for chunk in connector.read():
          fhwb.write(chunk)
      self.uncompressFile(workspace, outfileName)
    except Exception as ex:
      errmsg = f'failed writing outfile {outfilePath}'
      logger.error(errmsg)
      raise TaskError(errmsg)

  # -------------------------------------------------------------- #
  # uncompressFile
  # ---------------------------------------------------------------#
  def uncompressFile(self, workspace, outfileName):
    logger.info(f'{self.name}, extract by gunzip, {outfileName} ...')
    try:
      cmdArgs = ['gunzip',outfileName]
      self.sysCmd(cmdArgs,cwd=workspace)
    except Exception as ex:
      errmsg = f'{self.name}, extract by gunzip failed, {outfileName}'
      logger.error(errmsg)
      raise TaskError(errmsg)

