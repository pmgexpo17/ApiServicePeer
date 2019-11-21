# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError
from project.dataconvertA1.jhardhashR100 import Microservice

Hardhash = Microservice.subscriber()

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError
from .treeNode import TreeNodePrvdr
from collections import OrderedDict
import logging
import os, sys
import simplejson as json

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# JsonNormaliser
# ---------------------------------------------------------------#
class JsonNormaliser(Microservice):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
  '''
  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, jobId, taskNum, *args, **kwargs):
    try:
      logger.info(f'### JsonNormaliser {taskNum} is called ... ###')
      dbKey = f'{jobId}|workspace'
      workspace = self._leveldb[dbKey]
      dbKey = f'{jobId}|INPUT|{taskNum}|jsonFile'
      self.jsonFile = self._leveldb[dbKey]
      dbKey = f'{jobId}|XFORM|rootname'
      rootName = self._leveldb[dbKey]

      logger.info(f'### normalise workspace : {workspace}')
      logger.info(f'### task|{taskNum:02} input json file : {self.jsonFile}')
      logger.info('### xform root nodename : ' + rootName)

      jsonFilePath = workspace + '/' + self.jsonFile
      if not os.path.exists(jsonFilePath):
        errmsg = f'{self.jsonFile} does not exist in workspace'
        raise Exception(errmsg)      
      await self.arrange(jobId, taskNum)
      await self.run(jsonFilePath)
    except Exception as ex:
      logger.error(f'{self.name}, actor {self.actorId} errored', exc_info=True)
      raise TaskError(ex)

  #------------------------------------------------------------------#
  # run
  #------------------------------------------------------------------#
  async def run(self, jsonFilePath):
    logger.info(f'{self.name}, normalise start ...')
    try:
      with open(jsonFilePath) as jsfh:
        recnum = 0
        for jsRecord in jsfh:
          recnum += 1
          await self.normalise(recnum, jsRecord)
      logger.info(f'### {self.jsonFile} rowcount : {recnum}')
    except Exception as ex:
      errMsg = f'splitfile, recnum : {self.jsonFile}, {recnum}'
      logger.error(errMsg)
      raise

  #------------------------------------------------------------------#
  # normalise
  #------------------------------------------------------------------#
  async def normalise(self, recnum, jsRecord):
    logger.debug(f'### normalise, recnum : {recnum}')
    record = self.getJsObject(jsRecord)
    await self.rootLevel.normalise(record)

  #------------------------------------------------------------------#
  # getJsObject
  #------------------------------------------------------------------#
  def getJsObject(self, jsRecord):
    try:
      return json.loads(jsRecord, object_pairs_hook=OrderedDict)
    except ValueError as ex:
      raise Exception('Json conversion failure, ' + str(ex))

  #------------------------------------------------------------------#
  # arrange
  #------------------------------------------------------------------#
  async def arrange(self, jobId, taskNum):
    provider = TreeNodePrvdr.make(jobId, taskNum)
    # arrange a tree of nodes matching the treeNode dom configuration
    self.rootLevel = await provider.get('level')
    await self.rootLevel.arrange(provider)

#------------------------------------------------------------------#
# CsvComposer
#------------------------------------------------------------------#
class CsvComposer(Microservice):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
  '''
  #------------------------------------------------------------------#
  # runActor
  #------------------------------------------------------------------#
  async def runActor(self, jobId, taskNum, keyHigh, **kwargs):
    try:
      logger.info(f'### {self.name} is called ... ###')
      dbKey = f'{jobId}|XFORM|output|tableMap|{taskNum}'
      tableName, nodeList = self._leveldb[dbKey]
      dbKey = f'{jobId}|XFORM|csvPath|{taskNum}'
      csvPath = self._leveldb[dbKey]

      logger.info(f'### task|{taskNum:02} output csv file : {tableName}.csv')
      logger.info('### node name : ' + str(nodeList[0]))
      
      with open(csvPath,'w') as csvfh:
        writer = await CsvWriter.make(taskNum, nodeList[0], keyHigh)
        await writer.writeAll(csvfh, nodeList)
        logger.info(f'### {tableName} rowcount : {writer.total}')
    except Exception as ex:
      logger.error(f'actor {self.actorId} error', exc_info=True)
      raise TaskError(ex)

# -------------------------------------------------------------- #
# CsvComposer - end
# -------------------------------------------------------------- #  

#------------------------------------------------------------------#
# CsvWriter
#------------------------------------------------------------------#
class CsvWriter:
  def __init__(self, connector, taskNum, nodeName, keyHigh):
    self._hh = connector
    self.taskNum = taskNum
    self.nodeName = nodeName    
    self.keyHigh = keyHigh

  @property
  def name(self):
    return f'CsvWriter.{self.taskNum}.{self.nodeName}.{self.keyHigh}'

  @classmethod
  async def make(cls, taskNum, nodeName, keyHigh):
    connector = await Hardhash.connector(taskNum, 'CsvWriter')
    return cls(connector, taskNum, nodeName, keyHigh)

  #------------------------------------------------------------------#
  # writeAll
  #------------------------------------------------------------------#
  async def writeAll(self, csvFh, nodeList):
    await self.writeHeader(csvFh)
    self.total = 0      
    for nodeName in nodeList:
      await self.write(csvFh, nodeName)
      self.total += 1

  #------------------------------------------------------------------#
  # write
  #------------------------------------------------------------------#
  async def write(self, csvFh, nodeName):
    async for record in self.csvDataset(nodeName):
      csvFh.write(','.join(record) + '\n')

  #------------------------------------------------------------------#
  # csvDataset
  #------------------------------------------------------------------#
  def csvDataset(self, nodeName):
    keyLow = f'{nodeName}|{1:02}|{1:05}'
    keyHigh = f'{nodeName}|{self.keyHigh:02}|99999'
    return self._hh.select(keyLow,keyHigh,keysOnly=False)

  #------------------------------------------------------------------#
  # writeHeader
  #------------------------------------------------------------------#
  async def writeHeader(self, csvfh):
    dbKey = f'{self.nodeName}|header'
    record = await self._hh[dbKey]    
    logger.info(f'{self.name}, HEADER : {str(record)}')
    header = ','.join(record)
    csvfh.write(header + '\n')

#------------------------------------------------------------------#
# CsvWriter - end
#------------------------------------------------------------------#

