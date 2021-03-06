# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError
from .component import Microservice, TreeProvider
import logging
import os, sys

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# JsonNormaliser
# ---------------------------------------------------------------#
class JsonNormaliser(Microservice):
  '''
    ### Framework added attributes ###
      1. _hh : hardhash key-value datastore
  '''
  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  def runActor(self, jobId, taskNum, *args, **kwargs):
    try:
      logger.info(f'### JsonNormaliser {taskNum} is called ... ###')
      dbKey = f'{jobId}|workspace'
      workspace = self._hh[dbKey]
      dbKey = f'{jobId}|XFORM|input|{taskNum}|jsonFile'
      jsonFile = self._hh[dbKey]

      logger.info(f'### normalise workspace : {workspace}')
      logger.info(f'### task|{taskNum:02} input json file : {jsonFile}')

      jsonPath = f'{workspace}/{jsonFile}'
      if not os.path.exists(jsonPath):
        errmsg = f'{jsonFile} does not exist in workspace'
        raise Exception(errmsg)      
      self.arrange(taskNum)
      self.run(jsonPath, jsonFile)
    except Exception as ex:
      logger.error(f'{self.name}, actor {self.actorId} errored', exc_info=True)
      raise TaskError(ex)

	#------------------------------------------------------------------#
	# run
	#------------------------------------------------------------------#
  def run(self, jsonPath, jsonFile):
    logger.info(f'{self.name}, normalise start ...')
    try:
      with open(jsonPath) as jsfh:
        for jsRecord in jsfh:
          self.nodeTree.normalise(jsRecord)
      rowcount = self.nodeTree.result()
      logger.info(f'### {jsonFile} rowcount : {rowcount}')
    except Exception as ex:
      errMsg = f'jsonFile, recnum : {jsonFile}, {rowcount}'
      logger.error(errMsg)
      raise

	#------------------------------------------------------------------#
	# arrange
	#------------------------------------------------------------------#
  def arrange(self, taskNum):
    self.nodeTree = TreeProvider.get()
    self.nodeTree.arrange(taskNum)

#------------------------------------------------------------------#
# CsvComposer
#------------------------------------------------------------------#
class CsvComposer(Microservice):
  '''
    ### Framework added attributes ###
      1. _hh : hardhash key-value datastore
  '''
  #------------------------------------------------------------------#
	# runActor
	#------------------------------------------------------------------#
  def runActor(self, jobId, taskNum, keyHigh, **kwargs):
    try:
      logger.info(f'### {self.name} is called ... ###')
      dbKey = f'{jobId}|workspace'
      workspace = self._hh[dbKey]
      self.nodeTree = TreeProvider.get()
      tableName, nodeList = self.nodeTree.tableMap[taskNum]

      csvPath = f'{workspace}/{tableName}.csv'      

      writer = CsvWriter.make(taskNum, tableName, keyHigh)
      writer.writeAll(csvPath, nodeList)
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
  def __init__(self, connector, taskNum, tableName, keyHigh):
    self._hh = connector
    self.taskNum = taskNum
    self.tableName = tableName    
    self.keyHigh = keyHigh

  @property
  def name(self):
    return f'CsvWriter.{self.taskNum}.{self.tableName}.{self.keyHigh}'

  @classmethod
  def make(cls, taskNum, tableName, keyHigh):
    connector = Microservice.connector(taskNum, 'CsvWriter')
    return cls(connector, taskNum, tableName, keyHigh)

  #------------------------------------------------------------------#
	# writeAll
	#------------------------------------------------------------------#
  def writeAll(self, csvPath, nodeList):
    with open(csvPath,'w') as csvfh:
      self.writeHeader(csvfh, nodeList[0])
      total = 0
      # a flat table is a composite stack of 1 or more datasets 
      for nodeName in nodeList:
        self.write(csvfh, nodeName)
        total += 1
      logger.info(f'### {self.tableName} rowcount : {total}')

  #------------------------------------------------------------------#
	# write
	#------------------------------------------------------------------#
  def write(self, csvFh, nodeName):
    for record in self.csvDataset(nodeName):
      csvFh.write(','.join(record) + '\n')

  #------------------------------------------------------------------#
	# csvDataset
	#------------------------------------------------------------------#
  def csvDataset(self, nodeName):
    keyLow = f'{nodeName}|{1:02}|00000'
    keyHigh = f'{nodeName}|{self.keyHigh:02}|99999'
    return self._hh.select(keyLow,keyHigh)

  #------------------------------------------------------------------#
	# writeHeader
	#------------------------------------------------------------------#
  def writeHeader(self, csvfh, nodeName):
    dbkey = f'{nodeName}|header'
    logger.info(f'{self.name}, header key : {dbkey}')
    header = self._hh[dbkey]
    record = ','.join(header)
    csvfh.write(record + '\n')
