from project.dataconvertA1.jhardhashR100 import Microservice

Hardhash = Microservice.subscriber()

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError, Terminal
from .component import TableProvider, TreeProvider
import csv
import logging
import os, sys

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# CsvNormaliser
# ---------------------------------------------------------------#
class CsvNormaliser(Microservice):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
  '''
  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, jobId, taskNum, *args, **kwargs):
    try:     
      logger.info(f'### CsvNormaliser {taskNum} is called ... ###')
      dbKey = f'{jobId}|workspace'
      workspace = self._leveldb[dbKey]
      logger.info(f'### normalise workspace : {workspace}')

      await self.arrange(taskNum)
      logger.info(f'### task|{taskNum:02} input csv file : {self.tableName}.csv')

      csvFilePath = f'{workspace}/{self.tableName}.csv'
      if not os.path.exists(csvFilePath):
        errmsg = f'{self.csvFile} does not exist in workspace'
        raise Exception(errmsg)      
      await self.run(csvFilePath)
    except Exception as ex:
      logger.error(f'{self.name}, actor {self.actorId} errored', exc_info=True)
      raise TaskError(ex)

	#------------------------------------------------------------------#
	# arrange
	#------------------------------------------------------------------#
  async def arrange(self, taskNum):
    self._hh = await Hardhash.connector(taskNum, self.name)
    logger.info(f'{self.name}, got hardhash connector {self._hh.cid}')
    self.tableRow = TableProvider.get(taskNum)
    await self.tableRow.arrange(taskNum)
    self.tableName = self.tableRow.tableName

	#------------------------------------------------------------------#
	# run
	#------------------------------------------------------------------#
  async def run(self, csvFilePath):
    try:      
      with open(csvFilePath) as csvfh:
        csvReader = csv.reader(csvfh,quotechar='"', 
                                    doublequote=False, escapechar='\\')
        self.tableRow.prepare(csvReader)
        recnum = 0
        for record in csvReader:
          recnum += 1
          await self.tableRow.load(recnum, record)
        await self.putRowcount(recnum)
    except csv.Error as ex:
      errmsg = f'{self.tableName} normalise error, line : {csvReader.line_num}'
      logger.error(errmsg)
      raise Exception(ex)

	#------------------------------------------------------------------#
	# putRowCount
	#------------------------------------------------------------------#
  async def putRowcount(self, recnum):
    if self.tableRow.isRoot:
      dbkey = f'{self.tableName}|rowcount'
      await self._hh.put(dbkey, recnum)
    logger.info(f'### {self.tableName} rowcount : {recnum}')

# -------------------------------------------------------------- #
# JsonCompiler
# -------------------------------------------------------------- #  
class JsonCompiler(Microservice):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
  '''
  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, jobId, taskNum):
    try:
      logger.info(f'{self.name} is called ... ###')
      dbKey = f'{jobId}|workspace'
      workspace = self._leveldb[dbKey]
      dbKey = f'{jobId}|output|jsonFile'
      jsonFile = self._leveldb[dbKey]

      jsonPath = f'{workspace}/{jsonFile}'
      logger.info(f'### task workspace : {workspace}')
      logger.info(f'### output json file : {jsonFile}')
      await self.arrange(jobId, taskNum)
      await self.run(jsonPath)
    except Exception as ex:
      logger.error(f'{self.name}, actor {self.actorId} errored', exc_info=True)
      raise TaskError(ex)

	#------------------------------------------------------------------#
	# run
	#------------------------------------------------------------------#
  async def run(self, jsonPath):
    logger.info(f'{self.name}, compile start ...')
    with open(jsonPath,'w') as jsfh:
      for recnum in self.rootLevel.rowRange:
        jsObject = await self.rootLevel.compile(recnum)
        jsfh.write(jsObject + '\n')
    logger.info(f'### {self.name}, rowcount : {recnum}')

	#------------------------------------------------------------------#
	# arrange
	#------------------------------------------------------------------#
  async def arrange(self, jobId, taskNum):
    provider = await TreeProvider.make(jobId, taskNum)
    # arrange a tree of nodes matching the treeNode dom configuration
    self.rootLevel = await provider.get('level')
    await self.rootLevel.arrange(provider)

# -------------------------------------------------------------- #
# JsonCompiler - end
# -------------------------------------------------------------- #  
