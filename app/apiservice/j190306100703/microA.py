# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError, Terminal
from .component import Microservice, TreeProvider, TableRow
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
  def runActor(self, jobId, taskNum, *args, **kwargs):
    try:     
      logger.info(f'### CsvNormaliser {taskNum} is called ... ###')
      dbKey = f'{jobId}|workspace'
      workspace = self._leveldb[dbKey]
      logger.info(f'### normalise workspace : {workspace}')

      self.arrange(taskNum)
      logger.info(f'### task|{taskNum:02} input csv file : {self.tableName}.csv')

      csvPath = f'{workspace}/{self.tableName}.csv'
      if not os.path.exists(csvPath):
        errmsg = f'{self.csvFile} does not exist in workspace'
        raise Exception(errmsg)      
      self.run(csvPath)
    except Exception as ex:
      logger.error(f'{self.name}, actor {self.actorId} errored', exc_info=True)
      raise TaskError(ex)

  #------------------------------------------------------------------#
  # arrange
  #------------------------------------------------------------------#
  def arrange(self, taskNum):
    self.tableName, self.tableRow = TableRow.get(taskNum)
    self.tableRow.arrange(taskNum)

  #------------------------------------------------------------------#
  # run
  #------------------------------------------------------------------#
  def run(self, csvPath):
    try:      
      with open(csvPath) as csvfh:
        csvReader = csv.reader(csvfh,quotechar='"', 
                                    doublequote=False, escapechar='\\')
        self.tableRow.prepare(csvReader)
        for record in csvReader:
          self.tableRow.normalise(record)
        rowcount = self.tableRow.result()
        logger.info(f'### {self.tableName} rowcount : {rowcount}')
    except csv.Error as ex:
      errmsg = f'{self.tableName} normalise error, line : {csvReader.line_num}'
      logger.error(errmsg)
      raise Exception(ex)
    
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
  def runActor(self, jobId, taskNum):
    try:
      logger.info(f'{self.name} is called ... ###')
      dbKey = f'{jobId}|workspace'
      workspace = self._leveldb[dbKey]
      dbKey = f'{jobId}|output|jsonFile'
      jsonFile = self._leveldb[dbKey]

      jsonPath = f'{workspace}/{jsonFile}'
      logger.info(f'### task workspace : {workspace}')
      logger.info(f'### output json file : {jsonFile}')
      self.arrange(jobId, taskNum)
      self.run(jsonPath)
    except Exception as ex:
      logger.error(f'{self.name}, actor {self.actorId} errored', exc_info=True)
      raise TaskError(ex)

	#------------------------------------------------------------------#
	# run
	#------------------------------------------------------------------#
  def run(self, jsonPath):
    logger.info(f'{self.name}, compile start ...')
    with open(jsonPath,'w') as jsfh:
      for recnum in self.nodeTree.rowRange:
        jsObject = self.nodeTree.compile(recnum)
        jsfh.write(jsObject + '\n')
    logger.info(f'### {self.name}, rowcount : {recnum}')

	#------------------------------------------------------------------#
	# arrange
	#------------------------------------------------------------------#
  def arrange(self, jobId, taskNum):
    self.nodeTree = TreeProvider.get()
    self.nodeTree.arrange(taskNum)

# -------------------------------------------------------------- #
# JsonCompiler - end
# -------------------------------------------------------------- #  
