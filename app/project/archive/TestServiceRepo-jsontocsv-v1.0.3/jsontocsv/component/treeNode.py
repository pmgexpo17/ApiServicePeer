__all__ = [
  'TreeNodeRNA1',
  'TreeNodeFKA1',
  'TreeNodeUKA1',
  'TreeNodeLNA1']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import Note, TaskError
from apitools.transform import AbstractTreeNode, SeqNumber
import logging

logger = logging.getLogger('asyncio.microservice')

class TreeNode(AbstractTreeNode):  
	#------------------------------------------------------------------#
	# _arrange
	#------------------------------------------------------------------#
  def _arrange(self, taskNum):
    self.seqnum = SeqNumber.get(taskNum)

	#------------------------------------------------------------------#
	# getFkValue
	#------------------------------------------------------------------#
  def getFkValue(self, dataset):
    if not self.fkey:
      return None
    fkvalue = [dataset[key] for key in self.fkey]
    return '|'.join(fkvalue)

# -------------------------------------------------------------- #
# TreeNodeRNA1 - root node
# ---------------------------------------------------------------#
class TreeNodeRNA1(TreeNode):

	#------------------------------------------------------------------#
	# extract
  # - eval the next csvDataset, ready to put to the datastore
	#------------------------------------------------------------------#
  def extract(self, record):    
    self.seqnum.increment()
    self.dataset = record.pop(self.tableName)

	#------------------------------------------------------------------#
	# dump
	#------------------------------------------------------------------#
  def dump(self):
    logger.debug(f'{self.nodeName}, dumping {self.seqnum.next} ...')    
    dbkey = f'{self.nodeName}|{self.seqnum.next}'
    recordL = list(self.dataset.values())
    self._hh[dbkey] = recordL
    if not self._headerCheck:
      self.headerCheck()
      self.putHeader()

# -------------------------------------------------------------- #
# TreeNodeUKA1 - foreign key relation : One to Many
# ---------------------------------------------------------------#
class TreeNodeUKA1(TreeNode):

  # -------------------------------------------------------------- #
  # extract - 
  # -------------------------------------------------------------- #
  def extract(self):
    self.dataset = self.parent.dataset.pop(self.tableName)
    logMsg = f'extracted from parent dataset : {self.dataset} ...'
    logger.debug(f'{self.nodeName}, {logMsg}')

  # -------------------------------------------------------------- #
  # dump -
  # -------------------------------------------------------------- #
  def dump(self):
    logger.debug(f'{self.nodeName}, dumping {self.seqnum.next} ...')    
    if not self.dataset:
      logger.debug(f'{self.nodeName}, Attention. {self.seqnum.next} dataset is empty')
      return
    objkey = f'{self.nodeName}|{self.seqnum.next}'  
    for recnum, dataset in enumerate(self.dataset):
      fkValue = self.getFkValue(dataset)
      dbkey = f'{objkey}|{fkValue}|{recnum:02}'
      logger.debug(f'{self.nodeName}, {dbkey} ### {dataset} ...')
      recordL = list(dataset.values())
      self._hh[dbkey] = recordL
    if not self._headerCheck:
      self.headerCheck(dataset)
      self.putHeader(dataset)

# -------------------------------------------------------------- #
# TreeNodeFKA1 - foreign key relation : One to Many, Many to Many
# ---------------------------------------------------------------#
class TreeNodeFKA1(TreeNode):

  # -------------------------------------------------------------- #
  # extract -
  # -------------------------------------------------------------- #
  def extract(self):
    self.dataset = [] # datastore values
    for datasetA in self.parent.dataset:
      dataset = datasetA.pop(self.tableName)
      for record in dataset:
        self.dataset.append(record)
    logMsg = f'extracted from parent dataset : {self.dataset} ...'
    logger.debug(f'{self.nodeName}, {logMsg}')

  # -------------------------------------------------------------- #
  # dump -
  # -------------------------------------------------------------- #
  def dump(self):
    logger.debug(f'{self.nodeName}, dumping {self.seqnum.next} ...')        
    if not self.dataset:
      logger.debug(f'{self.nodeName}, Attention. {self.seqnum.next} dataset is empty')
      return
    objkey = f'{self.nodeName}|{self.seqnum.next}'
    for recnum, dataset in enumerate(self.dataset):
      fkValue = self.getFkValue(dataset)
      dbkey = f'{objkey}|{fkValue}|{recnum:02}'
      recordL = list(dataset.values())
      self._hh[dbkey] = recordL
    if not self._headerCheck:
      self.headerCheck(dataset)
      self.putHeader(dataset)

# -------------------------------------------------------------- #
# TreeNodeUKA1 - foreign key relation : One to Many, Many to Many
# -- LNA1 is a leaf node
# ---------------------------------------------------------------#
class TreeNodeLNA1(TreeNodeUKA1):

  # -------------------------------------------------------------- #
  # extract - the parent node handles the dataset list storage
  # -------------------------------------------------------------- #
  def extract(self, dataset):
    logger.debug(f'{self.nodeName}, extracting {self.seqnum.next} ...')
    recordL = list(dataset.values())
    self.parent.dataset.append(recordL)
    if not self._headerCheck:
      self.headerCheck(dataset)
      self.putHeader(dataset)

# -------------------------------------------------------------- #
# TreeNodeRNB1 - root node
# ---------------------------------------------------------------#
class TreeNodeRNB1(TreeNode):

	#------------------------------------------------------------------#
	# extract
  # - eval the next csvDataset, ready to put to the datastore
	#------------------------------------------------------------------#
  def extract(self, record):    
    self.seqnum.increment()
    self.dataset = record

	#------------------------------------------------------------------#
	# dump
	#------------------------------------------------------------------#
  async def dump(self):
    logger.debug(f'{self.nodeName}, dumping : {self.seqnum.next} ...')
    recordL = list(self.dataset.values())
    if not self._columnCheck:
      self.columnCheck()
    await self._hh.combine(self.nodeName, ['columns',None,self.columns])    
    await self._hh.combine(self.nodeName, ['record',None,recordL])

	#------------------------------------------------------------------#
	# transform
	#------------------------------------------------------------------#
  async def transform(self):
    logger.debug(f'{self.nodeName}, transform, next record : {self.seqnum.next} ...')    
    payload = {
      'nodeName': self.nodeName,
      'dbkey': [
        f'{self.nodeName}|header',
        f'{self.nodeName}|{self.seqnum.next}'
      ],
      'order': self.combine['order']
    }
    await self._hh.combine(None, payload)

# -------------------------------------------------------------- #
# TreeNodeFKB1 - for record combination
# ---------------------------------------------------------------#
class TreeNodeFKB1(TreeNode):

  # -------------------------------------------------------------- #
  # extract
  # -------------------------------------------------------------- #
  def extract(self, record):
    self.dataset = []

	#------------------------------------------------------------------#
	# transform
	#------------------------------------------------------------------#
  async def transform(self):
    logger.debug(f'{self.nodeName}, combining {self.seqnum.next} ...')    
    if not self._columnCheck:
      self.columnCheck()
    # pop foreign key to arrange for combination
    for fkey in self.combine['fkey']:
      self.dataset.pop(fkey)
    columns = list(self.dataset.keys())
    record = list(self.dataset.values())
    parentName = self.combine['nodeName']
    await self._hh.combine(parentName, ['columns',self.nodeName,columns])
    await self._hh.combine(parentName, ['record',self.nodeName,record])
