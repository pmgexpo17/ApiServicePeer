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
	# getUkValue
	#------------------------------------------------------------------#
  def getUkValue(self, dataset):
    if not self.ukey:
      return None
    ukvalue = [dataset[key] for key in self.ukey]
    return '|'.join(ukvalue)

# -------------------------------------------------------------- #
# TreeNodeRNA1 - root node
# ---------------------------------------------------------------#
class TreeNodeRNA1(TreeNode):

	#------------------------------------------------------------------#
	# extract
	#------------------------------------------------------------------#
  def extract(self, dataset):    
    logger.debug(f'{self.nodeName}, extracting {self.seqnum.next} ...')    
    self.seqnum.increment()
    self.ukValue = self.getUkValue(dataset)
    self.dataset = dataset    

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
# TreeNodeFKA1 - foreign key relation : One to Many
# -- FKA1 sets the foreign key
# ---------------------------------------------------------------#
class TreeNodeFKA1(TreeNode):

  # -------------------------------------------------------------- #
  # extract -
  # -------------------------------------------------------------- #
  def extract(self, dataset):
    logger.debug(f'{self.name}, fkey : {self.fkey}')
    self.fkValue = self.parent.ukValue
    self.dataset = []

  # -------------------------------------------------------------- #
  # dump -
  # -------------------------------------------------------------- #
  def dump(self):
    logger.debug(f'{self.nodeName}, dumping {self.seqnum.next} ...')    
    if not self.dataset:
      logger.debug(f'{self.nodeName}, Attention. {self.seqnum.next} dataset is empty')
      return
    objkey = f'{self.nodeName}|{self.seqnum.next}'
    for recnum, recordL in enumerate(self.dataset):
      dbkey = f'{objkey}|{self.fkValue}|{recnum:02}'
      logger.debug(f'{self.nodeName}, {dbkey} ### {recordL} ...')
      self._hh[dbkey] = recordL
    self.dataset = []

# -------------------------------------------------------------- #
# TreeNodeUKA1 - foreign key relation : One to Many, Many to Many
# -- UKA1 sets the unique key
# ---------------------------------------------------------------#
class TreeNodeUKA1(TreeNode):

  # -------------------------------------------------------------- #
  # extract -
  # -------------------------------------------------------------- #
  def extract(self, dataset):
    logger.debug(f'{self.nodeName}, extracting {self.seqnum.next} ...')
    self.ukValue = self.getUkValue(dataset)
    recordL = list(dataset.values())
    self.parent.dataset.append(recordL)
    if not self._headerCheck:
      self.headerCheck(dataset)
      self.putHeader(dataset)

  # -------------------------------------------------------------- #
  # dump -
  # -------------------------------------------------------------- #
  def dump(self):
    pass

	#------------------------------------------------------------------#
	# putHeader
	#------------------------------------------------------------------#
  def putHeader(self, dataset):
    logger.debug(f'{self.parent.nodeName}, dumping header ...')
    dbkey = f'{self.parent.nodeName}|header'
    recordL = list(dataset.keys())
    self._hh[dbkey] = recordL

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
