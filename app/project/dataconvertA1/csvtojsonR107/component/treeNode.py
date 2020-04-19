__all__ = [
  'TreeNodeRNA1',
  'TreeNodeFKA1',
  'TreeNodeFKB1']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note, TaskError
from apitools.transform import AbstractTreeNode
import pickle
import logging

logger = logging.getLogger('asyncio.microservice')

class TreeNode(AbstractTreeNode):  
	#------------------------------------------------------------------#
	# _arrange
	#------------------------------------------------------------------#
  def _arrange(self, taskNum):
    pass

# -------------------------------------------------------------- #
# TreeNodeRNA1
# ---------------------------------------------------------------#
class TreeNodeRNA1(TreeNode):

	#------------------------------------------------------------------#
	# compile
	#------------------------------------------------------------------#
  def compile(self, recnum):
    dbkey = f'{self.tableName}|{recnum:05}'
    self._dataset = {}
    self.dataset = self._hh[dbkey]
    self._dataset[self.tableName] = self.dataset
    logger.debug(f'{self.name},{dbkey} : {self.dataset}')

	#------------------------------------------------------------------#
	# export
	#------------------------------------------------------------------#
  def export(self):
    return json.dumps(self._dataset)

# -------------------------------------------------------------- #
# TreeNodeFKA1 - foreign key relation : One to Many
# ---------------------------------------------------------------#
class TreeNodeFKA1(TreeNode):

	#------------------------------------------------------------------#
	# compile
	#------------------------------------------------------------------#
  def compile(self):
    logger.debug(f'{self.name}, compiling {self.nodeName} ...')    
    self.dataset = self.getDataset()
    self.parent.dataset[self.tableName] = self.dataset

	#------------------------------------------------------------------#
	# getDataset
	#------------------------------------------------------------------#
  def getDataset(self):
    self.fkValue = self.getFkValue()
    dbkey = f'{self.nodeName}|{self.fkValue}'
    try:
      return self._hh[dbkey]
    except KeyError:
      return []

	#------------------------------------------------------------------#
	# getFkValue
	#------------------------------------------------------------------#
  def getFkValue(self):
    fkValue = [self.parent.dataset[key] for key in self.fkey]
    return '|'.join(fkValue)

# -------------------------------------------------------------- #
# TreeNodeFKB1 - foreign key relation : Many to Many
# ---------------------------------------------------------------#
class TreeNodeFKB1(TreeNode):

	#------------------------------------------------------------------#
	# compile
	#------------------------------------------------------------------#
  def compile(self):
    logger.debug(f'{self.name}, compiling {self.nodeName} ...')    
    replacePds = []
    for recordP in self.parent.dataset:
      recordP[self.tableName] = self.getDataset(recordP)
      replacePds.append(recordP)
    self.parent.dataset = replacePds

	#------------------------------------------------------------------#
	# getFkValue
	#------------------------------------------------------------------#
  def getFkValue(self, record):
    fkValue = [record[key] for key in self.fkey]
    return '|'.join(fkValue)

	#------------------------------------------------------------------#
	# getDataset
	#------------------------------------------------------------------#
  def getDataset(self, record):
    self.fkValue = self.getFkValue(record)
    dbkey = f'{self.nodeName}|{self.fkValue}'
    try:
      dataset = self._hh[dbkey]
      if dataset:
        return dataset
      return []
    except KeyError:
      return []  

# -------------------------------------------------------------- #
# TreeNodeRNB2 - root node
# ---------------------------------------------------------------#
class TreeNodeRNC1(TreeNode):

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
  def dump(self):
    logger.debug(f'{self.nodeName}, dumping : {self.seqnum.next} ...')
    recordL = list(self.dataset.values())
    if not self._columnCheck:
      self.columnCheck()
    self._hh.combine(self.nodeName, ['columns',None,self.columns])    
    self._hh.combine(self.nodeName, ['record',None,recordL])

	#------------------------------------------------------------------#
	# transform
	#------------------------------------------------------------------#
  def transform(self):
    logger.debug(f'{self.nodeName}, transform, next record : {self.seqnum.next} ...')    
    payload = {
      'nodeName': self.nodeName,
      'dbkey': [
        f'{self.nodeName}|header',
        f'{self.nodeName}|{self.seqnum.next}'
      ],
      'order': self.combine['order']
    }
    self._hh.combine(None, payload)

# -------------------------------------------------------------- #
# TreeNodeFKC1 - for record combination
# ---------------------------------------------------------------#
class TreeNodeFKC1(TreeNode):

  # -------------------------------------------------------------- #
  # extract
  # -------------------------------------------------------------- #
  def extract(self, record):
    self.dataset = []

	#------------------------------------------------------------------#
	# transform
	#------------------------------------------------------------------#
  def transform(self):
    logger.debug(f'{self.nodeName}, combining {self.seqnum.next} ...')    
    if not self._columnCheck:
      self.columnCheck()
    # pop foreign key to arrange for combination
    for fkey in self.combine['fkey']:
      self.dataset.pop(fkey)
    columns = list(self.dataset.keys())
    record = list(self.dataset.values())
    parentName = self.combine['nodeName']
    self._hh.combine(parentName, ['columns',self.nodeName,columns])
    self._hh.combine(parentName, ['record',self.nodeName,record])
