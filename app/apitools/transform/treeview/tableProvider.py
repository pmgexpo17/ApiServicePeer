__all__ = ['TableProviderA']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import Article, Note, TaskError
from .provider import AbstractProvider
from .tableRow import *
import logging

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# TableProviderA
# ---------------------------------------------------------------#
class TableProviderA(AbstractProvider):
  _instance = None

	#------------------------------------------------------------------#
	# apply
	#------------------------------------------------------------------#
  @classmethod
  def apply(cls, instance):
    del instance._nodes
    cls._instance = instance

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  @classmethod
  def get(cls, taskNum):
    typeKey = 'RNA1' if taskNum == 1 else 'FKA1'
    assets = cls._instance[f'T{taskNum}']
    logger.info(f'taskNum {taskNum} assets : {assets}')
    tableRow = cls._instance[typeKey]
    tableRow.merge(assets)
    return tableRow.tableName, tableRow

	#------------------------------------------------------------------#
	# add
	#------------------------------------------------------------------#
  def add(self, treeNode):
    try:
      self._nodes[treeNode.tableName].append(treeNode)
    except KeyError:
      self._nodes[treeNode.tableName] = [treeNode]      

	#------------------------------------------------------------------#
	# update
	#------------------------------------------------------------------#
  def update(self, taskNum, tableName):
    nodeList = self._nodes[tableName]
    for treeNode in nodeList:
      if tableName == treeNode.tableName:
        assets = {
          'columns': treeNode.columns,
          'tableName': treeNode.tableName,
          'nodeName': treeNode.nodeName,
          'fkey': treeNode.fkey,
          'recnum': 0}
        break
    self[f'T{taskNum}'] = assets

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, MemberKlass):
    tablePrvdr = cls({'RNA1':None,'FKA1':None})
    tablePrvdr['RNA1'] = TableRowRNA1.make(MemberKlass)
    tablePrvdr['FKA1'] = TableRowFKA1.make(MemberKlass)
    tablePrvdr._nodes = {}
    return tablePrvdr
