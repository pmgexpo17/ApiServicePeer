__all__ = ['TreeProvider','TableProviderA','AbstractTaskMember']
# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note
from apitools.transform import TreeProviderA, TableProviderA, AbstractTaskMember
from .microserviceHdh import Microservice
from .treeNode import *
from .treeLevel import *
import logging
import sys

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# TreeProvider
# ---------------------------------------------------------------#
class TreeProvider(TreeProviderA):

  # -------------------------------------------------------------- #
  # __start__
  # ---------------------------------------------------------------#
  @classmethod
  def __start__(cls, metaFile):
    if not cls._nodeTree:
      with open(metaFile,'r') as fhr:
        try:
          schema = Note(json.load(fhr))
          provider = cls.make(schema)
          logger.debug(f'########## schema is loaded, maxLevel : {provider.maxLevel}')
          del provider
        except ValueError as ex:
          errmsg = 'json load error: ' + str(ex) 
          raise Exception(errmsg)

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, schema):
    # provide this module as source for TreeNode and NodeTree objects
    logger.info(f"{cls.__name__}, making {schema.schemaDefn['schemaName']} node tree ...")
    treePrvdr = cls()
    nodeTree = treePrvdr.makeTree(schema.nodeTree)
    tablePrvdr = TableProviderA.make(TaskMember)
    for nodeName, config in schema.inputNodeDefn.items():
      logger.debug(f'{treePrvdr.name}, input node name : {nodeName}, config : {config}')
      node = treePrvdr._getNode(nodeName, config, TaskMember)
      nodeTree.add(node)
      treePrvdr.add(node)
      tablePrvdr.add(node)
    logger.debug(f'########### NODESET : {treePrvdr.nodeset}')

    nodeTree.tableMap = {}
    for recnum, tableName in enumerate(schema.schemaDefn['tables']):
      taskNum = recnum + 1
      nodeTree.tableMap[taskNum] = (tableName, schema.tableMap[tableName])
      tablePrvdr.update(taskNum, tableName)
    logger.debug(f'########### TABLES : {nodeTree.tableMap}')
    nodeTree.assemble(treePrvdr)
    cls._nodeTree = nodeTree
    tablePrvdr.apply(tablePrvdr)
    return treePrvdr

# -------------------------------------------------------------- #
# TaskMember
# ---------------------------------------------------------------#
class TaskMember(AbstractTaskMember):

	#------------------------------------------------------------------#
	# arrange
	#------------------------------------------------------------------#
  def arrange(self, taskNum):
    self.taskNum = taskNum
    logger.info(f'{self.name}, taskNum, nodeName : {taskNum}, {self.nodeName}')
    self._hh = Microservice.connector(taskNum, self.name)
    logger.info(f'{self.name}, got microservice connector {self._hh.cid}')
