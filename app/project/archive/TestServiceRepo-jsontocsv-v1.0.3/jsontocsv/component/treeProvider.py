# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note
from apitools.transform import TreeProviderA, AbstractTaskMember
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
    logger.info(f"{cls.__name__}, making {schema.schemaDefn['schemaName']} node tree ...")
    provider = cls()
    nodeTree = provider.makeTree(schema.nodeTree)
    for nodeName, config in schema.inputNodeDefn.items():
      logger.debug(f'{provider.name}, input node name : {nodeName}, config : {config}')
      node = provider._getNode(nodeName, config, TaskMember)
      nodeTree.add(node)
      provider.add(node)
    logger.debug(f'########### NODESET : {provider.nodeset}')

    nodeTree.tableMap = {}
    for recnum, tableName in enumerate(schema.schemaDefn['tables']):
      taskNum = recnum + 1
      nodeTree.tableMap[taskNum] = (tableName, schema.tableMap[tableName])
    logger.debug(f'########### TABLES : {nodeTree.tableMap}')
    nodeTree.assemble(provider)
    cls._nodeTree = nodeTree
    return provider

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
    self._arrange(taskNum)