__all__ = ['NodeTreeA1','TreeLevel']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note, TaskError
from .microserviceHdh import Microservice
from apitools.transform import NodeTree, TreeLevel
import logging
import sys

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# NodeTree
# ---------------------------------------------------------------#
class NodeTreeA1(NodeTree):

  def __init__(self, rootName):
    super().__init__(rootName)

  # -------------------------------------------------------------- #
  # add
  # ---------------------------------------------------------------#
  def add(self, treeNode):
    pass

  # -------------------------------------------------------------- #
  # arrange tree levels
  # ---------------------------------------------------------------#
  def arrange(self, taskNum):
    self._hh = Microservice.connector(taskNum, self.name)
    logger.info(f'{self.name}, got microservice connector {self._hh.cid}')
    super().arrange(taskNum)

  # -------------------------------------------------------------- #
  # compile
  # ---------------------------------------------------------------#
  def compile(self, recnum):
    logger.debug(f'{self.name}, compiling {recnum:05} ...')
    self._nodeLeft.compile(recnum)
    currLevel = self.next
    while currLevel:
      currLevel._compile()
      currLevel = currLevel.next
    return self._nodeLeft.export() 

  # -------------------------------------------------------------- #
  # rowRange
  # ---------------------------------------------------------------#
  @property
  def rowRange(self):
    dbkey = f'{self.rootName}|rowcount'
    rowcount = self._hh[dbkey]
    rowrange = int(rowcount) + 1
    return range(1, rowrange)

