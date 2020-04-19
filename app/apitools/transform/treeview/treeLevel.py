__all__ = ['NodeTree','TreeLevel']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note, TaskError
import logging
import sys

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# AbstractTreeLevel
# ---------------------------------------------------------------#
class AbstractTreeLevel:

  @property
  def name(self):
    return self.__class__.__name__

  # -------------------------------------------------------------- #
  # __init__
  # ---------------------------------------------------------------#
  def __init__(self, level=0):
    self.level = level
    self.next = None
    self.isRoot = True if level == 0 else False

  # -------------------------------------------------------------- #
  # _arrange
  # ---------------------------------------------------------------#
  def _arrange(self, taskNum):
    currNode = self._nodeLeft
    while currNode:
      currNode.arrange(taskNum)
      currNode = currNode.next

  # -------------------------------------------------------------- #
  # _assemble - add current tree level treeNodes
  # ---------------------------------------------------------------#
  def _assemble(self, provider):
    self._nodeLeft = currNode = provider.getNode()
    while currNode:
      currNode.next = provider.getNode()
      logger.debug(f'{self.name}, node : {str(currNode)}')
      currNode = currNode.next

  # -------------------------------------------------------------- #
  # NodeTree class exclusively handles tree level assembly
  # ---------------------------------------------------------------#
  def assemble(self):
    raise NotImplementedError(f'{self.name}.assemble is an abstract method')

  # -------------------------------------------------------------- #
  # findNode
  # ---------------------------------------------------------------#
  def findNode(self, nodeName):
    currNode = self._nodeLeft
    while currNode:
      if currNode.nodeName == nodeName:
        return currNode
      currNode = currNode.next
    return None

# -------------------------------------------------------------- #
# TreeLevel
# ---------------------------------------------------------------#
class TreeLevel(AbstractTreeLevel):

  # -------------------------------------------------------------- #
  # _compile - branch nodes
  # ---------------------------------------------------------------#
  def _compile(self):
    currNode = self._nodeLeft
    while currNode:
      currNode.compile()
      currNode = currNode.next

  # -------------------------------------------------------------- #
  # _extract - extract datasets for each current level node
  # ---------------------------------------------------------------#
  def _extract(self):
    currNode = self._nodeLeft
    while currNode:
      currNode.extract()
      currNode = currNode.next

  # -------------------------------------------------------------- #
  # _dump - dump datasets for each current level node
  # ---------------------------------------------------------------#
  def _dump(self):
    currNode = self._nodeLeft
    while currNode:
      currNode.dump()
      currNode = currNode.next

# -------------------------------------------------------------- #
# NodeTree
# ---------------------------------------------------------------#
class NodeTree(TreeLevel):

  def __init__(self, rootName):
    super().__init__()
    self.rootName = rootName
    self.tableMap = None
    self.rowcount = 0

  # -------------------------------------------------------------- #
  # add
  # ---------------------------------------------------------------#
  def add(self):
    raise NotImplementedError(f'{self.name}.add is an abstract method')

  # -------------------------------------------------------------- #
  # arrange tree levels
  # ---------------------------------------------------------------#
  def arrange(self, taskNum):
    self._nodeLeft.arrange(taskNum)
    currLevel = self
    while currLevel:
      currLevel._arrange(taskNum)
      currLevel = currLevel.next

  # -------------------------------------------------------------- #
  # assemble tree levels
  # ---------------------------------------------------------------#
  def assemble(self, provider):
    provider.prepare()
    self._assemble(provider)
    currLevel = self
    while provider.hasNext:
      currLevel.next = nextLevel = provider.getLevel()
      nextLevel._assemble(provider)
      currLevel = currLevel.next

  # -------------------------------------------------------------- #
  # count
  # ---------------------------------------------------------------#
  def count(self):    
    self.rowcount += 1
    logger.debug(f'{self.name}, normalise record {self.rowcount} ...')    

  # -------------------------------------------------------------- #
  # reset
  # ---------------------------------------------------------------#
  def reset(self):
    self.rowcount = 0

  # -------------------------------------------------------------- #
  # result
  # ---------------------------------------------------------------#
  def result(self):
    return self.rowcount
