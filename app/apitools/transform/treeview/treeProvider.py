__all__ = ['TreeProviderA']
# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note
from collections import deque
from .provider import AbstractProvider
from .tableRow import *
from .treeLevel import *
from .treeNode import *
import logging
import sys

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# TreeProviderA
# ---------------------------------------------------------------#
class TreeProviderA(AbstractProvider):
  _nodeTree = None

  def __init__(self):
    self.level = 1
    self.treeLevel = Note({'curr':None,'prev':None})
    self.nodeset = {}
    self.maxLevel = 1

  @property
  def name(self):
    return f'{self.__class__.__name__}'

	#------------------------------------------------------------------#
	# add
	#------------------------------------------------------------------#
  def add(self, node):
    try:
      logger.debug(f'{self.name}, add node {node.nodeName} to nodeset[{node.level}] ...')
      self.nodeset[node.level].append(node)
    except KeyError:
      self.nodeset[node.level] = [node]
    if self.maxLevel < node.level:
      self.maxLevel = node.level

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  @classmethod
  def get(cls):
    return cls._nodeTree

  # -------------------------------------------------------------- #
  # getLevel
  # ---------------------------------------------------------------#
  def getLevel(self):
    self.treeLevel.prev = self.treeLevel.curr
    self.treeLevel.curr = TreeLevel(self.level)
    self.prepare()
    return self.treeLevel.curr

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self):
    self._nodeset = deque(self.nodeset[self.level])
    self._nodeset.append(None)
    self.level += 1

  # -------------------------------------------------------------- #
  # getNode
  # ---------------------------------------------------------------#
  def getNode(self):
    treeNode = self._nodeset[0]
    logger.debug(f'### {self.name}, next nodeset : {self._nodeset}')
    if treeNode is None:
      return None
    self._nodeset.rotate(-1)
    if self.treeLevel.prev:
      treeNode.parent = self.treeLevel.prev.findNode(treeNode.parentKey)
    return treeNode

  # -------------------------------------------------------------- #
  # _getNode
  # ---------------------------------------------------------------#
  def _getNode(self, nodeName, config, MemberKlass):
    treeNode = Note(config)
    moduleName = type(self).__module__
    logger.debug(f'{self.name}, {nodeName} classTag : {treeNode.classTag}')
    try:
      className = 'TreeNode' + treeNode.classTag
      klass = getattr(sys.modules[moduleName], className)
      return klass.make(nodeName, config, MemberKlass)
    except AttributeError:      
      errMsg = f'{nodeName} classTag {treeNode.classTag} does not exist in {__name__}'
      raise Exception(errMsg)

  # -------------------------------------------------------------- #
  # makeTree
  # ---------------------------------------------------------------#
  def makeTree(self, config):
    try:
      moduleName = type(self).__module__
      classTag = config['classTag']
      className = 'NodeTree' + classTag
      logger.debug(f'{self.name}, nodeTree class : {className}')
      klass = getattr(sys.modules[moduleName], className)
      self.treeLevel.curr = nodeTree = klass(config['rootName'])
      return nodeTree
    except AttributeError:      
      errMsg = f'NodeTree classTag {classTag} does not exist in {__name__}'
      raise Exception(errMsg)

  # -------------------------------------------------------------- #
  # hasNext
  # ---------------------------------------------------------------#
  @property
  def hasNext(self):
    return self.level <= self.maxLevel

  # -------------------------------------------------------------- #
  # reset
  # ---------------------------------------------------------------#
  def reset(self):
    self.level = 1
