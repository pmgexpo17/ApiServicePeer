__all__ = ['NodeTreeA1']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError
from apitools.transform import NodeTree
import logging
import sys

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# NodeTreeA1
# ---------------------------------------------------------------#
class NodeTreeA1(NodeTree):

  def __init__(self, rootName):
    super().__init__(rootName)
    self.nodes = {}

  # -------------------------------------------------------------- #
  # add
  # ---------------------------------------------------------------#
  def add(self, treeNode):
    self.nodes[treeNode.nodeName] = treeNode

  # -------------------------------------------------------------- #
  # start - called on reading the next tree node
  # ---------------------------------------------------------------#
  def start(self, tag, record):
    try :
      if tag == 'Root':
        return
      self.level += 1
      nodeKey = f'{tag}|{self.level}' 
      logger.debug(f'{self.name}, nodeName :  {nodeKey}')
      self.nodes[nodeKey].extract(record)
    except KeyError:
      logger.warn(f'{self.name}, not found : {nodeKey}')

  # -------------------------------------------------------------- #
  # end - called on leaving the current tree node
  # ---------------------------------------------------------------#
  def end(self, tag):
    try:
      if tag == 'Root':
        return
      nodeKey = f'{tag}|{self.level}'
      self.nodes[nodeKey].dump()
      logger.debug(f'{self.name}, dump : {nodeKey}')      
      self.level -= 1
    except KeyError:
      pass

  def data(self, data):
    # Ignore data inside nodes
    pass

  def close(self):
    pass
