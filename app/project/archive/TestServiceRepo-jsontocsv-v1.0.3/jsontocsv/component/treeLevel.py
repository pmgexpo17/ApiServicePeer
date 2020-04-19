__all__ = ['NodeTreeA1','TreeLevel']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note, TaskError
from .microserviceHdh import Microservice
from apitools.transform import NodeTree, TreeLevel
from collections import OrderedDict
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
    pass

	#------------------------------------------------------------------#
	# convertJs
	#------------------------------------------------------------------#
  def convertJs(self, jsRecord):
    try:
      return json.loads(jsRecord, object_pairs_hook=OrderedDict)
    except ValueError as ex:
      raise Exception(f'Json conversion failure : {jsRecord}')

  # -------------------------------------------------------------- #
  # extract
  # ---------------------------------------------------------------#
  def extract(self, record):
    self._nodeLeft.extract(record)
    currLevel = self.next
    while currLevel:
      currLevel._extract()
      currLevel = currLevel.next

  # -------------------------------------------------------------- #
  # dump
  # ---------------------------------------------------------------#
  def dump(self):
    currLevel = self
    while currLevel:
      currLevel._dump()
      currLevel = currLevel.next

  # -------------------------------------------------------------- #
  # normalise - ETL - extract, transform, load/dump
  # - this module is the standard version, input and output nodeTree
  # - is identical, thus no transform is required
  # ---------------------------------------------------------------#
  def normalise(self, jsRecord):
    self.count()
    record = self.convertJs(jsRecord)
    self.extract(record)
    self.dump()
