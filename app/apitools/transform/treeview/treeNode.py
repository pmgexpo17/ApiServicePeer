__all__ = [
  'AbstractTreeNode',
  'SeqNumber']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import TaskError
import logging

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# AbstractTreeNode
# ---------------------------------------------------------------#
class AbstractTreeNode:

  def __init__(self, config):
    self.apply(config)
    self.dataset = None
    self._headerCheck = False
    self.next = None
    self.parent = None

  @property
  def name(self):
    return self.__class__.__name__

  def __repr__(self):
    return f'{self.name}.{self.nodeName}'

	#------------------------------------------------------------------#
	# apply
	#------------------------------------------------------------------#
  def apply(self, config):
    self.__dict__.update(config)

	#------------------------------------------------------------------#
	# _arrange
	#------------------------------------------------------------------#
  def _arrange(self, taskNum):
    raise NotImplementedError(f'{self.name}.arrange is an abstract method')

	#------------------------------------------------------------------#
	# dump
	#------------------------------------------------------------------#
  def dump(self):
    raise NotImplementedError(f'{self.name}.dump is an abstract method')

	#------------------------------------------------------------------#
	# make -- use MemberKlass to enable generic provider._getNode action
  # -- where MemberKlass is mixed into TreeNode providing the hardhash
  # -- connector provision method : arrange
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, nodeName, config, MemberKlass):
    className = f'{cls.__name__}-TaskMember'
    NodeKlass = type(className,(cls,MemberKlass),{})
    logger.info(f'{cls.__name__}, new dynamic class : {NodeKlass.__name__} ')
    node = NodeKlass(config)
    node.nodeName = nodeName
    tableName, level = nodeName.split('|')
    node.tableName = tableName
    node.level = int(level)
    node.isRoot = True if node.level == 1 else False  
    return node

	#------------------------------------------------------------------#
	# headerCheck
	#------------------------------------------------------------------#
  def headerCheck(self, dataset=None):
    logger.debug(f'{self.nodeName}, header check ...')
    if not dataset:
      dataset = self.dataset
    columnsDs = list(dataset.keys())
    if not columnsDs == self.columns:
      if set(columnsDs) ^ set(self.columns):
        specOnly = set(self.columns) - set(columnsDs)
        if specOnly:
          errTxt = 'some required columns do NOT exist in the actual dataset'
          errMsg = f'{self.nodeName}, {errTxt}\nMissing required columns : {specOnly}'
          raise TaskError(errMsg)
      else:
        warnMsg = 'dataset column order does not match the column definition'
        logger.warn(f'{self.nodeName}, {warnMsg}')
    self._headerCheck = True

	#------------------------------------------------------------------#
	# putHeader
	#------------------------------------------------------------------#
  def putHeader(self, dataset=None):
    logger.debug(f'{self.nodeName}, dumping header ...')
    if not dataset:
      dataset = self.dataset
    dbkey = f'{self.nodeName}|header'      
    recordL = list(dataset.keys())    
    self._hh[dbkey] = recordL

#------------------------------------------------------------------#
# SeqNumber
# - enables multitasking capacity
#------------------------------------------------------------------#
class SeqNumber:
  _instance = {}

  def __init__(self, taskNum):
    self._next = 0
    self._taskNum = taskNum

  # -------------------------------------------------------------- #
  # increment
  # ---------------------------------------------------------------#
  def increment(self):
    self._next += 1

  # -------------------------------------------------------------- #
  # next
  # ---------------------------------------------------------------#
  @property
  def next(self):
    return f'{self._taskNum:02}|{self._next:05}'

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  @classmethod
  def start(cls, taskNum):
    cls._instance[taskNum] = cls(taskNum)

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  @classmethod
  def get(cls, taskNum):
    try:
      return cls._instance[taskNum]
    except KeyError:
      logger.debug(f'### get SeqNumber instance for task {taskNum} ###')
      cls._instance[taskNum] = cls(taskNum)
      return cls._instance[taskNum]
