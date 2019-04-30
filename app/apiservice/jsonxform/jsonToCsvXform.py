# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
from abc import ABCMeta, abstractmethod
from apitools.hardhash import HardHashActor, HHProvider, EmptyViewResult
from collections import deque, OrderedDict
from threading import RLock
import logging
import os, sys, time
import simplejson as json
import zmq

logger = logging.getLogger('apipeer.multi')

# -------------------------------------------------------------- #
# JsonNormaliser
# ---------------------------------------------------------------#
class JsonNormaliser(HardHashActor):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)

  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  def runActor(self, jobId, taskNum):
    try:
      logger.info('### JsonNormaliser %d is called ... ###' % taskNum)
      dbKey = '%s|workspace' % jobId
      workSpace = self._leveldb[dbKey]
      dbKey = '%s|INPUT|%d|jsonFile' % (jobId, taskNum)
      self.jsonFile = self._leveldb[dbKey]
      dbKey = '%s|XFORM|rootname' % jobId
      rootName = self._leveldb[dbKey]

      logger.info('### normalise workspace : %s' % workSpace)
      logger.info('### task|%02d input json file : %s ' % (taskNum, self.jsonFile))
      logger.info('### xform root nodename : ' + rootName)

      jsonFilePath = workSpace + '/' + self.jsonFile
      if not os.path.exists(jsonFilePath):
        errmsg = '%s does not exist in workspace' % self.jsonFile
        raise Exception(errmsg)
      factory = MemberFactory(self._leveldb, self.actorId, jobId, taskNum)
      self.rootMember = factory.get(rootName)
      self.normalise(jsonFilePath)
    except Exception as ex:
      logger.error('actor %s error' % self.actorId, exc_info=True)
      raise BaseException(ex)

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, jsonFilePath):
    jsonDom = list(self.getJsDomAsQueue())      
    try:
      with open(jsonFilePath) as jsfh:
        recnum = 1
        for jsRecord in jsfh:
          self.evalJsonDom(jsRecord, jsonDom)
          recnum += 1
      logger.info('### %s rowcount : %d' % (self.jsonFile, recnum))
    except Exception as ex:
      errmsg = 'splitfile, recnum : %s, %d' % (self.jsonFile, recnum)
      logger.error(errmsg)
      raise

	#------------------------------------------------------------------#
	# evalJsonDom
	#------------------------------------------------------------------#
  def evalJsonDom(self, jsRecord, jsonDom):
    jsObject = self.getJsObject(jsRecord)
    self.rootMember.evalDataset(jsObject)
    for nextMember in jsonDom:
      nextMember.evalDataset()
    self.putDatasets(jsonDom)

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self):
    from collections import deque
    domQueue = deque(self.rootMember.getMembers())
    while domQueue:
      nextMember = domQueue.popleft()
      memberList = nextMember.getMembers()
      if memberList:
        domQueue.extendleft(memberList)        
      yield nextMember

	#------------------------------------------------------------------#
	# getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, jsRecord):
    try:
      return json.loads(jsRecord, object_pairs_hook=OrderedDict)
    except ValueError as ex:
      raise Exception('Json conversion failure, ' + str(ex))

	#------------------------------------------------------------------#
	# putDatasets
	#------------------------------------------------------------------#
  def putDatasets(self, jsonDom):
    hasObjects = True
    while hasObjects:
      hasObjects = self.rootMember.hasDataset
      for nextMember in jsonDom:
        hasObjects = hasObjects or nextMember.hasDataset
        if nextMember.isLeafNode:
          self.putDsByMember(nextMember)
      self.rootMember.putDataset()
    
	#------------------------------------------------------------------#
	# putDsByMember
	#------------------------------------------------------------------#
  def putDsByMember(self, nextMember):
    # child objects must be removed and put to db first before 
    # the parent object is put to db, so this means start at the
    # leaf node and traverse back up
    while not nextMember.isRoot:
      nextMember.putDataset()
      nextMember = nextMember.parent

# -------------------------------------------------------------- #
# MemberFactory
# ---------------------------------------------------------------#
class MemberFactory:

  def __init__(self, leveldb, actorId, jobId, taskNum):
    self._leveldb = leveldb
    self.actorId = actorId
    self.jobId = jobId
    self.taskNum = taskNum

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  def get(self, nodeName, parent=None):
    dbKey = '%s|XFORM|META|%s' % (self.jobId, nodeName)
    xformMeta = self._leveldb[dbKey]
    logger.info('jsonToCsvXform.MemberFactory - name, classTag : %s, %s ' 
        % (xformMeta['nodeName'], xformMeta['classTag']))
    try:
      className = 'JsonMember' + xformMeta['classTag']
      klass = getattr(sys.modules[__name__], className)
    except AttributeError:
      errmsg = 'xformMeta class %s does not exist in %s' % (className, __name__)
      raise Exception(errmsg)
    obj = klass(self, parent)
    obj.start(self.jobId, self.actorId, self.taskNum, xformMeta)
    return obj

  # -------------------------------------------------------------- #
  # getMembers
  # ---------------------------------------------------------------#
  def getMembers(self, parent):
    memberList = []
    for nodeName in parent.children:
      logger.info('%s member : %s ' % (parent.name, nodeName))
      memberList.append(self.get(nodeName, parent))
    return memberList

#------------------------------------------------------------------#
# RowIndex
# - enables multitasking csvWriter capacity
# - the related class is a singleton and cls.lock ensures that
# - concurrent writing is atomic
#------------------------------------------------------------------#
class RowIndex:
  lock = None
  rowCount = None
  taskNum = None

  # -------------------------------------------------------------- #
  # incCount
  # ---------------------------------------------------------------#
  @classmethod
  def incCount(cls):
    with cls.lock:
      cls.rowCount += 1
      return '%02d|%05d' % (cls.taskNum, cls.rowCount)

	#------------------------------------------------------------------#
	# start
  # - rowcount must be a class variable, to accmulate the total count
  # - when the json DOM contains multiple node class instances
	#------------------------------------------------------------------#
  @classmethod
  def start(cls, taskNum):
    if cls.lock is None:
      logger.info('### start %s counter ###' % cls.__name__)
      cls.lock = RLock()
      cls.taskNum = taskNum
      cls.rowCount = 0

#------------------------------------------------------------------#
# JsonMember
#------------------------------------------------------------------#
class JsonMember:
  __metaclass__ = ABCMeta
  lock = None

  def __init__(self, factory, parent):
    self._hh = None
    self.factory = factory
    self.parent = parent
    self.isRoot = False

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  @abstractmethod
  def _getDataset(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  @abstractmethod  
  def putDataset(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  def start(self, jobId, actorId, taskNum, xformMeta):
    self.applyMeta(jobId, taskNum, xformMeta)
    self._hh = HHProvider.start(jobId, actorId)

	#------------------------------------------------------------------#
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, jobId, taskNum, xformMeta):
    self.jobId = jobId
    self.name = xformMeta['nodeName']
    self.ukeyName = '|'.join(xformMeta['ukey']) if xformMeta['ukey'] else None
    self.fkeyName = '|'.join(xformMeta['parent']['fkey']) if not self.isRoot else None
    self.nullPolicy = xformMeta['nullPolicy']
    self.isLeafNode = xformMeta['children'] is None
    self.children = xformMeta['children']
    className = '%s%02d%s' % ('RowIndex',taskNum,xformMeta['tableTag'])
    if hasattr(sys.modules[__name__], className):
      self.rowIndex = getattr(sys.modules[__name__], className)
    else:
      rindexClass = type(className,(RowIndex,),{})
      rindexClass.start(taskNum)
      self.rowIndex = rindexClass

	#------------------------------------------------------------------#
	# getMembers
	#------------------------------------------------------------------#
  def getMembers(self):
    if self.isLeafNode:
      return None
    memberList = self.factory.getMembers(self)
    if self.isRoot:  
      return memberList
    memberList.reverse()
    return memberList

	#------------------------------------------------------------------#
	# evalDataset
  # - eval the next csvDataset, ready to put to the datastore
	#------------------------------------------------------------------#
  def evalDataset(self, csvDataset=None):
    self.csvDataset = csvDataset    
    self.hasDataset = False
    self.csvDataset = self.getDataset()
    if self.hasDataset:
      self.putHeader()

	#------------------------------------------------------------------#
	# getDataset
	#------------------------------------------------------------------#
  def getDataset(self):
    try:
      csvDataset = self._getDataset()
    except KeyError as ex:
      errmsg = '%s json object %s does not exist in csv dataset ' \
                                              % (self.name, str(ex))
      raise Exception(errmsg)
    if not csvDataset: # empty list or dict
      csvDataset = [] # if dict, convert empty dict to list
    elif not isinstance(csvDataset,list):
      csvDataset = [csvDataset]
    self.hasDataset = len(csvDataset) > 0
    return csvDataset    
    
	#------------------------------------------------------------------#
	# putHeader
	#------------------------------------------------------------------#
  def putHeader(self):
    csvObject = self.csvDataset[0]
    keyName = self.ukeyName if self.ukeyName else self.fkeyName
    keySet = set(keyName.split('|'))
    if not keySet.issubset(csvObject):
      errmsg = '%s key %s does not exist in json record' \
                                          % (self.name, keyName)
      raise Exception(errmsg)
    dbKey = '%s|header' % self.name
    self._hh[dbKey] = list(csvObject.keys())

	#------------------------------------------------------------------#
	# putCsvObject
	#------------------------------------------------------------------#
  def putCsvObject(self, csvObject):
    # self.rowIndex.incCount = klass.incCount
    rowIndex = self.rowIndex.incCount()
    dbKey = '%s|%s' % (self.name, rowIndex)
    self._hh[dbKey] = list(csvObject.values())

#------------------------------------------------------------------#
# JsonMemberRN1
#------------------------------------------------------------------#
class JsonMemberRN1(JsonMember):
  '''
  NormaliseJson, model - RootNode1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)
    self.isRoot = True

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  def _getDataset(self):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  def putDataset(self):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

#------------------------------------------------------------------#
# JsonMemberUKN1
#------------------------------------------------------------------#
class JsonMemberUKN1(JsonMember):
  '''
  JsonMemberJson, model - Unique Key Node 1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  def _getDataset(self):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  def putDataset(self, evalRoot=False):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

#------------------------------------------------------------------#
# JsonMemberJsonFKN1
#------------------------------------------------------------------#
class JsonMemberFKN1(JsonMember):
  '''
  JsonMemberJson, model - Foreign Key Node 1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)

  # -------------------------------------------------------------- #
  # _getDataset
  # -------------------------------------------------------------- #
  def _getDataset(self):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
	# putDataset
	#------------------------------------------------------------------#
  def putDataset(self, evalRoot=False):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')
    
#------------------------------------------------------------------#
# CsvComposer
#------------------------------------------------------------------#
class CsvComposer(HardHashActor):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)

  #------------------------------------------------------------------#
	# runActor
	#------------------------------------------------------------------#
  def runActor(self, jobId, jobRange, taskNum):
    try:
      logger.info('### CsvComposer %d is called ... ###' % taskNum)
      dbKey = '%s|XFORM|TABLENAME|%d' % (jobId, taskNum)
      tableName = self._leveldb[dbKey]
      dbKey = '%s|XFORM|NODEMAP|%d' % (jobId, taskNum)
      nodeList = self._leveldb[dbKey]
      dbKey = '%s|XFORM|CSVPATH|%d' % (jobId, taskNum)
      csvPath = self._leveldb[dbKey]

      logger.info('### task|%02d output csv file : %s.csv' % (taskNum, tableName))
      logger.info('### node list : ' + str(nodeList))

      with open(csvPath,'w') as csvfh:
        writer = CsvWriter(jobId, self.actorId, nodeList[0], jobRange)
        writer.writeAll(csvfh, nodeList)
        logger.info('### %s rowcount : %d' % (tableName, writer.total))
    except Exception as ex:
      logger.error('actor %s error' % self.actorId, exc_info=True)
      # apscheduler will only catch BaseException, EVENT_JOB_ERROR will not fire otherwise
      raise BaseException(ex)

#------------------------------------------------------------------#
# CsvWriter
#------------------------------------------------------------------#
class CsvWriter:
  def __init__(self, jobId, actorId, nodeName, jobRange):
    self._hh = HHProvider.start(jobId, actorId)
    self.nodeName = nodeName    
    self.jobRange = jobRange

  #------------------------------------------------------------------#
	# writeAll
	#------------------------------------------------------------------#
  def writeAll(self, csvFh, nodeList):
    self.writeHeader(csvFh)
    self.total = 0      
    for nodeName in nodeList:
      self.write(csvFh, nodeName)

  #------------------------------------------------------------------#
	# write
	#------------------------------------------------------------------#
  def write(self, csvFh, nodeName):
    for record in self.csvDataset(nodeName):
      csvFh.write(','.join(record) + '\n')
      self.total += 1

  #------------------------------------------------------------------#
	# csvDataset
	#------------------------------------------------------------------#
  def csvDataset(self, nodeName):
    try:
      keyLow = '%s|%02d|%05d' % (nodeName,1,1)
      keyHigh = '%s|%02d|%05d' % (nodeName,self.jobRange,99999)
      return self._hh.select(keyLow,keyHigh,keysOnly=False)
    except EmptyViewResult:
      logger.warn('!!! Empty view returned')
      return []

  #------------------------------------------------------------------#
	# writeHeader
	#------------------------------------------------------------------#
  def writeHeader(self, csvfh):
    dbKey = '%s|header' % self.nodeName
    record = ','.join(self._hh[dbKey])
    csvfh.write(record + '\n')
