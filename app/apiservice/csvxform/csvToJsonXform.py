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
from apitools.hardhash import HardHashActor, HHProvider
from collections import deque, OrderedDict
import csv
import logging
import os, sys, time
import simplejson as json
import subprocess
import zmq

logger = logging.getLogger('apipeer.multi')

# -------------------------------------------------------------- #
# NormaliserFactory
# ---------------------------------------------------------------#
class NormaliserFactory(HardHashActor):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)

  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  def runActor(self, jobId, taskNum):
    try:
      logger.info(f'### NormaliserFactory {taskNum} is called ... ###')
      # unique nodeName and xformMeta is mapped by taskNum
      ##-------------------------------------------------------------##
      # NOTE : this section will be moved into the start method to
      # make a generic interface for automatic programmming
      ##-------------------------------------------------------------##
      dbKey = '%s|XFORM|META|%d' % (jobId, taskNum)
      xformMeta = self._leveldb[dbKey]
      logger.info('csvToJsonXform.NormaliserFactory - name, classTag : %s, %s ' 
          % (xformMeta['nodeName'], xformMeta['classTag']))
      try:
        className = 'Normalise' + xformMeta['classTag']
        klass = getattr(sys.modules[__name__], className)
      except AttributeError:
        errmsg = '%s class does not exist in %s' % (className, __name__)
        raise Exception(errmsg)
      dbKey = '%s|workspace' % jobId
      workSpace = self._leveldb[dbKey]

      csvFileName = '%s.csv' % xformMeta['tableName']
      logger.info('### normalise workspace : ' + workSpace)
      logger.info('### csv filename : ' + csvFileName)
      csvFilePath = '%s/%s' % (workSpace, csvFileName)
      if not os.path.exists(csvFilePath):
        errmsg = '%s does not exist in workspace' % csvFileName
        raise Exception(errmsg)

      obj = klass()
      obj.start(jobId, self.actorId, xformMeta)
      obj.normalise(csvFilePath)
    except Exception as ex:
      logger.error('actor %s errored', self.actorId, exc_info=True)
      raise

#------------------------------------------------------------------#
# Normaliser
#------------------------------------------------------------------#
class Normaliser:
  __metaclass__ = ABCMeta

  def __init__(self):
    self._hh = None
    self.isRoot = False

  # -------------------------------------------------------------- #
  # _putObjects
  # -------------------------------------------------------------- #
  @abstractmethod
  def _putObjects(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  def start(self, jobId, actorId, xformMeta):
    self.applyMeta(jobId, xformMeta)
    self._hh = HHProvider.start(jobId, actorId)

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, csvFilePath):
    try:      
      with open(csvFilePath) as csvfh:
        csvReader = csv.reader(csvfh,quotechar='"', 
                                    doublequote=False, escapechar='\\')
        keys = next(csvReader)
        dbKey = '%s|columns' % self.name
        self._hh[dbKey] = keys
        self.recnum = 0
        for values in csvReader:
          self.recnum += 1
          self.putObjects(keys, values)
        if self.isRoot:
          dbKey = 'ROOT|rowcount'
          self._hh[dbKey] = self.recnum
          dbKey = '%s|rowcount' % self.name
          self._hh[dbKey] = self.recnum
          logger.info('### ROOT %s rowcount, key, value : %s, %d' % (self.name, dbKey, self.recnum))
        logger.info('#### %s rowcount : %d' % (self.name, self.recnum))
    except csv.Error as ex:
      errmsg = 'normalise error, dataset : %s, line: %d' \
                                      % (self.name, csvReader.line_num)
      logger.error(errmsg)
      raise Exception(ex)

	#------------------------------------------------------------------#
	# putObjects
	#------------------------------------------------------------------#
  def putObjects(self, keys, values):
    try:
      self._putObjects(keys, values)
    except KeyError as ex:
      errmsg = 'key error : %s, all keys : %s' % (str(ex), str(keys))
      raise Exception(errmsg)

	#------------------------------------------------------------------#
	# getUkValue
	#------------------------------------------------------------------#
  def getUkValue(self, record):
    ukvalue = [record[key] for key in self.ukey]
    return '|'.join(ukvalue)

	#------------------------------------------------------------------#
	# getFkValue
	#------------------------------------------------------------------#
  def getFkValue(self, record):
    fkvalue = [record[key] for key in self.fkey]
    return '|'.join(fkvalue)

	#------------------------------------------------------------------#
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, jobId, xformMeta):
    self.jobId = jobId 
    self.name = xformMeta['nodeName']
    self.ukey = xformMeta['ukey']
    self.hasParent = xformMeta['parent'] is not None
    self.fkey = xformMeta['parent']['fkey'] if self.hasParent else None

#------------------------------------------------------------------#
# NormaliseRN1
#------------------------------------------------------------------#
class NormaliseRN1(Normaliser):
  '''
  Normaliser, model - RootNode1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)
    self.isRoot = True

	#------------------------------------------------------------------#
  # _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

#------------------------------------------------------------------#
# NormaliseUKN1
#------------------------------------------------------------------#
class NormaliseUKN1(Normaliser):
  '''
  Normaliser, model - Unique Key Node 1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)

	#------------------------------------------------------------------#
  # _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

#------------------------------------------------------------------#
# NormaliseFKN1
#------------------------------------------------------------------#
class NormaliseFKN1(Normaliser):
  '''
  Normaliser, model - Foreign Key Node 1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)

	#------------------------------------------------------------------#
  # _putObjects
	#------------------------------------------------------------------#
  def _putObjects(self, keys,values):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

# -------------------------------------------------------------- #
# CompilerFactory
# ---------------------------------------------------------------#
class CompilerFactory:
  
  def __init__(self, leveldb, jobId, actorId):
    self._hh = None
    self._leveldb = leveldb
    self.jobId = jobId
    self.actorId = actorId

  # -------------------------------------------------------------- #
  # get
  # ---------------------------------------------------------------#
  def get(self, nodeName, parent=None):
    dbKey = '%s|XFORM|META|%s' % (self.jobId, nodeName)
    xformMeta = self._leveldb[dbKey]
    logger.info('csvToJsonXform.CompilerFactory - name, classTag : %s, %s ' 
        % (xformMeta['tableName'], xformMeta['classTag']))
    try:
      className = 'Compile' + xformMeta['classTag']
      klass = getattr(sys.modules[__name__], className)
    except AttributeError:
      errmsg = 'xformMeta class %s does not exist in %s' % (className, __name__)
      raise Exception(errmsg)
    obj = klass(self, parent)
    obj.start(self.jobId, self.actorId, xformMeta)
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
# Compiler
#------------------------------------------------------------------#
class Compiler:
  __metaclass__ = ABCMeta

  def __init__(self, factory, parent):
    self._hh = None
    self.factory = factory
    self.parent = parent
    self.isRoot = False
    self.ukeys = []
    self.fkeyMap = {}
    self.jsObject = {}

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  @abstractmethod
  def compile(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # getJsObject
  # -------------------------------------------------------------- #
  @abstractmethod
  def getJsObject(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# putJsObject
	#------------------------------------------------------------------#
  @abstractmethod
  def putJsObject(self, *args, **kwargs):
    pass

	#------------------------------------------------------------------#
	# start
	#------------------------------------------------------------------#
  def start(self, jobId, actorId, xformMeta):
    self.applyMeta(jobId, xformMeta)
    self._hh = HHProvider.start(jobId, actorId)

	#------------------------------------------------------------------#
	# applyMeta
	#------------------------------------------------------------------#
  def applyMeta(self, jobId, xformMeta):
    self.jobId = jobId
    self.name = xformMeta['nodeName']
    self.ukeyName = '|'.join(xformMeta['ukey']) if xformMeta['ukey'] else None
    self.fkeyName = '|'.join(xformMeta['parent']['fkey']) if not self.isRoot else None
    self.nullPolicy = xformMeta['nullPolicy']
    self.isLeafNode = xformMeta['children'] is None
    self.children = xformMeta['children']
    self.ukeyType = None
    self.subType = None
    if self.isRoot:
      return
    ukeyPolicy = xformMeta['parent']['ukeyPolicy']    
    self.ukeyType = ukeyPolicy['type']
    self.subType = ukeyPolicy['subType']

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
	# build
	#------------------------------------------------------------------#
  def build(self):
    for ukey in self.parent.ukeys:
      jsObject = self.parent.jsObject[ukey]
      if jsObject: # test if parent obj != {}
        jsObject[self.name] = self.getJsObject(ukey)
        self.parent.jsObject[ukey] = jsObject

	#------------------------------------------------------------------#
	# getEmptyObj
  # only relevent if subType == HASH
	#------------------------------------------------------------------#
  def getEmptyObj(self):
    if self.nullPolicy['IncEmptyObj']:
      dbKey = '%s|columns' % self.name
      columns = self._hh[dbKey]
      return OrderedDict([(colname, "") for colname in columns])
    return {}

#------------------------------------------------------------------#
# CompileRN1
#------------------------------------------------------------------#
class CompileRN1(Compiler):
  '''
  CompileJson, model - RootNode1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)
    self.isRoot = True

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  def compile(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
  # getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
  # putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
	# setRestoreMode
	#------------------------------------------------------------------#
  def setRestoreMode(self, mode):
    self._hh.setRestoreMode(mode)

	#------------------------------------------------------------------#
	# rowCount
	#------------------------------------------------------------------#
  @property
  def rowCount(self):
    dbKey = '%s|rowcount' % self.name
    return int(self._hh[dbKey])

#------------------------------------------------------------------#
# CompileUKN1
#------------------------------------------------------------------#
class CompileUKN1(Compiler):
  '''
  CompileJson, model - Unique Key Node1
  ''' 
  def __init__(self, *args):
    super().__init__(*args)

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  def compile(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
  # getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
  # putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

#------------------------------------------------------------------#
# CompileJsonFKN1
#------------------------------------------------------------------#
class CompileFKN1(Compiler):
  '''
  CompileJson, model - Foreign Key Node1
  Foreign Key model is defined by hasUkey == False
  In this case, the ukeyPolicy type, subType are always OneToMany, LIST respectively
  ''' 
  def __init__(self, *args):
    super().__init__(*args)

  # -------------------------------------------------------------- #
  # compile
  # -------------------------------------------------------------- #
  def compile(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
  # getJsObject
	#------------------------------------------------------------------#
  def getJsObject(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

	#------------------------------------------------------------------#
  # putJsObject
	#------------------------------------------------------------------#
  def putJsObject(self, *args, **kwargs):
    raise NotImplementedError('for enterprise enquires, contact pmg7670@gmail.com')

#------------------------------------------------------------------#
# JsonCompiler
#------------------------------------------------------------------#
class JsonCompiler(HardHashActor):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)

  #------------------------------------------------------------------#
	# runActor
	#------------------------------------------------------------------#
  def runActor(self, jobId, taskNum):
    try:
      logger.info('### JsonCompiler %d is called ... ###' % taskNum)
      dbKey = '%s|XFORM|rootname' % jobId
      self.rootName = self._leveldb[dbKey]
      logger.info('### xform root nodename : ' + self.rootName)
      self.compileJson(jobId)
    except Exception as ex:
      logger.error('actor %s errored', self.actorId, exc_info=True)
      raise

  #------------------------------------------------------------------#
	# compileJson
	#------------------------------------------------------------------#
  def compileJson(self, jobId):
    self.factory = CompilerFactory(self._leveldb, jobId, self.actorId)
    self.rootMember = self.factory.get(self.rootName)
    jsonDom = list(self.getJsDomAsQueue())
    for rootKey in self.rootKeySet():
      self.compileJsObject(rootKey, jsonDom)
    logger.info('### csvToJson compile step is done ...')

	#------------------------------------------------------------------#
  # compileJsObject
  #
  # Convert the python object to json text and write to the output file
  # Return a policy object, adding all the child components to the
  # policy details object
	#------------------------------------------------------------------#
  def compileJsObject(self, rootKey, jsonDom):
    self.rootMember.compile(rootKey)
    for nextMember in jsonDom:
      nextMember.compile()
    for nextMember in jsonDom:
      if nextMember.isLeafNode:
        self.buildJsObject(nextMember)
    self.rootMember.putJsObject(rootKey)

	#------------------------------------------------------------------#
	# buildJsObject
  # - child object build depends on parent keys so this means start 
  # - at the leaf node and traverse back up
	#------------------------------------------------------------------#
  def buildJsObject(self, nextMember):
    while not nextMember.isRoot:
      nextMember.build()
      nextMember = nextMember.parent

	#------------------------------------------------------------------#
	# getJsDomAsQueue
	#------------------------------------------------------------------#
  def getJsDomAsQueue(self):
    from collections import deque
    logger.info("### root name  : " + self.rootName)
    domQueue = deque(self.rootMember.getMembers())
    while domQueue:
      nextMember = domQueue.popleft()
      memberList = nextMember.getMembers()
      if memberList:
        domQueue.extendleft(memberList)        
      yield nextMember

  #------------------------------------------------------------------#
	# rootKeySet
	#------------------------------------------------------------------#
  def rootKeySet(self):
    rowCount = self.rootMember.rowCount

    logger.info('### %s row count : %d' % (self.rootName, rowCount))
    for rowNum in range(1, rowCount+1):
      yield '%05d' % rowNum

#------------------------------------------------------------------#
# JsonComposer
#------------------------------------------------------------------#
class JsonComposer(HardHashActor):
  def __init__(self, leveldb, actorId):
    super().__init__(leveldb, actorId)

  #------------------------------------------------------------------#
	# runActor
	#------------------------------------------------------------------#
  def runActor(self, jobId, taskNum):
    try:
      logger.info('### JsonComposer %d is called ... ###' % taskNum)
      dbKey = '%s|workspace' % jobId
      self.workSpace = self._leveldb[dbKey]
      dbKey = '%s|OUTPUT|jsonFile' % jobId
      self.jsonFile = self._leveldb[dbKey]
      dbKey = '%s|XFORM|rootname' % jobId
      self.rootName = self._leveldb[dbKey]

      logger.info('### task workspace : ' + self.workSpace)
      logger.info('### output json file : ' + self.jsonFile)
      logger.info('### xform root nodename : ' + self.rootName)

      self.writeJsonFile(jobId)
      self.compressFile()
    except Exception as ex:
      logger.error('actor %s errored', self.actorId, exc_info=True)
      raise

  #------------------------------------------------------------------#
	# rootKeySet
	#------------------------------------------------------------------#
  def rootKeySet(self):
    rowCount = self.rootMember.rowCount    
    logger.info('### %s row count : %d' % (self.rootName, rowCount))

    for rowNum in range(1, rowCount+1):
      yield '%05d' % rowNum

  # -------------------------------------------------------------- #
  # writeJsonFile
  # ---------------------------------------------------------------#
  def writeJsonFile(self, jobId):
    factory = CompilerFactory(self._leveldb, jobId, self.actorId)
    self.rootMember = factory.get(self.rootName)
    self.rootMember.setRestoreMode('JSON')
    jsonPath = '%s/%s' % (self.workSpace, self.jsonFile)
    with open(jsonPath,'w') as jsfh:
      for rootKey in self.rootKeySet():
        jsObject = self.rootMember.getJsObject(rootKey)
        jsfh.write(jsObject + '\n')

  # -------------------------------------------------------------- #
  # compressFile
  # ---------------------------------------------------------------#
  def compressFile(self):
    logger.info('gzip %s ...' % self.jsonFile)
    subprocess.call(['gzip',self.jsonFile],cwd=self.workSpace)
