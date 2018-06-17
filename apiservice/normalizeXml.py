# Copyright (c) 2018 Peter A McGill
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. 
#
import os, sys, time
import logging
import json
import requests, re
from apiservice import AppDelegate, logger

# -------------------------------------------------------------- #
# NormalizeXml
# ---------------------------------------------------------------#
class NormalizeXml(AppDirector, StreamPrvdr):

  def __init__(self, leveldb, jobId):
    super(WcEmltnDirector, self).__init__(leveldb, jobId)
    self.appType = 'delegate'
    self.state.hasNext = True
    self.resolve = ResolveUnit()
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    _pmeta = self.getProgramMeta()
    pmeta = _pmeta['NormalizeXml']
    _globals = pmeta['globals'] # append global vars in this list
    del(pmeta['globals'])
    if _globals[0] == '*':
      pmeta.update(_pmeta['Global'])
    else:
      for item in _globals:
        pmeta[item] = _pmeta['Global'][item]    
    self.resolve.pmeta = pmeta
    self.tsXref = self.resolve._start()

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    logger.info('state transition is resolved, advancing ...')
    if self.state.transition == 'RENDER_STREAM' and not self.resolve.toRender:
      self.state.transition = 'NA'
      self.state.inTransition = False 
      self.state.complete = True
      self.state.hasNext = False

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#                                          
  def getProgramMeta(self):

    dbKey = 'PMETA|' + self.state.jobId
    try:
      _pmeta = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEEOWWW! pmeta json document not found : {}'.format(dbKey))
    return json.loads(_pmeta)

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'RENDER_STREAM':
      self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, errorMsg):
    self.putApiRequest(500)

	# -------------------------------------------------------------- #
	# renderStream
	# ---------------------------------------------------------------#
  def renderStream(self, tableName):
    startKey = '%s|%s|ROW1' % (self.tsxref, tableName)
    endKey = '%s|%s|ROW%d' % (self.tsxref, tableName, self.rowcount+1)
    itemIter = self._leveldb.RangeIter(startKey, endKey)
    def generate():
      while True:
        try:
          key, item = itemIter.next()
          row = self.dlm.join(item)
          yield row + '\n'
        except StopIteration:
          break
    self.resolve.toRender.remove(tableName)
    return Response(generate(), mimetype='text')

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'wcEmltnInputPrvdr:WcEmltnInputPrvdr'
    pdata = (self.jobId,classRef,json.dumps({'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","kwargs":%s,"args":[]}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/job/1'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# ResolveUnit
# ---------------------------------------------------------------#
class ResolveUnit(AppResolveUnit):
  def __init__(self):
    self.__dict__['NORMALIZE'] = self.NORMALIZE
    self.__dict__['STREAM_REPEAT'] = self.STREAM_REPEAT
    self.pmeta = None

	# -------------------------------------------------------------- #
	# NORMALIZE
	# ---------------------------------------------------------------#
  def NORMALIZE(self):

    parser = XMLParser(target=self, recover=True)

    logger.info('parsing : ' + self.pmeta['inputXml'])
    with open(self.pmeta['inputXml'], 'r') as fhr:
      parser.feed('<Root>\n')
      for xmlRecord in fhr:
        self.next()
        try:
          parser.feed(xmlRecord)
        except ParseError as ex:
          logger.error(str(ex))
    parser.feed('<\Root>\n')
    self.complete()
    parser.close()
    self.state.next = 'STREAM_REPEAT'
    self.state.hasNext = True
    return self.state

	# -------------------------------------------------------------- #
	# STREAM_REPEAT
	# ---------------------------------------------------------------#
  def STREAM_REPEAT(self, tableName):

    if self.state.transition == 'NA':
      self.toRender = self.tzrByName.keys()
      del(self.tzrByKey)
      del(self.tzrByName)
      self.state.transition = 'RENDER_STREAM'
      self.state.inTransition = True
      self.state.hasNext = True

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self):
    dbKey = 'TSXREF|' + self.jobId
    self.tsXref = self._leveldb.Get(dbKey)
    xmlSchema = '%s/%s/%s' %  (self.pmeta['ciwork'],self.tsXref,self.pmeta['xmlSchema'])
    self.importXmlSchema(xmlSchema)
    self.checkXmlExists()
    return self.tsXref

  # -------------------------------------------------------------- #
  # importXmlSchema
  # ---------------------------------------------------------------#
  def importXmlSchema(self, xmlSchema):
    self.depth = 0
    self.rowcount = 0
    self.tzrByKey = {}
    tzrByName = {}      
    with open(xmlSchema,'r') as fhr:
      schemaDoc = json.load(fhr)
    tableMap = schemaDoc['tableMap']
    tableDefn = schemaDoc['tableDefn']
    columnOrder = schemaDoc['columnOrder']
    endKey = schemaDoc['endKey']
    for tableName, detail in tableDefn.items():
      logger.debug('tableName : %s, details : %s' % (tableName, str(detail)))
      tablizer = XmlTablizer(_leveldb,timestamp,detail,tableName)
      try :
        tablizer.endKey = endKey[tableName]  
      except KeyError:
        tablizer.endKey = columnOrder[tableName][0]
      tablizer.COLS['order'] = columnOrder[tableName]
      tzrByName[tableName] = tablizer

    columnDefn = schemaDoc['columnDefn']
    for nodeKey, tableName in tableMap.items():
      tablizer = tzrByName[tableName]
      tablizer.COLS[nodeKey] = columnDefn[nodeKey]
      self.tzrByKey[nodeKey] = tablizer
    self.tzrByName = tzrByName

  # -------------------------------------------------------------- #
  # checkXmlExists
  # ---------------------------------------------------------------#                                          
  def checkXmlExists(self):
    logger.info('[START] checkXmlExists')

    inputXmlPath = '%s/inputXml/%s' % (self.pmeta['linkBase'],self.pmeta['inputXmlFile'])
    try:
      self.runProcess(['ls', inputXmlPath])
    except Exception:
      errmsg = 'wcEmulation inputXmlFile is not found : ' +  inputXmlPath
      logger.error(errmsg)
      raise Exception(errmsg)
    else:
      logger.info('wcEmulation inputXmlFile : ' + inputXmlPath)
    logger.info('[END] checkInputFile')

  # -------------------------------------------------------------- #
  # next
  # ---------------------------------------------------------------#
  def next(self):
    self.rowcount += 1
    self.depth = 0
    logger.debug('parsing next xml record[%d] ... ' % self.rowcount)

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, tag, attrib):
    self.depth += 1
    try :
      nodeKey = '%s|%d' % (tag, self.depth)
    except KeyError:
      logger.warn('XmlTablizer has no mapping for node : ' + nodeKey)
    else:
      self.tzrByKey[nodeKey].addNext(nodeKey, attrib)

  # -------------------------------------------------------------- #
  # end
  # ---------------------------------------------------------------#
  def end(self, tag):
    try :
      nodeKey = '%s|%d' % (tag, self.depth)
      tablizer = self.tzrByKey[nodeKey]      
    except KeyError:
      pass
    else:
      if tablizer.endKey == nodeKey:
        branchId = '%d%d' % (self.rowcount, self.depth)
        tablizer.putNext(branchId)
    finally:
      self.depth -= 1

  def data(self, data):
    # Ignore data inside nodes
    pass

  def close(self):
    # Nothing special to do here
    pass
  
  def complete(self):
    logger.info('xml row count : %d' % self.rowcount)

# -------------------------------------------------------------- #
# XmlTablizer
# ---------------------------------------------------------------#
class XmlTablizer(object):
  
  def __init__(self, leveldb, tsxref, detail, tableName, tsxref):
    self._leveldb = leveldb
    self.tsxref = tsxref
    self.count = 0
    self.ukeyRef = None
    self.ukeyDefn = None
    if detail['ukeyDefn']:
      self.ukeyRef = detail['ukeyDefn'].pop(0)
      self.ukeyDefn = detail['ukeyDefn']
    self.fkeyRef = detail['fkeyRef']
    self.tableName = tableName
    self._record = {}
    self.COLS = {}

  # -------------------------------------------------------------- #
  # retrieve
  # ---------------------------------------------------------------#
  def retrieve(self, dbKey, noValue=None):
    try:
      record = self._leveldb.Get(dbKey)
      return json.loads(record)
    except KeyError:
      logger.error('key not found in key storage : ' + dbKey)
      return noValue

  # -------------------------------------------------------------- #
  # addNext
  # ---------------------------------------------------------------#
  def addNext(self, nodeKey, attrib):
    try:
      self.COLS[nodeKey]
    except KeyError:
      return
    record = []
    for colItem in self.COLS[nodeKey]:
      colValue = self.getAttr(colItem[0], colItem[1], attrib, nodeKey)
      record.append(colValue)    
    self._record[nodeKey] = record
    if self.ukeyRef == nodeKey:
      self.putUniqKey(nodeKey)
    

  # -------------------------------------------------------------- #
  # putNext
  # ---------------------------------------------------------------#
  def putNext(self, branchId):
    self.count += 1
    record = self.getForeignKey()
    nodeIndex = '%s%d' % (branchId,self.count)
    dbKey = '%s|%s|ROW%s' % (self.tsxref, self.tableName, nodeIndex)
    for nodeKey in self.COLS['order']:
      record += self._record[nodeKey]
    self._leveldb.Put(dbKey, json.dumps(record))
    logger.debug('%s : %s' % (dbKey, str(record)))

  # -------------------------------------------------------------- #
  # getAttr
  # ---------------------------------------------------------------#
  def getAttr(self, colName, noValue, attrib, nodeKey):
    try:
      return attrib[colName]
    except KeyError:
      logger.debug('%s xml node attribute not found : %s' % (nodeKey, colName))
      return noValue
    
  # -------------------------------------------------------------- #
  # putUniqKey
  # ---------------------------------------------------------------#
  def putUniqKey(self, nodeKey):

    record = self.getForeignKey()      
    dbKey = '%s|%s|UKEY' % (self.tsxref, self.tableName)
    for index in self.ukeyDefn:
      record.append(self._record[nodeKey][index])
    self._leveldb.Put(dbKey,json.dumps(record))
    logger.debug('%s : %s' % (dbKey, str(record)))

  # -------------------------------------------------------------- #
  # getForeignKey
  # ---------------------------------------------------------------#
  def getForeignKey(self):
    if self.fkeyRef:
      dbKey = '%s|%s|UKEY' % (self.tsxref, self.fkeyRef)
      return self.retrieve(dbKey,noValue=[])
    return []
