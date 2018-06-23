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
from apiservice import AppDirector, AppResolveUnit, SasScriptPrvdr, StreamPrvdr, logger
from flask import Response
from lxml.etree import XMLParser, ParseError
from threading import RLock
import os, sys, time
import logging
import json
import requests, re
from StringIO import StringIO

# -------------------------------------------------------------- #
# NormalizeXml
# ---------------------------------------------------------------#
class NormalizeXml(AppDirector, StreamPrvdr):

  def __init__(self, leveldb, jobId, caller):
    super(NormalizeXml, self).__init__(leveldb, jobId)
    self.scaller = caller[0] #scaller, the super program
    self.caller = caller[1]  #caller, the calling program
    self.appType = 'delegate'
    self.state.hasNext = True
    self.resolve = ResolveUnit(leveldb)
    self.resolve.state = self.state
    self.dlm = '<>'
    self.lock = RLock()

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    scriptPrvdr = WcScriptPrvdr(self._leveldb,self.jobId,self.scaller)
    tsXref, pmeta = scriptPrvdr()
    self.tsXref = tsXref
    self.resolve._start(tsXref, pmeta)

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    if self.state.transition == 'RENDER_STREAM' and not self.resolve.toRender:
      logger.info('state transition is resolved, advancing ...')
      self.state.transition = 'NA'
      self.state.inTransition = False 
      self.state.complete = True
      self.state.hasNext = False
    self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    logger.info('state transition : ' + self.state.transition)
    if self.state.transition == 'RENDER_STREAM':
      self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # onComplete
  # ---------------------------------------------------------------#
  def onComplete(self):
    pass

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, errorMsg):
    self.putApiRequest(500)

	# -------------------------------------------------------------- #
	# renderStream
	# ---------------------------------------------------------------#
  def renderStream(self, tableName):
    logger.info('RENDER_STREAM : ' + tableName)
    startKey = '%s|%s|ROW1' % (self.tsXref, tableName)
    endKey = '%s|%s|ROW:' % (self.tsXref, tableName)
    itemIter = self._leveldb.RangeIter(startKey, endKey)
    def generate():
      while True:
        try:
          key, item = itemIter.next()
          try:
            row = self.dlm.join(json.loads(item))
          except Exception as ex:
            logger.error('json loads failed : ' + str(ex))
            raise
          yield row + '\n'
        except StopIteration:
          break
    self.resolve.toRender.remove(tableName)
    return Response(generate(), status=201, mimetype='text/html')

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'wcEmltnInputPrvdr:WcEmltnInputPrvdr'
    pdata = (self.caller,classRef,json.dumps({'callee':self.jobId,'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","args":[],"kwargs":%s}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/job/1'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

# -------------------------------------------------------------- #
# ResolveUnit
# ---------------------------------------------------------------#
class ResolveUnit(AppResolveUnit):

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.__dict__['NORMALISE_XML'] = self.NORMALISE_XML
    self.__dict__['RENDER_STREAM'] = self.RENDER_STREAM
    self.pmeta = None

	# -------------------------------------------------------------- #
	# NORMALISE_XML
	# ---------------------------------------------------------------#
  def NORMALISE_XML(self):

    parser = XMLParser(target=self, recover=True)
    logger.info('parsing : ' + self.inputXmlFile)
    with open(self.inputXmlFile, 'r') as fhr:
      parser.feed('<Root>\n')
      for xmlRecord in fhr:
        self.next()
        try:
          parser.feed(xmlRecord)
        except ParseError as ex:
          logger.error(str(ex))
        except KeyError as ex:
          logger.error('!! parser key error !!' + str(ex))
    parser.feed('<\Root>\n')
    self.complete()
    parser.close()
    self.state.next = 'RENDER_STREAM'
    self.state.hasNext = True
    return self.state

	# -------------------------------------------------------------- #
	# RENDER_STREAM
	# ---------------------------------------------------------------#
  def RENDER_STREAM(self):

    if self.state.transition == 'NA':
      self.toRender = self.tzrByName.keys()
      del(self.tzrByKey)
      del(self.tzrByName)
      self.state.next = 'COMPLETE'
      self.state.transition = 'RENDER_STREAM'
      self.state.inTransition = True
      self.state.hasNext = True
    return self.state
    
  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, tsXref, pmeta):
    logger.info('[START] NormalizeXml._start')
    self.state.current = 'NORMALISE_XML'
    self.tsXref = tsXref
    logger.info('tsXref : ' + tsXref)
    self.pmeta = pmeta
    xmlSchema = '%s/assets/%s' %  (self.pmeta['ciwork'],self.pmeta['xmlSchema'])
    self.importXmlSchema(xmlSchema)
    self.checkXmlExists()

  # -------------------------------------------------------------- #
  # importXmlSchema
  # ---------------------------------------------------------------#
  def importXmlSchema(self, xmlSchema):
    logger.info('[START] NormalizeXml.importXmlSchema')
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
      tablizer = XmlTablizer(self._leveldb,self.tsXref,detail,tableName)
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
    logger.info('[START] NormalizeXml.checkXmlExists')

    inputXmlFile = '%s/inputXml/%s' % (self.pmeta['linkBase'],self.pmeta['inputXmlFile'])
    if os.path.exists(inputXmlFile):
      logger.info('wcEmulation inputXmlFile : ' + inputXmlFile)
    else:
      errmsg = 'wcEmulation inputXmlFile is not found : ' +  inputXmlFile
      logger.error(errmsg)
      raise Exception(errmsg)
    self.inputXmlFile = inputXmlFile

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
      self.tzrByKey[nodeKey]
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
  
  def __init__(self, leveldb, tsXref, detail, tableName):
    self._leveldb = leveldb
    self.tsXref = tsXref
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
    #record = []
    record = {}
    for colItem in self.COLS[nodeKey]:
      colName, colValue = self.getAttr(colItem, attrib, nodeKey)
      record[colName] = colValue
      #record.append(colValue)    
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
    dbKey = '%s|%s|ROW%s' % (self.tsXref, self.tableName, nodeIndex)
    for nodeKey in self.COLS['order']:
      columns = [colItem[0] for colItem in self.COLS[nodeKey]]
      for colName in columns:
        record.append(self._record[nodeKey][colName])
    self._leveldb.Put(dbKey, json.dumps(record))
    logger.debug('%s : %s' % (dbKey, str(record)))

  # -------------------------------------------------------------- #
  # getAttr
  # ---------------------------------------------------------------#
  def getAttr(self, colItem, attrib, nodeKey):
    colName, nullVal = colItem[:2]
    try:
      return (colName, attrib[colName])
    except KeyError:
      logger.debug('%s xml node attribute not found : %s' % (nodeKey, colName))
      return (colName, nullVal)
    
  # -------------------------------------------------------------- #
  # putUniqKey
  # ---------------------------------------------------------------#
  def putUniqKey(self, nodeKey):

    record = self.getForeignKey()      
    dbKey = '%s|%s|UKEY' % (self.tsXref, self.tableName)
    columns = [colItem[0] for colItem in self.COLS[nodeKey]]
    for index in self.ukeyDefn:
      colName = columns[index]
      record.append(self._record[nodeKey][colName])
    self._leveldb.Put(dbKey,json.dumps(record))
    logger.debug('%s : %s' % (dbKey, str(record)))

  # -------------------------------------------------------------- #
  # getForeignKey
  # ---------------------------------------------------------------#
  def getForeignKey(self):
    if self.fkeyRef:
      dbKey = '%s|%s|UKEY' % (self.tsXref, self.fkeyRef)
      return self.retrieve(dbKey,noValue=[])
    return []

# -------------------------------------------------------------- #
# WcScriptPrvdr
# ---------------------------------------------------------------#
class WcScriptPrvdr(SasScriptPrvdr):

  def __init__(self, leveldb, jobId, scaller):
    self._leveldb = leveldb
    self.jobId = jobId
    self.scaller = scaller
    self.pmeta = None

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):
    pmeta = self.getProgramMeta()
    dbKey = 'TSXREF|' + self.scaller
    try:
      tsXref = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! tsXref param not found : ' + dbKey)
    return (tsXref, pmeta)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    dbKey = 'PMETA|' + self.scaller
    try:
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! pmeta json document not found : ' + dbKey)

    pmetaAll = json.loads(pmetadoc)
    pmeta = pmetaAll['NormalizeXml']
    _globals = pmeta['globals'] # append global vars in this list
    del(pmeta['globals'])
    if _globals[0] == '*':
      pmeta.update(pmetaAll['Global'])
    else:
      for item in _globals:
        pmeta[item] = pmetaAll['Global'][item]
    return pmeta
