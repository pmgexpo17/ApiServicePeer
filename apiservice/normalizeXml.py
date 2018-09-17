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
from apibase import AppDirector, AppResolvar, MetaReader, logger
from apitools.wcemltn import WcEmailPrvdr
from flask import Response
from lxml.etree import XMLParser, ParseError
import leveldb
import os, sys, time
import json
import requests

# -------------------------------------------------------------- #
# NormalizeXml
# ---------------------------------------------------------------#
class NormalizeXml(AppDirector):

  def __init__(self, leveldb, jobId, caller):
    super(NormalizeXml, self).__init__(leveldb, jobId)
    self.scaller = caller[0] #scaller, the super program
    self.caller = caller[1]  #caller, the calling program
    self._type = 'delegate'
    self.state.hasNext = True
    self.resolve = Resolvar(leveldb)
    self.resolve.state = self.state

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#                                          
  def _start(self):
    WcEmailPrvdr.subscribe('NormalizeXml')
    metaPrvdr = MetaPrvdr(self._leveldb,self.jobId,self.scaller)
    tsXref, pmeta = metaPrvdr()
    self.dlm = pmeta['delimiter']
    self.tsXref = tsXref
    self.resolve._start(tsXref, pmeta)

  # -------------------------------------------------------------- #
  # advance
  # ---------------------------------------------------------------#                                          
  def advance(self, signal=None):
    self.state.current = self.state.next
    return self.state

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):      
    if self.state.complete:
      logger.info('normalize xml complete, so quicken ...')
      self.putApiRequest(201)

  # -------------------------------------------------------------- #
  # putApiRequest
  # ---------------------------------------------------------------#
  def putApiRequest(self, signal):
    classRef = 'wcInputPrvdr:WcInputPrvdr'
    pdata = (self.caller,classRef,json.dumps({'callee':self.jobId,'signal':signal}))
    params = '{"type":"director","id":"%s","service":"%s","args":[],"kwargs":%s}' % pdata
    data = [('job',params)]
    apiUrl = 'http://localhost:5000/api/v1/smart'
    response = requests.post(apiUrl,data=data)
    logger.info('api response ' + response.text)

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    # if error is due to delegate failure then don't post an email
    if self.state.inTransition:
      self.putApiRequest(500)
      return
    # if WcResolvar has caught an exception an error mail is ready to be sent
    if not WcEmailPrvdr.hasMailReady('NormalizeXml'):
      method = 'normalizeXml.WcResolvar.' + self.state.current
      errdesc = 'system error'
      self.sendMail('ERR1',method,errdesc,str(ex))
    else:
      WcEmailPrvdr.sendMail('NormalizeXml')
    self.putApiRequest(500)

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  def sendMail(self,*args):      
    WcEmailPrvdr.sendMail('NormalizeXml',*args)

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.__dict__['EVAL_XML_SCHEMA'] = self.EVAL_XML_SCHEMA
    self.__dict__['NORMALIZE_XML'] = self.NORMALIZE_XML
    self.pmeta = None

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, tsXref, pmeta):
    logger.info('normalizeXml.Resolvar._start')
    self.state.current = 'EVAL_XML_SCHEMA'
    self.tsXref = tsXref
    logger.info('tsXref : ' + tsXref)
    self.pmeta = pmeta
    self.checkXmlExists()
      
  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    WcEmailPrvdr.newMail('NormalizeXml',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # checkXmlExists
  # ---------------------------------------------------------------#
  def checkXmlExists(self):
    self.method = 'normalizeXml.Resolvar.checkXmlExists'
    logger.info('normalizeXml.Resolvar.checkXmlExists')

    inputXmlFile = self.pmeta['xmlFilePath'] + '/' + self.pmeta['inputXmlFile']
    if os.path.exists(inputXmlFile):
      logger.info('wcEmulation inputXmlFile : ' + inputXmlFile)
    else:
      errmsg = 'wcEmulation inputXmlFile is not found : ' +  inputXmlFile
      self.newMail('ERR1','user error',errmsg)
      raise Exception(errmsg)
    self.inputXmlFile = inputXmlFile

	# -------------------------------------------------------------- #
	# EVAL_XML_SCHEMA
	# ---------------------------------------------------------------#
  def EVAL_XML_SCHEMA(self):
    self.method = 'normalizeXml.Resolvar.evalXmlSchema'
    try:
      self.evalXmlSchema(self)
    except Exception as ex:
      errdesc = 'xml schema doc : ' + self.xmlSchema
      self.newMail('ERR1',errdesc,str(ex))
      raise
      
	# -------------------------------------------------------------- #
	# evalXmlSchema
	# ---------------------------------------------------------------#
  def evalXmlSchema(self):
    self.xmlSchema = '%s/assets/%s' % (self.pmeta['ciwork'],self.pmeta['xmlSchema'])
    self.depth = 0
    self.rowcount = 0
    self.tzrByKey = {}
    tzrByName = {}      
    with open(self.xmlSchema,'r') as fhr:
      schemaDoc = json.load(fhr)
    tableMap = schemaDoc['tableMap']
    tableDefn = schemaDoc['tableDefn']
    columnOrder = schemaDoc['columnOrder']
    endKey = schemaDoc['endKey']
    self.batch = leveldb.WriteBatch()
    for tableName, detail in tableDefn.items():
      if not detail['keep']:
        continue
      logger.debug('tableName : %s, details : %s' % (tableName, str(detail)))
      #tablizer = XmlTablizer(self._leveldb,self.tsXref,detail,tableName)
      tablizer = XmlTablizer(self._leveldb,self.batch,self.tsXref,detail,tableName)
      try :
        tablizer.endKey = endKey[tableName]  
      except KeyError:
        tablizer.endKey = columnOrder[tableName][0]
      tablizer.COLS['order'] = columnOrder[tableName]
      tzrByName[tableName] = tablizer

    columnDefn = schemaDoc['columnDefn']
    for nodeKey, tableName in tableMap.items():
      try:
        tablizer = tzrByName[tableName]
      except KeyError:
        continue
      tablizer.COLS[nodeKey] = columnDefn[nodeKey]
      self.tzrByKey[nodeKey] = tablizer
    self.tzrByName = tzrByName
    self.state.next = 'NORMALIZE_XML'
    self.state.hasNext = True
    return self.state

	# -------------------------------------------------------------- #
	# NORMALIZE_XML
	# ---------------------------------------------------------------#
  def NORMALIZE_XML(self):
    try:
      self.normalizeXml(self)
    except Exception as ex:
      errdesc = 'xml input file : ' + self.inputXmlFile
      self.newMail('ERR1',errdesc,str(ex))
      raise

	# -------------------------------------------------------------- #
	# normalizeXml
	# ---------------------------------------------------------------#
  def normalizeXml(self):
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
    parser.close()
    self.state.complete = True
    self.state.hasNext = False
    self.state.hasSignal = True
    return self.state

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
      logger.debug('XmlTablizer has no mapping for node : ' + nodeKey)
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
    self._leveldb.Write(self.batch, sync=True)
    logger.info('!! Xml row count : %d !!' % self.rowcount)

# -------------------------------------------------------------- #
# XmlTablizer
# ---------------------------------------------------------------#
class XmlTablizer(object):
  
  def __init__(self, leveldb, batch, tsXref, detail, tableName):
    self._leveldb = leveldb
    self._batch = batch
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
    record = []
    for colItem in self.COLS[nodeKey]:
      record += self.getAttr(colItem[0], colItem[1], attrib, nodeKey)
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
      record += self._record[nodeKey]
    self._batch.Put(dbKey, json.dumps(record))
    logger.debug('%s : %s' % (dbKey, str(record)))

  # -------------------------------------------------------------- #
  # getAttr
  # ---------------------------------------------------------------#
  def getAttr(self, colName, nullVal, attrib, nodeKey):
    try:
      return [attrib[colName]]
    except KeyError:
      logger.debug('%s xml node attribute not found : %s' % (nodeKey, colName))
      return [nullVal]
    
  # -------------------------------------------------------------- #
  # putUniqKey
  # ---------------------------------------------------------------#
  def putUniqKey(self, nodeKey):

    record = self.getForeignKey()      
    dbKey = '%s|%s|UKEY' % (self.tsXref, self.tableName)
    for index in self.ukeyDefn:
      record.append(self._record[nodeKey][index])
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
  # putRowSize
  # ---------------------------------------------------------------#
  def putRowSize(self):
    
    dbKey = '%s|%s|ROWSIZE' % (self.tsXref, self.tableName)
    self._leveldb.Put(dbKey,'%d' % self.count)

# -------------------------------------------------------------- #
# MetaPrvdr
# ---------------------------------------------------------------#
class MetaPrvdr(MetaReader):

  def __init__(self, leveldb, jobId, scaller):
    super(MetaPrvdr, self).__init__(leveldb)
    self.jobId = jobId
    self.scaller = scaller

  # -------------------------------------------------------------- #
  # __call__
  # ---------------------------------------------------------------#
  def __call__(self):
    pmeta = self.getProgramMeta()
    self.method = 'normalizeXml.MetaPrvdr.__call__'
    try:
      dbKey = 'TSXREF|' + self.scaller
      tsXref = self._leveldb.Get(dbKey)
      return (tsXref, pmeta)
    except KeyError:
      errmsg = 'EEOWW! tsXref param not found : ' + dbKey
      self.newMail('ERR1','leveldb lookup failed',errmsg)
      raise Exception

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
    WcEmailPrvdr.newMail('NormalizeXml',bodyKey,self.method,*args)

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  def getProgramMeta(self):
    self.method = 'normalizeXml.MetaPrvdr.getProgramMeta'
    try:
      dbKey = 'PMETA|' + self.scaller
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      errmsg = 'EEOWW! pmeta json document not found : ' + dbKey
      self.newMail('ERR1','leveldb lookup failed',errmsg)
      raise Exception(errmsg)

    try:
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
    except Exception as ex:
      self.newMail('ERR1','compile pmeta failed',str(ex))
      raise
