__all__ = ['TableRowRNA1','TableRowFKA1','AbstractTaskMember']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import Article, Note, TaskError
import logging

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# AbstractRow
# ---------------------------------------------------------------#
class AbstractRow(Article):

  def __init__(self):
    self._hh = None

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  def __repr__(self):
    return f'{self.__class__.__name__}.{self.nodeName}'

	#------------------------------------------------------------------#
	# headerCheck
	#------------------------------------------------------------------#
  def headerCheck(self, header):
    logger.debug(f'{self.nodeName}, header check ...')
    columnsDs = header
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
	# prepare
	#------------------------------------------------------------------#
  def prepare(self, csvReader):
    self._header = dataset = next(csvReader)    
    self.headerCheck(dataset)
    dbkey = f'{self.nodeName}|header'
    self._hh[dbkey] = dataset
    self.recnum = 0

	#------------------------------------------------------------------#
	# result
	#------------------------------------------------------------------#
  def result(self):
    return self.recnum

	#------------------------------------------------------------------#
	# getFkValue
	#------------------------------------------------------------------#
  def getFkValue(self, record):
    fkvalue = [record[key] for key in self.fkey]
    return '|'.join(fkvalue)

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, record):
    raise NotImplementedError(f'{self.name}.normalise is an abstract method')

# -------------------------------------------------------------- #
# TableRow
# ---------------------------------------------------------------#
class TableRow(AbstractRow):

  # -------------------------------------------------------------- #
  # make - merge TableRowXXA1 with TaskMember to make arrange available
  # ---------------------------------------------------------------#
  @classmethod
  def make(cls, MemberKlass):
    className = f'{cls.__name__}-TaskMember'
    RowKlass = type(className,(cls,MemberKlass),{})
    logger.info(f'{cls.__name__}, new dynamic class : {RowKlass.__name__} ')
    return RowKlass()

# -------------------------------------------------------------- #
# TableRowRNA1
# ---------------------------------------------------------------#
class TableRowRNA1(TableRow):

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, record):
    self.recnum += 1
    dbkey = f'{self.tableName}|{self.recnum:05}'
    if self.recnum == 1:
      logger.info(f'{self.nodeName}, record {self.recnum} : {record}')
    self._hh[dbkey] = dict(zip(self._header,record))

	#------------------------------------------------------------------#
	# result
	#------------------------------------------------------------------#
  def result(self):
    dbkey = f'{self.tableName}|rowcount'
    self._hh[dbkey] = self.recnum
    return self.recnum

# -------------------------------------------------------------- #
# TableRowFKA1
# ---------------------------------------------------------------#
class TableRowFKA1(TableRow):

	#------------------------------------------------------------------#
	# normalise
	#------------------------------------------------------------------#
  def normalise(self, record):
    self.recnum += 1    
    recordA = dict(zip(self._header,record))
    fkValue = self.getFkValue(recordA)
    if self.recnum == 1:
      logger.info(f'{self.nodeName}, record {self.recnum} : {record}')
    dbkey = f'{self.nodeName}|{fkValue}'
    self._hh.append(dbkey, recordA)

# -------------------------------------------------------------- #
# AbstractTaskMember - implement arrange to get a hardhash connector
# ---------------------------------------------------------------#
class AbstractTaskMember:
	#------------------------------------------------------------------#
	# arrange
	#------------------------------------------------------------------#
  def arrange(self, taskNum):
    raise NotImplementedError(f'{self.name}.arrange is an abstract method')
