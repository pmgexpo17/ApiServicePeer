# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AppCooperator, TaskError
from . import promote, iterate
import logging

logger = logging.getLogger('asyncio.smart')

# -------------------------------------------------------------- #
# ClientB
# ---------------------------------------------------------------#
class ClientB(AppCooperator):
  def __init__(self, actorId):
    super().__init__(actorId)
    self.state.hasNext = True
    self.resolve = None

  # -------------------------------------------------------------- #
  # __getitem__
  # ---------------------------------------------------------------#
  def __getitem__(self, key):
    if key in self._quicken:
      return self._quicken[key]
    if key in self.__dict__:
      return self.__dict__[key]
    raise TypeError(f'{key} is not a valid {self.name} attribute')

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  @promote('clientB')
  def start(self, jobId, jobMeta, **kwargs):
    logger.info(f'{self.name} is starting ...')
    self.jobId = jobId
    self.resolve = Resolvar(jobId)
    self.resolve.state = self.state    
    self.resolve.start(jobId, jobMeta)
     
  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  def onError(self, ex):
    state = self.state
    actorId, actorName = self.tell()
    if state.status != 'STARTED':
      logMsg = f'actor error, job was {state.status}, aborting ...'
    else:
      logMsg = f'actor error, job {self.jobId} was {state.status}, aborting ...'

    logMsg = f'{logMsg}\nActor, {actorName}, {actorId},'
    logger.error(logMsg, exc_info=True)

  # -------------------------------------------------------------- #
  # destroy
  # -------------------------------------------------------------- #
  def destroy(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # stop
  # -------------------------------------------------------------- #
  def stop(self, *args, **kwargs):
    pass

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AppResolvar, Note, TaskError
import os

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):
  '''
    ### Framework added attributes ###
      1. _leveldb : key-value datastore
      2. request : Requests session connection for making api calls
  '''
  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, jobId, jobMeta):
    self.state.current = 'DOWNLOAD_ZIPFILE'
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    logger.info(f'{self.name}, starting job {self.jobId} ...')

  # -------------------------------------------------------------- #
  # DOWNLOAD_ZIPFILE
  # -------------------------------------------------------------- #  
  @iterate('clientB')
  def DOWNLOAD_ZIPFILE(self):
    self.prepareDownload()

  # -------------------------------------------------------------- #
  # FINAL_HANDSHAKE
  # -------------------------------------------------------------- #  
  @iterate('clientB')
  def FINAL_HANDSHAKE(self):
    pass

  # -------------------------------------------------------------- #
  # prepareDownload
  # ---------------------------------------------------------------#
  def prepareDownload(self):
    # the itemKey context is opposite, to check if the applied category 
    # exists in registered consumerCategories
    jpacket = {'eventKey':f'REPO|{self.jobId}','itemKey':'csvToXml'}
    repo = self.query(Note(jpacket))
    
    apiBase = self._leveldb['apiBase']
    sysPath = f'{apiBase}/{repo.sysPath}'
    if not os.path.exists(sysPath):
      errmsg = f'output repo path does not exist : {sysPath}'
      raise TaskError(errmsg)
    
    catPath = self.jmeta.category
    if catPath not in repo.consumerCategories:
      errmsg = 'consumer category branch %s does not exist in %s' \
                                % (catPath, str(repo.consumerCategories))
      raise TaskError(errmsg)
  
    repoPath = f'{sysPath}/{catPath}/{self.jobId}'
    logger.info('output json file repo path : ' + repoPath)

    try:
      self.sysCmd(['mkdir','-p',repoPath])
    except TaskError as ex:
      logger.error('output repo path creation failed')
      raise

    csvGZipfile = f'{self.jobId}.{self.jmeta.fileExt}'
    logger.info('output csv tar gzipfile : ' + csvGZipfile)

    dbKey = f'{self.jobId}|datastream|workspace'
    self._leveldb[dbKey] = repoPath
    dbKey = f'{self.jobId}|datastream|outfile'
    self._leveldb[dbKey] = csvGZipfile
