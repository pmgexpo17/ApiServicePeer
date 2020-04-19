# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, AppResolvar, Note, TaskError
import os

# -------------------------------------------------------------- #
# Resolvar
# ---------------------------------------------------------------#
class Resolvar(AppResolvar):

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
    self.downloadJsonFile()

  # -------------------------------------------------------------- #
  # FINAL_HANDSHAKE
  # -------------------------------------------------------------- #  
  @iterate('clientB')
  def FINAL_HANDSHAKE(self):
    pass

  # -------------------------------------------------------------- #
  # downloadJsonFile
  # ---------------------------------------------------------------#
  def downloadJsonFile(self):
    # the itemKey context is opposite, to check if the applied category 
    # exists in registered consumerCategories
    jpacket = {'eventKey':f'REPO|{self.jobId}','itemKey':'jsonToCsv'}
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
  
    repoPath = f'{sysPath}/{catPath}'
    logger.info('output json gzipfile repo path : ' + repoPath)

    jsonZipfile = f'{self.jobId}.{self.jmeta.fileExt}'
    logger.info('output json gzipfile : ' + jsonZipfile)

    dbKey = f'{self.jobId}|datastream|workspace'
    self._leveldb[dbKey] = repoPath
    dbKey = f'{self.jobId}|datastream|outfile'
    self._leveldb[dbKey] = jsonZipfile
