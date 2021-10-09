import json
import logging
import sys
from logging.handlers import RotatingFileHandler

from .component import Article, Note
from .connector import MemCache, ConnProvider, Connector, ConnWATC
from .future import SafeFuture
from .terminal import Terminal
from .routine import create_task, findNumeric, numPattern, parseNumeric

# -------------------------------------------------------------- #
# addLogHandler
# ---------------------------------------------------------------#
def addLogHandler(logger, logfile=None):
  if logfile:
    handler = RotatingFileHandler(logfile, maxBytes=5000000, backupCount=10)
  else:
    handler = logging.StreamHandler(sys.stdout)
  logFormat = '%(levelname)s:%(asctime)s,%(filename)s:%(lineno)d %(message)s'
  logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
  handler.setFormatter(logFormatter)
  logger.addHandler(handler)
  return handler

logLevels = {
  "DEBUG": logging.DEBUG, 
  "WARN": logging.WARN, 
  "INFO": logging.INFO, 
  "DEBUG": logging.DEBUG}

#-----------------------------------------------------------------#
# loadJsonConfig - configJs type must = python3 pathlib.Path
#-----------------------------------------------------------------#
def loadJsonConfig(configJs, recursive=False):
  if not configJs.exists():
    errmsg = "ApiServicePeer configuation file does not exist.\n file : {}"
    raise Exception(errmsg.format(configJs))
  config = json.loads(configJs.read_bytes())
  return Note(config, recursive)
