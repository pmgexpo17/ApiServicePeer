#!/usr/bin/env python3
from apibase import addHandler
from logging.handlers import RotatingFileHandler
import asyncio
import logging
import os, sys
import subprocess

# -------------------------------------------------------------- #
# ApiPeer
# ---------------------------------------------------------------#
class ApiPeer:

  @staticmethod    
  def make(apiBase):
    logPath = f'{apiBase}/log'
    if not os.path.exists(logPath):
      subprocess.call(['mkdir','-p',logPath])

    dbPath = f'{apiBase}/database'
    if not os.path.exists(dbPath):
      subprocess.call(['mkdir','-p',dbPath])

    logger1 = logging.getLogger('asyncio.server')
    logfile = f'{logPath}/apiServer.log'
    addHandler(logger1, logfile=logfile)
    addHandler(logger1)
    logger1.setLevel(logging.INFO)
    global logger
    logger = logger1

    logger2 = logging.getLogger('asyncio.smart')
    logfile = f'{logPath}/apiSmart.log'
    addHandler(logger2, logfile=logfile)
    addHandler(logger2)
    logger2.setLevel(logging.INFO)

    logger3 = logging.getLogger('asyncio.microservice')
    logfile = f'{logPath}/apiMicroservice.log'
    addHandler(logger3, logfile=logfile)
    logger3.setLevel(logging.INFO)

    logger4 = logging.getLogger('asyncio.tools')
    logfile = f'{logPath}/apiTools.log'
    addHandler(logger4, logfile=logfile)
    logger4.setLevel(logging.INFO)

    logger5 = logging.getLogger('asyncio.hardhash')
    logfile = f'{logPath}/apiHardhash.log'
    addHandler(logger5, logfile=logfile)
    addHandler(logger5)
    logger5.setLevel(logging.INFO)

    logger6 = logging.getLogger('asyncio.broker')
    logfile = f'{logPath}/apiBroker.log'
    addHandler(logger6, logfile=logfile)
    addHandler(logger6)
    logger6.setLevel(logging.INFO)
