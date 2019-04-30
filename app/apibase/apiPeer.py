#!/usr/bin/env python3
from apibase import JobService, JobPacket
from apiservice.saaspolicy import ContractError
from aiohttp import web
import asyncio
import logging
import os, sys
import simplejson as json
import subprocess

# -------------------------------------------------------------- #
# SmartJob
# ---------------------------------------------------------------#
class SmartJob(web.View):

  async def post(self):
    try:
      prvdr = self.request.app['jobService']
      method = self.request.match_info.get('method', 'promote')
      rdata = await self.request.json()
      if 'job' not in rdata:
        return {'status':400, 'error': "required param 'job' not found"}
      jdata = rdata['job']
      logger.info('job packet : ' + str(jdata))
      response = prvdr[method](jdata)
    except Exception as ex:
      logger.exception('smart job failed\n%s',str(ex))
      return web.json_response({'status': 500,'error': str(ex)}, status=500)
    else:
      return web.json_response(response, status=201)

  async def delete(self):
    try:
      prvdr = self.request.app['jobService']
      method = self.request.match_info.get('method', 'promote')
      rdata = await self.request.json()
      if 'job' not in rdata:
        return {'status':400, 'error': "required param 'job' not found"}
      jdata = rdata['job']
      logger.info('job packet : ' + str(jdata))
      response = prvdr.delete(jdata)
    except Exception as ex:
      logger.exception('smart job failed\n%s',str(ex))
      return web.json_response({'status': 500,'error': str(ex)}, status=500)
    else:
      return web.json_response(response, status=201)

# -------------------------------------------------------------- #
# MultiTask
# ---------------------------------------------------------------#
class MultiTask(web.View):

  async def post(self):
    try:
      prvdr = self.request.app['jobService']
      taskRange = self.request.match_info['taskRange']
      #rdata = self.request._rdata
      rdata = await self.request.json()
      if 'job' not in rdata:
        return {'status':400, 'error': "required param 'job' not found"}
      jdata = rdata['job']
      logger.info('job packet : ' + str(jdata))
      response = prvdr.multiTask(jdata,taskRange=taskRange)
    except Exception as ex:
      logger.exception('multiTask job failed\n%s',str(ex))
      return web.json_response({'status': 500,'error': str(ex)}, status=500)
    else:
      return web.json_response(response, status=201)

# -------------------------------------------------------------- #
# SyncTask
# ---------------------------------------------------------------#
class SyncTask(web.View):

  async def post(self):
    try:
      prvdr = self.request.app['jobService']
      #rdata = self.request._rdata
      rdata = await self.request.json()
      if 'job' not in rdata:
        return {'status':400, 'error': "required param 'job' not found"}
      jdata = rdata['job']
      logger.info('job packet : ' + str(jdata))
      return await prvdr.resolve(self.request, jdata)
    except Exception as ex:
      logger.exception('sync job failed\n%s',str(ex))
      return web.json_response({'status': 500,'error': str(ex)}, status=500)

# -------------------------------------------------------------- #
# ServiceManager
# ---------------------------------------------------------------#
class ServiceManager(web.View):

  # GET: get the service load status
  def get(self):
    prvdr = self.request.app['jobService']
    serviceName = self.request.match_info['serviceName']      
    return prvdr.getLoadStatus(serviceName)

  # POST: reload a service module
  async def post(self):
    prvdr = self.request.app['jobService']
    serviceName = self.request.match_info['serviceName']
    rdata = await self.request.json()
    if 'module' not in rdata:
      return {'status':400, 'error': "required param 'module' not found"}
    moduleName = rdata['module']
    logger.info(f'service, module : {serviceName}, {moduleName}')
    return prvdr.reloadModule(serviceName, moduleName)
    
  # PUT : load all service modules
  async def put(self):
    prvdr = self.request.app['jobService']
    serviceName = self.request.match_info['serviceName']
    rdata = await self.request.json()
    if 'service' not in rdata:
      return {'status':400, 'error': "required param 'service' not found"}
    serviceRef = rdata['service']
    logger.info('service : ' + str(serviceRef))
    return prvdr.loadService(serviceName, serviceRef)

# -------------------------------------------------------------- #
# Ping
# ---------------------------------------------------------------#
class Ping(web.View):

  async def get(self):
    logger.info('ping request ...')
    return web.json_response({'status': 200,'pid': os.getpid()}, status=200)

# -------------------------------------------------------------- #
# SaasMeta
# ---------------------------------------------------------------#
class SaasMeta(web.View):

  async def get(self):
    try:
      prvdr = self.request.app['jobService']
      #rdata = self.request._rdata
      rdata = await self.request.json()
      if 'job' not in rdata:
        return {'status':400, 'error': "required param 'job' not found"}
      jdata = rdata['job']
      logger.info('job packet : ' + str(jdata))

      try:      
        return prvdr.runQuery(jdata)
      except ContractError as ex:
        return web.json_response({'status': 400,'error': str(ex)}, status=400)
    except Exception as ex:
      return web.json_response({'status': 500,'error': str(ex)}, status=500)

# -------------------------------------------------------------- #
# SaasAdmin
# ---------------------------------------------------------------#
class SaasAdmin(web.View):

  def getUriVars(self):
    return [self.request.match_info[key] for key in ('owner', 'product', 'category')]

  async def get(self):
    try:
      prvdr = self.request.app['jobService']
      #rdata = self.request._rdata
      rdata = await self.request.json()
      if 'job' not in rdata:
        return {'status':400, 'error': "required param 'job' not found"}
      jdata = rdata['job']
      logger.info('job packet : ' + str(jdata))

      uriVars = self.getUriVars()
      logger.info('uri vars : {} '.format(uriVars) )

      if 'args' in jdata:
        jdata['args'] = uriVars + jdata['args']
      else:    
        jdata['args'] = uriVars
      try:      
        return prvdr.resolve(jdata)
      except ContractError as ex:
        return web.json_response({'status': 400,'error': str(ex)}, status=400)
    except Exception as ex:
      return web.json_response({'status': 500,'error': str(ex)}, status=500)

  async def put(self):
    try:
      prvdr = self.request.app['jobService']
      #rdata = self.request._rdata
      rdata = await self.request.json()
      if 'job' not in rdata:
        return {'status':400, 'error': "required param 'job' not found"}
      jdata = rdata['job']
      logger.info('job packet : ' + str(jdata))

      owner, product, category = self.getUriVars()
      label = jdata['label'].upper()

      if 'metadoc' not in rdata:
        return {'status': 400,'error': "required param 'metadoc' not found"}

      dbKey = 'SAAS|%s|%s|%s|%s' % (label, owner, product, category)
      logger.info('SAAS put key : ' + dbKey)

      prvdr.db[dbKey] = rdata['metadoc']

      msg = 'metadoc added for domain : %s' % dbKey
      return web.json_response({'status': 201,'msg': msg}, status=201)
    except Exception as ex:
      return web.json_response({'status': 500,'error': str(ex)}, status=500)

# -------------------------------------------------------------- #
# addHandler
# ---------------------------------------------------------------#
def addHandler(logger, logfile=None):
  if logfile:
    handler = logging.FileHandler(filename=logfile)
  else:
    handler = logging.StreamHandler(sys.stdout)
  logFormat = '%(levelname)s:%(asctime)s,%(filename)s:%(lineno)d %(message)s'
  logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
  handler.setFormatter(logFormatter)
  logger.addHandler(handler)

# -------------------------------------------------------------- #
# on_shutdown
# ---------------------------------------------------------------#
async def on_shutdown(app):
  prvdr = app['jobService']
  await prvdr.shutdown()

# -------------------------------------------------------------- #
# ApiPeer
# ---------------------------------------------------------------#
class ApiPeer:

  @staticmethod    
  def make(apiBase):
    logPath = '%s/log' % apiBase
    if not os.path.exists(logPath):
      subprocess.call(['mkdir','-p',logPath])

    logger1 = logging.getLogger('apipeer.server')
    logfile = '%s/apiServer.log' % logPath
    addHandler(logger1, logfile=logfile)
    addHandler(logger1)
    logger1.setLevel(logging.INFO)
    global logger
    logger = logger1

    logger2 = logging.getLogger('apipeer.smart')
    logfile = '%s/apiSmart.log' % logPath
    addHandler(logger2, logfile=logfile)
    addHandler(logger2)
    logger2.setLevel(logging.INFO)

    logger3 = logging.getLogger('apipeer.multi')
    logfile = '%s/apiMultiTask.log' % logPath
    addHandler(logger3, logfile=logfile)
    logger3.setLevel(logging.INFO)

    logger4 = logging.getLogger('apipeer.tools')
    logfile = '%s/apiTools.log' % logPath
    addHandler(logger4, logfile=logfile)
    logger4.setLevel(logging.INFO)

    dbPath = '%s/database/metastore' % apiBase
    if not os.path.exists(dbPath):
      subprocess.call(['mkdir','-p',dbPath])
    return dbPath
  
  @staticmethod
  async def start(apiBase, register, port, sslMeta=None):

    dbPath = ApiPeer.make(apiBase)
    jobService = JobService.make(dbPath, register)

    app = web.Application()
    app.router.add_routes([
      web.post('/api/v1/smart',SmartJob),
      web.post('/api/v1/smart/{method}',SmartJob),
      web.delete('/api/v1/smart',SmartJob),
      web.post('/api/v1/multi/{taskRange}',MultiTask),
      web.post('/api/v1/sync',SyncTask),
      web.post('/api/v1/service/{serviceName}',ServiceManager),
      web.get('/api/v1/ping',Ping),
      web.get('/api/v1/saas/meta',SaasMeta),
      web.get('/api/v1/saas/{owner}/{product}/{category}',SaasAdmin),
      web.put('/api/v1/saas/{owner}/{product}/{category}',SaasAdmin)
    ])
    app['jobService'] = jobService
    app.on_shutdown.append(on_shutdown)

    runner = web.AppRunner(app)
    await runner.setup()

    if sslMeta:
      ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
      ssl_context.load_cert_chain(sslMeta['certFile'], sslMeta['keyFile'])
      site = web.TCPSite(runner, '127.0.0.1', port, ssl_context=ssl_context)
    else:
      site = web.TCPSite(runner, '127.0.0.1', port)

    logger.info(f'Serving app on https://localhost:{port}')
    await site.start()

    return runner
