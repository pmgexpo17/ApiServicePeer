import logging
import sys

logger = logging.getLogger('apipeer.server')

# -------------------------------------------------------------- #
# ServiceRegistry
# ---------------------------------------------------------------#
class ServiceRegistry():
	
  def __init__(self):
    self._modules = {}
    self._services = {}
    
  # ------------------------------------------------------------ #
  # isLoaded
  # -------------------------------------------------------------#
  def isLoaded(self, serviceName):
    try:
      self._services[serviceName]
    except KeyError:
      return False
    else:
      return True

  # ------------------------------------------------------------ #
  # loadModules
  # -------------------------------------------------------------#
  def loadModules(self, serviceName, serviceRef):
    self._services[serviceName] = {}
    for module in serviceRef:
      self.loadModule(serviceName, module['name'], module['fromList'])
	
  # ------------------------------------------------------------ #
  # _loadModule : wrap load module
  # -------------------------------------------------------------#
  def loadModule(self, serviceName, moduleName, fromList):
    self._services[serviceName][moduleName] = fromList
    try:
      self._modules[moduleName]
    except KeyError:
      self._loadModule(moduleName, fromList)
		
  # ------------------------------------------------------------ #
  # _loadModule : execute load module
  # -------------------------------------------------------------#
  def _loadModule(self, moduleName, fromList):    
    # reduce moduleName to the related fileName for storage
    _module = '.'.join(moduleName.split('.')[-2:])
    logger.info('%s is loaded as : %s' % (moduleName, _module))
    self._modules[_module] = __import__(moduleName, fromlist=[fromList])

  # ------------------------------------------------------------ #
  # reloadHelpModule
  # -------------------------------------------------------------#
  def reloadHelpModule(self, serviceName, helpModule):
    try:
      serviceRef = self._services[serviceName]
    except KeyError as ex:
      return ({'status':400,'errdesc':'KeyError','error':str(ex)}, 400)
    for moduleName in serviceRef:
      #_module = moduleName.split('.')[-1]
      _module = '.'.join(moduleName.split('.')[-2:])
      self._modules[_module] = None
    if helpModule not in sys.modules:
      warnmsg = '### support module %s does not exist in sys.modules'
      logger.warn(warnmsg % helpModule)
    else:
      importlib.reload(sys.modules[helpModule])
    for moduleName, fromList in serviceRef.items():
      self._loadModule(moduleName, fromList)
    logger.info('support module is reloaded : ' + helpModule)
    return {'status': 201,'service': serviceName,'module': helpModule}
	
  # ------------------------------------------------------------ #
  # reloadModule
  # -------------------------------------------------------------#
  def reloadModule(self, serviceName, moduleName):
    try:
      serviceRef = self._services[serviceName]
      fromList = serviceRef[moduleName]
    except KeyError:
      return self.reloadHelpModule(serviceName,moduleName)

    # reduce moduleName to the related fileName for storage
    #_module = moduleName.split('.')[-1]
    _module = '.'.join(moduleName.split('.')[-2:])
    self._modules[_module] = None
    importlib.reload(sys.modules[moduleName])    
    logger.info('%s is reloaded as : %s' % (moduleName, _module))
    self._modules[_module] = __import__(moduleName, fromlist=[fromList])
    return {'status': 201,'service': serviceName,'module': moduleName}
	
  # ------------------------------------------------------------ #
  # getClassName
  # -------------------------------------------------------------#
  def getClassName(self, classRef):

    if ':' not in classRef:
      raise ValueError('Invalid classRef %s, expecting module:className' % classRef)
    moduleName, className = classRef.split(':')

    try:
      module = self._modules[moduleName]
    except KeyError:
      raise Exception('Service module name not found in register : ' + moduleName)

    if not hasattr(module, className):
      raise Exception('Service classname not found in service register : ' + className)
    
    return (module, className)
