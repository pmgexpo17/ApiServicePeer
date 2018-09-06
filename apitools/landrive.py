from appDirector import SysCmdUnit

class LandrivePrvdr(SysCmdUnit):

  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#
  def _start(self, pmeta):
    logger.info('landrive.LandrivePrvdr._start')    
    self.pmeta = pmeta

  # -------------------------------------------------------------- #
  # landriveMount - depreciated
  # ---------------------------------------------------------------#
  def mount(self):
    self.mountPath = 'webapi/csvchecker/session'
    logger.info('landrive mount path : ' + self.mountPath)
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if not unMounted:
      logger.info('landrive is already mounted : ' + self.mountPath)
      return

    csvLocation = self.pmeta['csvBase'] + '/' + self.pmeta['csvPath']
    logger.info('csv checker mount path : ' + localPath)

    runDir = os.environ['HOME'] + '/.csvchecker'
    self.sysCmd(['mkdir',runDir])
    if not os.path.exists(runDir + '/landrive.sh'):
      landrive_sh = self.pmeta['assetLib'] + '/landrive.sh'
      self.sysCmd(['cp',landrive_sh,runDir])
      
    cifsEnv = runDir + '/cifs_env'
    with open(cifsEnv,'w') as fhw:
      fhw.write('ADUSER=%s\n' % os.environ['USER'])
      fhw.write("DFSPATH='%s'\n" % localPath)
      fhw.write('MOUNTINST=%s\n' % self.mountPath)

    credentials = os.environ['HOME'] + '/.landrive'
    sysArgs = ['sudo','landrive.sh','--mountcifs','--cifsenv','cifs_env','--credentials',credentials]
    self.sysCmd(sysArgs,cwd=runDir)

    sysArgs = ['grep','-w',self.mountPath,'/etc/mtab']
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if unMounted:
      errmsg = 'failed to mount landrive : ' + self.mountPath
      logger.error(errmsg)
      raise Exception(errmsg)

    linkPath = self.pmeta['linkBase']
    logger.info('landrive symlink : ' + linkPath)
    self.landrive = '/lan/%s/%s' % (os.environ['USER'], self.mountPath)
    logger.info('landrive : ' + self.landrive)
    if not os.path.exists(linkPath):
      self.sysCmd(['ln','-s', self.landrive, linkPath])
