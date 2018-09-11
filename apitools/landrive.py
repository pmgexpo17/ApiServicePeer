from subprocess import call as subcall
import json
#import logging
import os
import re


class LandrivePrvdr(object):
  
  def __init__(self, mountHost, mountPath, runDir):
    self.mountHost = mountHost
    self.mountPath = mountPath
    self.runDir = runDir

  # ------------------------------------------------------------ #
  # sysCmd
  # - use for os commands so return code is handled correctly
  # -------------------------------------------------------------#
  def sysCmd(self, sysArgs, stdin=None, stdout=None, cwd=None):

    try:
      scriptname = self.__class__.__name__
      if stdout:
        return subcall(sysArgs, stdin=stdin, stdout=stdout, stderr=stdout, cwd=cwd)  
      else:
        return subcall(sysArgs, stdin=stdin, cwd=cwd)
    except OSError as ex:
      errmsg = '%s syscmd failed : %s' % (scriptname, str(ex))
      raise Exception(errmsg)

  # -------------------------------------------------------------- #
  # mount a landrive
  # ---------------------------------------------------------------#
  def _mount(self, func):

    print('mount host path : ' + self.mountHost)

    if not os.path.exists(self.runDir + '/landrive.sh'):
      self.sysCmd(['mkdir','-p',self.runDir])
      self.sysCmd(['cp','landrive.sh',self.runDir])

    cifsEnv = self.runDir + '/cifs_env'
    with open(cifsEnv,'w') as fhw:
      fhw.write('ADUSER=%s\n' % os.environ['USER'])
      fhw.write("DFSPATH='%s'\n" % self.mountHost)
      fhw.write('MOUNTINST=%s\n' % self.mountPath)

    credentials = os.environ['HOME'] + '/.landrive'
    sysArgs = ['sudo','landrive.sh',func,'--cifsenv','cifs_env','--credentials',credentials]
    self.sysCmd(sysArgs,cwd=self.runDir)

  # -------------------------------------------------------------- #
  # mount a landrive
  # ---------------------------------------------------------------#
  def mount(self):
    print('landrive mount mountPath : ' + self.mountPath)
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if not unMounted:
      print('landrive is already mounted : ' + self.mountPath)
      return

    print('mount host path : ' + self.mountHost)

    self._mount('--mountcifs')

    sysArgs = ['grep','-w',self.mountPath,'/etc/mtab']
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if unMounted:
      errmsg = 'failed to mount landrive : ' + self.mountPath
      print(errmsg)
      raise Exception(errmsg)

    linkPath = '/data/' + self.mountPath
    print('landrive symlink : ' + linkPath)
    self.landrive = '/lan/%s/%s' % (os.environ['USER'], self.mountPath)
    print('landrive : ' + self.landrive)
    if not os.path.exists(linkPath):
      self.sysCmd(['ln','-s', self.landrive, linkPath])

  # -------------------------------------------------------------- #
  # unmount a landrive
  # ---------------------------------------------------------------#
  def unmount(self):
    print('landrive mount mountPath : ' + self.mountPath)
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if unMounted:
      print('landrive is already unmounted : ' + self.mountPath)
      return

    self._mount('--umountcifs')
    
    sysArgs = ['grep','-w',self.mountPath,'/etc/mtab']
    unMounted = self.sysCmd(['grep','-w',self.mountPath,'/etc/mtab'])
    if not unMounted:
      errmsg = 'failed to unmount landrive : ' + self.mountPath
      print(errmsg)
      raise Exception(errmsg)
    
    
if __name__ == '__main__':
  runDir = os.environ['HOME'] + '/.webapi'  
  localPath = '//int.corp.sun/GroupData/actuary/rateengn/Commercial/workerscomp/emulation/pm/devApps/csvChecker'
  #localPath = '//int.corp.sun/GroupData/actuary/PiDevApps/CsvCheckerApi'
  mountPath = 'apiServicePeer'
  landriver = LandrivePrvdr(localPath, mountPath, runDir)
  landriver.mount()
