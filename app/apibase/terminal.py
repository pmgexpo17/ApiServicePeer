__all__= [
  'AbstractSystemUnit',
  'Terminal']

from subprocess import Popen, PIPE, call as subcall

# -------------------------------------------------------------- #
# AbstractSystemUnit
# ---------------------------------------------------------------#
class AbstractSystemUnit:
  apiBase = None

  @classmethod
  def __start__(cls, apiBase):
    cls.apiBase = apiBase

# -------------------------------------------------------------- #
# Terminal
# ---------------------------------------------------------------#
class Terminal(AbstractSystemUnit):

  @property
  def name(self):
    return self.__class__.__qualname__

  # ------------------------------------------------------------ #
  # sysCmd
  # - use for os commands so return code is handled correctly
  # - if shell is false, sysCall is an args list, else a system call string
  # -------------------------------------------------------------#
  def sysCmd(self, sysCall, stdin=None, stdout=None, cwd=None, shell=False):

    try:
      if stdout:
        return subcall(sysCall, stdin=stdin, stdout=stdout, stderr=stdout, cwd=cwd, shell=shell)  
      else:
        return subcall(sysCall, stdin=stdin, cwd=cwd, shell=shell)
    except OSError as ex:
      errmsg = f'{self.name}.sysCmd failed, args : {str(sysCall)}\nError : {str(ex)}'
      raise Exception(errmsg)

  # ------------------------------------------------------------ #
  # proc
  # -------------------------------------------------------------#
  def proc(self, sysArgs, cwd=None, stdin=None, stdout=None):
    
    try:
      if stdin:
        prcss = Popen(sysArgs,stdin=PIPE,cwd=cwd)
        prcss.communicate(stdin)
        return
      elif stdout:
        prcss = Popen(sysArgs,stdout=stdout,stderr=stdout,cwd=cwd)
        prcss.communicate()
        return
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE,cwd=cwd)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        errmsg = f'{self.name}.proc failed, args : {str(sysArgs)}\nError : {stderr}'
        raise Exception(errmsg)
      return stdout
    except OSError as ex:
      errmsg = f'{self.name}.proc failed, args : {str(sysArgs)}\nError : {str(ex)}'
      raise Exception(errmsg)

