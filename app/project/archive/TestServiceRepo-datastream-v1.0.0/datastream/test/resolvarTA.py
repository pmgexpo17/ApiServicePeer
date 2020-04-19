# -------------------------------------------------------------- #
# Resolvar
# - Framework added properties : _leveldb
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
    self.state.current = 'UNIT_TEST1'
    self.jobId = jobId
    self.jmeta = jobMeta
    self.hostName = jobMeta.hostName
    logger.info(f'{self.name}, starting job {self.jobId} ...')

  # -------------------------------------------------------------- #
  # UNIT_TEST1
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def UNIT_TEST1(self):
    pass

  # -------------------------------------------------------------- #
  # UNIT_TEST2
  # -------------------------------------------------------------- #  
  @iterate('serviceA')
  def UNIT_TEST2(self):
    pass
