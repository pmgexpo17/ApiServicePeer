import logging
import pickle

logger = logging.getLogger('asyncio')

try:
  pickleMode = pickle.DEFAULT_PROTOCOL
except AttributeError:
  pickleMode = pickle.HIGHEST_PROTOCOL

#================================================================#
# Note
#===============================================================-#
class Note:
  def __init__(self, packet={}, recursive=True):
    self._update(packet, recursive)

  def __call__(self, packet={}):
    self._update(packet, False)
    return self
  
  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    return getattr(self, key)

  def __setitem__(self, key, value):
    self.__dict__[key] = value
    
  @property
  def body(self):
    return self.__dict__.copy()

  # copy the selected attribute and return it converted if it is a dict type
  def annote(self, key, recursive=True) -> object:
    value = self[key]
    if isinstance(value, dict):
      value = value.copy()
      return Note(value, recursive)
    return value
  
  # emulate dict.get
  def get(self, key, default=None):
    if hasattr(self, key):
      return getattr(self, key)
    return default
  
  def hasAttr(self, *attrNames):
    for attrName in attrNames:
      branches = attrName.split(".")
      currNode = self
      for nodeName in branches:
        if isinstance(currNode, dict):
          if not nodeName in currNode:
            return False
        elif not hasattr(currNode, nodeName):
          return False
        currNode = currNode[nodeName]
    return True

  def hasAttrValue(self, attrName, value):
    if self.hasAttr(attrName):
      return self[attrName] == value
    return False

  def merge(self, packet, recursive=False):
    if not isinstance(packet, dict):
      raise TypeError('dict.update requires a dict argument')
    self._update(packet, recursive)

  # reduce the body back to dict data
  def rawcopy(self, outNote=True, pop=[]):
    body = self.body
    for key, value in body.items():
      if isinstance(value, Note):
        body[key] = value.body
    for key in pop:
      body.pop(key, None)
    if outNote:
      return Note(body, False)
    return body

  # rename an attribute
  def rename(self, fkey, tkey: str):
    if not (isinstance("fkey",str) and isinstance("tkey",str)):
      return
    if fkey in self.__dict__ and tkey not in self.__dict__:
      if "-" in tkey:
        tkey = tkey.replace("-","_")
      self.__dict__[tkey] = self.__dict__.pop(fkey)

  def select(self, *keys, recursive=True) -> object:
    if not keys:
      return self.copy()
    packet = {key:self.__dict__[key] for key in keys if key in self.__dict__}
    return Note(packet,recursive)

  # return the selected key values in a list
  def tell(self, *keys):
    return [self.get(key) for key in keys]

  def remove(self, *args):
    for key in args:
      if isinstance(key, str):
        self.__dict__.pop(key, None)

  # the main construct and update method that, on detecting a dict, converts it to Note
  def _update(self, packet, recursive):
    if not isinstance(packet, dict):
      logger.warn("Note constructor expects packet to be a dict type")
      return packet
    # self.__dict__.update(packet)
    for key, value in packet.items():
      if "-" in key:
        key = key.replace("-","_")
      self.__dict__[key] = value
    if not recursive:
      return
    for key, value in packet.items():
      if isinstance(value, dict):
        # don't convert beyond first level
        self.__dict__[key] = Note(packet=value, recursive=False)

#================================================================#
# Article
#===============================================================-#
class Article(Note):

  # QuConn equivalent of Conn using Article.deserialize - construct Article from dict
  @classmethod
  def deducce(cls, packet: dict)-> object:
    if isinstance(packet, dict):
      return cls(packet)
    return packet

  # for default socket Connector
  @classmethod
  def deserialize(cls, bpacket: bytearray):
    packet = pickle.loads(bpacket)
    logger.debug("Deserialized article packet : \n{}".format(packet))
    if isinstance(packet, dict):
      return cls(packet)
    return packet


  # QuConn equivalent of Conn using article.serialize - reduces Article to a raw dict collection
  def reducce(self)-> dict:
    return self.rawcopy(outNote=False)

  def serialize(self)-> bytearray:
    packet = self.rawcopy(outNote=False)
    logger.debug("Serialized article packet : \n{}".format(packet))
    return bytearray(pickle.dumps(packet, pickleMode))
