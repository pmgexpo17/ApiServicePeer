import logging
import pickle

logger = logging.getLogger('asyncio')

try:
  pickleMode = pickle.DEFAULT_PROTOCOL
except AttributeError:
  pickleMode = pickle.HIGHEST_PROTOCOL

#================================================================#
# Note - new Note to replace previous scope and model
#===============================================================-#
class Note:
  def __init__(self, packet={}, recursive=True):
    self._update(packet, recursive)
    
  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    return getattr(self, key)

  def __setitem__(self, key, value):
    self.__dict__[key] = value
    
  @property
  def body(self):
    return self.__dict__.copy()
  
  def bodify(self, deNote=False):
    if not deNote:
      return self.__dict__.copy()
    body = self.__dict__.copy()
    for key, value in body.items():
      if isinstance(value, Note):
        body[key] = value.__dict__
    return body

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
        if not hasattr(currNode, nodeName):
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

  # rename an attribute
  def rename(self, fkey, tkey: str):
    if not (isinstance("fkey",str) and isinstance("tkey",str)):
      return
    if fkey in self.__dict__ and tkey not in self.__dict__:
      if "-" in tkey:
        tkey = tkey.replace("-","_")
      self.__dict__[tkey] = self.__dict__.pop(fkey)

  # copy the selected Note attributes and if dict child nodes exist convert them to Note
  def select(self, *keys, recursive=True) -> object:
    if not keys:
      return self.copy()
    packet = {key:self.__dict__[key] for key in keys if key in self.__dict__}
    return Note(packet,recursive)

  # return the selected key values in a list
  def tell(self, *keys):
    return [self.__dict__[key] for key in keys if key in self.__dict__]

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
# Article - new Article to replace previous scope and model
#===============================================================-#
class Article(Note):

  @classmethod
  def deserialize(cls, bpacket: bytearray):
    packet = pickle.loads(bpacket)
    logger.debug("Deserialized article packet : \n{}".format(packet))
    return cls(packet)

  def serialize(self)-> bytearray:
    packet = self.body
    for key, value in packet.items():
      if isinstance(value, Note):
        packet[key] = value.body
    logger.debug("Serialized article packet : \n{}".format(packet))
    return bytearray(pickle.dumps(packet, pickleMode))
