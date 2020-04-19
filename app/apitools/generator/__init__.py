from .jobGenerator import AbstractMakeKit, AbstractGenerator, RenderError
from .jobGeneratorA1 import JobGeneratorA1
from .jobGeneratorB1 import JobGeneratorB1

class Generator:

  @classmethod
  def start(cls, apiBase):
    AbstractMakeKit.startFw(apiBase)