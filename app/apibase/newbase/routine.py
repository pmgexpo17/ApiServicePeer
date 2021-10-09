import asyncio
import logging
import re

from typing import List, Union

logger = logging.getLogger('asyncio')

numPattern = r"(?<![a-zA-Z:])[-+]?\d*\.?\d+"

#-----------------------------------------------------------------#
# create_task
#-----------------------------------------------------------------#
def create_task(aw) -> asyncio.Task:
  loop = asyncio.get_event_loop()
  return loop.create_task(aw)

#-----------------------------------------------------------------#
# getNumeric
#-----------------------------------------------------------------#
def getNumeric(param: str) -> Union[float, int]:
  """
  Function that returns a numeric from a random data string.
  :param data:
  :return:
  """
  if not isinstance(param, str):
    return param

  if "." in param:
    return float(param)
  return int(param)

#-----------------------------------------------------------------#
# parseFloat
#-----------------------------------------------------------------#
def parseFloat(param: str) -> float:
  """
  Function that returns a float from a random data string.
  :param data:
  :return:
  """
  if not isinstance(param, str):
    return param

  value = findNumeric(param)[0]
  return float(value)

#-----------------------------------------------------------------#
# parseInt
#-----------------------------------------------------------------#
def parseInt(param: str) -> int:
  """
  Function that returns an int from a random data string.
  :param data:
  :return:
  """
  if not isinstance(param, str):
    return param

  value = findNumeric(param)[0]
  return int(value)

#-----------------------------------------------------------------#
# parseNumeric
#-----------------------------------------------------------------#
def parseNumeric(param: str) -> Union[float, int]:
  """
  Function that returns a numeric value from a string.

  :param string: str: The raw numeric string.
  :return: Result: The numeric value retrieved from the string.
  """
  if not isinstance(param, str):
    return param
  
  value = findNumeric(param)[0]

  if "." in value:
    return float(value)
  return int(value)

#-----------------------------------------------------------------#
# findNumeric
#-----------------------------------------------------------------#
def findNumeric(param: str) -> List[str]:
  try:
    return re.findall(numPattern, param)
  except IndexError:
    return '0'
