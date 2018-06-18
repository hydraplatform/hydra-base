"""
  Hydra Type Registry

  Collects Hydra Types (classes having DataType as their base) and
  constructs a `typemap` mapping the unique identifying str tag
  attribute of each type to the corresponding class.
"""
import json
import inspect
import sys

from .Types import Array, Scalar, Timeseries, Descriptor, Dataframe
from .Types import DataType as Datatype_Base


def isHydraType(cls):
    """ Predicate for inspect.getmembers(): defines what constitutes a 'Hydra Type' """
    return inspect.isclass(cls) and cls is not Datatype_Base and Datatype_Base in inspect.getmro(cls)


hydra_types = tuple(cls for _,cls in inspect.getmembers(sys.modules[__name__], isHydraType))
typemap     = { t.tag : t for t in hydra_types }


class HydraObjectFactory(object):
    """
      Abstract class serving as a factory for the creation of Hydra Types
    """
    @staticmethod
    def fromJSON(encstr, tmap=typemap):
        pass


    @classmethod
    def valueFromDataset(cls, datatype, value, metadata=None, tmap=typemap):
        """
          Return the value contained by dataset argument, after casting to
          correct type and performing type-specific validation
        """
        obj = cls.fromDataset(datatype, value, metadata=metadata, tmap=tmap)
        return obj.value


    @staticmethod
    def fromDataset(datatype, value, metadata=None, tmap=typemap):
        """
          Return a representation of dataset argument as an instance

          of the class corresponding to its datatype
        """
        return tmap[datatype.upper()].fromDataset(value, metadata=metadata)

