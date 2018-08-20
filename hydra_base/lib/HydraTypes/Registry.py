"""
  Hydra Type Registry

  Collects Hydra Types (classes having DataType as their base) and
  constructs a `typemap` mapping the unique identifying str tag
  attribute of each type to the corresponding class.
"""
import json
import inspect
import sys

typemap = {}
from .Types import Array, Scalar, Timeseries, Descriptor, Dataframe
from .Types import DataType as Datatype_Base


class HydraObjectFactory(object):
    """
      Abstract class serving as a factory for the creation of Hydra Types
    """
    @staticmethod
    def fromJSON(encstr, tmap=None):
        if tmap is None:
            tmap = typemap
        pass

    @classmethod
    def valueFromDataset(cls, datatype, value, metadata=None, tmap=None):
        """
          Return the value contained by dataset argument, after casting to
          correct type and performing type-specific validation
        """
        if tmap is None:
            tmap = typemap
        obj = cls.fromDataset(datatype, value, metadata=metadata, tmap=tmap)
        return obj.value

    @staticmethod
    def fromDataset(datatype, value, metadata=None, tmap=None):
        """
          Return a representation of dataset argument as an instance

          of the class corresponding to its datatype
        """
        if tmap is None:
            tmap = typemap
        return tmap[datatype.upper()].fromDataset(value, metadata=metadata)

