import json
import inspect
import sys

from Types import Array, Scalar, Timeseries, Descriptor, Dataframe
from Types import DataType as Datatype_Base


def isHydraType(cls):
    """ Predicate for inspect.getmembers(): defines what constitutes a 'Hydra Type' """
    return inspect.isclass(cls) and cls is not Datatype_Base and Datatype_Base in inspect.getmro(cls)


hydra_types = tuple(cls for _,cls in inspect.getmembers(sys.modules[__name__], isHydraType))
typemap     = { t.tag : t for t in hydra_types }


class HydraObjectFactory(object):
    @staticmethod
    def fromJSON(encstr, tmap=typemap):
        pass


    @classmethod
    def valueFromDataset(cls, datatype, value, metadata=None, tmap=typemap):
        obj = cls.fromDataset(datatype, value, metadata=metadata, tmap=tmap)
        return obj.value


    @staticmethod
    def fromDataset(datatype, value, metadata=None, tmap=typemap):
        return tmap[datatype.upper()].fromDataset(value, metadata=metadata)

