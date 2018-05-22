import json
import inspect
import sys

from Types import Array, Boolean, Enumeration, Scalar, Timeseries, HydraTypeError
from Types import DataType as Datatype_Base


def isHydraType(cls):
   return inspect.isclass(cls) and cls is not Datatype_Base and Datatype_Base in inspect.getmro(cls)


hydra_types = tuple(cls for _,cls in inspect.getmembers(sys.modules[__name__], isHydraType))
typemap     = { t.tag : t for t in hydra_types }


class HydraObjectFactory(object):
    @staticmethod
    def fromJSON(encstr, tmap=typemap):
        tag = ""
        try:
            jo = json.loads(encstr)
            tag = jo.keys()[0]
        except ValueError:
            print("Malformed JSON: {0}".format(encstr))
            return 

        try:
            return tmap[tag](encstr=encstr)
        except KeyError:
            print("Invalid Hydra Type: {0}".format(tag))


    @staticmethod
    def getData(dset, tmap=typemap):
        # TODO: modify type.fromJSON to match dset.as_json
        obj = tmap[dset.type.upper()].fromJSON(dset.as_json())
        return obj.value

    @staticmethod
    def valueFromDataset(dset, tmap=typemap):
        obj = tmap[dset.type.upper()].fromDataset(dset)
        return obj.value


    @staticmethod
    def clone(src, tmap=typemap):
        return tmap[src.tag](encstr=src.json)
