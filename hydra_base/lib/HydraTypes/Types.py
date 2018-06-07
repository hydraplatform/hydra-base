import json
import pandas as pd
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
import collections

from Encodings import ScalarJSON, ArrayJSON, DescriptorJSON, DataframeJSON, TimeseriesJSON
from hydra_base.exceptions import HydraError


class HydraTypeError(HydraError):
    def __init__(self, instance):
        pass


""" Abstract base class for data types"""
class DataType(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def skeleton(self):
        pass

    @abstractproperty
    def tag(self):
        pass

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def json(self):
        pass

    @abstractmethod
    def fromDataset(self):
        pass


class Scalar(DataType):
    tag      = "SCALAR"
    ctor_fmt = ("value",)
    skeleton = "[%f]"
    json     = ScalarJSON()

    def __init__(self, value):
        self.value = value
        self.validate()

    @classmethod
    def fromJSON(cls, encstr):
        pass

    @classmethod
    def fromDataset(cls, value, metadata=None):
        return cls(value)

    def validate(self):
        try:
            float(self.value)
        except ValueError as e:
            raise HydraTypeError(self)


class Array(DataType):
    tag      = "ARRAY"
    skeleton = "[%f, ...]"
    json     = ArrayJSON()

    def __init__(self, encstr):
        self.value = encstr
        self.validate()

    def validate(self):
        j = json.loads(self.value)
        assert len(j) > 1

    @classmethod
    def fromDataset(cls, value, metadata=None):
        return cls(value)


class Descriptor(DataType):
    tag      = "DESCRIPTOR"
    skeleton = "%s"
    json     = DescriptorJSON()

    def __init__(self, data):
        self.value = data
        self.validate()

    def validate(self):
        pass

    @classmethod
    def fromDataset(cls, value, metadata=None):
        if metadata and metadata.get('data_type') == 'hashtable':
            try:
                df = pd.read_json(unicode(value))
                data = df.transpose().to_json()
            except Exception:
                noindexdata = json.loads(unicode(value))
                indexeddata = {0:noindexdata}
                data = json.dumps(indexeddata)
            return cls(data)
        else:
            return cls(unicode(value))


class Dataframe(DataType):
    tag      = "DATAFRAME"
    skeleton = "%s"
    json     = DataframeJSON()

    def __init__(self, data):
        self.value = data
        self.validate()

    def validate(self):
        pass

    @classmethod
    def fromDataset(cls, value, metadata=None):
        ts = pd.read_json(unicode(value), convert_axes=False)
        return cls(ts)


class Timeseries(DataType):
    tag      = "TIMESERIES"
    skeleton = "[%s, ...]"
    json     = TimeseriesJSON()


    def __init__(self, ts):
        self.value = ts.to_json(date_format='iso', date_unit='ns')
        self.validate()

    def validate(self):
        print("[self.value] {0} ({1})".format(self.value, type(self.value)))
        base_ts = pd.Timestamp("01-01-1970")
        jd = json.loads(self.value, object_pairs_hook=collections.OrderedDict)
        for k,v in jd.iteritems():
            print("[v] {0} ({1})".format(v, type(v)))
            print("[d]...")
            print([unicode(d) for d in v])
            for date in (unicode(d) for d in v.keys()):
                ts = pd.Timestamp(date)
                print(ts, type(ts))
                assert isinstance(ts, base_ts.__class__)


    @classmethod
    def fromDataset(cls, value, metadata=None):
        print("[value] {0} ({1})".format(value, type(value)))
        ordered_jo = json.loads(unicode(value), object_pairs_hook=collections.OrderedDict)
        print("[ordered_jo[ {0} ({1})".format(ordered_jo, type(ordered_jo)))
        ts = pd.DataFrame.from_dict(ordered_jo, orient="index")
        return cls(ts)

