import json
import math
import six
import pandas as pd
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
import collections

from Encodings import ScalarJSON, ArrayJSON, DescriptorJSON, DataframeJSON, TimeseriesJSON
from hydra_base.exceptions import HydraError


class DataType(object):
    """ The DataType class serves as an abstract base class for data types"""
    __metaclass__ = ABCMeta

    @abstractproperty
    def skeleton(self):
        pass

    @abstractproperty
    def tag(self):
        pass

    @abstractproperty
    def value(self):
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
    def fromDataset(cls, value, metadata=None):
        return cls(value)

    def validate(self):
        f = float(self.value)
        assert not math.isnan(f) # Excludes NaN etc

    def get_value(self):
        return str(self._value)

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)


class Array(DataType):
    tag      = "ARRAY"
    skeleton = "[%f, ...]"
    json     = ArrayJSON()

    def __init__(self, encstr):
        self.value = encstr
        self.validate()

    @classmethod
    def fromDataset(cls, value, metadata=None):
        return cls(value)

    def validate(self):
        j = json.loads(self.value)
        assert len(j) > 0           # Sized
        assert iter(j) is not None  # Iterable
        assert j.__getitem__        # Container
        assert not isinstance(j, six.string_types) # Exclude strs

    def get_value(self):
        return self._value

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)



class Descriptor(DataType):
    """ Unused obsolete type """
    tag      = "DESCRIPTOR"
    skeleton = "%s"
    json     = DescriptorJSON()

    def __init__(self, data):
        self.value = data
        self.validate()

    @classmethod
    def fromDataset(cls, value, metadata=None):
        if metadata and metadata.get('data_type') == 'hashtable':
            try:
                df = pd.read_json(six.text_type(value))
                data = df.transpose().to_json()
            except Exception:
                noindexdata = json.loads(six.text_type(value))
                indexeddata = {0:noindexdata}
                data = json.dumps(indexeddata)
            return cls(data)
        else:
            return cls(six.text_type(value))


    def validate(self):
        pass

    def get_value(self):
        return self._value

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)


class Dataframe(DataType):
    tag      = "DATAFRAME"
    skeleton = "%s"
    json     = DataframeJSON()

    def __init__(self, data):
        self.value = data
        self.validate()

    @classmethod
    def fromDataset(cls, value, metadata=None):
        try:
            ordered_jo = json.loads(six.text_type(value), object_pairs_hook=collections.OrderedDict)
            df = pd.DataFrame.from_dict(ordered_jo)
        except ValueError as e:
            """ Raised on scalar types used as pd.DataFrame values
                in absence of index arg
            """
            raise HydraError(e.message)

        return cls(df)


    def validate(self):
        assert isinstance(self._value, pd.DataFrame)
        assert not self._value.empty


    def get_value(self):
        return self._value.to_json()

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)


class Timeseries(DataType):
    tag      = "TIMESERIES"
    skeleton = "[%s, ...]"
    json     = TimeseriesJSON()

    def __init__(self, ts):
        self.value = ts.to_json(date_format='iso', date_unit='ns')
        self.validate()

    @classmethod
    def fromDataset(cls, value, metadata=None):
        ordered_jo = json.loads(six.text_type(value), object_pairs_hook=collections.OrderedDict)
        ts = pd.DataFrame.from_dict(ordered_jo, orient="index")
        return cls(ts)


    def validate(self):
        base_ts = pd.Timestamp("01-01-1970")
        jd = json.loads(self.value, object_pairs_hook=collections.OrderedDict)
        for k,v in jd.iteritems():
            for date in (six.text_type(d) for d in v.keys()):
                ts = pd.Timestamp(date)
                print(ts, type(ts))
                assert isinstance(ts, base_ts.__class__) # Same type as known valid ts


    def get_value(self):
        return self._value

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)
