import json
import pandas as pd
from abc import ABCMeta, abstractmethod, abstractproperty

from Encodings import BooleanJSON, ScalarJSON, EnumJSON, ArrayJSON


class HydraTypeError(Exception):
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
        js = json.loads(encstr)[cls.tag]
        print(js, js["value"])
        return cls(*[js[arg] for arg in cls.ctor_fmt])

    @classmethod
    def fromDataset(cls, dset):
        return cls(dset.value)

    def validate(self):
        try:
            float(self.value)
        except ValueError as e:
            raise HydraTypeError(self)



class Boolean(DataType):
    tag      = "BOOL"
    skeleton = "[%s, %s]"
    json     = BooleanJSON()

    def __init__(self, *args, **kwargs):
        encstr = kwargs.get("encstr")
        if encstr:
            self.json = encstr
        else:
            self.true, self.false = args[0], args[1]
        self.validate()

    def validate(self):
        if self.true == None or self.false == None or self.true == self.false:
            raise HydraTypeError(self)


class Enumeration(DataType):
    tag      = "ENUM"
    skeleton = "[%s, ...]"
    json     = EnumJSON()

    def __init__(self, *args, **kwargs):
        encstr = kwargs.get("encstr")
        if encstr:
            self.json = encstr
        else:
            self.values = args[0]
            self.validate()

    def validate(self):
        if len(set(self.values)) != len(self.values):
            self.errmsg = "Duplicate element"
            raise HydraTypeError(self)

    def __getitem__(self, idx):
        return self.values[idx]

    def __setitem__(self, idx, val):
        self.values[idx] = val



class Timeseries(DataType):
    tag      = "TIMESERIES"
    skeleton = "[%s, ...]"
    json     = None


    def __init__(self, ts):
        self.value = ts.to_json(date_format='iso', date_unit='ns')
        self.validate()

    def validate(self):
        pass

    @classmethod
    def fromDataset(cls, dset):
        ts = pd.read_json(unicode(dset.value), convert_axes=False)
        return cls(ts)

    

class Array(DataType):
    tag      = "ARRAY"
    skeleton = "[%f, ...]"
    json     = ArrayJSON()

    def __init__(self, encstr):
        self.value = encstr
        self.validate()

    def validate(self):
        json.loads(self.value)

    @classmethod
    def fromDataset(cls, dset):
        return cls(dset.value)
