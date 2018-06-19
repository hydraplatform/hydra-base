"""
  Types that can be represented by a dataset are defined here

  Each Hydra type must subclass DataType and implement the
  required abstract properties and methods.  The form of each
  class' constructor is not part of the interface and is left
  to the implementer.
"""
import json
import math
import six
import pandas as pd
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
import collections
from hydra_base import config

from .Encodings import ScalarJSON, ArrayJSON, DescriptorJSON, DataframeJSON, TimeseriesJSON
from hydra_base.exceptions import HydraError

import logging
log = logging.getLogger(__name__)


class DataType(object):
    """ The DataType class serves as an abstract base class for data types"""
    __metaclass__ = ABCMeta

    @abstractproperty
    def skeleton(self):
        """ Reserved for future use """
        pass

    @abstractproperty
    def tag(self):
        """ A str which uniquely identifies this type and serves as its key in
            the Registry.typemap dict
        """
        pass

    @abstractproperty
    def value(self):
        """ This type's representation of the value contained within
            a dataset of the same type
        """
        pass

    @abstractmethod
    def validate(self):
        """ Raises (any) exception if the dataset's value argument
            cannot be correctly represented as this type
        """
        pass

    @abstractmethod
    def json(self):
        """ Reserved for future use """
        pass

    @abstractmethod
    def fromDataset(self):
        """ Factory method which performs any required transformations
            on a dataset argument, invokes the type's ctor, and returns
            the resulting instance
        """
        pass



class Scalar(DataType):
    tag      = "SCALAR"
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
            raise HydraError(str(e))

        return cls(df)


    def validate(self):
        assert isinstance(self._value, pd.DataFrame)
        assert not self._value.empty


    def get_value(self):
        return self._value.to_json()

    def set_value(self, val):
        self._value = val
        try:
            """ Use validate test to confirm is pd.DataFrame... """
            self.validate()
        except AssertionError:
            """ ...otherwise attempt as json..."""
            try:
                ordered_jo = json.loads(six.text_type(val), object_pairs_hook=collections.OrderedDict)
                df = pd.DataFrame.from_dict(ordered_jo)
                self._value = df
                self.validate()
            except Exception as e:
                """ ...and fail if neither """
                raise HydraError(str(e))

    value = property(get_value, set_value)


class Timeseries(DataType):
    tag      = "TIMESERIES"
    skeleton = "[%s, ...]"
    json     = TimeseriesJSON()

    def __init__(self, ts):
        self.value = ts
        self.validate()

    @classmethod
    def fromDataset(cls, value, metadata=None):
        ordered_jo = json.loads(six.text_type(value), object_pairs_hook=collections.OrderedDict)
        ts = pd.DataFrame.from_dict(ordered_jo)
        return cls(ts)


    def validate(self):
        base_ts = pd.Timestamp("01-01-1970")
        #TODO: We need a more permanent solution to seasonal/repeating timeseries
        seasonal_year = config.get('DEFAULT','seasonal_year', '1678')
        seasonal_key = config.get('DEFAULT', 'seasonal_key', '9999')
        jd = json.loads(self.value, object_pairs_hook=collections.OrderedDict)
        for k,v in jd.items():
            for date in (six.text_type(d) for d in v.keys()):
                #A date with '9999' in it is a special case, but is an invalid year
                #for pandas, so replace it with the first year allowed by pandas -- 1678
                if date.find(seasonal_key) >= 0:
                    date = date.replace(seasonal_key, seasonal_year)

                ts = pd.Timestamp(date)
                print(ts, type(ts))
                assert isinstance(ts, base_ts.__class__) # Same type as known valid ts


    def get_value(self):
        return self._value.to_json(date_format='iso', date_unit='ns')

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)
