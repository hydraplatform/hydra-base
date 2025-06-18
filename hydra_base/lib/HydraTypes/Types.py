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
import numpy as np
import pandas as pd
from io import StringIO
from abc import abstractmethod, abstractproperty
from datetime import datetime
import collections
from hydra_base import config

from .Encodings import ScalarJSON, ArrayJSON, DescriptorJSON, DataframeJSON, TimeseriesJSON
from hydra_base.exceptions import HydraError

import logging
log = logging.getLogger(__name__)


class DataType(object):
    """ The DataType class serves as an abstract base class for data types"""
    def __init_subclass__(cls):
        tag = cls.tag
        name = cls.name

        # Register class with hydra
        from .Registry import typemap
        if tag in typemap:
            raise ValueError('Type with tag "{}" already registered.'.format(tag))
        else:
            typemap[tag] = cls
            log.info('Registering data type "{}".'.format(tag))



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
    def fromDataset(cls, value, metadata=None):
        """ Factory method which performs any required transformations
            on a dataset argument, invokes the type's ctor, and returns
            the resulting instance
        """
        pass


class Scalar(DataType):
    tag      = "SCALAR"
    name     = "Scalar"
    skeleton = "[%f]"
    json     = ScalarJSON()

    def __init__(self, value):
        super(Scalar, self).__init__()
        self.value = value
        self.validate()

    @classmethod
    def fromDataset(cls, value, metadata=None):
        return cls(value)

    def validate(self):
        f = float(self.value)

    def get_value(self):
        return str(self._value)

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)


class Array(DataType):
    tag      = "ARRAY"
    name     = "Array"
    skeleton = "[%f, ...]"
    json     = ArrayJSON()

    def __init__(self, encstr):
        super(Array, self).__init__()
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
    tag      = "DESCRIPTOR"
    name     = "Descriptor"
    skeleton = "%s"
    json     = DescriptorJSON()

    def __init__(self, data):
        super(Descriptor, self).__init__()
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
    name     = "Data Frame"
    skeleton = "%s"
    json     = DataframeJSON()

    def __init__(self, data):
        super(Dataframe, self).__init__()
        self.value = data
        self.validate()

    @classmethod
    def fromDataset(cls, value, metadata=None):

        df = cls._create_dataframe(value)

        return cls(df)

    @classmethod
    def _create_dataframe(cls, value):
        """
            Builds a dataframe from the value
        """
        try:

            ordered_jo = json.loads(six.text_type(value), object_pairs_hook=collections.OrderedDict)

            #Pandas does not maintain the order of dicts, so we must break the dict
            #up and put it into the dataframe manually to maintain the order.

            cols = list(ordered_jo.keys())

            if len(cols) == 0:
                raise ValueError("Dataframe has no columns")

            #Assume all sub-dicts have the same set of keys
            if isinstance(ordered_jo[cols[0]], list):
                index = range(len(ordered_jo[cols[0]]))
            else:
                #cater for when the indices are not the same by identifying
                #all the indices, and then making a set of them.
                longest_index = []
                for col in ordered_jo.keys():
                    index = list(ordered_jo[col].keys())
                    if len(index) > len(longest_index):
                        longest_index = index
                index = longest_index

            df = pd.read_json(StringIO(value), convert_axes=False)

            #Make both indices the same type, so they can be compared
            df.index = df.index.astype(str)
            new_index = pd.Index(index).astype(str)

            #Now reindex the dataframe so that the index is in the correct order,
            #as per the data in the DB, and not with the default pandas ordering.
            new_df = df.reindex(new_index)

            #If the reindex didn't work, don't use that value
            if new_df.isnull().sum().sum() != len(df.index):
                df = new_df


        except ValueError as e:
            """ Raised on scalar types used as pd.DataFrame values
                in absence of index arg
            """
            log.exception(e)
            raise HydraError(str(e))

        except AssertionError as e:
            log.warning("An error occurred creating the new data frame: %s. Defaulting to a simple read_json"%(e))
            df = pd.read_json(value).fillna(0)

        return df

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
                df = self.__class__._create_dataframe(val)
                self._value = df
                self.validate()
            except Exception as e:
                """ ...and fail if neither """
                raise HydraError(str(e))

    value = property(get_value, set_value)

class Timeseries(DataType):
    tag      = "TIMESERIES"
    name     = "Time Series"
    skeleton = "[%s, ...]"
    json     = TimeseriesJSON()

    def __init__(self, ts):
        super(Timeseries, self).__init__()
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

                assert isinstance(ts, base_ts.__class__) # Same type as known valid ts


    def get_value(self):
        return self._value.to_json(date_format='iso', date_unit='ns')

    def set_value(self, val):
        self._value = val

    value = property(get_value, set_value)
