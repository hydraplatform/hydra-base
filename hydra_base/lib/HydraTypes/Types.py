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
from marshmallow import Schema, fields, post_load, ValidationError, validate
import enum
import collections
from hydra_base import config
from . import custom_fields

from .Encodings import ScalarJSON, ArrayJSON, DescriptorJSON, DataframeJSON, TimeseriesJSON
from hydra_base.exceptions import HydraError

import logging
log = logging.getLogger(__name__)


class DataType(Schema):
    """ The DataType class serves as an abstract base class for data types"""
    is_simple = False

    def __init_subclass__(cls, **kwargs):
        # Register class with hydra
        from .Registry import typemap

        tag = cls.tag
        if tag in typemap:
            raise ValueError('Type with tag "{}" already registered.'.format(tag))
        else:
            typemap[tag] = cls

    def validate(self, data, **kwargs):
        if len(self.fields) == 1:
            for field in self.fields:
                data = {field: data}
        return super().validate(data, **kwargs)

    def load(self, data, **kwargs):
        if len(self.fields) == 1:
            for field in self.fields:
                data = {field: data}
        return super().load(data, **kwargs)

    @post_load
    def make_obj(self, data):
        if len(self.fields) == 1:
            for field in self.fields:
                return data[field]
        return data


class Scalar(DataType):
    tag = "SCALAR"
    is_simple = True
    value = fields.Float()


def validate_length(value):
    if len(value) < 1:
        ValidationError('Length of list must be at least 1.')


class Array(DataType):
    tag = "ARRAY"
    is_simple = False
    value = fields.List(fields.Raw, validate=validate_length)


class Descriptor(DataType):
    tag = "DESCRIPTOR"
    is_simple = True
    value = fields.Str()


class Dataframe(DataType):
    tag      = "DATAFRAME"
    is_simple = False
    dataframe = fields.Dict(values=fields.Dict(values=fields.Raw, keys=fields.Str()),
                            keys=fields.Str())

    @post_load
    def make_obj(self, data):
        """
            Builds a dataframe from the value
        """
        value = data['dataframe']
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
                index = list(ordered_jo[cols[0]].keys())
            data = []
            for c in cols:
                if isinstance(ordered_jo[c], list):
                    data.append(ordered_jo[c])
                else:
                    data.append(ordered_jo[c].values())

            #This goes in 'sideways' (cols=index, index=cols), so it needs to be transposed after to keep
            #the correct structure
            df = pd.DataFrame(data, columns=index, index=cols).transpose()

        except ValueError as e:
            """ Raised on scalar types used as pd.DataFrame values
                in absence of index arg
            """
            raise HydraError(str(e))

        except AssertionError as e:
            log.warn("An error occurred creating the new data frame: %s. Defaulting to a simple read_json"%(e))
            df = pd.read_json(value).fillna(0)

        return df


class Timeseries(Dataframe):
    tag      = "TIMESERIES"
    dataframe = fields.Dict(values=fields.Dict(values=fields.Raw, keys=fields.DateTime()),
                            keys=fields.Str())

    @post_load
    def make_obj(self, data):
        df = super().make_obj(data)
        df.index = pd.to_datetime(df.index)
        return df


class AnnualProfile(DataType):
    tag = "ANNUALPROFILE"

    frequency_choices = {
        'DAILY': {'size': 366, 'label': 'Daily'},
        'WEEKLY': {'size': 52, 'label': 'Weekly'},
        'MONTHLY': {'size': 12, 'label': 'Monthly'},
    }

    frequency = fields.String(validate=validate.OneOf(choices=frequency_choices.keys()))
    values = fields.List(fields.Float())

    # TODO add validator for length of values for given frequency


class NodeReference(DataType):
    tag = 'NODEREFERENCE'
    node_id = custom_fields.NodeField()


