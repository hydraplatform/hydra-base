import pytest
import json
import datetime
import collections

import hydra_base as hb
import pandas as pd

from hydra_base.exceptions import HydraError
from hydra_base.lib.HydraTypes.Registry import HydraObjectFactory
from hydra_base.lib.HydraTypes.Types import Scalar, Array

import logging
log = logging.getLogger("objects")


def generator(size):
    """
        To avoid python 2/3 compatibility issues, use a custom generator instead of 'range' directly
    """
    for i in range(size):
        yield i

""" Type arguments """

scalar_valid_values       = [ 46, -1, 0, 7.7, -0.0 ]
scalar_invalid_values     = [ "one", pd, {} ]

array_valid_values        = [ [-2, -1, 0, 1, 2], list(range(32)), [ 0.5e-3, 0.5, 0.5e3 ] ]
array_invalid_values      = [ generator(32), 77, {}, "justastring" ]

timeseries_valid_values   = [ {"0": {"1979 Feb 2 0100":7, "01:00 2 Feb 1979":9}}, {"0":{"2012":12, "2013":13, "2014":14}}, {"0":{"18:00 31 August 1977":100}} ]
timeseries_invalid_values = [ "otheriterable", list(range(12)), {"JAN":1, "FEB":2, "MAR":3, "APR":4}, set(), ["01:00 30 Feb 1979"] ]

dataframe_valid_values    = [ {"data" : {"fr": "ame"}}, {"one": ["first"], "two": ["second"]}, {"n":{"e":{"s":{"t":{"e":"d"}}}}} ]
dataframe_invalid_values  = [ 77, set(), {"one": "first", "two": "second"} ]


""" Scalar type tests """

@pytest.mark.parametrize("value", scalar_valid_values)
def test_create_scalar(value):
    scalar_dataset = hb.lib.objects.Dataset({'type':'scalar', 'value': value})
    value = scalar_dataset.parse_value()


@pytest.mark.parametrize("value", scalar_invalid_values)
def test_fail_create_scalar(value):
    with pytest.raises(HydraError):
        scalar_dataset = hb.lib.objects.Dataset({'type':'scalar', 'value': value})
        value = scalar_dataset.parse_value()


@pytest.fixture(params=scalar_valid_values)
def make_scalar(request):
    scalar_dataset = hb.lib.objects.Dataset({'type':'scalar', 'value': request.param})
    o = HydraObjectFactory.fromDataset(scalar_dataset.type, scalar_dataset.value)
    Fixret = collections.namedtuple("fixret", ("obj", "fixarg"))
    yield Fixret(o, request.param)     # Expose current fixture arg to test


def test_raw_scalar(make_scalar):
    assert type(make_scalar.obj) == Scalar
    assert make_scalar.obj.tag == Scalar.tag
    assert make_scalar.obj.value is not None
    assert make_scalar.obj.value == str(make_scalar.fixarg)
    assert make_scalar.obj.validate() is None



""" Array type tests """

@pytest.mark.parametrize("value", array_valid_values)
def test_create_array(value):
    array_dataset = hb.lib.objects.Dataset({'type':'array', 'value': json.dumps(value)})
    value = array_dataset.parse_value()


@pytest.mark.parametrize("value", array_invalid_values)
def test_fail_create_array(value):
    with pytest.raises( (HydraError, TypeError) ):
        print(value)
        array_dataset = hb.lib.objects.Dataset({'type':'array', 'value': json.dumps(value)})
        value = array_dataset.parse_value()


@pytest.fixture(params=array_valid_values)
def make_array(request):
    array_dataset = hb.lib.objects.Dataset({'type':'array', 'value': json.dumps(request.param) })
    o = HydraObjectFactory.fromDataset(array_dataset.type, array_dataset.value)
    Fixret = collections.namedtuple("fixret", ("obj", "fixarg"))
    yield Fixret(o, request.param)     # Expose current fixture arg to test


def test_raw_array(make_array):
    assert type(make_array.obj) == Array
    assert make_array.obj.tag == Array.tag
    assert make_array.obj.value is not None
    assert make_array.obj.value == str(make_array.fixarg)
    assert make_array.obj.validate() is None


""" Timeseries type tests """

@pytest.mark.parametrize("value", timeseries_valid_values)
def test_create_timeseries(value):
    timeseries_dataset = hb.lib.objects.Dataset({'type':'timeseries', 'value': json.dumps(value)})
    value = timeseries_dataset.parse_value()


@pytest.mark.parametrize("value", timeseries_invalid_values)
def test_fail_create_timeseries(value):
    with pytest.raises( (HydraError, TypeError, ValueError) ):
        timeseries_dataset = hb.lib.objects.Dataset({'type':'timeseries', 'value': json.dumps(value)})
        value = timeseries_dataset.parse_value()

""" DataFrame type tests """

@pytest.mark.parametrize("value", dataframe_valid_values)
def test_create_dataframe(value):
    dataframe_dataset = hb.lib.objects.Dataset({'type':'dataframe', 'value': json.dumps(value)})
    value = dataframe_dataset.parse_value()


@pytest.mark.parametrize("value", dataframe_invalid_values)
def test_fail_create_dataframe(value):
    with pytest.raises( (HydraError, TypeError, ValueError) ):
        dataframe_dataset = hb.lib.objects.Dataset({'type':'dataframe', 'value': json.dumps(value)})
        value = dataframe_dataset.parse_value()


def test_dataframe_order_preserved():
    #make the index deliberately non-ordered
    index = ['0', '2', '1']
    cols = ['A']
    data = ['x', 'y', 'z']

    df = pd.DataFrame(data, columns=cols, index=index)

    dataframe_dataset = hb.lib.objects.Dataset({'type':'dataframe', 'value': df.to_json()})
    parsed_value = dataframe_dataset.parse_value()

    assert parsed_value == df.to_json()


def test_dataframe_time_index_order_preserved():
    #Test to make sure date time indices remain in the order they are sent to
    #hydra, if they are not ordered by time for whatever reason.

    #make the index deliberately non-ordered
    index = ['2010-02-01', '2005-01-01', '2012-01-01']
    cols = ['A']
    data = ['x', 'y', 'z']

    df = pd.DataFrame(data, columns=cols, index=index)

    dataframe_dataset = hb.lib.objects.Dataset({'type':'dataframe', 'value': df.to_json()})
    parsed_value = dataframe_dataset.parse_value()

    assert parsed_value == df.to_json()
