import pytest
import json
import datetime
import collections

import hydra_base as hb
import pandas as pd

from hydra_base.exceptions import HydraError
from hydra_base.lib.HydraTypes.Registry import HydraObjectFactory, HydraTypeError
from hydra_base.lib.HydraTypes.Types import Scalar, Array

import logging
log = logging.getLogger("objects")


""" Type arguments """

scalar_valid_values       = [ 46, -1, 0, 7.7, -0.0 ]
scalar_invalid_values     = [ "one", None, pd, {} ]

array_valid_values        = [ [-2, -1, 0, 1, 2], range(32), [ 0.5e-3, 0.5, 0.5e3 ] ]
array_invalid_values      = [ "otheriterable", xrange(32), 77, {} ]

timeseries_valid_values   = [ ["JAN", "FEB", "MAR"], range(12) ]
timeseries_invalid_values = [ "otheriterable", xrange(12), pd, set() ]


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


if __name__ == "__main__":
    pytest.main(['-v', __file__])
