#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#

import logging
log = logging.getLogger(__name__)

from decimal import Decimal
from functools import wraps
import pandas as pd

import inspect
import json
import six
import sys

from collections import namedtuple

from hydra_base import config
from hydra_base.exceptions import PermissionError



# Python 2 and 3 compatible string checking
try:
    basestring
except NameError:
    basestring = str

def count_levels(value):
    """
        Count how many levels are in a dict:
        scalar, list etc = 0
        {} = 0
        {'a':1} = 1
        {'a' : {'b' : 1}} = 2
        etc...
    """
    if not isinstance(value, dict) or len(value) == 0:
        return 0
    elif len(value) == 0:
        return 0 #An emptu dict has 0
    else:
        nextval = list(value.values())[0]
        return 1 + count_levels(nextval)

def flatten_dict(value, target_depth=1, depth=None):
    """
        Take a hashtable with multiple nested dicts and return a
        dict where the keys are a concatenation of each sub-key.

        The depth of the returned array is dictated by target_depth, defaulting to 1

        ex: {'a' : {'b':1, 'c': 2}} ==> {'a_b': 1, 'a_c': 2}

        Assumes a constant structure actoss all sub-dicts. i.e. there isn't
        one sub-dict with values that are both numbers and sub-dicts.
    """

    #failsafe in case someone specified null
    if target_depth is None:
        target_depth = 1

    values = list(value.values())
    if len(values) == 0:
        return {}
    else:
        if depth is None:
            depth = count_levels(value)

        if isinstance(values[0], dict) and len(values[0]) > 0:
            subval = list(values[0].values())[0]
            if not isinstance(subval, dict) != 'object':
                return value

            if target_depth >= depth:
                return value

            flatval = {}
            for k in value.keys():
                subval = flatten_dict(value[k], target_depth, depth-1)
                for k1 in subval.keys():
                    flatval[str(k)+"_"+str(k1)] = subval[k1];
            return flatval
        else:
            return value

def generate_data_hash(dataset_dict):

    d = dataset_dict
    if d.get('metadata') is None:
        d['metadata'] = {}

    hash_string = "%s %s %s %s %s"%(
                                str(d['name']),
                                str(d['unit_id']),
                                str(d['type']),
                                d['value'],
                                d['metadata'])

    log.debug("Generating data hash from: %s", hash_string)

    data_hash = hash(hash_string)

    log.debug("Data hash: %s", data_hash)

    return data_hash

def get_val(dataset, timestamp=None):
    """
        Turn the string value of a dataset into an appropriate
        value, be it a decimal value, array or time series.

        If a timestamp is passed to this function,
        return the values appropriate to the requested times.

        If the timestamp is *before* the start of the timeseries data, return None
        If the timestamp is *after* the end of the timeseries data, return the last
        value.

        The raw flag indicates whether timeseries should be returned raw -- exactly
        as they are in the DB (a timeseries being a list of timeseries data objects,
        for example) or as a single python dictionary

    """

    val = dataset.get_value()

    if dataset.type == 'array':
        #TODO: design a mechansim to retrieve this data if it's stored externally
        return json.loads(val)

    elif dataset.type == 'descriptor':
        return str(val)
    elif dataset.type == 'scalar':
        return Decimal(str(val))
    elif dataset.type == 'timeseries':
        #TODO: design a mechansim to retrieve this data if it's stored externally

        seasonal_year = config.get('DEFAULT','seasonal_year', '1678')
        seasonal_key = config.get('DEFAULT', 'seasonal_key', '9999')
        val = val.replace(seasonal_key, seasonal_year)

        timeseries = pd.read_json(val, convert_axes=True)

        if isinstance(timeseries.index, pd.DatetimeIndex):
            if timeseries.index.tz is None:
                timeseries = timeseries.tz_localize('UTC')
            else:
                timeseries = timeseries.tz_convert('UTC')


        if timestamp is None:
            return timeseries
        else:
            try:
                idx = timeseries.index
                #Seasonal timeseries are stored in the year
                #1678 (the lowest year pandas allows for valid times).
                #Therefore if the timeseries is seasonal,
                #the request must be a seasonal request, not a
                #standard request

                if type(idx) == pd.DatetimeIndex:
                    if set(idx.year) == set([int(seasonal_year)]):
                        if isinstance(timestamp,  list):
                            seasonal_timestamp = []
                            for t in timestamp:
                                t_1900 = t.replace(year=int(seasonal_year))
                                seasonal_timestamp.append(t_1900)
                            timestamp = seasonal_timestamp
                        else:
                            timestamp = [timestamp.replace(year=int(seasonal_year))]

                pandas_ts = timeseries.reindex(timestamp, method='ffill')

                #If there are no values at all, just return None
                if len(pandas_ts.dropna()) == 0:
                    return None

                #Replace all numpy NAN values with None
                pandas_ts = pandas_ts.where(pandas_ts.notnull(), None)

                val_is_array = False
                if len(pandas_ts.columns) > 1:
                    val_is_array = True

                if val_is_array:
                    if type(timestamp) is list and len(timestamp) == 1:
                        ret_val = pandas_ts.loc[timestamp[0]].values.tolist()
                    else:
                        ret_val = pandas_ts.loc[timestamp].values.tolist()
                else:
                    col_name = pandas_ts.loc[timestamp].columns[0]
                    if type(timestamp) is list and len(timestamp) == 1:
                        ret_val = pandas_ts.loc[timestamp[0]].loc[col_name]
                    else:
                        ret_val = pandas_ts.loc[timestamp][col_name].values.tolist()

                return ret_val

            except Exception as e:
                log.critical("Unable to retrive data. Check timestamps.")
                log.critical(e)

def get_json_as_string(json_string_or_dict):
    """
        Take a dict or string and return a string.
        The dict will be json dumped.
        The string will json parsed to check for json validity. In order to deal
        with strings which have been json encoded multiple times, keep json decoding
        until a dict is retrieved or until a non-json structure is identified.
    """

    if isinstance(json_string_or_dict, dict):
        return json.dumps(json_string_or_dict)

    if(isinstance(json_string_or_dict, six.string_types)):
        try:
            return get_json_as_string(json.loads(json_string_or_dict))
        except:
            return json_string_or_dict

def get_json_as_dict(json_string_or_dict):
    """
        Take a dict or string and return a dict if the data is json-encoded.
        The string will json parsed to check for json validity. In order to deal
        with strings which have been json encoded multiple times, keep json decoding
        until a dict is retrieved or until a non-json structure is identified.
    """

    if isinstance(json_string_or_dict, dict):
        return json_string_or_dict

    if(isinstance(json_string_or_dict, six.string_types)):
        try:
            return get_json_as_dict(json.loads(json_string_or_dict))
        except:
            return json_string_or_dict


class NullAdapterType(type):
    """
      Metaclass for NullAdapter. Overriding __getattr__ here
      enables class and static attrs to be simulated.
    """
    def __getattr__(self, *args, **kwargs):
        return self


class NullAdapter(metaclass=NullAdapterType):
    """
      A class that does nothing.
      This may be used in place of any adapter in order to disable
      the functionality of that adapter type without requiring other
      code to be changed.
      From the caller's perspective, all attributes exist on this class,
      including nested attributes, and all are callable.
    """
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, *args, **kwargs):
        return self

    def __getitem__(self, idx):
        return None

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration


def export(func):
    """
      This defines a decorator which passes through the target
      function without modification, but appends its name to
      the calling module's '__all__' variable so that it is
      exported when using 'from <mod> import *' syntax.
    """
    if not hasattr(sys.modules[func.__module__], "__all__"):
        setattr(sys.modules[func.__module__], "__all__", tuple())
    sys.modules[func.__module__].__all__ += (func.__name__,)
    return func


def organisation_admin(func):
    """
        Decorator which allows the wrapped function to be
        called only by administrators of the organisation
        specified in the <argname> argument to the wrapped
        function.

        Note that a Hydra Admin user is considered an
        organisation administrator.
    """
    org_id_arg, uid_arg = "organisation_id", "user_id"
    @wraps(func)
    def wrapper(*args, **kwargs):
        from hydra_base.lib.usergroups import is_organisation_administrator
        av = inspect.getargvalues(inspect.currentframe())
        org_id = av.locals[av.keywords][org_id_arg]
        uid = av.locals[av.keywords][uid_arg]
        if is_organisation_administrator(uid=uid, organisation_id=org_id):
            return func(*args, **kwargs)
        else:
            raise PermissionError(f"User {uid} does not have permission to call {func.__name__}")

    return wrapper


def usergroup_admin(func):
    """
        Decorator which allows the wrapped function to be
        called only by administrators of the usergroup
        specified in the <argname> argument to the wrapped
        function.

        Note that a Hydra Admin user is considered a
        usergroup administrator.
    """
    group_id_arg, uid_arg = "group_id", "user_id"
    @wraps(func)
    def wrapper(*args, **kwargs):
        from hydra_base.lib.usergroups import is_usergroup_administrator
        av = inspect.getargvalues(inspect.currentframe())
        group_id = av.locals[av.keywords][group_id_arg]
        uid = av.locals[av.keywords][uid_arg]
        if is_usergroup_administrator(uid=uid, group_id=group_id):
            return func(*args, **kwargs)
        else:
            raise PermissionError(f"User {uid} does not have permission to call {func.__name__}")

    return wrapper
