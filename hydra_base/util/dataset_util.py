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
from decimal import Decimal

from operator import mul
from ..exceptions import HydraError, ValidationError
import numpy as np
import pandas as pd
import re
from .hydra_dateutil import get_datetime
log = logging.getLogger(__name__)

def array_dim(arr):
    """Return the size of a multidimansional array.
    """
    dim = []
    while True:
        try:
            dim.append(len(arr))
            arr = arr[0]
        except TypeError:
            return dim

def check_array_struct(array):
    """
        Check to ensure arrays are symmetrical, for example:
        [[1, 2, 3], [1, 2]] is invalid
    """

    #If a list is transformed into a numpy array and the sub elements
    #of this array are still lists, then numpy failed to fully convert
    #the list, meaning it is not symmetrical.
    try:
        arr = np.array(array)
    except:
        raise HydraError("Array %s is not valid."%(array,))
    if type(arr[0]) is list:
        raise HydraError("Array %s is not valid."%(array,))


def arr_to_vector(arr):
    """Reshape a multidimensional array to a vector.
    """
    dim = array_dim(arr)
    tmp_arr = []
    for n in range(len(dim) - 1):
        for inner in arr:
            for i in inner:
                tmp_arr.append(i)
        arr = tmp_arr
        tmp_arr = []
    return arr


def vector_to_arr(vec, dim):
    """Reshape a vector to a multidimensional array with dimensions 'dim'.
    """
    if len(dim) <= 1:
        return vec
    array = vec
    while len(dim) > 1:
        i = 0
        outer_array = []
        for m in range(reduce(mul, dim[0:-1])):
            inner_array = []
            for n in range(dim[-1]):
                inner_array.append(array[i])
                i += 1
            outer_array.append(inner_array)
        array = outer_array
        dim = dim[0:-1]

        return array

def _get_val(val, full=False):
    """
        Get the value(s) of a dataset as a single value or as 1-d list of
        values. In the special case of timeseries, when a check is for time-based
        criteria, you can return the entire timeseries.
    """
    try:
        val = val.strip()
    except:
        pass

    logging.debug("%s, type=%s", val, type(val))

    if isinstance(val, float):
        return val

    if isinstance(val, int):
        return val


    if isinstance(val, np.ndarray):
        return list(val)

    try:
        val = float(val)
        return val
    except:
        pass

    try:
        val = int(val)
        return val
    except:
        pass

    if type(val) == pd.DataFrame:

        if full:
            return val

        newval = []
        values = val.values
        for v in values:
            newv = _get_val(v)
            if type(newv) == list:
                newval.extend(newv)
            else:
                newval.append(newv)
        val = newval

    elif type(val) == dict:

        if full:
            return val

        newval = []
        for v in val.values():
            newv = _get_val(v)
            if type(newv) == list:
                newval.extend(newv)
            else:
                newval.append(newv)
        val = newval

    elif type(val) == list or type(val) == np.ndarray:
        newval = []
        for arr_val in val:
            v = _get_val(arr_val)
            newval.append(v)
        val = newval
    return val

def get_restriction_as_dict(restriction_xml):
    """
    turn:
    ::

            <restrictions>
                <restriction>
                    <type>MAXLEN</type>
                    <value>3</value>
                </restriction>
                <restriction>
                    <type>VALUERANGE</type>
                    <value><item>1</item><item>10</item></value>
                </restriction>
            </restrictions>

    into:
    ::

        {
            'MAXLEN' : 3,
            'VALUERANGE' : [1, 10]
        }

    """
    restriction_dict = {}
    if restriction_xml is None:
        return restriction_dict

    if restriction_xml.find('restriction') is not None:
        restrictions = restriction_xml.findall('restriction')
        for restriction in restrictions:
            restriction_type = restriction.find('type').text
            restriction_val  = restriction.find('value')
            val = None
            if restriction_val is not None:
                if restriction_val.text.strip() != "":
                    val = _get_val(restriction_val.text)
                else:
                    items = restriction_val.findall('item')
                    val = []
                    for item in items:
                        val.append(_get_val(item.text))
            restriction_dict[restriction_type] = val
    return restriction_dict


def validate_ENUM(in_value, restriction):
    """
        Test to ensure that the given value is contained in the provided list.
        the value parameter must be either a single value or a 1-dimensional list.
        All the values in this list must satisfy the ENUM
    """
    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_ENUM(subval, restriction)
    else:
        if value not in restriction:
            raise ValidationError("ENUM : %s"%(restriction))

def validate_BOOLYN(in_value, restriction):
    """
        Restriction is not used here. It is just present to be
        in line with all the other validation functions

    """
    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_BOOLYN(subval, restriction)
    else:
        if value not in ('Y', 'N'):
            raise ValidationError("BOOLYN")


def validate_BOOL10(value, restriction):
    value = _get_val(value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_BOOL10(subval, restriction)
    else:
        if value not in (1, 0):
            raise ValidationError("BOOL10")

def validate_NUMPLACES(in_value, restriction):
    """
        the value parameter must be either a single value or a 1-dimensional list.
        All the values in this list must satisfy the condition
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_NUMPLACES(subval, restriction)
    else:
        restriction = int(restriction) # Just in case..
        dec_val = Decimal(str(value))
        num_places = dec_val.as_tuple().exponent * -1 #exponent returns a negative num
        if restriction != num_places:
            raise ValidationError("NUMPLACES: %s"%(restriction))

def validate_VALUERANGE(in_value, restriction):
    """
        Test to ensure that a value sits between a lower and upper bound.
        Parameters: A Decimal value and a tuple, containing a lower and upper bound,
        both as Decimal values.
    """
    if len(restriction) != 2:
        raise ValidationError("Template ERROR: Only two values can be specified in a date range.")
    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_VALUERANGE(subval, restriction)
    else:
        min_val = Decimal(restriction[0])
        max_val = Decimal(restriction[1])
        val     = Decimal(value)
        if val < min_val or val > max_val:
            raise ValidationError("VALUERANGE: %s, %s"%(min_val, max_val))

def validate_DATERANGE(value, restriction):
    """
        Test to ensure that the times in a timeseries fall between a lower and upper bound
        Parameters: A timeseries in the form [(datetime, val), (datetime, val)..]
        and a tuple containing the lower and upper bound as datetime objects.
    """
    if len(restriction) != 2:
        raise ValidationError("Template ERROR: Only two values can be specified in a date range.")

    if type(value) == pd.DataFrame:
        dates = [get_datetime(v) for v in list(value.index)]
    else:
        dates = value

    if type(dates) is list:
        for date in dates:
            validate_DATERANGE(date, restriction)
        return

    min_date = get_datetime(restriction[0])
    max_date = get_datetime(restriction[1])
    if value < min_date or value > max_date:
        raise ValidationError("DATERANGE: %s <%s> %s"%(min_date,value,max_date))

def validate_MAXLEN(value, restriction):
    """
        Test to ensure that a list has the prescribed length.
        Parameters: A list and an integer, which defines the required length of
        the list.
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]
    else:
        return

    if len(value) > restriction:
        raise ValidationError("MAXLEN: %s"%(restriction))

def validate_NOTNULL(value, restriction):
    """
        Restriction is not used here. It is just present to be
        in line with all the other validation functions

    """

    if value is None or str(value).lower == 'null':
        raise ValidationError("NOTNULL")

def validate_ISNULL(value, restriction):
    """
        Restriction is not used here. It is just present to be
        in line with all the other validation functions

    """

    if value is not None and str(value).lower != 'null':
        raise ValidationError("ISNULL")

def validate_EQUALTO(in_value, restriction):
    """
        Test to ensure that a value is equal to a prescribed value.
        Parameter: Two values, which will be compared for equality.
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_EQUALTO(subval, restriction)
    else:
        if value != restriction:
            raise ValidationError("EQUALTO: %s"%(restriction))

def validate_NOTEQUALTO(in_value, restriction):
    """
        Test to ensure that a value is NOT equal to a prescribed value.
        Parameter: Two values, which will be compared for non-equality.
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_NOTEQUALTO(subval, restriction)
    else:
        if value == restriction:
            raise ValidationError("NOTEQUALTO: %s"%(restriction))

def validate_LESSTHAN(in_value, restriction):
    """
        Test to ensure that a value is less than a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_LESSTHAN(subval, restriction)
    else:
        try:
            if value >= restriction:
                raise ValidationError("LESSTHAN: %s"%(restriction))
        except TypeError:
            # Incompatible types for comparison.
            raise ValidationError("LESSTHAN: Incompatible types %s"%(restriction))


def validate_LESSTHANEQ(value, restriction):
    """
        Test to ensure that a value is less than or equal to a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_LESSTHANEQ(subval, restriction)
    else:
        try:
            if value > restriction:
                raise ValidationError("LESSTHANEQ: %s" % (restriction))
        except TypeError:
            # Incompatible types for comparison.
            raise ValidationError("LESSTHANEQ: Incompatible types %s"%(restriction))


def validate_GREATERTHAN(in_value, restriction):
    """
        Test to ensure that a value is greater than a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_GREATERTHAN(subval, restriction)
    else:
        try:
            if value <= restriction:
                raise ValidationError("GREATERTHAN: %s" % (restriction))
        except TypeError:
            # Incompatible types for comparison.
            raise ValidationError("GREATERTHAN: Incompatible types %s"%(restriction))

def validate_GREATERTHANEQ(value, restriction):
    """
        Test to ensure that a value is greater than or equal to a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_GREATERTHANEQ(subval, restriction)
    else:
        try:
            if value < restriction:
                raise ValidationError("GREATERTHANEQ: %s" % (restriction))
        except TypeError:
            # Incompatible types for comparison.
            raise ValidationError("GREATERTHANEQ: Incompatible types %s"%(restriction))

def validate_MULTIPLEOF(in_value, restriction):
    """
        Test to ensure that a value is a multiple of a specified restriction value.
        Parameters: Numeric value and an integer
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(in_value)
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_MULTIPLEOF(subval, restriction)
    else:
        try:
            if value % restriction != 0:
                raise ValidationError("MULTIPLEOF: %s" % (restriction))
        except TypeError:
            # Incompatible types for comparison.
            raise ValidationError("MULTIPLEOF: Incompatible types %s"%(restriction))

def validate_SUMTO(in_value, restriction):
    """
        Test to ensure the values of a list sum to a specified value:
        Parameters: a list of numeric values and a target to which the values
        in the list must sum
    """
    #Sometimes restriction values can accidentally be put in the template <item>100</items>,
    #Making them a list, not a number. Rather than blowing up, just get value 1 from the list.
    if type(restriction) is list:
        restriction = restriction[0]

    value = _get_val(in_value, full=True)

    if len(value) == 0:
        return

    flat_list = _flatten_value(value)

    try:
        sum(flat_list)
    except:
        raise ValidationError("List cannot be summed: %s"%(flat_list,))

    if sum(flat_list) != restriction:
        raise ValidationError("SUMTO: %s"%(restriction))

def validate_INCREASING(in_value, restriction):
    """
        Test to ensure the values in a list are increasing.
        Parameters: a list of values and None. The none is there simply
        to conform with the rest of the validation routines.
    """

    flat_list = _flatten_value(in_value)

    previous = None
    for a in flat_list:
        if previous is None:
            previous = a
            continue
        try:
            if a < previous:
                raise ValidationError("INCREASING")
        except TypeError:
            raise  ValueError("INCREASING: Incompatible types")
        previous = a

def validate_DECREASING(in_value,restriction):
    """
        Test to ensure the values in a list are decreasing.
        Parameters: a list of values and None. The none is there simply
        to conform with the rest of the validation routines.
    """
    flat_list = _flatten_value(in_value)

    previous = None
    for a in flat_list:
        if previous is None:
            previous = a
            continue
        try:
            if a > previous:
                raise ValidationError("DECREASING")
        except TypeError:
            raise  ValueError("DECREASING: Incompatible types")
        previous = a

def validate_EQUALTIMESTEPS(value, restriction):
    """
        Ensure that the timesteps in a timeseries are equal. If a restriction
        is provided, they must be equal to the specified restriction.

        Value is a pandas dataframe.
    """
    if len(value) == 0:
        return

    if type(value) == pd.DataFrame:
        if str(value.index[0]).startswith('9999'):
            tmp_val = value.to_json().replace('9999', '1900')
            value = pd.read_json(tmp_val)


    #If the timeseries is not datetime-based, check for a consistent timestep
    if type(value.index) == pd.Int64Index:
        timesteps = list(value.index)
        timestep = timesteps[1] - timesteps[0]
        for i, t in enumerate(timesteps[1:]):
            if timesteps[i] - timesteps[i-1] != timestep:
                raise ValidationError("Timesteps not equal: %s"%(list(value.index)))


    if not hasattr(value.index, 'inferred_freq'):
        raise ValidationError("Timesteps not equal: %s"%(list(value.index),))

    if restriction is None:
        if value.index.inferred_freq is None:
            raise ValidationError("Timesteps not equal: %s"%(list(value.index),))
    else:
        if value.index.inferred_freq != restriction:
            raise ValidationError("Timesteps not equal: %s"%(list(value.index),))

validation_func_map = dict(
    ENUM = validate_ENUM,
    BOOLYN = validate_BOOLYN,
    BOOL10 = validate_BOOL10,
    NUMPLACES = validate_NUMPLACES,
    VALUERANGE = validate_VALUERANGE,
    DATERANGE = validate_DATERANGE,
    MAXLEN = validate_MAXLEN,
    EQUALTO = validate_EQUALTO,
    NOTEQUALTO = validate_NOTEQUALTO,
    LESSTHAN = validate_LESSTHAN,
    LESSTHANEQ = validate_LESSTHANEQ,
    GREATERTHAN = validate_GREATERTHAN,
    GREATERTHANEQ = validate_GREATERTHANEQ,
    MULTIPLEOF = validate_MULTIPLEOF,
    SUMTO = validate_SUMTO,
    INCREASING = validate_INCREASING,
    DECREASING = validate_DECREASING,
    EQUALTIMESTEPS = validate_EQUALTIMESTEPS,
    NOTNULL        = validate_NOTNULL,
    ISNULL         = validate_ISNULL,
)

def validate_value(restriction_dict, inval):
    if len(restriction_dict) == 0:
        return

    try:
        for restriction_type, restriction in restriction_dict.items():
            func = validation_func_map.get(restriction_type)
            if func is None:
                raise Exception("Validation type %s does not exist"%(restriction_type,))
            func(inval, restriction)
    except ValidationError as e:
        log.exception(e)
        err_val = re.sub('\s+', ' ', str(inval)).strip()
        if len(err_val) > 60:
            err_val = "%s..."%err_val[:60]
        raise HydraError("Validation error (%s). Val %s does not conform with rule %s"%(restriction_type, err_val, e.message))
    except Exception as e:
        log.exception(e)
        raise HydraError("An error occurred in validation. (%s)"%(e))

def _flatten_value(value):
    """
        1: Turn a multi-dimensional array into a 1-dimensional array
        2: Turn a timeseries of values into a single 1-dimensional array
    """

    if type(value) == pd.DataFrame:
        value = value.values.tolist()

    if type(value) != list:
        raise ValidationError("Value %s cannot be processed."%(value))

    if len(value) == 0:
        return

    flat_list = _flatten_list(value)

    return flat_list

def _flatten_list(l):
    flat_list = []
    for item in l:
        if type(item) is list:
            flat_list.extend(_flatten_list(item))
        else:
            flat_list.append(item)
    return flat_list

if __name__ == '__main__':
    pass
