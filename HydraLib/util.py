# (c) Copyright 2013, 2014, University of Manchester
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
from HydraException import HydraError
import numpy as np
import pandas as pd
from dateutil import get_datetime
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

def create_dict(arr):
    return {'array': [create_sub_dict(arr)]}

def create_sub_dict(arr):
    if arr is None:
        return None 

    #Either the array contains sub-arrays or values
    vals = None
    sub_arrays = []
    for sub_val in arr:
        if type(sub_val) is list:
            sub_dict = create_sub_dict(sub_val)
            sub_arrays.append(sub_dict)
        else:
            #if any of the elements of the array is NOT a list,
            #then there are no sub arrays
            vals = arr 
            break

    if vals:
        return {'item': vals}

    if sub_arrays:
        return {'array': sub_arrays}

def parse_array(arr):
    """
        Take a dictionary and turn it into an array as follows:
        ::

         {'array': ['item' : [1, 2, 3]}]} -> [1, 2, 3]
        
        Or for a more complex array:
        ::

         {'array' :[
             {'array': [ 'item' : [1, 2, 3]} ]}
             {'array': [ 'item' : [1, 2, 3]} ]} 
         ]} -> [[1, 2, 3], [4, 5, 6]]
    """
    ret_arr = []
    if arr.get('array'):
        sub_arr = arr['array']
        if len(sub_arr) > 1:
            for s in sub_arr:
                ret_arr.append(parse_array(s))
        else:
            return parse_array(sub_arr[0])
    elif arr.get('item'):
        for x in arr['item']:
            try:
                val = float(x)
            except:
                val = str(x)
            ret_arr.append(val)
        return ret_arr
    else:
        raise ValueError("Something has gone wrong parsing an array.")
    return ret_arr

def _get_val(val):
    try:
        val = val.strip()
    except:
        pass
    
    try: 
        val = int(val)
        return val
    except:
        pass

    try:
        val = float(val)
        return val
    except:
        pass

  #  try:
  #      val = get_datetime(val)
  #  except:
  #      pass

    if type(val) == dict:
        newval = []
        if val.get('ts_values'):
            for ts_val in val['ts_values']:
                t = get_datetime(ts_val['ts_time'])
                v = _get_val(ts_val['ts_value'])
                newval.append((t, v))
        if val.get('arr_data'):
            arr = val['arr_data']
            for arr_val in arr:
                v = _get_val(arr_val)
                newval.append(v)
        val = newval

    elif type(val) == list:
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
                    <value><item>3</item></value>
                </restriction>
                <restriction>
                    <type>SUMTO</type>
                    <value><item>4</item></value>
                </restriction> 
            </restrictions>

    into:
    ::

        {
            'MAXLEN' : 3,
            'SUMTO' : 4
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

class ValidationError(Exception):
    pass


def validate_ENUM(value, restriction):
    """
        Test to ensure that the given value is contained in the provided list.
        the value parameter must be either a single value or a 1-dimensional list.
        All the values in this list must satisfy the ENUM
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_ENUM(subval, restriction)
    else:
        if value not in restriction:
            raise ValidationError("ENUM : %s"%(restriction))

def validate_BOOLYN(value, restriction):
    """
        Restriction is not used here. It is just present to be
        in line with all the other validation functions

    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_BOOLYN(subval, restriction)

    if value not in ('Y', 'N'):
        raise ValidationError("BOOLYN")


def validate_BOOL10(value, restriction):
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_BOOL10(subval, restriction)

    if value not in (1, 0):
        raise ValidationError("BOOL10")

def validate_NUMPLACES(value, restriction):
    """
        the value parameter must be either a single value or a 1-dimensional list.
        All the values in this list must satisfy the condition
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_NUMPLACES(subval, restriction)

    restriction = int(restriction) # Just in case..
    dec_val = Decimal(value)
    num_places = dec_val.as_tuple().exponent * -1 #exponent returns a negative num
    if restriction != num_places:
        raise ValidationError("NUMPLACES: %s"%(num_places))

def validate_VALUERANGE(value, restriction):
    """
        Test to ensure that a value sits between a lower and upper bound.
        Parameters: A Decimal value and a tuple, containing a lower and upper bound,
        both as Decimal values.
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_VALUERANGE(subval, restriction)

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
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                ts_time = subval[0]
            elif type(subval) is dict:
                ts_time = subval['ts_time']
            validate_DATERANGE(ts_time, restriction)
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
    if len(value) > restriction:
        raise ValidationError("MAXLEN: %s"%(restriction))

def validate_EQUALTO(value, restriction):
    """
        Test to ensure that a value is equal to a prescribed value.
        Parameter: Two values, which will be compared for equality.
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_EQUALTO(subval, restriction)

    if value != restriction:
        raise ValidationError("EQUALTO: %s"%(restriction))

def validate_NOTEQUALTO(value, restriction):
    """
        Test to ensure that a value is NOT equal to a prescribed value.
        Parameter: Two values, which will be compared for non-equality.
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_NOTEQUALTO(subval, restriction)

    if value == restriction:
        raise ValidationError("NOTEQUALTO: %s"%(restriction))

def validate_LESSTHAN(value, restriction):
    """
        Test to ensure that a value is less than a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_LESSTHAN(subval, restriction)

    if value >= restriction:
        raise ValidationError("LESSTHAN: %s"%(restriction))


def validate_LESSTHANEQ(value, restriction):
    """
        Test to ensure that a value is less than or equal to a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_LESSTHANEQ(subval, restriction)

    if value > restriction:
        raise ValidationError("LESSTHANEQ: %s"%(restriction))

def validate_GREATERTHAN(value, restriction):
    """
        Test to ensure that a value is greater than a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_GREATERTHAN(subval, restriction)

    if value <= restriction:
        raise ValidationError("GREATERTHAN: %s"%(restriction))

def validate_GREATERTHANEQ(value, restriction):
    """
        Test to ensure that a value is greater than or equal to a prescribed value.
        Parameter: Two values, which will be compared for the difference..
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_GREATERTHANEQ(subval, restriction)

    if value < restriction:
        raise ValidationError("GREATERTHANEQ: %s"%(restriction))

def validate_MULTIPLEOF(value, restriction):
    """
        Test to ensure that a value is a multiple of a specified restriction value.
        Parameters: Numeric value and an integer
    """
    if type(value) is list:
        for subval in value:
            if type(subval) is tuple:
                subval = subval[1]
            validate_MULTIPLEOF(subval, restriction)

    if value % restriction != 0:
        raise ValidationError("MULTIPLEOF: %s"%(restriction))

def validate_SUMTO(value, restriction):
    """
        Test to ensure the values of a list sum to a specified value:
        Parameters: a list of numeric values and a target to which the values
        in the list must sum
    """
    if len(value) == 0:
        return
    if type(value) != list:
        raise ValidationError("Value %s cannot be summed."%(value))
   
    flat_list = _flatten_value(value)
    
    try:
        sum(flat_list)
    except:
        raise ValidationError("List cannot be summed: %s"%(flat_list,))

    if sum(flat_list) != restriction:
        raise ValidationError("SUMTO: %s"%(restriction))

def validate_INCREASING(value,restriction):
    """
        Test to ensure the values in a list are increasing.
        Parameters: a list of values and None. The none is there simply
        to conform with the rest of the validation routines.
    """

    flat_list = _flatten_value(value)

    previous = None
    for a in flat_list:
        if previous is None:
            previous = a
            continue
        if a < previous:
            raise ValidationError("INCREASING")
        previous = a

def validate_DECREASING(value,restriction):
    """
        Test to ensure the values in a list are decreasing.
        Parameters: a list of values and None. The none is there simply
        to conform with the rest of the validation routines.
    """
    flat_list = _flatten_value(value)

    previous = None
    for a in flat_list:
        if previous is None:
            previous = a
            continue
        if a > previous:
            raise ValidationError("INCREASING")
        previous = a

def validate_EQUALTIMESTEPS(value, restriction):
    """
        Test to ensure the values of a list sum to a specified value:
        Parameters: a list of numeric values and a target to which the values
        in the list must sum
    """
    if len(value) == 0:
        return

    if type(value) != list:
        raise ValidationError("Value %s cannot be validated. It is not a list."%(value))

    ts_val_keys = []
    ts_vals     = []
    for ts_instance in value:
        if type(ts_instance) is tuple:
            ts_val_keys.append(ts_instance[0])
            ts_vals.append(ts_instance[1])
        else:
            ts_val_keys.append(ts_instance['ts_time'])
            ts_vals.append(ts_instance['ts_value'])

    test_pd = pd.DataFrame(ts_vals, index=pd.Series(ts_val_keys))
    
    if not hasattr(test_pd.index, 'inferred_freq'):
        raise ValidationError("Timesteps not equal: %s"%(ts_val_keys,))

    if restriction is None:
        if test_pd.index.inferred_freq is None:
            raise ValidationError("Timesteps not equal: %s"%(ts_val_keys,))
    else:
        if test_pd.index.inferred_freq != restriction:
            raise ValidationError("Timesteps not equal: %s"%(ts_val_keys,))

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
)

def validate_value(restriction_dict, inval):
    if len(restriction_dict) == 0:
        return

    val = _get_val(inval)
    #log.warn("%s -> %s", inval, val)
    try:
        for restriction_type, restriction in restriction_dict.items():
            func = validation_func_map.get(restriction_type)
            if func is None:
                raise Exception("Validation type %s does not exist"%(restriction_type,))
            func(val, restriction)
    except ValidationError, e:
        if len(str(inval)) > 100:
            val = "%s..."%str(inval)[:100]
        raise HydraError("Validation error. Val %s does not conform with rule %s" 
                         %(val, e.message))
    except Exception, e:
        raise HydraError("An error occurred in validation. (%s)"%(e))

def _flatten_value(value):
    """
        1: Turn a multi-dimensional array into a 1-dimensional array
        2: Turn a timeseries of values into a single 1-dimensional array
    """

    if type(value) != list:
        raise ValidationError("Value %s cannot be processed."%(value))
    
    if len(value) == 0:
        return

    if type(value[0]) == tuple:
        flat_list = _flatten_timeseries(value)
    else:
        flat_list = _flatten_list(value)

    return flat_list

def _flatten_timeseries(value):
    flat_list = []
    for timestep in value:
        val = timestep[1]
        if type(val) is list:
            flat_sub_list = _flatten_list(val)
            flat_list.extend(flat_sub_list)
        else:
            flat_list.append(val)

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
