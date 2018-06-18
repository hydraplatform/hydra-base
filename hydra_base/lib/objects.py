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

import json
import six

import logging
log = logging.getLogger(__name__)

from datetime import datetime
from ..exceptions import HydraError

from .HydraTypes.Registry import HydraObjectFactory

from ..util import generate_data_hash, get_layout_as_dict, get_layout_as_string
from .. import config
import pandas as pd

class JSONObject(dict):
    """
        A dictionary object whose attributes can be accesed via a '.'.
        Pass in a nested dictionary, a SQLAlchemy object or a JSON string.
    """
    def __init__(self, obj_dict={}, parent=None, extras={}):

        if isinstance(obj_dict, six.string_types):
            try:
                obj = json.loads(obj_dict)
                assert isinstance(obj, dict), "JSON string does not evaluate to a dict"
            except Exception:
                log.critical(obj_dict)
                log.critical(parent)
                raise ValueError("Unable to read string value. Make sure it's JSON serialisable")
        elif hasattr(obj_dict, '_asdict') and obj_dict._asdict is not None:
            #A special case, trying to load a SQLAlchemy object, which is a 'dict' object
            obj = obj_dict._asdict()
        elif hasattr(obj_dict, '__dict__') and len(obj_dict.__dict__) > 0:
            #A special case, trying to load a SQLAlchemy object, which is a 'dict' object
            obj = obj_dict.__dict__
        elif isinstance(obj_dict, dict):
            obj = obj_dict
        else:
            #last chance...try to cast it as a dict. Do this for sqlalchemy result proxies.
            try:
                obj = dict(obj_dict)
            except:
                log.critical("Error with value: %s" , obj_dict)
                raise ValueError("Unrecognised value. It must be a valid JSON dict, a SQLAlchemy result or a dictionary.")

        for k, v in obj.items():
            if isinstance(v, JSONObject):
                setattr(self, k, v)
            elif k == 'layout':
                #Layout is often valid JSON, but we dont want to treat it as a JSON object necessarily
                dict_layout = get_layout_as_dict(v)
                setattr(self, k, dict_layout)
            elif isinstance(v, dict):
                #TODO what is a better way to identify a dataset?
                if 'unit' in v or 'metadata' in v or 'type' in v:
                    setattr(self, k, Dataset(v, obj_dict))
                else:
                    setattr(self, k, JSONObject(v, obj_dict))
            elif isinstance(v, list):
                #another special case for datasets, to convert a metadata list into a dict
                if k == 'metadata' and obj_dict is not None:
                    setattr(self, k, JSONObject(obj_dict.get_metadata_as_dict()))
                else:
                    l = [JSONObject(item, obj_dict) for item in v]
                    setattr(self, k, l)
            #Special case for SQLAlchemy objects, to stop them recursing up and down
            elif hasattr(v, '_sa_instance_state')\
                    and v._sa_instance_state is not None\
                    and v != parent\
                    and hasattr(obj_dict, '_parents')\
                    and obj_dict._parents is not None\
                    and v.__tablename__ not in obj_dict._parents:
                if v.__tablename__.lower() == 'tdataset':
                    l = Dataset(v, obj_dict)
                else:
                    l = JSONObject(v, obj_dict)
                setattr(self, k, l)
            #Special case for SQLAlchemy objects, to stop them recursing up and down
            elif hasattr(v, '_sa_instance_state')\
                    and v._sa_instance_state is not None\
                    and v != parent\
                    and hasattr(obj_dict, '_parents')\
                    and obj_dict._parents is not None\
                    and v.__tablename__ in obj_dict._parents:
                continue
            else:
                if k == '_sa_instance_state':
                    continue

                if parent is not None and type(v) == type(parent):
                    continue

                try:
                    int(v)
                    if v.find('.'):
                        if int(v.split('.')[0]) == int(v):
                            v = int(v)
                    else:
                        v = int(v)
                except:
                    pass

                try:
                    if not isinstance(v, int):
                        v = float(v)
                except:
                    pass

                if isinstance(v, datetime):
                    v = six.text_type(v)

                setattr(self, six.text_type(k), v)

        for k, v in extras.items():
            setattr(self, k, v)

    def __getattr__(self, name):
        # Make sure that "special" methods are returned as before.

        # Keys that start and end with "__" won't be retrievable via attributes
        if name.startswith('__') and name.endswith('__'):
            return super(JSONObject, self).__getattr__(name)
        else:
            return self.get(name, None)

    def __setattr__(self, key, value):
        self[key] = value

    def as_json(self):

        return json.dumps(self)

    def get_layout(self):
        if self.get('layout') is not None:
            return get_layout_as_string(self.layout)
        else:
            return None

    #Only for type attrs. How best to generalise this?
    def get_properties(self):
        if self.get('properties') and self.get('properties') is not None:
            return six.text_type(self.properties)
        else:
            return None

class ResourceScenario(JSONObject):
    def __init__(self, rs):
        super(ResourceScenario, self).__init__(rs)
        for k, v in rs.items():
            if k == 'dataset':
                setattr(self, k, Dataset(v))


class Dataset(JSONObject):

    def __getattr__(self, name):

        # Keys that start and end with "__" won't be retrievable via attributes
        if name.startswith('__') and name.endswith('__'):
            return super(JSONObject, self).__getattr__(name)

        else:
            return self.get(name, None)

    def __setattr__(self, name, value):
        if name == 'value':
            value = six.text_type(value)
        super(Dataset, self).__setattr__(name, value)

    def parse_value(self):
        """
            Turn the value of an incoming dataset into a hydra-friendly value.
        """
        try:
            if self.value is None:
                log.warn("Cannot parse dataset. No value specified.")
                return None

            # attr_data.value is a dictionary but the keys have namespaces which must be stripped
            data = six.text_type(self.value)

            if data.upper().strip() in ("NULL", ""):
                return "NULL"

            data = data[0:100]
            log.info("[Dataset.parse_value] Parsing %s (%s)", data, type(data))

            return HydraObjectFactory.valueFromDataset(self.type, self.value, self.get_metadata_as_dict())

        except Exception as e:
            log.exception(e)
            raise HydraError("Error parsing value %s: %s"%(self.value, e))


    def get_metadata_as_dict(self, user_id=None, source=None):
        """
        Convert a metadata json string into a dictionary.

        Args:
            user_id (int): Optional: Insert user_id into the metadata if specified
            source (string): Optional: Insert source (the name of the app typically) into the metadata if necessary.

        Returns:
            dict: THe metadata as a python dictionary
        """

        if self.metadata is None or self.metadata == "":
            return {}

        metadata_dict = self.metadata if isinstance(self.metadata, dict) else json.loads(self.metadata)

        # These should be set on all datasets by default, but we don't enforce this rigidly
        metadata_keys = [m.lower() for m in metadata_dict]
        if user_id is not None and 'user_id' not in metadata_keys:
            metadata_dict['user_id'] = six.text_type(user_id)

        if source is not None and 'source' not in metadata_keys:
            metadata_dict['source'] = six.text_type(source)

        return { k : six.text_type(v) for k, v in metadata_dict.items() }


    def get_hash(self, val, metadata):

        if metadata is None:
            metadata = self.get_metadata_as_dict()

        if val is None:
            value = self.parse_value()
        else:
            value = val

        dataset_dict = {'name'     : self.name,
                        'unit'     : self.unit,
                        'type'     : self.type.lower(),
                        'value'    : value,
                        'metadata' : metadata,}

        data_hash = generate_data_hash(dataset_dict)

        return data_hash
