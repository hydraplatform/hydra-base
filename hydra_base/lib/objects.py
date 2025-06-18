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
import enum
import logging
import six

from bson.objectid import ObjectId
from bson.errors import InvalidId
from datetime import datetime

from hydra_base.lib.storage import MongoStorageAdapter

from ..exceptions import HydraError
from ..util import (
    generate_data_hash,
    get_json_as_dict,
    get_json_as_string
)

from .HydraTypes.Registry import HydraObjectFactory


log = logging.getLogger(__name__)
mongo = MongoStorageAdapter()
VALID_JSON_FIRST_CHARS = ['{', '[']


class JSONObject(dict):
    """
        A dictionary object whose attributes can be accesed via a '.'.
        Pass in a nested dictionary, a SQLAlchemy object or a JSON string.
    """
    def __init__(self, obj_dict={}, parent=None, extras={}, normalize=True):
        
        if normalize:
            obj = self.normalise_input(obj_dict)
        else:
            obj = obj_dict

        for k in obj:
            v = obj[k]
            if k == "value_ref":
                continue

            if isinstance(v, JSONObject):
                self[k] = v
            elif k == 'layout':
                #Layout is often valid JSON, but we dont want to treat it as a JSON object necessarily
                dict_layout = get_json_as_dict(v)
                self[k] = dict_layout
            elif isinstance(v, dict):
                #TODO what is a better way to identify a dataset?
                if 'unit_id' in v or 'unit' in v or 'metadata' in v or 'type' in v:
                    self[k] = Dataset(v, obj_dict)
                #The value on a dataset should remain untouched
                elif k == 'value':
                    self[k] = v
                else:
                    self[k] = JSONObject(v, obj_dict, normalize=normalize)
            elif isinstance(v, list):
                #another special case for datasets, to convert a metadata list into a dict
                if k == 'metadata' and obj_dict is not None:
                    if hasattr(obj_dict, 'get_metadata_as_dict'):
                        self[k] = JSONObject(obj_dict.get_metadata_as_dict())
                    else:
                        metadata_dict = JSONObject()
                        if hasattr(obj_dict, 'get'):#special case for resource data and row proxies
                            for m in obj_dict.get('metadata', []):
                                metadata_dict[m.key] = m.value
                        self[k] = metadata_dict

                else:
                    is_list_of_objects = True
                    if len(v) > 0:
                        if isinstance(v[0], (float, int)):
                            is_list_of_objects = False
                        elif isinstance(v[0], six.string_types) and len(v[0]) == 0:
                            is_list_of_objects = False
                        elif isinstance(v[0], six.string_types) and v[0][0] not in VALID_JSON_FIRST_CHARS:
                            is_list_of_objects=False

                    if is_list_of_objects is True:
                        l = [JSONObject(item, obj_dict) for item in v]
                    else:
                        l = v

                    self[k] = l
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
                self[k] = l
            #Special case for SQLAlchemy objects, to stop them recursing up and down
            elif hasattr(v, '_sa_instance_state')\
                    and v._sa_instance_state is not None\
                    and v != parent\
                    and hasattr(obj_dict, '_parents')\
                    and obj_dict._parents is not None\
                    and v.__tablename__ in obj_dict._parents:
                continue
            elif isinstance(v, enum.Enum):
                self[k] = v.value
            else:

                if k == '_sa_instance_state':
                    continue

                if parent is not None and type(v) == type(parent):
                    continue

                if isinstance(v, str) and v.replace('.', '', 1).isdigit():
                    v = float(v) if '.' in v else int(v)

                try:
                    if not isinstance(v, int):
                        v = float(v)
                except:
                    pass

                if isinstance(v, datetime):
                    v = six.text_type(v)

                self[six.text_type(k)] = v

        for k, v in extras.items():
            self[k] = v

    def normalise_input(self, obj_dict):
        """
            Pre-process the input dict to ensure that it is compatible with a JSONObject
        """
        asdict_fn = getattr(obj_dict, "asdict", None)
        if asdict_fn is not None and callable(asdict_fn):
            rd = obj_dict.asdict()
            for k, v in rd.items():
                self[k] = v

        if isinstance(obj_dict, str):
            try:
                obj = json.loads(obj_dict)
                assert isinstance(obj, dict), "JSON string does not evaluate to a dict"
            except (AssertionError, json.decoder.JSONDecodeError) as e:
                log.critical("Error with value: %s" , obj_dict)
                log.critical(parent)
                raise ValueError("Unable to read string value. Make sure it's JSON serialisable") from e
        elif hasattr(obj_dict, '_asdict') and obj_dict._asdict is not None:
            """
            The argument is a SQLAlchemy object. This originated from a
            Class.column query so there was no instance to trigger the
            value descriptor's __get__ and the external lookup must be
            performed here.
            """
            obj = obj_dict._asdict()
            if obj.get("value") is not None:
                try:
                    """
                    ref_key may be not None but also not a valid oid string, so
                    must handle InvalidId from ObjectId and possible TypeError
                    if oid inst is created but then matches no document.
                    """
                    oid = ObjectId(obj["value"])
                    doc = mongo.get_document_by_oid_inst(oid)
                    obj["value"] = doc["value"]
                except (TypeError, InvalidId):
                    """ The value wasn't an valid ObjectID, keep the current value """
                    pass
        elif hasattr(obj_dict, '__dict__') and len(obj_dict.__dict__) > 0:
            obj = obj_dict.__dict__
            """
            Handle indirect references.
            The sqlalchemy attr "value_ref" is in the instance __dict__
            but the "value" descriptor class attr is not.
            The "value_ref" must remain present in the __dict__ for
            later external db lookup, but should not be present in the
            returned object whereas the "value" should.
            """
            if "value_ref" in obj:
                if obj_dict.value:
                    obj["value"] = obj_dict.value
        elif isinstance(obj_dict, dict):
            """
            The argument is a dict of uncertain provenance. This can
            originate from SQLAlchemy row._asdict() so must be
            handled similarly.
            """
            obj = obj_dict
            ref_key = obj.get("value")
            if ref_key:
                try:
                    oid = ObjectId(ref_key)
                    doc = mongo.get_document_by_oid_inst(oid)
                    obj["value"] = doc["value"]
                except (TypeError, InvalidId):
                    """ The value wasn't an valid ObjectID, keep the current value """
                    pass
        else:
            #last chance...try to cast it as a dict. Do this for sqlalchemy result proxies.
            try:
                obj = dict(obj_dict)
            except:
                log.critical("Error with value: %s" , obj_dict)
                raise ValueError("Unrecognised value. It must be a valid JSON dict, a SQLAlchemy result or a dictionary.")
        return obj

    def __getattr__(self, name):
        # Make sure that "special" methods are returned as before.

        # Keys that start and end with "__" won't be retrievable via attributes
        if name == '__table__':#special case for SQLAlchemy objects
            return self.get('__table__')
        elif name.startswith('__') and name.endswith('__'):
            return super(JSONObject, self).__getattr__(name)
        else:
            return self.get(name, None)

    def __setattr__(self, key, value):
        self[key] = value

    def as_json(self):

        return json.dumps(self)

    def get_layout(self):
        """
            Return the 'layout' attribute as a json string
            this is a shorcut for backward compatibility.
            calls the `get_json("layout")` function internally

        """
        return self.get_json('layout')

    def get_json(self, key):
        """
            General function to take an attribute of the object, such as
            layout or app data, which is expected to be in JSON format, and
            return it as a JSON blob,

        """
        if self.get(key) is not None:
            return get_json_as_string(self[key])
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
                self[k] = Dataset(v)


class Dataset(JSONObject):

    def __init__(self, dataset={}, parent=None, extras={}):

        super(Dataset, self).__init__(dataset, parent=parent, extras=extras)


    def __getattr__(self, name):

        # Keys that start and end with "__" won't be retrievable via attributes
        if name.startswith('__') and name.endswith('__'):
            return super(JSONObject, self).__getattr__(name)
        else:
            return self.get(name, None)

    def __setattr__(self, name, value):
        if name == 'value' and value is not None:
            value = six.text_type(value)
        super(Dataset, self).__setattr__(name, value)

    def get_value(self):
        """
            This function is here to match the equivalent one on the tDataset class in model.py
            so that the get_value function can be used without throwing an exception
        """
        return self.value

    def parse_value(self):
        """
            Turn the value of an incoming dataset into a hydra-friendly value.
        """
        try:
            if self.value is None:
                log.warning("Cannot parse dataset. No value specified.")
                return None

            # attr_data.value is a dictionary but the keys have namespaces which must be stripped
            data = six.text_type(self.value)

            if data.upper().strip() in ("NULL", ""):
                return "NULL"

            data = data[0:100]
            log.debug("[Dataset.parse_value] Parsing %s (%s)", data, type(data))

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
        metadata_keys = sorted([m for m in metadata_dict], key=lambda x:x.lower())

        return { k : str(metadata_dict[k]) for k in metadata_keys }


    def get_hash(self, val, metadata):

        if metadata is None:
            metadata = self.get_metadata_as_dict()

        if val is None:
            value = self.parse_value()
        else:
            value = val

        dataset_dict = {'unit_id'  : self.unit_id,
                        'type'     : self.type,
                        'value'    : value,
                        'metadata' : metadata}

        data_hash = generate_data_hash(dataset_dict)

        return data_hash
