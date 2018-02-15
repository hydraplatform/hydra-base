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

import logging
log = logging.getLogger(__name__)

from datetime import datetime
from builtins import str  # treat unicode as str in Python 2
from ..exceptions import HydraError

from ..util import generate_data_hash
from .. import config
import zlib
import pandas as pd

class JSONObject(dict):
    """
        A dictionary object whose attributes can be accesed via a '.'.
        Pass in a nested dictionary, a SQLAlchemy object or a JSON string.
    """
    def __init__(self, obj_dict={}, parent=None):

        if isinstance(obj_dict, str):
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
                setattr(self, k, v)
            elif isinstance(v, dict):
                #TODO what is a better way to identify a dataset?
                if 'data_units' in v:
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
            elif hasattr(v, '_sa_instance_state') and v != parent: #Special case for SQLAlchemy obhjects

                l = JSONObject(v)
                setattr(self, k, l)
            else:
                if k == '_sa_instance_state':# or hasattr(v, '_sa_instance_state'): #Special case for SQLAlchemy obhjects
                    continue
                if type(v) == type(parent):
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

                if k == 'layout':
                    if v is not None:
                        #Check if the layout is valid json or evaluates to a dict.
                        try:
                            v = json.loads(v)
                        except:
                            try:
                                v = eval(v)
                            except:
                                continue
                        #We're only interested in dicts
                        if not isinstance(v, dict):
                            continue
                        v = JSONObject(v)
                    else:
                        v = {}

                if isinstance(v, datetime):
                    v = str(v)

                #Put in 'id' where a DB table has mytable_id, for example
                #Turn something like 'tProject.project_id' into 'tProject.id'
                if hasattr(obj_dict, '__tablename__') and obj_dict.__tablename__ is not None:
                    #compare with tResourceAttr => resourceattr
                    formattedname = obj_dict.__tablename__.lower()[1:]
                    if k.find('_id') >= 0:
                        formattedk = k.replace('_', '')
                        #turn resource_attr_id => resourceattr
                        if formattedk[0:-2] == formattedname:
                            setattr(self, 'id', v)

                    if k.find('_name') >= 0:
                        setattr(self, 'name', v)

                    if k.find('_description') >= 0:
                        setattr(self, 'description', v)


                setattr(self, str(k), v)

    def __getattr__(self, name):
        # Make sure that "special" methods are returned as before.

        # Keys that start and end with "__" won't be retrievable via attributes
        if name.startswith('__') and name.endswith('__'):
            return super(JSONObject, self).__getattr__(name)
        else:
            return self.get(name, None)

    def __setattr__(self, name, value):
        super(JSONObject, self).__setattr__(name, value)
        self[name] = value

    def as_json(self):

        return json.dumps(self)

    def get_layout(self):
        if self.get('layout') is not None:
            if isinstance(self.layout, str):
                return self.layout
            else:
                return json.dumps(self.layout)
        else:
            return None

    #Only for type attrs. How best to generalise this?
    def get_properties(self):
        if self.get('properties') and self.get('properties') is not None:
            return str(self.properties)
        else:
            return None

class ResourceScenario(JSONObject):
    def __init__(self, rs):
        super(ResourceScenario, self).__init__(rs)
        for k, v in rs.items():
            if k == 'value':
                setattr(self, k, Dataset(v))


class Dataset(JSONObject):
    def parse_value(self):
        """
            Turn the value of an incoming dataset into a hydra-friendly value.
        """
        try:
            #attr_data.value is a dictionary,
            #but the keys have namespaces which must be stripped.


            if self.value is None:
                log.warn("Cannot parse dataset. No value specified.")
                return None

            data = str(self.value)

            if data.upper().strip() == 'NULL':
                return 'NULL'

            if data.strip() == '':
                return "NULL"

            if len(data) > 100:
                log.debug("Parsing %s", data[0:100])
            else:
                log.debug("Parsing %s", data)

            if self.type == 'descriptor':
                #Hack to work with hashtables. REMOVE AFTER DEMO
                if self.get_metadata_as_dict().get('data_type') == 'hashtable':
                    try:
                        df = pd.read_json(data)
                        data = df.transpose().to_json() 
                    except Exception:
                        noindexdata = json.loads(data)
                        indexeddata = {0:noindexdata}
                        data = json.dumps(indexeddata)
                return data
            elif self.type == 'scalar':
                return data
            elif self.type == 'timeseries':
                timeseries_pd = pd.read_json(data)
                #Epoch doesn't work here because dates before 1970 are not
                # supported in read_json. Ridiculous.
                ts = timeseries_pd.to_json(date_format='iso', date_unit='ns')
                if len(data) > int(config.get('db', 'compression_threshold', 1000)):
                    return zlib.compress(ts)
                else:
                    return ts
            elif self.type == 'array':
                #check to make sure this is valid json
                json.loads(data)
                if len(data) > int(config.get('db', 'compression_threshold', 1000)):
                    return zlib.compress(data)
                else:
                    return data
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

        metadata_dict = {}

        if isinstance(self.metadata, dict):
            metadata_dict = self.metadata
        else:
            metadata_dict = json.loads(self.metadata)

        #These should be set on all datasests by default, but we don't
        #want to enforce this rigidly
        metadata_keys = [m.lower() for m in metadata_dict]
        if user_id is not None and 'user_id' not in metadata_keys:
            metadata_dict['user_id'] = str(user_id)

        if source is not None and 'source' not in metadata_keys:
            metadata_dict['source'] = str(source)

        for k, v in metadata_dict.items():
            metadata_dict[k] = str(v)

        return metadata_dict

    def get_hash(self, val, metadata):

        if metadata is None:
            metadata = self.get_metadata_as_dict()

        if val is None:
            value = self.parse_value()
        else:
            value = val

        dataset_dict = {'data_name' : self.name,
                    'data_units': self.unit,
                    'data_dimen': self.dimension,
                    'data_type' : self.type.lower(),
                    'value'     : value,
                    'metadata'  : metadata,}

        data_hash = generate_data_hash(dataset_dict)

        return data_hash
