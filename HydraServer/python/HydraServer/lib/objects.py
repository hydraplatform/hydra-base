
import json

import logging
log = logging.getLogger(__name__)

from datetime import datetime

from HydraLib.HydraException import HydraError

from HydraServer.util import generate_data_hash
from HydraLib import config
import zlib
import pandas as pd

class JSONObject(dict):
    """
        A dictionary object whose attributes can be accesed via a '.'.
        Pass in a nested dictionary, a SQLAlchemy object or a JSON string. 
    """
    def __init__(self, obj_dict, parent=None):
        if isinstance(obj_dict, str) or isinstance(obj_dict, unicode):
            try:
                obj = json.loads(obj_dict)
                assert isinstance(obj, dict), "JSON string does not evaluate to a dict"
            except Exception:
                raise ValueError("Unable to read string value. Make sure it's JSON serialisable")
        elif hasattr(obj_dict, '__dict__'):
            #A special case, trying to load a SQLAlchemy object, which is a 'dict' object
            obj = obj_dict.__dict__
        elif isinstance(obj_dict, dict):
            obj = obj_dict
        else:
            log.critical("Error with value: %s" , obj_dict)
            raise ValueError("Unrecognised value. It must be a valid JSON dict, a SQLAlchemy result or a dictionary.")

        for k, v in obj.items():
            if isinstance(v, dict):
                setattr(self, k, JSONObject(v, obj_dict))
            elif isinstance(v, list):
                #another special case for datasets, to convert a metadata list into a dict
                if k == 'metadata':
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
                    v = int(v)
                except:
                    pass

                try:
                    v = float(v)
                except:
                    pass

                if k == 'layout' and v is not None:
                    v = JSONObject(v)

                if isinstance(v, datetime):
                    v = str(v)

                setattr(self, k, v)

    def __getattr__(self, name):
        return self.get(name, None)

    def __setattr__(self, name, value):
        super(JSONObject, self).__setattr__(name, value)
        self[name] = value

    def as_json(self):

        return json.dumps(self)

    def get_layout(self):
        return None
        if hasattr(self, 'layout'):
            return self.layout
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

            if len(data) > 100:
                log.debug("Parsing %s", data[0:100])
            else:
                log.debug("Parsing %s", data)

            if self.type == 'descriptor':
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
        except Exception, e:
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

        if type(self.metadata) == dict:
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
