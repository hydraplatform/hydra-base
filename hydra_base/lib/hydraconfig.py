"""
  Library functions for Hydra configuration
"""

from hydra_base import db
from hydra_base.exceptions import (
    HydraError,
    ResourceNotFoundError,
    PermissionError
)

from hydra_base.db.model.hydraconfig import (
    ConfigKey,
    config_key_type_map
)


""" Config Keys: Key:Value pairs of config settings """

def register_config_key(key_name, key_type, **kwargs):
    if not (key_cls := config_key_type_map.get(key_type, None)):
        raise HydraError(f"Invalid ConfigKey type '{key_type}'")

    key = key_cls(name=key_name)
    db.DBSession.add(key)
    db.DBSession.flush()

    return key

def unregister_config_key(key):
    pass

def list_config_keys(like=None, **kwargs):
    query = db.DBSession.query(ConfigKey)
    if like:
        query = query.filter(ConfigKey.name.like(f"%{like}%"))

    keys = query.all()
    return [key.name for key in keys]

def set_config_value(key, value):
    pass

def get_config_value(key):
    pass


""" Config Sets: Archived versions of complete configurations """

def create_configset(set_name):
    pass

def delete_configset(set_name):
    pass

def apply_configset(set_name):
    pass

def list_configsets():
    pass

def list_configset_versions(set_name):
    pass


""" Config Groups: A named collection of Config Keys """

def create_config_group(group_name):
    pass

def delete_config_group(group_name):
    pass

def list_config_groups():
    pass

def add_config_key_to_group(key_name, group_name):
    pass

def remove_config_key_from_group(key_name, group_name):
    pass
