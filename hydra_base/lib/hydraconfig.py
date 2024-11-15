"""
  Library functions for Hydra configuration
"""

""" Config Keys: Key:Value pairs of config settings """

def register_config_key(key):
    pass

def unregister_config_key(key):
    pass

def list_config_keys():
    pass

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
