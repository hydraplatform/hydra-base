"""
  Utilities to migrate from Hydra.ini config format
  to DB-based config table via hydra_base.lib.hydraconfig
"""
import configparser
import os
import transaction

from hydra_base import db
from hydra_base.lib.hydraconfig import (
    register_config_key,
    unregister_config_key,
    list_config_keys,
    config_key_set_value,
    config_key_get_value,
    export_config_as_json
)


if not db.DBSession:
    db.connect()


def ini_to_configset(ini_filename):
    db_config_schema = make_db_config_schema(ini_filename)
    return db_config_schema

def make_db_config_schema(ini_filename):
    exclude_sections = ("mysqld",)
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(ini_filename)

    # Values for "home_dir" and "hydra_base_dir" must be
    # set to allow for interpolation into later values
    home_dir = os.environ.get("HYDRA_HOME_DIR", '~')
    hydra_base_dir = os.environ.get("HYDRA_BASE_DIR", os.getcwd())
    #config.set("DEFAULT", "home_dir", os.path.expanduser(home_dir))
    #config.set("DEFAULT", "hydra_base_dir", os.path.expanduser(hydra_base_dir))

    db_config_schema = {}
    for key in config["DEFAULT"]:
            try:
                value = config["DEFAULT"].get(key, raw=True)
            except configparser.InterpolationSyntaxError:
                value = config["DEFAULT"].get(key, raw=True)
            try:
                value = int(value, 10)
                key_type = "integer"
            except ValueError:
                key_type = "string"

            if key in db_config_schema:
                raise ValueError(f"Duplicate key: {key}")

            db_config_schema[key] = {
                "type": key_type,
                "value": value
            }

    for section in config.sections():
        if section in exclude_sections:
            continue
        for key in config._sections[section].keys():
            try:
                #value = config[section][key]
                value = config[section].get(key, raw=True)
            except configparser.InterpolationSyntaxError:
                value = config[section].get(key, raw=True)
            key_name = f"{section}_{key}"
            try:
                value = int(value, 10)
                key_type = "integer"
            except ValueError:
                key_type = "string"

            if key_name in db_config_schema:
                raise ValueError(f"Duplicate key: {key_name}")

            db_config_schema[key_name] = {
                "type": key_type,
                "value": value
            }

    return db_config_schema


def make_config_from_schema(schema):
    for name, key in schema.items():
        register_config_key(name, key["type"])
        config_key_set_value(name, key["value"])

    transaction.commit()


def get_all_config_keys():
    keys = list_config_keys()
    return {k: config_key_get_value(k) for k in keys}


def delete_all_config_keys():
    keys = list_config_keys()
    for key in keys:
        unregister_config_key(key)

    transaction.commit()
