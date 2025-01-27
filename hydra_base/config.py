#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import glob
import os
import re
import sys

from hydra_base import db


PYTHONVERSION = sys.version_info
if PYTHONVERSION >= (3,2):
    import configparser as ConfigParser
else:
    import ConfigParser
import logging

global CONFIG
CONFIG = None

global localfiles
global localfile
global repofile
global repofiles
global userfile
global userfiles
global sysfile
global sysfiles

def load_config():
    """Load a config file. This function looks for a config (*.ini) file in the
    following order::

        (1) ./*.ini
        (2) ~/.config/hydra/
        (3) /etc/hydra
        (4) /path/to/hydra_base/*.ini

    (1) will override (2) will override (3) will override (4). Parameters not
    defined in (1) will be taken from (2). Parameters not defined in (2) will
    be taken from (3).  (3) is the config folder that will be checked out from
    the svn repository.  (2) Will be be provided as soon as an installable
    distribution is available. (1) will usually be written individually by
    every user."""
    global localfiles
    global localfile
    global repofile
    global repofiles
    global userfile
    global userfiles
    global sysfile
    global sysfiles
    global CONFIG
    logging.basicConfig(level='INFO')

    from hydra_base.lib.hydraconfig import (
        apply_configset,
        list_config_keys,
        config_key_get_value,
        config_key_set_value,
        register_config_key
    )
    from pprint import pprint

    modulepath = os.path.dirname(os.path.abspath(__file__))
    home_dir = os.environ.get("HYDRA_HOME_DIR", '~')
    hydra_base_dir = os.environ.get("HYDRA_BASE_DIR", modulepath)
    configset = os.environ.get("HYDRA_CONFIGSET", "default_configset.json")


    if not db.DBSession:
        db.connect()
    keys = list_config_keys()
    if len(keys) == 0:
        # No existing configset has been loaded
        # Load set specified by env or default
        # and register substitution keys
        with open(configset, 'r') as fp:
            cs_json = fp.read()
        apply_configset(cs_json)

        try:
            register_config_key("home_dir", "string")
            config_key_set_value("home_dir", home_dir)
        except Exception:
            pass

        try:
            register_config_key("hydra_base_dir", "string")
            config_key_set_value("hydra_base_dir", hydra_base_dir)
        except Exception:
            pass

    CONFIG = True
    #db.DBSession = None
    #keys = list_config_keys()
    #kp = {k: config_key_get_value(k) for k in keys}
    #pprint(kp)

    return


def read_env_db_config():
    return {
      "hydra_db_server": os.environ.get("HYDRA_DB_SERVER"),
      "hydra_db_name": os.environ.get("HYDRA_DB_NAME"),
      "hydra_db_user": os.environ.get("HYDRA_DB_USER"),
      "hydra_db_passwd": os.environ.get("HYDRA_DB_PASSWD"),
      "hydra_db_autocreate": os.environ.get("HYDRA_DB_AUTOCREATE"),
      "hydra_mysql_pool_preping": os.environ.get("HYDRA_MYSQL_POOL_PREPING"),
      "hydra_mysql_pool_size": os.environ.get("HYDRA_MYSQL_POOL_SIZE"),
      "hydra_mysql_pool_recycle": os.environ.get("HYDRA_MYSQL_POOL_RECYCLE"),
      "hydra_mysql_pool_timeout": os.environ.get("HYDRA_MYSQL_POOL_TIMEOUT"),
      "hydra_mysql_max_overflow": os.environ.get("HYDRA_MYSQL_MAX_OVERFLOW")
    }

def read_env_startup_config():
    return {
      "hydra_cachetype": os.environ.get("HYDRA_CACHETYPE"),
      "hydra_cachehost": os.environ.get("HYDRA_CACHEHOST"),
      "hydra_log_confpath": os.environ.get("HYDRA_LOG_CONFPATH"),
      "hydra_log_filedir": os.environ.get("HYDRA_LOG_FILEDIR")
    }

def get_startup_config():
    db_config = read_env_db_config()
    db_config["url"] = f"mysql+mysqldb://{db_config['hydra_db_user']}:{db_config['hydra_db_passwd']}"\
                       f"@{db_config['hydra_db_server']}/{db_config['hydra_db_name']}"

    db_config.update(read_env_startup_config())
    return db_config

def make_value_substitutions(value):
    if not isinstance(value, str):
        return value

    p = r"__([a-zA-Z_]+)__"
    tokens = re.findall(p, value)
    for token in tokens:
        try:
            tkey = token.strip('_').lower()
            tval = config_key_get_value(tkey)
            value = value.replace(token, tval)
        except Exception:
            pass  # Do not substitute invalid keys

    return value

def get(*args, default=None):
    from hydra_base.lib.hydraconfig import (
        config_key_get_value
    )
    """
      The section delineated below is a temporary
      routine to allow calls from the hydra_client
      module which use the old "section+option"
      form of config.get to succeed.
      This is required for tests to pass in CI
      and should be removed on merge and update
      of hydra_client.
    """
    # Temporary CI adjustment begins
    import inspect
    sf = inspect.stack()[1]
    mod = inspect.getmodule(sf[0])
    if mod.__name__.lower().startswith("hydra_client"):
        if args[0].lower() == "default":
            key = args[1]
        else:
            key = f"{args[0]}_{args[1]}"
        if len(args) == 3:
            default = args[2]
    else:
        key = args[0]
        if len(args) == 2:
            default = args[1]

    # Temporary CI adjustment ends

    try:
        value = config_key_get_value(key)
        value = make_value_substitutions(value)
        print(f"{key} = {value}")
        return value
    except:
        return default
