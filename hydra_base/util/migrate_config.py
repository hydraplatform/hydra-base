"""
  Utilities to migrate from Hydra.ini config format
  to DB-based config table via hydra_base.lib.hydraconfig
"""
import configparser
import os


def ini_to_configset(ini_filename):
    db_config_schema = make_db_config_schema(ini_filename)
    return db_config_schema

def make_db_config_schema(ini_filename):
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(ini_filename)

    # Values for "home_dir" and "hydra_base_dir" must be
    # set to allow for interpolation into later values
    home_dir = os.environ.get("HYDRA_HOME_DIR", '~')
    hydra_base_dir = os.environ.get("HYDRA_BASE_DIR", os.getcwd())
    config.set("DEFAULT", "home_dir", os.path.expanduser(home_dir))
    config.set("DEFAULT", "hydra_base_dir", os.path.expanduser(hydra_base_dir))

    db_config_schema = {}
    for section in ["DEFAULT", *config.sections()]:
        for key in config[section]:
            try:
                value = config[section][key]
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

if __name__ == "__main__":
    ini_file = "hydra_base/hydra.ini"
    config = ini_to_configset(ini_file)
    print(config)
