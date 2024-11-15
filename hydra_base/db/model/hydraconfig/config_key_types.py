import logging
from abc import ABC, abstractmethod


log = logging.getLogger(__name__)
config_key_type_map = {}


class ConfigKey(ABC):
    subclass_type_key = "config_key_type"
    key_name_max_length = 200
    key_desc_max_length = 1000

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        keyattr = __class__.subclass_type_key
        configkey_type = getattr(cls, keyattr, None)
        if not configkey_type or not isinstance(configkey_type, str):
            raise NotImplementedError(f"ConfigKey subclass {cls.__name__} does not define a '{keyattr}' attribute")
        config_key_type_map[configkey_type] = cls
        log.debug(f"Registered ConfigKey type '{cls.__name__}' with key '{configkey_type}'")


class ConfigKey_Integer(ConfigKey):
    config_key_type = "integer"
    # min, max


class ConfigKey_String(ConfigKey):
    config_key_type = "string"
    # max_len


class ConfigKey_Boolean(ConfigKey):
    config_key_type = "boolean"
    # True, False only: not 'Y' 'N' 'X' etc


class ConfigKey_Uri(ConfigKey):
    config_key_type = "uri"
    # Includes paths with file:// scheme


class ConfigKey_Json(ConfigKey):
    config_key_type = "json"
    # Unvalidated json object


if __name__ == "__main__":
    print(config_key_type_map)
