"""
  Types and definitions for Hydra config values
"""
from hydra_base.db.model.base import *


__all__ = ["ConfigKey", "config_key_type_map"]

config_key_type_map = {}


class ConfigKey(Base):
    __tablename__ = "tConfigKey"

    key_name_max_length = 200
    key_desc_max_length = 1000

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(key_name_max_length), nullable=False, unique=True)
    description = Column(String(key_desc_max_length))
    type = Column(String(40))


    __mapper_args__ = {
        "polymorphic_on": type,
        "polymorphic_identity": "configkey"
    }

    subclass_type_key = "config_key_type"

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        keyattr = __class__.subclass_type_key
        configkey_type = getattr(cls, keyattr, None)
        if not configkey_type or not isinstance(configkey_type, str):
            raise NotImplementedError(f"ConfigKey subclass {cls.__name__} does not define a '{keyattr}' attribute")
        config_key_type_map[configkey_type] = cls
        log.debug(f"Registered ConfigKey type '{cls.__name__}' with key '{configkey_type}'")

    def __init__(self, name, desc=None):
        if not name:
            raise ValueError(f"ConfigKey requires a valid name, not '{name}'")

        self.name = name
        self.description = desc if desc else ""


class ConfigKey_Integer(ConfigKey):
    config_key_type = "integer"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }

    # min, max
    def __init__(self, name, desc=None):
        super().__init__(name, desc)



class ConfigKey_String(ConfigKey):
    config_key_type = "string"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }
    # max_len


class ConfigKey_Boolean(ConfigKey):
    config_key_type = "boolean"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }
    # True, False only: not 'Y' 'N' 'X' etc


class ConfigKey_Uri(ConfigKey):
    config_key_type = "uri"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }
    # Includes paths with file:// scheme


class ConfigKey_Json(ConfigKey):
    config_key_type = "json"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }
    # Unvalidated json object
