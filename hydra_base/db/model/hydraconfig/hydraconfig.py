"""
  Types and definitions for Hydra config values
"""
import base64
import binascii

from hydra_base.db.model.base import *
from hydra_base.exceptions import HydraError

from sqlalchemy.orm import (
    Mapped,
    mapped_column
)


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


class HasValue:
    _value: Mapped[str] = mapped_column(String(2000), nullable=True, use_existing_column=True)


class ConfigKey_Integer(ConfigKey, HasValue):
    config_key_type = "integer"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }


    def __init__(self, name, desc=None):
        super().__init__(name, desc)

    @property
    def value(self):
        if self._value is None:
            return None

        return int(self._value)

    @value.setter
    def value(self, val):
        if val is None:
            return

        try:
            _ = int(val)
        except (TypeError, ValueError):
            raise HydraError(f"Config Key {self.name} requires an integer value, not {val}")

        self._value = val


class ConfigKey_String(ConfigKey, HasValue):
    config_key_type = "string"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }

    def __init__(self, name, desc=None):
        super().__init__(name, desc)

    @property
    def value(self):
        return str(self._value)

    @value.setter
    def value(self, val):
        self._value = str(val)


class ConfigKey_Boolean(ConfigKey, HasValue):
    config_key_type = "boolean"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }

    def __init__(self, name, desc=None):
        super().__init__(name, desc)

    @property
    def value(self):
        return self._value == "True"

    @value.setter
    def value(self, val):
        if val not in (True, False):
            raise HydraError(f"Config Key {self.name} with Boolean type accepts only True or False value")

        self._value = "True" if val else "False"


class ConfigKey_Base64(ConfigKey, HasValue):
    config_key_type = "base64"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }

    def __init__(self, name, desc=None):
        super().__init__(name, desc)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        try:
            _ = base64.b64decode(val, validate=True)
        except (AttributeError, TypeError, binascii.Error) as e:
            raise HydraError(f"Config Key {self.name} with Base64 "
                             f"type accepts only valid Base64 strings") from e

        self._value = val


class ConfigKey_Uri(ConfigKey, HasValue):
    config_key_type = "uri"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }
    # Includes paths with file:// scheme


class ConfigKey_Json(ConfigKey, HasValue):
    config_key_type = "json"

    __mapper_args__ = {
        "polymorphic_identity": config_key_type
    }
    # Unvalidated json object
