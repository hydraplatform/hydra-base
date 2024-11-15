"""
  Types and definitions for Hydra config values
"""

from hydra_base.db.model.base import *
from hydra_base.db.model.hydraconfig import (
    ConfigKey,
    config_key_type_map
)

from sqlalchemy import Enum

__all__ = ["ConfigKeyRecord",]


class ConfigKeyRecord(AuditMixin, Base, Inspect):
    __tablename__ = "tConfigKey"

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(ConfigKey.key_name_max_length), nullable=False, unique=True)
    description = Column(String(ConfigKey.key_desc_max_length))
    type = Column(Enum(*config_key_type_map.keys()))


if __name__ == "__main__":
    print(config_key_type_map)
