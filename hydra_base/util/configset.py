import hmac
import json

from hydra_base import db
from hydra_base.db.model.hydraconfig import ConfigKey


config_set_secret_key = "dev_only_secret_key"

class ConfigSet:
    def __init__(self):
        pass

    def serialise_all_keys(self):
        keys = db.DBSession.query(ConfigKey).all()
        configset = {key.name: {"value": key.value, "rules": key.rules} for key in keys}
        return json.dumps(configset)
