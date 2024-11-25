import datetime
import hmac
import json

from hydra_base import db
from hydra_base.db.model.hydraconfig import ConfigKey


config_set_secret_key = b"dev_only_secret_key"

class ConfigSet:
    mac_hash = "sha256"
    def __init__(self, name, description=""):
        if not name:
            raise ValueError(f"ConfigSet requires a valid name, not: {name}")
        self.name = name
        self.desc = description

    def save_keys_to_configset(self):
        configstate= {
            "name": self.name,
            "description": self.desc,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "keys": self.all_keys_as_dict()
        }

        digest = self.generate_mac(json.dumps(configstate))
        configstate["digest"] = digest
        return configstate

    def load_configset(self, state):
        if isinstance(state, str):
            state = json.loads(state)

        loaded_digest = state.pop("digest")
        calculated_digest = self.generate_mac(json.dumps(state))
        if loaded_digest != calculated_digest:
            raise ValueError(f"ConfigSet digest is {calculated_digest} but claimed is {loaded_digest}")

        state["digest"] = calculated_digest
        return state

    def all_keys_as_dict(self):
        keys = db.DBSession.query(ConfigKey).all()
        return {key.name: {"type": key.type, "value": key.value, "rules": key.rules} for key in keys}

    def generate_mac(self, state):
        digest = hmac.digest(key=config_set_secret_key, msg=state.encode("utf8"), digest=ConfigSet.mac_hash)
        return digest.hex()
