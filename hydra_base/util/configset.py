"""
  Utilities for the management of ConfigKeys
"""
import datetime
import hmac
import json

from hydra_base import db
from hydra_base.db.model.hydraconfig import ConfigKey
from hydra_base.lib.hydraconfig import (
    register_config_key,
    config_key_set_value,
    config_key_set_rule,
    config_key_set_description
)


config_set_secret_key = b"dev_only_secret_key"

class ConfigSet:
    mac_hash = "sha256"

    def __init__(self, name, description=""):
        self.name = name
        self.desc = description

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if not name:
            raise ValueError(f"ConfigSet requires a valid name, not: {name}")
        self._name = name

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

    def verify_configset(self, state):
        """
          Verifies that the state argument contains a valid
          hmac digest whose "message" corresponds to the contents
          of the other fields in the state.

          Raises ValueError if the equivalent hmac calculated here
          differs from that claimed by the input state.
        """
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
        if len(keys) == 0:
            return {}
        return {
            key.name: {
                "type": key.type,
                "value": key.value,
                "rules": key.rules,
                "description": key.description
            } for key in keys
        }

    def generate_mac(self, state):
        digest = hmac.digest(key=config_set_secret_key, msg=state.encode("utf8"), digest=ConfigSet.mac_hash)
        return digest.hex()

    def apply_configset_to_db(self, state):
        """
          1. Serialise existing state
          2. Verify integrity of new state
          3. Delete existing state
          4. Create new keys
          5. Set new validation rules
          6. Load new values
          7. Verify state loaded in 3-5 matches input state
          8. OK if so, else restore original state from 1
        """
        old_state = self.save_keys_to_configset()
        new_state = self.verify_configset(state)
        self._delete_all_keys()
        self.load_keys_from_state(new_state)
        # Verify update has succeeded and return old state if so
        trial_state = self.save_keys_to_configset()
        if trial_state["keys"] == new_state["keys"]:
            return old_state
        # Otherwise restore state to that before call
        self._delete_all_keys()
        self.load_keys_from_state(old_state)
        # Returns None on failure to update

    def _delete_all_keys(self):
        keys = db.DBSession.query(ConfigKey).all()
        for key in keys:
            db.DBSession.delete(key)
        db.DBSession.flush()

    def load_keys_from_state(self, state):
        for key_name, key in state["keys"].items():
            register_config_key(key_name, key["type"])
            rules = json.loads(key["rules"])
            for rule_name, rule_val in rules.items():
                config_key_set_rule(key_name, rule_name, rule_val)
            config_key_set_value(key_name, key["value"])
            config_key_set_description(key_name, key["description"])
