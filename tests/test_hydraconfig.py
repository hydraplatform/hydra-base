import base64
import math
import pytest

from hydra_base.exceptions import HydraError
from hydra_base.lib.hydraconfig import (
    config_key_type_map,
    ConfigKey
)
from hydra_base.db.model.hydraconfig.validators import (
    ConfigKeyIntegerValidator,
    ConfigKeyStringValidator
)

class TestConfigKeyValidators():
    """ Standalone tests of Config Key validator classes """
    def test_create_validator(self):
        """
          Do Validators register their subclass-specific rules?
        """
        iv = ConfigKeyIntegerValidator()
        assert len(iv.rules) == 2
        assert len(iv.active_rules) == 0

        sv = ConfigKeyStringValidator()
        assert len(sv.rules) == 2
        assert len(sv.active_rules) == 0

    def test_validator_with_rules_spec(self):
        # Integer Validator
        too_low = 1
        too_high = 12
        valid_value = 7
        rules_spec = '{"min_value": 2, "max_value": 9}'
        iv = ConfigKeyIntegerValidator(rules_spec)
        assert len(iv.active_rules) == 2
        assert iv.validate(valid_value) is None
        with pytest.raises(ValueError):
            iv.validate(too_low)
        with pytest.raises(ValueError):
            iv.validate(too_high)

        # String Validator
        too_short = "one"
        too_long = "eleven"
        valid_value = "five"
        rules_spec = '{"min_length": 4, "max_length": 5}'
        sv = ConfigKeyStringValidator(rules_spec)
        assert len(sv.active_rules) == 2
        assert sv.validate(valid_value) is None
        with pytest.raises(ValueError):
            sv.validate(too_short)
        with pytest.raises(ValueError):
            sv.validate(too_long)

    def test_validate_integer_key(self):
        key_value = 12
        max_value = 7
        iv = ConfigKeyIntegerValidator()
        # Validation succeeds as no rules are active
        assert iv.validate(key_value) is None
        iv.set_rule("max_value", max_value)
        # Validation of the same value now fails due to active rule
        with pytest.raises(ValueError):
            iv.validate(key_value)

    def test_validate_string_key(self):
        key_value = "A string value"  # len 14
        max_length = 7
        sv = ConfigKeyStringValidator()
        # Validation succeeds as no rules are active
        assert sv.validate(key_value) is None
        sv.set_rule("max_length", max_length)
        # Validation of the same value now fails due to active rule
        with pytest.raises(ValueError):
            sv.validate(key_value)


class TestHydraConfig():
    def test_valid_config_key_type_map(self):
        """
          Ensure that all basic config key subclasses are
          registered and map to the correct type
        """
        assert config_key_type_map is not None

        required_types = ("integer", "string", "boolean", "base64", "uri", "json")
        for rtype in required_types:
            assert rtype in config_key_type_map
            assert issubclass(config_key_type_map[rtype], ConfigKey)

    def test_define_config_key(self, client):
        """
          Can only keys with valid names and types be registered
          and are these correctly retrieved?
        """
        with pytest.raises(HydraError):
            client.register_config_key("test_key_name", "invalid type")

        with pytest.raises(ValueError):
            client.register_config_key("", "integer")

        itk = client.register_config_key("integer_test_key", "integer")
        assert itk.name == "integer_test_key"
        assert itk.type == "integer"

        all_keys = client.list_config_keys()
        assert itk.name in all_keys

    def test_set_config_key_value(self, client):
        """
          Can a key be assigned an appropriate value and
          this value then retrieved as the correct type?
        """
        # ConfigKey_Integer
        key_name = "integer_value_test_key"
        key_value = 46
        _ = client.register_config_key(key_name, "integer")
        client.set_config_key_value(key_name, key_value)
        ret_value = client.get_config_key_value(key_name)
        assert isinstance(ret_value, int)
        assert ret_value == key_value
        # Can existing key value be changed?
        new_value = 77
        client.set_config_key_value(key_name, new_value)
        new_ret_value = client.get_config_key_value(key_name)
        assert isinstance(new_ret_value, int)
        assert new_ret_value == new_value

        # Invalid int values should be rejected
        with pytest.raises(HydraError):
            client.set_config_key_value(key_name, math.nan)

        # ConfigKey_String
        key_name = "string_value_test_key"
        key_value = "A string value"
        _ = client.register_config_key(key_name, "string")
        client.set_config_key_value(key_name, key_value)
        ret_value = client.get_config_key_value(key_name)
        assert isinstance(ret_value, str)
        assert ret_value == key_value


        # ConfigKey_Boolean
        key_name = "boolean_value_test_key"
        key_value = True
        _ = client.register_config_key(key_name, "boolean")
        client.set_config_key_value(key_name, key_value)
        ret_value = client.get_config_key_value(key_name)
        assert isinstance(ret_value, bool)
        assert ret_value == key_value

        # Invalid bool values should be rejected
        with pytest.raises(HydraError):
            client.set_config_key_value(key_name, 'Y')

        # ConfigKey_Base64
        key_name = "base64_value_test_key"
        raw_bytes = bytes([73, 110, 112, 117, 116, 32, 118, 97, 108, 117, 101])
        b64_bytes = base64.b64encode(raw_bytes)
        key_value = b64_bytes.decode("utf8")
        _ = client.register_config_key(key_name, "base64")
        client.set_config_key_value(key_name, key_value)
        ret_value = client.get_config_key_value(key_name)
        assert isinstance(ret_value, str)
        ret_bytes = base64.b64decode(ret_value, validate=True)
        assert ret_bytes == raw_bytes

        # Non-b64 strings should be rejected
        with pytest.raises(HydraError):
            client.set_config_key_value(key_name, "Not Base64")
