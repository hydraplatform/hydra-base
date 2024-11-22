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


@pytest.fixture
def config_group():
    group_name = "Test ConfigKey Group"
    group_desc = "Description of test group"
    group = client.create_config_group(group_name, group_desc)
    yield group
    client.delete_config_group()


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

    def test_serialise_validator(self):
        min_value = 5
        max_value = 12
        initial_state = '{"max_value": null, "min_value": null}'
        updated_rules = f'{{"max_value": {max_value}, "min_value": {min_value}}}'
        iv = ConfigKeyIntegerValidator()
        assert iv.as_json == initial_state
        iv.set_rule("max_value", max_value)
        iv.set_rule("min_value", min_value)
        assert iv.as_json == updated_rules


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
        client.config_key_set_value(key_name, key_value)
        ret_value = client.config_key_get_value(key_name)
        assert isinstance(ret_value, int)
        assert ret_value == key_value
        # Can existing key value be changed?
        new_value = 77
        client.config_key_set_value(key_name, new_value)
        new_ret_value = client.config_key_get_value(key_name)
        assert isinstance(new_ret_value, int)
        assert new_ret_value == new_value

        # Invalid int values should be rejected
        with pytest.raises(HydraError):
            client.config_key_set_value(key_name, math.nan)

        # ConfigKey_String
        key_name = "string_value_test_key"
        key_value = "A string value"
        _ = client.register_config_key(key_name, "string")
        client.config_key_set_value(key_name, key_value)
        ret_value = client.config_key_get_value(key_name)
        assert isinstance(ret_value, str)
        assert ret_value == key_value


        # ConfigKey_Boolean
        key_name = "boolean_value_test_key"
        key_value = True
        _ = client.register_config_key(key_name, "boolean")
        client.config_key_set_value(key_name, key_value)
        ret_value = client.config_key_get_value(key_name)
        assert isinstance(ret_value, bool)
        assert ret_value == key_value

        # Invalid bool values should be rejected
        with pytest.raises(HydraError):
            client.config_key_set_value(key_name, 'Y')

        # ConfigKey_Base64
        key_name = "base64_value_test_key"
        raw_bytes = bytes([73, 110, 112, 117, 116, 32, 118, 97, 108, 117, 101])
        b64_bytes = base64.b64encode(raw_bytes)
        key_value = b64_bytes.decode("utf8")
        _ = client.register_config_key(key_name, "base64")
        client.config_key_set_value(key_name, key_value)
        ret_value = client.config_key_get_value(key_name)
        assert isinstance(ret_value, str)
        ret_bytes = base64.b64decode(ret_value, validate=True)
        assert ret_bytes == raw_bytes

        # Non-b64 strings should be rejected
        with pytest.raises(HydraError):
            client.config_key_set_value(key_name, "Not Base64")

    def test_integer_key_validation(self, client):
        key_name = "integer_validation_test_key"
        key_value = 46
        max_value = 47
        min_value = 12
        _ = client.register_config_key(key_name, "integer")
        assert len(client.config_key_get_rule_types(key_name)) == 2
        assert len(client.config_key_get_active_rules(key_name)) == 0
        # Permissable as no rules are yet active
        client.config_key_set_value(key_name, key_value)
        # Set a max_value rule
        client.config_key_set_rule(key_name, "max_value", max_value)
        assert len(client.config_key_get_active_rules(key_name)) == 1
        # Now raises...
        with pytest.raises(ValueError):
            client.config_key_set_value(key_name, max_value+1)
        # ...and value has remained unchanged
        assert client.config_key_get_value(key_name) == key_value
        # Set a min_value rule
        client.config_key_set_rule(key_name, "min_value", min_value)
        # Raises again...
        with pytest.raises(ValueError):
            client.config_key_set_value(key_name, min_value-1)
        # ...and value has remained unchanged
        assert client.config_key_get_value(key_name) == key_value
        # Clear all rules...
        num_cleared = client.config_key_clear_all_rules(key_name)
        # ...two rules were cleared...
        assert num_cleared == 2
        # ...and previously rejected values now accepted
        client.config_key_set_value(key_name, max_value+1)
        client.config_key_set_value(key_name, min_value-1)

    def test_string_key_validation(self, client):
        key_name = "string_validation_test_key"
        key_value = "string key value"  # len 16
        min_length = 12
        max_length = 18
        _ = client.register_config_key(key_name, "string")
        assert len(client.config_key_get_rule_types(key_name)) == 2
        assert len(client.config_key_get_active_rules(key_name)) == 0
        # Permissable as no rules are yet active
        client.config_key_set_value(key_name, key_value)
        # Set a max_length rule
        client.config_key_set_rule(key_name, "max_length", max_length)
        # Rule is now active...
        assert len(client.config_key_get_active_rules(key_name)) == 1
        # ...so value exceeding max_length fails...
        with pytest.raises(ValueError):
            client.config_key_set_value(key_name, key_value+"extra text")
        # ...and original value is unchanged
        assert client.config_key_get_value(key_name) == key_value
        # Set a min_length rule
        client.config_key_set_rule(key_name, "min_length", min_length)
        # Additional rule is now active...
        assert len(client.config_key_get_active_rules(key_name)) == 2
        # ...so value shorter than min_length is rejected
        with pytest.raises(ValueError):
            client.config_key_set_value(key_name, key_value[:8])
        # ...and original value remains unchanged
        assert client.config_key_get_value(key_name) == key_value
        # Clear all rules
        num_cleared = client.config_key_clear_all_rules(key_name)
        assert num_cleared == 2
        # Previously rejected values may now be set
        client.config_key_set_value(key_name, key_value+"extra text")
        client.config_key_set_value(key_name, key_value[:8])


class TestConfigKeyGroups():
    def test_create_config_group(self, client):
        group_name = "Test ConfigKey Group"
        group_desc = "Description of test group"
        # Create a group and verify presence
        client.create_config_group(group_name, group_desc)
        groups = client.list_config_groups()
        group_names = {g.name for g in groups}
        assert group_name in group_names
        # Verify unable to add group with same name
        with pytest.raises(HydraError):
            client.create_config_group(group_name, group_desc)
        # Verify group can be deleted
        client.delete_config_group(group_name)
        groups = client.list_config_groups()
        group_names = {g.name for g in groups}
        assert group_name not in group_names

    def test_add_config_key_to_group(self, client):
        group_name = "Membership Test ConfigKey Group"
        group_desc = "Description of test group"
        key_name = "group_membership_test_key"
        key_value = 46
        _ = client.register_config_key(key_name, "integer")
        client.config_key_set_value(key_name, key_value)
        client.create_config_group(group_name, group_desc)
        # Newly created group must be empty
        new_group_keys = client.config_group_list_keys(group_name)
        assert len(new_group_keys) == 0
        client.add_config_key_to_group(key_name, group_name)
        group_keys = client.config_group_list_keys(group_name)
        assert key_name in group_keys
        # Now remove the key
        client.remove_config_key_from_group(key_name, group_name)
        group_keys = client.config_group_list_keys(group_name)
        assert key_name not in group_keys

    def test_delete_populated_group(self, client):
        group_name = "Populated ConfigKey Group"
        group_desc = "Description of test group"
        key_name = "populated_group_test_key"
        key_value = 46
        _ = client.register_config_key(key_name, "integer")
        client.config_key_set_value(key_name, key_value)
        client.create_config_group(group_name, group_desc)
        client.add_config_key_to_group(key_name, group_name)
        client.delete_config_group(group_name)
        # The member key and its value are unaffected by group deletion
        assert key_name in client.list_config_keys()
        assert key_value == client.config_key_get_value(key_name)
