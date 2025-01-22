import base64
import copy
import inspect
import json
import math
import pytest
import random
import string

from hydra_base.exceptions import HydraError
from hydra_base.lib.hydraconfig import (
    config_key_type_map,
    ConfigKey
)
from hydra_base.db.model.hydraconfig.validators import (
    ConfigKeyIntegerValidator,
    ConfigKeyStringValidator
)
from hydra_base.util.configset import ConfigSet


# Util funcs

def make_integer_key_with_value(client, key_name):
    key_value = random.randint(2**7, 2**9)
    key_description = generate_key_description(key_type="integer")
    key = client.register_config_key(key_name, "integer", description=key_description)
    val_diff = random.randint(1, key_value//2)
    min_value, max_value = key_value-val_diff, key_value+val_diff
    client.config_key_set_rule(key_name, "min_value", min_value)
    client.config_key_set_rule(key_name, "max_value", max_value)
    client.config_key_set_value(key_name, key_value)
    return key.name


def make_string_key_with_value(client, key_name):
    key_len = random.randint(2**4, 2**6)
    key_value = "".join(random.choices(string.ascii_lowercase, k=key_len))
    key_description = generate_key_description(key_type="string")
    key = client.register_config_key(key_name, "string", description=key_description)
    len_diff = random.randint(1, key_len//2)
    min_length, max_length = key_len-len_diff, key_len+len_diff
    client.config_key_set_rule(key_name, "min_length", min_length)
    client.config_key_set_rule(key_name, "max_length", max_length)
    client.config_key_set_value(key_name, key_value)
    return key.name


def make_boolean_key_with_value(client, key_name):
    key_value = random.getrandbits(1)
    key_description = generate_key_description(key_type="boolean")
    key = client.register_config_key(key_name, "boolean", description=key_description)
    client.config_key_set_value(key_name, key_value)
    return key.name


def generate_key_description(src="\x80.", key_type=""):
    dmap = {
        "\x80": ["\x81 \x84 for a key of type \xC0"],
        "\x81": ["An \x82", "A \x83", "The"],
        "\x82": ["appropriate", "apt", "example", "illustrative"],
        "\x83": ["fitting", "suitable", "relevant", "particular", "placeholder", "basic", "typical"],
        "\x84": ["description", "comment", "overview", "explanation"]
    }
    maptop = 0x85
    out = []
    idx = -1
    while True:
        idx += 1
        try:
            c = src[idx]
        except IndexError:
            break
        oc = ord(c)
        if oc < 128:
            out.append(c)
            continue
        else:
            if oc < maptop:
                mapline = dmap[c]
                out.append(generate_key_description(src=random.choice(mapline), key_type=key_type))
                continue
        if oc == 0xC0:
            out.append(key_type)
            continue

    return "".join(out)


@pytest.fixture
def config_group():
    group_name = "Test ConfigKey Group"
    group_desc = "Description of test group"
    group = client.create_config_group(group_name, group_desc)
    yield group
    client.delete_config_group()


@pytest.fixture
def random_keys(client):
    """
      Returns a function which...
        - Generates and registers n_keys ConfigKeys of random
          types, including appropriate validator rule settings
          and a key value which passes validation.
        - Deletes these keys on return unless the optional
          do_tidy argument is overwritten to be False.
    """
    pending_tidy = []
    _do_tidy = True
    def _random_keys(n_keys, do_tidy=True):
        key_gen_funcs = {
          "integer": make_integer_key_with_value,
          "string": make_string_key_with_value,
          "boolean": make_boolean_key_with_value
        }
        nonlocal _do_tidy, pending_tidy
        _do_tidy = do_tidy
        prefix_len = 3
        existing_key_prefixes = set(k[:prefix_len] for k in pending_tidy)
        key_prefixes = set()
        key_names = []
        for idx in range(n_keys):
            while True:
                key_prefix = "".join(random.choices(string.ascii_lowercase, k=prefix_len))
                if key_prefix not in key_prefixes | existing_key_prefixes:
                    key_prefixes.add(key_prefix)
                    break
            key_name = f"{key_prefix} test key"
            key_func = key_gen_funcs[random.choice([*key_gen_funcs])]
            key_names.append(key_func(client, key_name))

        if do_tidy:
            pending_tidy += key_names
        return key_names

    yield _random_keys
    if _do_tidy:
        for key_name in pending_tidy:
            try:
                client.unregister_config_key(key_name)
            except HydraError:
                pass


class TestFixtures():
    def test_fixture_reentrancy(self, client, random_keys):
        """
          Verify random_keys fixture is reentrant wrt to
          multiple calls to the returned func in the same
          fixture scope.
        """
        num_keys = 16
        # First fix func call returns new keys and has them pending deletion
        keys0 = random_keys(num_keys)
        assert len(keys0) == num_keys
        rkpt0 = inspect.getclosurevars(random_keys).nonlocals["pending_tidy"]
        assert len(rkpt0) == num_keys
        for key in keys0:
            assert key in rkpt0
        # Second fix func call returns new keys but do_tidy is False
        # so pending deletion not expanded
        keys1 = random_keys(num_keys, do_tidy=False)
        assert len(keys1) == num_keys
        rkpt1 = inspect.getclosurevars(random_keys).nonlocals["pending_tidy"]
        assert len(rkpt1) == num_keys
        for key in keys1:
            assert key not in rkpt1
        # As a result of do_tidy=False, we have to do tidy
        # or duplicates could occur
        for key_name in keys1:
            client.unregister_config_key(key_name)
        # Third fix func call returns new keys and adds these to
        # pending deletion
        keys2 = random_keys(num_keys)
        assert len(keys2) == num_keys
        rkpt2 = inspect.getclosurevars(random_keys).nonlocals["pending_tidy"]
        assert len(rkpt2) == 2*num_keys
        for key in keys0 + keys2:
            assert key in rkpt2


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
        """
          Does a validator have the correct initial state
          and can it be serialised to the correct format?
        """
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
        client.unregister_config_key("integer_test_key")

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
        client.unregister_config_key(key_name)

        # ConfigKey_String
        key_name = "string_value_test_key"
        key_value = "A string value"
        _ = client.register_config_key(key_name, "string")
        client.config_key_set_value(key_name, key_value)
        ret_value = client.config_key_get_value(key_name)
        assert isinstance(ret_value, str)
        assert ret_value == key_value
        client.unregister_config_key(key_name)


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
        client.unregister_config_key(key_name)

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
        client.unregister_config_key(key_name)

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
        client.unregister_config_key(key_name)

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
        client.unregister_config_key(key_name)


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
        # Verify that groupless key reports no group
        no_group_name = client.config_key_get_group_name(key_name)
        assert no_group_name is None
        # Add key to group and confirm membership
        client.add_config_key_to_group(key_name, group_name)
        group_keys = client.config_group_list_keys(group_name)
        assert key_name in group_keys
        # Now remove the key
        client.remove_config_key_from_group(key_name, group_name)
        group_keys = client.config_group_list_keys(group_name)
        assert key_name not in group_keys
        client.unregister_config_key(key_name)

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
        client.unregister_config_key(key_name)


class TestConfigSets:
    def test_config_set_save_and_verify(self, random_keys):
        """
          Can a ConfigSet be created, serialised and then
          detect any modifications to the serialised version?
        """
        _ = random_keys(16)
        cs = ConfigSet("Test Configset")
        state = cs.save_keys_to_configset()
        # Tamper with serialised set
        modified = copy.deepcopy(state)
        first_key = next(iter(modified["keys"]))
        key_val = modified["keys"].pop(first_key)
        modified["keys"]["modifed_name"] = key_val
        # Naughtiness is detected...
        with pytest.raises(ValueError):
            cs.verify_configset(modified)
        # ...but original unmodified state can be loaded
        loaded = cs.verify_configset(state)

    def test_apply_configset_to_db(self, client, random_keys):
        """
          1. Serialises initial state
          2. Deletes this and replaces with temporary ConfigKeys
          3. Re-loads the initial state
          4. Confirms this returns the temporary state
          5. Confirms the final state is equal to initial state
        """
        num_keys = 16
        key_names = random_keys(num_keys, do_tidy=False)
        cs = ConfigSet("Configset")
        # Save initial state
        initial_state = cs.save_keys_to_configset()
        # Manually delete initial keys
        for key_name in key_names:
            client.unregister_config_key(key_name)
        key_names.clear()
        # Generate new state
        new_key_names = random_keys(num_keys, do_tidy=False)
        # Save second state
        second_state = cs.save_keys_to_configset()
        # Reload the initial state
        ret_state = cs.apply_configset_to_db(initial_state)
        # The temporary key state was returned...
        assert ret_state["keys"] == second_state["keys"]
        # ...which means the initial key state was restored
        final_state = cs.save_keys_to_configset()
        assert final_state["keys"] == initial_state["keys"]
        # Initial keys were restored by load so delete again
        for key_name in final_state["keys"]:
            client.unregister_config_key(key_name)

    def test_configset_api_export_json(self, client, random_keys):
        num_keys = 16
        key_names = random_keys(num_keys)
        cs_json = client.export_config_as_json("Configset API test keys", "ConfigSet API test desc")
        cs = json.loads(cs_json)
        assert len(cs["keys"]) == num_keys
        for key_name in key_names:
            assert key_name in cs["keys"]

    def test_apply_configset(self, client, random_keys):
        # Backup pre-test config state
        initial_keys = client.list_config_keys()
        initial_state = client.export_config_as_json("Initial state", "Initial state desc")
        for key_name in initial_keys:
            client.unregister_config_key(key_name)

        num_keys = 16
        # Create an initial state
        key_names = random_keys(num_keys)
        # Export this as json
        cs_json = client.export_config_as_json("Configset API test keys", "ConfigSet API test desc")
        # Then delete state
        for key_name in key_names:
            client.unregister_config_key(key_name)
        # Confirm no loaded state
        all_keys = client.list_config_keys()
        assert len(all_keys) == 0
        # Apply the exported json configset
        old_state = client.apply_configset(cs_json)
        # Re-export the loaded state
        applied_json = client.export_config_as_json("Applied keys")
        # And confirm this is equal to initial state
        orig_state = json.loads(cs_json)
        applied_state = json.loads(applied_json)
        assert orig_state["keys"] == applied_state["keys"]

        # Restore initial config state
        _ = client.apply_configset(initial_state)
