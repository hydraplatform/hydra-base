"""
  Library functions for Hydra configuration
"""

from hydra_base import db
from hydra_base.exceptions import (
    HydraError,
    ResourceNotFoundError,
    PermissionError
)

from hydra_base.db.model.hydraconfig import (
    ConfigKey,
    config_key_type_map,
    ConfigGroup,
    ConfigGroupKeys
)

from sqlalchemy.exc import (
    IntegrityError,
    NoResultFound
)


""" Config Keys: Key:Value pairs of config settings """

def register_config_key(key_name, key_type, **kwargs):
    if not (key_cls := config_key_type_map.get(key_type, None)):
        raise HydraError(f"Invalid ConfigKey type '{key_type}'")

    key = key_cls(name=key_name)
    try:
        db.DBSession.add(key)
        db.DBSession.flush()
    except IntegrityError:
        raise HydraError(f"ConfigKey with name '{key_name}' exists")

    return key

def unregister_config_key(key_name, **kwargs):
    key = _get_config_key_by_name(key_name)
    db.DBSession.delete(key)
    db.DBSession.flush()

def list_config_keys(like=None, **kwargs):
    query = db.DBSession.query(ConfigKey)
    if like:
        query = query.filter(ConfigKey.name.like(f"%{like}%"))

    keys = query.all()
    return [key.name for key in keys]

def config_key_set_value(key_name, value, **kwargs):
    key = _get_config_key_by_name(key_name)
    key.value = value
    db.DBSession.flush()

def config_key_get_value(key_name, **kwargs):
    key = _get_config_key_by_name(key_name)
    return key.value

def _get_config_key_by_name(key_name):
    try:
        key = db.DBSession.query(ConfigKey).filter(ConfigKey.name == key_name).one()
    except NoResultFound:
        raise HydraError(f"No ConfigKey found with name: {key_name}")

    return key

""" Validation related functions """

def config_key_get_rule_types(key_name, **kwargs):
    key = _get_config_key_by_name(key_name)
    if validator := getattr(key, "validator", None):
        return [*validator.rules]

def config_key_get_rule_description(key_name, rule_name, **kwargs):
    pass

def config_key_get_active_rules(key_name, **kwargs):
    key = _get_config_key_by_name(key_name)
    if validator := getattr(key, "validator", None):
        return {rule.name: rule.value for rule in validator.active_rules.values()}
    else:
        return {}

def config_key_set_rule(key_name, rule_name, value, **kwargs):
    key = _get_config_key_by_name(key_name)
    if validator := getattr(key, "validator", None):
        validator.set_rule(rule_name, value)

def config_key_clear_rule(key_name, rule_name, **kwargs):
    key = _get_config_key_by_name(key_name)
    if validator := getattr(key, "validator", None):
        validator.clear_rule(rule_name)

def config_key_clear_all_rules(key_name, **kwargs):
    rules = config_key_get_active_rules(key_name, **kwargs)
    if rules is None or len(rules) == 0:
        return 0

    for rule_name in rules:
        config_key_clear_rule(key_name, rule_name)

    return len(rules)



""" Config Sets: Archived versions of complete configurations """

def create_configset(set_name):
    pass

def delete_configset(set_name):
    pass

def apply_configset(set_name):
    pass

def list_configsets():
    pass

def list_configset_versions(set_name):
    pass


""" Config Groups: A named collection of Config Keys """

def create_config_group(group_name, group_desc=None, **kwargs):
    group = ConfigGroup(name=group_name, description=group_desc)
    try:
        db.DBSession.add(group)
        db.DBSession.flush()
    except IntegrityError:
        raise HydraError(f"ConfigGroup with name '{group_name}' exists")

def delete_config_group(group_name, **kwargs):
    group = _get_config_group_by_name(group_name)
    db.DBSession.delete(group)
    db.DBSession.flush()

def list_config_groups(**kwargs):
    groups = db.DBSession.query(ConfigGroup).all()
    return groups

def _get_config_group_by_name(group_name):
    try:
        group = db.DBSession.query(ConfigGroup).filter(ConfigGroup.name == group_name).one()
    except NoResultFound:
        raise HydraError(f"No ConfigGroup with name: {group_name}")

    return group

def add_config_key_to_group(key_name, group_name, **kwargs):
    key = _get_config_key_by_name(key_name)
    group = _get_config_group_by_name(group_name)
    gk = ConfigGroupKeys(group_id=group.id, key_id=key.id)
    db.DBSession.add(gk)
    db.DBSession.flush()

def config_group_list_keys(group_name, **kwargs):
    group = _get_config_group_by_name(group_name)
    return group.keys

def config_key_get_group_name(key_name, **kwargs):
    key = _get_config_key_by_name(key_name)
    return key.group

def remove_config_key_from_group(key_name, group_name, **kwargs):
    group = _get_config_group_by_name(group_name)
    key = _get_config_key_by_name(key_name)
    qfilter = {
        ConfigGroupKeys.group_id == group.id,
        ConfigGroupKeys.key_id == key.id
    }
    gk = db.DBSession.query(ConfigGroupKeys).filter(*qfilter).one()
    db.DBSession.delete(gk)
    db.DBSession.flush()
