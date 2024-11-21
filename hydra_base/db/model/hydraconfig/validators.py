import json

from abc import ABC, abstractmethod
from dataclasses import dataclass
from json import JSONDecodeError

from hydra_base.exceptions import HydraError


class KeyValidator(ABC):
    @dataclass
    class Rule():
        name: str
        description: str
        value: type

    class ValidatorEncoder(json.JSONEncoder):
        def default(self, inst):
            return {r.name: r.value for r in inst.rules}

    def __init__(self, rules, rules_spec):
        self._rules = {}
        for rule in rules:
            if rule["name"] in self._rules:
                raise HydraError(f"Rule {rule['name']} already exists")

            new_rule = KeyValidator.Rule(rule["name"], rule["description"], rule["value"])
            self._rules[new_rule.name] = new_rule

        if rules_spec is not None:
            if isinstance(rules_spec, str):
                try:
                    rvi = json.loads(rules_spec)
                except JSONDecodeError as e:
                    raise HydraError(f"Invalid rules_spec: {rules_spec}") from e
                for rule_name, rule_value in rvi.items():
                   self.set_rule(rule_name, rule_value)
            else:
                raise TypeError(f"Invalid Rules specification: {rules_spec}")

    def set_rule(self, name, value):
        rule = self._get_rule(name)
        rule.value = value
        if parent := getattr(self, "key", None):
            parent.rules = self.as_json

    def clear_rule(self, name):
        rule = self._get_rule(name)
        rule.value = None
        if parent := getattr(self, "key", None):
            parent.rules = self.as_json

    def _get_rule(self, name):
        if not (rule := self._rules.get(name)):
            raise ValueError(f"No rule named '{name}' defined")

        return rule

    @property
    def active_rules(self):
        return {rule.name: rule for rule in self._rules.values() if rule.value is not None}

    @property
    def rules(self):
        return self._rules.values()

    @property
    def as_json(self):
        return json.dumps(self, cls=self.ValidatorEncoder)

    @property
    def key(self):
        if hasattr(self, "_key"):
            return self._key

    @key.setter
    def key(self, key):
        self._key = key
        self._key.rules = self.as_json

    @abstractmethod
    def validate(self):
        pass


class ConfigKeyIntegerValidator(KeyValidator):
    rule_types = [
        {"name": "max_value",
         "description": "The maximum integer value of this key",
         "value": None},
        {"name": "min_value",
         "description": "The minimum integer value of this key",
         "value": None}
    ]

    def __init__(self, rules_spec=None):
        super().__init__(self.__class__.rule_types, rules_spec)


    def validate(self, value):
        if parent := getattr(self, "key", None):
            err_prefix = f"ConfigKey {parent.name}: "
        else:
            err_prefix = ""

        if max_value := self.active_rules.get("max_value"):
            if value > max_value.value:
                raise ValueError(f"{err_prefix}value of {value} exceeds maximum of {max_value.value}")
        if min_value := self.active_rules.get("min_value"):
            if value < min_value.value:
                raise ValueError(f"{err_prefix}value of {value} beneath minimum of {min_value.value}")


class ConfigKeyStringValidator(KeyValidator):
    rule_types = [
        {"name": "max_length",
         "description": "The maximum length of this key's string value",
         "value": None},
        {"name": "min_length",
         "description": "The minimum length of this key's string value",
         "value": None},
    ]

    def __init__(self, rules_spec=None):
        super().__init__(self.__class__.rule_types, rules_spec)

    def validate(self, value):
        if max_length := self.active_rules.get("max_length"):
            if len(value) > max_length.value:
                raise ValueError("over max")
        if min_length := self.active_rules.get("min_length"):
            if len(value) < min_length.value:
                raise ValueError("under min")
