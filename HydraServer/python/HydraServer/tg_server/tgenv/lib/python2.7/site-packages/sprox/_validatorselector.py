"""
validatorselecter Module

this contains the class which allows the ViewConfig to select the appropriate validator for the given field

Classes:
Name                               Description
ValidatorSelecter                     Parent Class
SAValidatorSelector                   Selecter Based on sqlalchemy field types
DatabaseViewValidatorSelector         Database View always selects the same validator
TableDefValidatorSelector             Table def fields use the same validator

Exceptions:
None

Functions:
None


Copyright (c) 2007-10 Christopher Perkins
Original Version by Christopher Perkins 2007
Released under MIT license.
"""

from formencode import Invalid
from formencode.validators import StringBool, Number, UnicodeString as FEUnicodeString, Email, Int

try: #pragma: no cover
    import tw2.forms
    from tw2.core.validation import *
    class UnicodeString(FEUnicodeString):
        outputEncoding = None
except ImportError: #pragma: no cover
    from tw.forms.validators import *
    DateTimeValidator = DateValidator

class ValidatorSelector(object):
    _name_based_validators = {}

    def __new__(cls, *args, **kw):
        bases = cls.mro()
        chained_attrs = ['_name_based_validators']
        for base in bases:
            for attr in chained_attrs:
                if hasattr(base, attr):
                    current =  getattr(cls, attr)
                    current.update(getattr(base, attr))
        return object.__new__(cls)

    def __getitem__(self, field):
        return self.select(field)

    def __init__(self, *args, **kw):
        pass

    @property
    def name_based_validators(self):
        validators = self._do_get_name_based_validators()
        validators.update(self._name_based_validators)
        return validators

    def select(self, field):
        return UnicodeString

    def _do_get_name_based_validators(self):
        return {}
