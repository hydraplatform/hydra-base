"""
validatorselecter Module

this contains the class which allows the ViewConfig to select the appropriate validator for the given field

Classes:
Name                               Description
MongoValidatorSelecter             Class to aid in the selection of a validator for a mongo document field

Exceptions:
None

Functions:
None


Copyright (c) 2009 Christopher Perkins
Original Version by Christopher Perkins 2009
Released under MIT license.

"""

from sprox.validatorselector import ValidatorSelector
from ming import schema as s
try:
    import ming.odm as o
except ImportError: #pragma: no cover
    import ming.orm as o
import inspect

from formencode.validators import StringBool, Number, UnicodeString as FEUnicodeString, Email, Int
try: #pragma: no cover
    import tw2.forms
    from tw2.core.validation import *
    class UnicodeString(FEUnicodeString):
        outputEncoding = None
except ImportError: #pragma: no cover
    from tw.forms.validators import *
    DateTimeValidator = DateValidator
    BoolValidator = StringBool

class MingValidatorSelector(ValidatorSelector):

    default_validators = {
    s.Bool: BoolValidator,
    s.Int: Int,
    s.Float: Number,
    s.DateTime: DateTimeValidator,
    s.Binary: None,
    s.Value: None,
    s.ObjectId: None,
    s.String: UnicodeString,
    }

    def select(self, field):

        #xxx: make this a "one of" validator
        if isinstance(field, o.RelationProperty):
            return UnicodeString

        field_type = getattr(field, 'field_type', None)
        if field_type is None:
            f = getattr(field, 'field', None)
            if f is not None:
                field = field.field
                field_type = field.type

        type_ = s.String
        for t in list(self.default_validators.keys()):
            if isinstance(field_type, s.OneOf):
                break
            if inspect.isclass(field_type) and issubclass(field_type, t):
                type_ = t
                break

        validator_type = self.default_validators.get(type_, None)
        return validator_type
