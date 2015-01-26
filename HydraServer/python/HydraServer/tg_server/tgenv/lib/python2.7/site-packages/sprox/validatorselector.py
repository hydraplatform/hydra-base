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
from sprox._validatorselector import ValidatorSelector
import warnings

try:
    from .sa.validatorselector import SAValidatorSelector as _SAValidatorSelector

    class SAValidatorSelector(_SAValidatorSelector):
        def __init__(self, *args, **kw):
            warnings.warn('This class has moved to the sprox.sa.validatorselector module.') # pragma: no cover
            _SAValidatorSelector.__init__(self, *args, **kw) # pragma: no cover
except ImportError: # pragma: no cover
    pass # pragma: no cover