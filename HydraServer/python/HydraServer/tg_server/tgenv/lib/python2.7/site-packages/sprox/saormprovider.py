"""
saprovider Module

this contains the class which allows dbsprockets to interface with sqlalchemy.

Classes:
Name                               Description
SAProvider                         sqlalchemy metadata/crud provider

Exceptions:
None

Functions:
None

This module is deprecated

Copyright (c) 2007-10 Christopher Perkins
Original Version by Christopher Perkins 2007
Released under MIT license.
"""

from .sa.provider import SAORMProvider as _SAORMProvider, SAORMProviderError as _SAORMProviderError
import warnings

class SAORMProviderError(_SAORMProviderError):
    def __init__(self, *args, **kw):
        warnings.warn('This class has moved to the sprox.sa.provider module.')
        _SAORMProviderError.__init__(self, *args, **kw)

class SAORMProvider(_SAORMProvider):
    def __init__(self, *args, **kw):
        warnings.warn('This class has moved to the sprox.sa.provider module.')
        _SAORMProvider.__init__(self, *args, **kw)

