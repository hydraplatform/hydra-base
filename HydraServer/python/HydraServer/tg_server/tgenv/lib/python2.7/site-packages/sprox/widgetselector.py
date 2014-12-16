"""
widgetselecter Module

this contains the class which allows the ViewConfig to select the appropriate widget for the given field

Classes:
Name                               Description
WidgetSelecter                     Parent Class
SAWidgetSelector                   Selecter Based on sqlalchemy field types
DatabaseViewWidgetSelector         Database View always selects the same widget
TableDefWidgetSelector             Table def fields use the same widget

Exceptions:
None

Functions:
None


Copyright (c) 2007-10 Christopher Perkins
Original Version by Christopher Perkins 2007Database
Released under MIT license.
"""
import warnings

from sprox._widgetselector import *

try:
    from sprox.sa.widgetselector import SAWidgetSelector as _SAWidgetSelector

    class SAWidgetSelector(_SAWidgetSelector):
        def __init__(self, *args, **kw):
            warnings.warn('This class has moved to the sprox.sa.widgetselector module.') # pragma: no cover
            _SAWidgetSelector.__init__(self, *args, **kw) # pragma: no cover
except ImportError: # pragma: no cover
    pass # pragma: no cover


