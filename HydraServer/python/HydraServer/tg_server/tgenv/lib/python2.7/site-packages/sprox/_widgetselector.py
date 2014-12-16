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


Copyright (c) 2007 Christopher Perkins
Original Version by Christopher Perkins 2007Database
Released under MIT license.
"""

try: #pragma: no cover
    from tw2.core import Widget
    from tw2.forms.widgets import *
except ImportError as e: #pragma: no cover
    from tw.api import Widget
    from tw.forms.fields import *

from sprox.widgets import *

class WidgetSelector(object):
    def select(self, field):
        return Widget

class EntitiesViewWidgetSelector(WidgetSelector):
    def select(self, field):
        return EntityLabelWidget

class EntityDefWidgetSelector(WidgetSelector):
    def select(self, field):
        return EntityDefWidget

class RecordViewWidgetSelector(WidgetSelector):
    def select(self, field):
        return RecordFieldWidget

