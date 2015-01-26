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
from sqlalchemy.types import *
from sprox.widgets import *

from sqlalchemy.schema import Column
from sqlalchemy.orm import SynonymProperty
from sprox.sa.support import PropertyLoader, Binary, LargeBinary
from sprox._widgetselector import WidgetSelector

text_field_limit=100


class SAWidgetSelector(WidgetSelector):

    default_widgets = {
    String:   TextField,
    Integer:  TextField,
    Numeric:  TextField,
    DateTime: SproxCalendarDateTimePicker,
    Date:     SproxCalendarDatePicker,
    Time:     SproxTimePicker,
    Binary:   FileField,
    LargeBinary: FileField,
    BLOB:   FileField,
    PickleType: TextField,
    Boolean: SproxCheckBox,
#    NullType: TextField
    }

    default_name_based_widgets = {}

    default_multiple_select_field_widget_type = PropertyMultipleSelectField
    default_single_select_field_widget_type = PropertySingleSelectField

    def select(self, field):

        if hasattr(field, 'name'):
            if field.name in self.default_name_based_widgets:
                return self.default_name_based_widgets[field.name]

            if field.name.lower() == 'password':
                return PasswordField

        if hasattr(field, 'key') and field.key.lower() == 'password':
            return PasswordField

        # this is really the best we can do, since we cannot know
        # what type the field represents until execution occurs.
        if isinstance(field, SynonymProperty):
            #fix to handle weird synonym prop stuff
            if isinstance(field.descriptor, property) or field.descriptor.__class__.__name__.endswith('SynonymProp'):
                return TextField

        if isinstance(field, PropertyLoader):
            if field.uselist:
                return self.default_multiple_select_field_widget_type
            return self.default_single_select_field_widget_type

        type_ = String
        for t in list(self.default_widgets.keys()):
            if isinstance(field.type, t):
                type_ = t
                break

        widget = self.default_widgets[type_]
        if widget is TextField and hasattr(field.type, 'length') and (field.type.length is None or field.type.length>text_field_limit):
            widget = TextArea
        return widget
