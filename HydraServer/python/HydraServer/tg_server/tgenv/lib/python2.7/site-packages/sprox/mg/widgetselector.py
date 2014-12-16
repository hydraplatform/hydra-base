"""
widgetselecter Module

this contains the class which allows the ViewConfig to select the appropriate widget for the given field

Classes:
Name                               Description
MingWidgetSelecter                Aid for the selection of a widget to correspond to a Ming Document field

Exceptions:
None

Functions:
None


Copyright (c) 2009 Christopher Perkins
Original Version by Christopher Perkins 2009
Released under MIT license.

Mongo Contributions by Jorge Vargas
"""

from sprox.widgetselector import WidgetSelector
from sprox.widgets import *

from ming import schema as S
try:
    from ming.odm.property import RelationProperty, ManyToOneJoin, OneToManyJoin
except ImportError: #pragma: no cover
    from ming.orm.property import RelationProperty, ManyToOneJoin, OneToManyJoin

try:
    from ming.odm.property import ManyToManyListJoin
except: #pragma: no cover
    class ManyToManyListJoin:
        pass

class MingWidgetSelector(WidgetSelector):

    default_multiple_select_field_widget_type = PropertyMultipleSelectField
    default_single_select_field_widget_type = PropertySingleSelectField
    default_name_based_widgets = {}

    default_widgets = {
    S.Bool: SproxCheckBox,
    S.Int: TextField,
    S.Float: TextField,
    S.String: TextField,
    S.DateTime: SproxCalendarDateTimePicker,
    S.Binary: FileField,
    S.Value: Label,
    S.ObjectId: TextField
    }
    def select(self,field):
        if hasattr(field, 'name') and field.name:
            if field.name in self.default_name_based_widgets:
                return self.default_name_based_widgets[field.name]

            if field.name.lower() == 'password':
                return PasswordField

        if isinstance(field, RelationProperty):
            join = field.join
            if isinstance(join, ManyToOneJoin):
                return self.default_single_select_field_widget_type
            if isinstance(join, (OneToManyJoin, ManyToManyListJoin)):
                return self.default_multiple_select_field_widget_type
            raise NotImplementedError("Unknown join type %r" % join)	# pragma: no cover
        
        f = getattr(field, 'field', None)
        if f is not None:
            schemaitem = S.SchemaItem.make(field.field.type)
            if isinstance(schemaitem, S.OneOf):
                return self.default_single_select_field_widget_type
        else:
            return TextField 

        #i don't think this works in the latest ming
        sprox_meta = getattr(field, "sprox_meta", {})
        if sprox_meta.get("narrative"):
            return TextArea
        sprox_meta = getattr(field, 'sprox_meta', None)
        if sprox_meta and 'password' in sprox_meta:
            return PasswordField
        
        return self.default_widgets.get(schemaitem.__class__, TextField)

