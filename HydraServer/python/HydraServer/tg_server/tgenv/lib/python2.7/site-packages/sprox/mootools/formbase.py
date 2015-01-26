"""
Mootools Formbase Module

Classes to create Mootools forms (client side validation!)

Copyright (c) 2008 Christopher Perkins
Original Version by Christopher Perkins 2008
Released under MIT license.
"""

from sprox.formbase import FormBase, EditableForm, AddRecordForm
from sprox.sa.widgetselector import SAWidgetSelector
from sprox.widgets import TableForm
from tw.mootools.forms import CustomisedForm

#class MootoolsSAWidgetSelector(SAWidgetSelector):
#    """Mootools-Specific Widget Selector"""
#    default_multiple_select_field_widget_type = SproxMootoolsSelectShuttleField

class MootoolsTableForm(TableForm, CustomisedForm):pass

class MootoolsFormBase(FormBase):
    """FormBase for Mootools

    see :class:`sprox.formbase.FormBase`

    """
    __base_widget_type__ = MootoolsTableForm
#    __widget_selector_type__ = MootoolsSAWidgetSelector

class MootoolsEditableForm(EditableForm):
#    __widget_selector_type__ = MootoolsSAWidgetSelector
    __base_widget_type__ = MootoolsTableForm
