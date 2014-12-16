from tw.api import Widget
from tw.forms import CalendarDatePicker, CalendarDateTimePicker, TableForm, DataGrid
from tw.forms.fields import (SingleSelectField, MultipleSelectField, InputField, HiddenField,
                             TextField, FileField, PasswordField, TextArea, Label)

from formencode.schema import Schema
from formencode.validators import StringBool
from formencode import Invalid

class SproxMethodPutHiddenField(HiddenField):
    available_engines = ['mako', 'genshi']
    template="sprox.widgets.tw1widgets.templates.hidden_put"

class SproxCalendarDatePicker(CalendarDatePicker):
    date_format = '%Y-%m-%d'

class SproxTimePicker(CalendarDateTimePicker):
    date_format = '%H:%M:%S'

class SproxCalendarDateTimePicker(CalendarDateTimePicker):
    date_format = '%Y-%m-%d %H:%M:%S'

class SproxDataGrid(DataGrid):
    available_engines = ['mako', 'genshi']
    template = "sprox.widgets.tw1widgets.templates.datagrid"
    params = ['pks', 'controller', 'xml_fields']
    xml_fields = ['actions']

class ContainerWidget(Widget):
    template = "genshi:sprox.widgets.tw1widgets.templates.container"
    params = ["controller",]

class TableLabelWidget(Widget):
    template = "genshi:sprox.widgets.tw1widgets.templates.tableLabel"
    params = ["identifier", "controller"]

class ModelLabelWidget(Widget):
    available_engines = ['mako', 'genshi']
    template = "sprox.widgets.tw1widgets.templates.modelLabel"
    params = ["identifier", "controller"]

class EntityLabelWidget(Widget):
    template = "genshi:sprox.widgets.tw1widgets.templates.entityLabel"
    params = ["entity", "controller"]

class RecordViewWidget(Widget):
    template = "genshi:sprox.widgets.tw1widgets.templates.recordViewTable"
    params = ["entity"]

class RecordFieldWidget(Widget):
    template = "genshi:sprox.widgets.tw1widgets.templates.recordField"
    params = ['field_name']

class TableDefWidget(Widget):
    template = "genshi:sprox.widgets.tw1widgets.templates.tableDef"
    params = ["identifier"]

class EntityDefWidget(Widget):
    available_engines = ['genshi']
    template = "sprox.widgets.tw1widgets.templates.entityDef"
    params = ["entity"]

class TableWidget(Widget):
    available_engines = ['genshi']
    template = "genshi:sprox.widgets.tw1widgets.templates.table"

class SproxTableForm(TableForm):
    available_engines = ['mako', 'genshi']
    validator = Schema(ignore_missing_keys=True, allow_extra_fields=True)
    template = "sprox.widgets.tw1widgets.templates.tableForm"

#custom checkbox widget since I am not happy with the behavior of the TW one
class SproxCheckBox(InputField):
    available_engines = ['mako', 'genshi']
    template = "sprox.widgets.tw1widgets.templates.checkbox"
    validator = StringBool
    def update_params(self, d):
        InputField.update_params(self, d)
        try:
            checked = self.validator.to_python(d.value)
        except Invalid:
            checked = False
        d.attrs['checked'] = checked or None

class PropertyMixin(Widget):
    params = ['entity', 'field_name', 'provider', 'dropdown_field_names']

    def _my_update_params(self, d, nullable=False):
        entity = self.entity
        options = self.provider.get_dropdown_options(self.entity, self.field_name, self.dropdown_field_names)
        if nullable:
            options.append([None,"-----------"])
        if len(options) == 0:
            return {}
        d['options']= options

        return d

class PropertySingleSelectField(SingleSelectField, PropertyMixin):
    params=["nullable", "disabled"]
    nullable=False
    disabled=False
    def update_params(self, d):
        self._my_update_params(d,nullable=self.nullable)
        SingleSelectField.update_params(self, d)
        return d

class PropertyMultipleSelectField(MultipleSelectField, PropertyMixin):
    params=["disabled"]
    disabled=False
    def update_params(self, d):
        self._my_update_params(d)
        MultipleSelectField.update_params(self, d)
        return d

