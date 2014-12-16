from formencode import Invalid
from tw2.core import Widget, Param, DisplayOnlyWidget, ValidationError
from tw2.forms import (CalendarDatePicker, CalendarDateTimePicker, TableForm, DataGrid,
                       SingleSelectField, MultipleSelectField, InputField, HiddenField,
                       TextField, FileField, CheckBox, PasswordField, TextArea)
from tw2.forms import Label as tw2Label
from sprox._compat import unicode_text


class Label(tw2Label):
    def prepare(self):
        self.text = unicode_text(self.value)
        super(Label, self).prepare()

class SproxMethodPutHiddenField(HiddenField):
    name = '_method'

    def prepare(self):
        self.value = 'PUT'
        super(SproxMethodPutHiddenField, self).prepare()

class ContainerWidget(DisplayOnlyWidget):
    template = "genshi:sprox.widgets.tw2widgets.templates.container"
    controller = Param('controller', attribute=False, default=None)
    css_class = "containerwidget"
    id_suffix = 'container'

class TableLabelWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.tableLabel"
    controller = Param('controller', attribute=False, default=None)
    identifier = Param('identifier', attribute=False)

class ModelLabelWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.modelLabel"
    controller = Param('controller', attribute=False, default=None)
    identifier = Param('identifier', attribute=False)

class EntityLabelWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.entityLabel"
    controller = Param('controller', attribute=False, default=None)
    entity = Param('entity', attribute=False)
    css_class = "entitylabelwidget"

class RecordViewWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.recordViewTable"
    entity = Param('entity', attribute=False, default=None)

class RecordFieldWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.recordField"
    field_name = Param('field_name', attribute=False)
    css_class = "recordfieldwidget"

class TableDefWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.tableDef"
    identifier = Param('identifier', attribute=False)

class EntityDefWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.entityDef"
    entity = Param('entity', attribute=False)

class TableWidget(Widget):
    template = "genshi:sprox.widgets.tw2widgets.templates.table"

class SproxCalendarDatePicker(CalendarDatePicker):
    date_format = '%Y-%m-%d'

class SproxTimePicker(CalendarDateTimePicker):
    date_format = '%H:%M:%S'

class SproxCalendarDateTimePicker(CalendarDateTimePicker):
    date_format = '%Y-%m-%d %H:%M:%S'

class SproxDataGrid(DataGrid):
    template = "sprox.widgets.tw2widgets.templates.datagrid"

    pks = Param('pks', attribute=False),
    xml_fields = Param('xml_fields', attribute=False, default=['actions'])
    value = []

class SproxCheckBox(CheckBox):
    def prepare(self):
        super(SproxCheckBox, self).prepare()
        self.attrs['value'] = 'true'

class PropertySingleSelectField(SingleSelectField):
    entity = Param('entity', attribute=False, default=None)
    provider = Param('provider', attribute=False, default=None)
    field_name = Param('field_name', attribute=False, default=None)
    dropdown_field_names = Param('dropdown_field_names', attribute=False, default=None)
    nullable = Param('nullable', attribute=False, default=False)
    disabled = Param('disabled', attribute=False, default=False)
    prompt_text = None

    def prepare(self):
        #This is required for ming
        entity = self.__class__.entity

        options = self.provider.get_dropdown_options(entity, self.field_name, self.dropdown_field_names)
        self.options = [(unicode_text(k), unicode_text(v)) for k,v in options]
        if self.nullable:
            self.options.append(['', "-----------"])

        if not self.value:
            self.value = ''

        self.value = unicode_text(self.value)
        super(PropertySingleSelectField, self).prepare()

class PropertyMultipleSelectField(MultipleSelectField):
    entity = Param('entity', attribute=False, default=None)
    provider = Param('provider', attribute=False, default=None)
    field_name = Param('field_name', attribute=False, default=None)
    dropdown_field_names = Param('dropdown_field_names', attribute=False, default=None)
    nullable = Param('nullable', attribute=False, default=False)
    disabled = Param('disabled', attribute=False, default=False)

    def _safe_validate(self, validator, value, state=None):
        try:
            value = validator.to_python(value, state=state)
            validator.validate_python(value, state=state)
            return value
        except Invalid:
            return Invalid

    def _validate(self, value, state=None):
        value = value or []
        if not isinstance(value, (list, tuple)):
            value = [value]
        if self.validator:
            value = [self._safe_validate(self.validator, v) for v in value]
        self.value = [v for v in value if v is not Invalid]
        return self.value

    def prepare(self):
        #This is required for ming
        entity = self.__class__.entity

        options = self.provider.get_dropdown_options(entity, self.field_name, self.dropdown_field_names)
        self.options = [(unicode_text(k), unicode_text(v)) for k,v in options]

        if not self.value:
            self.value = []

        self.value = [unicode_text(v) for v in self.value]
        super(PropertyMultipleSelectField, self).prepare()
