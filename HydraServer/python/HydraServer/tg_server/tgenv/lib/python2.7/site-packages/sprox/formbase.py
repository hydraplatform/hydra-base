"""
formbase Module

Classes to create form widgets.

Copyright (c) 2008 Christopher Perkins
Original Version by Christopher Perkins 2008
Released under MIT license.
"""

import warnings

try: #pragma: no cover
    from tw2.core import Widget
    from tw2.core.widgets import WidgetMeta
    from tw2.forms import HiddenField, TableForm
except ImportError: #pragma: no cover
    from tw.api import Widget
    from tw.forms import HiddenField, TableForm
    class WidgetMeta(object):
        """TW2 WidgetMetaClass"""

import inspect
from sprox.util import name2label, is_widget, is_widget_class

from sprox.widgets import SproxMethodPutHiddenField, CalendarDatePicker, CalendarDateTimePicker
from .viewbase import ViewBase, ViewBaseError
from formencode import Schema, All
from formencode import Validator
from formencode.validators import String
from sprox.validators import UnicodeString

from sprox.validators import UniqueValue
from sprox.metadata import FieldsMetadata
from sprox.viewbase import ViewBase, ViewBaseError

class FilteringSchema(Schema):
    """This makes formencode work for most forms, because some wsgi apps append extra values to the parameter list."""
    filter_extra_fields = True
    allow_extra_fields = True

class Field(object):
    """Used to handle the case where you want to override both a validator and a widget for a given field"""
    def __init__(self, widget=None, validator=None):
        self.widget = widget
        self.validator = validator

class FormBase(ViewBase):
    """

    :Modifiers:


    Modifiers defined in this class

    +-----------------------------------+--------------------------------------------+------------------------------+
    | Name                              | Description                                | Default                      |
    +===================================+============================================+==============================+
    | __base_widget_type__              | What widget to use for the form.           | TableForm                    |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __widget_selector_type__          | What class to use for widget selection.    | SAWidgetSelector             |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __validator_selector_type__       | What class to use for validator selection. | SAValidatorSelector          |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __require_fields__                | Specifies which fields are required.       | []                           |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __check_if_unique__               | Set this to True for "new" forms.  This    | False                        |
    |                                   | causes Sprox to check if there is an       |                              |
    |                                   | existing record in the database which      |                              |
    |                                   | matches the field data.                    |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_validators__              | A dictionary of validators indexed by      | {}                           |
    |                                   | fieldname.                                 |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_validator_types__         | Types of validators to use for each field  | {}                           |
    |                                   | (allow sprox to set the attribute of the   |                              |
    |                                   | validators).                               |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __base_validator__                | A validator to attch to the form.          | None                         |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __validator_selector__            | What object to use to select field         | None                         |
    |                                   | validators.                                |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __metadata_type__                 | What metadata type to use to get schema    | FieldsMetadata               |
    |                                   | info on this object                        |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __possible_field_names__          | list or dict of names to use for discovery | None                         |
    |                                   | of field names for dropdowns.              |                              |
    |                                   | (None uses the default list from           |                              |
    |                                   | :class:`sprox.configbase:ConfigBase`.)     |                              |
    |                                   | A dict provides field-level granularity    |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+

    Modifiers inherited from :class:`sprox.viewbase.ViewBase`

    +-----------------------------------+--------------------------------------------+------------------------------+
    | Name                              | Description                                | Default                      |
    +===================================+============================================+==============================+
    | __field_widgets__                 | A dictionary of widgets to replace the     | {}                           |
    |                                   | ones that would be chosen by the selector  |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_widget_types__            | A dictionary of types of widgets, allowing | {}                           |
    |                                   | sprox to determine the widget args         |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __widget_selector__               | an instantiated object to use for widget   | None                         |
    |                                   | selection.                                 |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+

    Modifiers inherited from :class:`sprox.configbase.ConfigBase`


    :Example Usage:

    One of the more useful things sprox does for you is to fill in the arguments to a drop down automatically.
    Here is the userform, limited to just the town field, which gets populated with the towns.

    >>> from sprox.formbase import FormBase
    >>> class UserOnlyTownForm(FormBase):
    ...    __model__ = User
    ...    __limit_fields__ = ['town']
    >>>
    >>> town_form = UserOnlyTownForm(session)
    >>>
    >>> print(town_form()) # doctest: +XML
    <form enctype="multipart/form-data" method="post">
         <span class="error"></span>
        <table >
        <tr class="odd"  id="sx_town:container">
            <th><label for="sx_town">Town</label></th>
            <td >
                <select name="town" id="sx_town">
             <option value="1">Arvada</option>
             <option value="2">Denver</option>
             <option value="3">Golden</option>
             <option value="4">Boulder</option>
             <option selected="selected" value="">-----------</option>
    </select>
                <span id="sx_town:error"></span>
            </td>
        </tr>
        <tr class="error"><td colspan="2">
            <input type="hidden" name="sprox_id" value="" id="sprox_id"/>
            <span id=":error"></span>
        </td></tr>
    </table>
    		<input type="submit" value="Save"/>
    </form>

    Forms created with sprox can be validated as you would any other widget.
    >>> class UserOnlyTownForm(FormBase):
    ...    __model__ = User
    ...    __limit_fields__ = ['town']
    ...    __require_fields__ = ['town']
    >>> town_form = UserOnlyTownForm(session)
    >>> town_form.validate(params={'sprox_id':1})
    Traceback (most recent call last):
    ...
    ValidationError



    """
    __require_fields__     = None
    __check_if_unique__    = False

    #object overrides
    __base_widget_type__       = TableForm

    @property
    def __widget_selector_type__(self):
        return self.__provider__.default_widget_selector_type

    __validator_selector__      = None

    @property
    def __validator_selector_type__(self):
        return self.__provider__.default_validator_selector_type

    __field_validators__       = None
    __field_validator_types__  = None
    __base_validator__         = FilteringSchema

    __metadata_type__ = FieldsMetadata

    __possible_field_names__      = None
    __dropdown_field_names__      = None

    def _do_init_attrs(self):
        super(FormBase, self)._do_init_attrs()
        if self.__require_fields__ is None:
            self.__require_fields__ = []
        if self.__field_validators__ is None:
            self.__field_validators__ = {}
        if self.__validator_selector__ is None:
            self.__validator_selector__ = self.__validator_selector_type__(self.__provider__)
        if self.__field_validator_types__ is None:
            self.__field_validator_types__ = {}
        if self.__possible_field_names__ is None:
            if self.__dropdown_field_names__ is not None:
                warnings.warn('The __dropdown_field_names__ attribute is deprecated', DeprecationWarning)
                self.__possible_field_names__ = self.__dropdown_field_names__
            else:
                self.__possible_field_names__ = self.__possible_field_name_defaults__

        #bring in custom declared validators
        for attr in dir(self):
            if not attr.startswith('__'):
                value = getattr(self, attr)
                if isinstance(value, Field):
                    widget = value.widget
                    if is_widget(widget):
                        if not getattr(widget, 'id', None):
                            raise ViewBaseError('Widgets must provide an id argument for use as a field within a ViewBase')
                        self.__add_fields__[attr] = widget
                    try:
                        if is_widget_class(widget):
                            self.__field_widget_types__[attr] = widget
                    except TypeError:
                        pass
                    validator = value.validator
                    if isinstance(validator, Validator):
                        self.__field_validators__[attr] = validator
                    try:
                        if issubclass(validator, Validator):
                            self.__field_validator_types__[attr] = validator
                    except TypeError:
                        pass
                if isinstance(value, Validator):
                    self.__field_validators__[attr] = value
                    continue
                try:
                    if issubclass(value, Validator):
                        self.__field_validator_types__[attr] = value
                except TypeError:
                    pass

    def validate(self, params, state=None):
        """A pass-thru to the widget's validate function."""
        return self.__widget__.validate(params, state)

    def _do_get_widget_args(self):
        """Override this method to define how the class get's the
           arguments for the main widget
        """
        d = super(FormBase, self)._do_get_widget_args()
        if self.__base_validator__ is not None:
            d['validator'] = self.__base_validator__

        #TW2 widgets cannot have a FormEncode Schema as validator, only plain validators instances
        if hasattr(Widget, 'req'):
            current_validator = d.get('validator')
            if current_validator is FilteringSchema:
                d.pop('validator', None)

        return d

    def _do_get_field_widget_args(self, field_name, field):
        """Override this method do define how this class gets the field
        widget arguemnts
        """
        args = super(FormBase, self)._do_get_field_widget_args( field_name, field)
        v = self.__field_validators__.get(field_name, self._do_get_field_validator(field_name, field))
        if self.__provider__.is_relation(self.__entity__, field_name):
            args['entity'] = self.__entity__
            args['field_name'] = field_name
            if isinstance(self.__possible_field_names__, dict) and field_name in self.__possible_field_names__:
                view_names = self.__possible_field_names__[field_name]
                if not isinstance(view_names, list):
                    view_names = [view_names]
                args['dropdown_field_names'] = view_names
            elif isinstance(self.__possible_field_names__, list):
                args['dropdown_field_names'] = self.__possible_field_names__
        if v:
            args['validator'] = v
        return args

    def _do_get_fields(self):
        """Override this function to define what fields are available to the widget.
        """
        fields = super(FormBase, self)._do_get_fields()
        provider = self.__provider__
        field_order = self.__field_order__ or []
        add_fields = list(self.__add_fields__.keys())
        for relation in provider.get_relations(self.__entity__):
            # do not remove field if it is listed in field_order
            for rel in provider.relation_fields(self.__entity__, relation):
                if rel not in field_order and rel in fields and rel not in add_fields:
                    fields.remove(rel)
        if 'sprox_id' not in fields:
            fields.append('sprox_id')
        return fields

    def _do_get_field_widgets(self, fields):
        widgets = super(FormBase, self)._do_get_field_widgets(fields)
        widgets['sprox_id'] = HiddenField('sprox_id', validator=String(if_missing=None))
        return widgets

    def _do_get_field_validator(self, field_name, field):
        """Override this function to define how a field validator is chosen for a given field.
        """
        v_type = self.__field_validator_types__.get(field_name, self.__validator_selector__[field])
        if field_name in self.__require_fields__ and v_type is None:
            v_type = UnicodeString
        if v_type is None:
            return
        args = self._do_get_validator_args(field_name, field, v_type)
        v = v_type(**args)
        if self.__check_if_unique__ and self.__provider__.is_unique_field(self.__entity__, field_name):
            v = All(UniqueValue(self.__provider__, self.__entity__, field_name), v)
        return v

    def _do_get_validator_args(self, field_name, field, validator_type):
        """Override this function to define how to get the validator arguments for the field's validator.
        """
        args = {}
        args['not_empty'] = (not self.__provider__.is_nullable(self.__entity__, field_name)) or \
                             field_name in self.__require_fields__
        args['required'] = args['not_empty']

        widget_type = self._do_get_field_widget_type(field_name, field)
        if widget_type and (issubclass(widget_type, CalendarDatePicker) or
                            issubclass(widget_type, CalendarDateTimePicker)):
            widget_args = super(FormBase, self)._do_get_field_widget_args(field_name, field)
            args['format'] = widget_args.get('date_format', widget_type.date_format)

        if hasattr(field, 'type') and hasattr(field.type, 'length') and\
           issubclass(validator_type, String):
            args['max'] = field.type.length

        return args

class EditableForm(FormBase):
    """A form for editing a record.
    :Modifiers:

    see :class:`sprox.formbase.FormBase`

    """
    def _do_get_disabled_fields(self):
        fields = self.__disable_fields__[:]
        fields.append(self.__provider__.get_primary_field(self.__entity__))
        return fields

    def _do_get_fields(self):
        """Override this function to define what fields are available to the widget.

        """
        fields = super(EditableForm, self)._do_get_fields()
        primary_field = self.__provider__.get_primary_field(self.__entity__)
        if primary_field not in fields:
            fields.append(primary_field)
        
        if '_method' not in fields:
            fields.append('_method')

        return fields

    def _do_get_field_widgets(self, fields):
        widgets = super(EditableForm, self)._do_get_field_widgets(fields)
        widgets['_method'] = SproxMethodPutHiddenField(id='sprox_method',
                                                       validator=UnicodeString(if_missing=None))
        return widgets

    __check_if_unique__ = False

class AddRecordForm(FormBase):
    """An editable form who's purpose is record addition.

    :Modifiers:

    see :class:`sprox.formbase.FormBase`

    +-----------------------------------+--------------------------------------------+------------------------------+
    | Name                              | Description                                | Default                      |
    +===================================+============================================+==============================+
    | __check_if_unique__               | Set this to True for "new" forms.  This    | True                         |
    |                                   | causes Sprox to check if there is an       |                              |
    |                                   | existing record in the database which      |                              |
    |                                   | matches the field data.                    |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+

    Here is an example registration form, as generated from the vase User model.

    >>> from sprox.formbase import AddRecordForm
    >>> from formencode.validators import FieldsMatch
    >>> from sprox.widgets import PasswordField, TextField
    >>> form_validator =  FieldsMatch('password', 'verify_password',
    ...                                 messages={'invalidNoMatch': 'Passwords do not match'})
    >>> class RegistrationForm(AddRecordForm):
    ...     __model__ = User
    ...     __require_fields__     = ['password', 'user_name', 'email_address']
    ...     __omit_fields__        = ['_password', 'groups', 'created', 'user_id', 'town']
    ...     __field_order__        = ['user_name', 'email_address', 'display_name', 'password', 'verify_password']
    ...     __base_validator__     = form_validator
    ...     email_address          = TextField
    ...     display_name           = TextField
    ...     verify_password        = PasswordField('verify_password')
    >>> registration_form = RegistrationForm()
    >>> print(registration_form()) # doctest: +XML
    <form enctype="multipart/form-data" method="post">
         <span class="error"></span>
        <table >
        <tr class="odd required"  id="sx_user_name:container">
            <th><label for="sx_user_name">User Name</label></th>
            <td >
                <input name="user_name" type="text" id="sx_user_name" value=""/>
                <span id="sx_user_name:error"></span>
            </td>
        </tr>
        <tr class="even required"  id="sx_email_address:container">
            <th><label for="sx_email_address">Email Address</label></th>
            <td >
                <input name="email_address" type="text" id="sx_email_address"/>
                <span id="sx_email_address:error"></span>
            </td>
        </tr>
        <tr class="odd"  id="sx_display_name:container">
            <th><label for="sx_display_name">Display Name</label></th>
            <td >
                <input name="display_name" type="text" id="sx_display_name" value=""/>
                <span id="sx_display_name:error"></span>
            </td>
        </tr>
        <tr class="even required"  id="sx_password:container">
            <th><label for="sx_password">Password</label></th>
            <td >
                <input type="password" name="password" id="sx_password"/>
                <span id="sx_password:error"></span>
            </td>
        </tr>
        <tr class="odd"  id="verify_password:container">
            <th><label for="verify_password">Verify Password</label></th>
            <td >
                <input type="password" name="verify_password" id="verify_password"/>
                <span id="verify_password:error"></span>
            </td>
        </tr>
        <tr class="error"><td colspan="2">
            <input type="hidden" name="sprox_id" value="" id="sprox_id"/>
            <span id=":error"></span>
        </td></tr>
    </table>
        <input type="submit" value="Save"/>
    </form>


    What is unique about the AddRecord form, is that if the fields in the database are labeled unique, it will
    automatically vaidate against uniqueness for that field.  Here is a simple user form definition, where the
    user_name in the model is unique:


    >>> class AddUserForm(AddRecordForm):
    ...     __entity__ = User
    ...     __limit_fields__ = ['user_name']
    >>> user_form = AddUserForm(session)
    >>> user_form.validate(params={'sprox_id':'asdf', 'user_name':'asdf'}) # doctest: +SKIP
    Traceback (most recent call last):
    ...
    Invalid: user_name: That value already exists

    The validation fails because there is already a user with the user_name 'asdf' in the database


    """
    __check_if_unique__ = True

    def _do_init_attrs(self):
        super(AddRecordForm, self)._do_init_attrs()

        pkey = self.__provider__.get_primary_field(self.__entity__)
        if pkey not in self.__omit_fields__:
            self.__omit_fields__.append(pkey)

    def _do_get_disabled_fields(self):
        fields = self.__disable_fields__[:]
        fields.append(self.__provider__.get_primary_field(self.__entity__))
        return fields


class DisabledForm(FormBase):
    """A form who's set of fields is disabled.


    :Modifiers:

    see :class:`sprox.formbase.FormBase`

    Here is an example disabled form with only the user_name and email fields.

    >>> from sprox.test.model import User
    >>> from sprox.formbase import DisabledForm
    >>> class DisabledUserForm(DisabledForm):
    ...     __model__ = User
    ...     __limit_fields__ = ['user_name', 'email_address']
    >>> disabled_user_form = DisabledUserForm()
    >>> print(disabled_user_form(values=dict(user_name='percious', email='chris@percious.com')))  # doctest: +XML
    <form enctype="multipart/form-data" method="post">
         <span class="error"></span>
        <table >
        <tr class="odd"  id="sx_user_name:container">
            <th><label for="sx_user_name">User Name</label></th>
            <td >
                <input name="user_name" value="" disabled="disabled" type="text" id="sx_user_name"/>
                <span id="sx_user_name:error"></span>
            </td>
        </tr>
        <tr class="even"  id="sx_email_address:container">
            <th><label for="sx_email_address">Email Address</label></th>
            <td >
                <textarea disabled="disabled" name="email_address" id="sx_email_address"></textarea>
                <span id="sx_email_address:error"></span>
            </td>
        </tr>
        <tr class="error"><td colspan="2">
            <input type="hidden" name="user_name" id="disabled_user_name"/>
            <input type="hidden" name="email_address" id="disabled_email_address"/>
            <input type="hidden" name="sprox_id" value="" id="sprox_id"/>
            <span id=":error"></span>
        </td></tr>
    </table>
        <input type="submit" value="Save"/>
    </form>


    You may notice in the above example that disabled fields pass in a hidden value for each disabled field.

    """


    def _do_get_disabled_fields(self):
        return self.__fields__
