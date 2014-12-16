import inspect
from sprox.util import name2label, is_widget, is_widget_class

try: #pragma: no cover
    from tw2.core import Widget, Deferred
    from tw2.core.widgets import WidgetMeta
    from tw2.forms import HiddenField
except ImportError: #pragma: no cover
    from tw.api import Widget
    from tw.forms import HiddenField
    class WidgetMeta(object):
        """TW2 WidgetMetaClass"""

from .configbase import ConfigBase, ConfigBaseError

from .widgetselector import WidgetSelector

#sa 0.5 support
try:  #pragma:no cover
    from sqlalchemy.types import Enum
except:  #pragma:no cover
    class Enum:
        pass

class ClassViewer(object):
    """class wrapper to expose items of a class.  Needed to pass classes to TW as params"""
    def __init__(self, klass):
        self.__name__ = klass.__name__
        

class ViewBaseError(Exception):pass

class ViewBase(ConfigBase):
    """

    :Modifiers:

    +-----------------------------------+--------------------------------------------+------------------------------+
    | Name                              | Description                                | Default                      |
    +===================================+============================================+==============================+
    | __widget_selector_type__          | What class to use for widget selection.    | WidgetSelector               |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_widgets__                 | A dictionary of widgets to replace the     | {}                           |
    |                                   | ones that would be chosen by the selector  |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_widget_types__            | A dictionary of types of widgets, allowing | {}                           |
    |                                   | sprox to determine the widget args         |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_widget_args__             | A dictionary of types of args for widgets, | {}                           |
    |                                   | you to override the args sent to the fields|                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __base_widget_type__              | The base widget for this config            | Widget                       |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __base_widget_args__              | Args to pass into the widget overrides any | {}                           |
    |                                   | defaults that are set in sprox creation    |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __widget_selector__               | an instantiated object to use for widget   | None                         |
    |                                   | selection.                                 |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+

    Also, see the :mod:`sprox.configbase` modifiers.
    """
    __field_widgets__      = None
    __field_widget_types__ = None
    __field_widget_args__  = None
    __ignore_field_names__ = None

    #object overrides
    __base_widget_type__       = Widget
    __base_widget_args__       = None
    __widget_selector_type__   = WidgetSelector
    __widget_selector__        = None


    def _do_init_attrs(self):
        super(ViewBase, self)._do_init_attrs()
        if self.__base_widget_args__ is None:
            self.__base_widget_args__ = {}
        if self.__field_widgets__ is None:
            self.__field_widgets__ = {}
        if self.__field_widget_args__ is None:
            self.__field_widget_args__ = {}
        if self.__field_widget_types__ is None:
            self.__field_widget_types__ = {}
        if self.__widget_selector__ is None:
            self.__widget_selector__ = self.__widget_selector_type__()

        if self.__ignore_field_names__ is None:
            self.__ignore_field_names__ = ['sprox_id', '_method']

        for attr in dir(self):
            if not attr.startswith('__'):
                value = getattr(self, attr)
                if is_widget(value):
                    if not getattr(value, 'id', None):
                        raise ViewBaseError('Widgets must provide an id argument for use as a field within a ViewBase')
                    self.__add_fields__[attr] = value
                try:
                    if is_widget_class(value):
                        self.__field_widget_types__[attr] = value
                except TypeError:
                    pass

    @property
    def __widget__(self):
        widget = getattr(self, '___widget__', None)
        if not widget:
            self.___widget__ = self.__base_widget_type__(**self.__widget_args__)
        return self.___widget__

    #try to act like a widget as much as possible
    def __call__(self, *args, **kw):
        return self.display(*args, **kw)

    def display(self, *args, **kw):
        if 'value' not in kw and args:
            args = list(args)
            kw['value'] = args.pop(0)
        return self.__widget__.display(*args, **kw)

    @property
    def __widget_args__(self):
        return self._do_get_widget_args()

    def _do_get_widget_args(self):
        widget_dict = self._do_get_field_widgets(self.__fields__)

        field_widgets = []
        for key in self.__fields__:
            if key not in widget_dict:
                continue
            value = widget_dict[key]
            #sometimes a field will have two widgets associated with it (disabled fields)
            if hasattr(value,'__iter__'):
                field_widgets.extend(value)
                continue
            field_widgets.append(value)

        d = dict(children=field_widgets)
        d.update(self.__base_widget_args__)
        return d

    def _do_get_disabled_fields(self):
        return self.__disable_fields__

    def _do_get_field_widget_args(self, field_name, field):
        # toscawidgets does not like ids that have '.' in them.  This does not
        # work for databases with schemas.
        field_name = field_name.replace('.', '_')
        args = {}

        #this is sort of a hack around TW evaluating _some_ params that are classes.
        entity = field
        if inspect.isclass(field):
            entity = ClassViewer(field)

        if hasattr(Widget, 'req'):
            args.update({'id':'sx_'+field_name, 'key':field_name})
        else: #pragma: no cover
            args.update({'id':field_name})

        args.update({'name':field_name,
                'identity':self.__entity__.__name__+'_'+field_name,
                'entity':entity, 'provider':self.__provider__,
                'label':name2label(field_name), 'label_text':name2label(field_name)})
        field_default_value = self.__provider__.get_field_default(entity)
        if field_default_value[0]:
            if hasattr(Widget, 'req'):
                if callable(field_default_value[1]):
                    args['value'] = Deferred(field_default_value[1])
                else:
                    args['value'] = field_default_value[1]
            else: #pragma: no cover
                args['default'] = field_default_value[1]

        #enum support works completely differently.
        #if isinstance(entity, Column) and isinstance(entity.type, Enum):
        #    args['options'] = entity.type.enums

        if field_name in self.__field_attrs__:
            args['attrs'] = self.__field_attrs__[field_name]

        provider_widget_args = self.__provider__.get_field_provider_specific_widget_args(self.__entity__, field, field_name)
        if provider_widget_args:
            args.update(provider_widget_args)

        if field_name in self.__field_widget_args__:
            args.update(self.__field_widget_args__[field_name])
        return args

    def __create_hidden_fields(self):
        fields = {}
        fields['sprox_id'] = HiddenField(id='sprox_id')

        for field_name in self.__hide_fields__:
            if field_name not in self.__omit_fields__:
                args = {}
                try:
                    field = self.__metadata__[field_name]
                    args = self._do_get_field_widget_args(field_name, field)
                except KeyError: #pragma: no cover
                    pass
                if field_name in self.__field_widget_args__:
                    args.update(self.__field_widget_args__[field_name])
                fields[field_name] = HiddenField(**args)

        return fields

    def _do_get_field_widget_type(self, field_name, field):
        return self.__field_widget_types__.get(field_name, self.__widget_selector__.select(field))

    # This was a typo once, keeping it around for backwards compatibility
    _do_get_field_wiget_type = _do_get_field_widget_type

    def _do_get_field_widgets(self, fields):

        metadata_keys = list(self.__metadata__.keys())
        widgets = {}
        for field_name in fields:
            if field_name in self.__field_widgets__:
                widgets[field_name] = self.__field_widgets__[field_name]
                continue
            if field_name in self.__add_fields__:
                widget = self.__add_fields__[field_name]
                if widget is None:
                    widget = Widget(field_name)
                widgets[field_name] = widget
                continue
            if field_name in self.__ignore_field_names__:
                continue
            if field_name in self.__hide_fields__:
                continue
            if field_name not in metadata_keys:
                continue

            field = self.__metadata__[field_name]
            field_widget_type = self._do_get_field_widget_type(field_name, field)
            field_widget_args = self._do_get_field_widget_args(field_name, field)

            if field_name in self._do_get_disabled_fields():
                # in this case, we display the current field, disabling it, and also add
                # a hidden field into th emix
                field_widget_args['disabled'] = True
                field_widget_args['attrs'] = {'disabled':True}

                if hasattr(Widget, 'req'):
                    hidden_id='disabled_' + field_name.replace('.','_')
                else: #pragma: no cover
                    hidden_id=field_name.replace('.','_')

                widgets[field_name] = (HiddenField(id=hidden_id, key=field_name,
                                                   name=field_name,
                                                   identifier=field_name),
                                       field_widget_type(**field_widget_args))
            else:
                widgets[field_name] = field_widget_type(**field_widget_args)

        widgets.update(self.__create_hidden_fields())
        return widgets
