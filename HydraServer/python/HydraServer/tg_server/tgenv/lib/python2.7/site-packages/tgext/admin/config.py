import inspect

try:
    from sqlalchemy.orm import class_mapper
    from sqlalchemy.orm.exc import UnmappedClassError
except ImportError:
    pass

from tgext.crud import CrudRestController

from .layouts import BasicAdminLayout
from sprox.fillerbase import RecordFiller, AddFormFiller

from sprox.providerselector import ProviderTypeSelector, ProviderTypeSelectorError


class CrudRestControllerConfig(object):
    defaultCrudRestController = CrudRestController
    layout = BasicAdminLayout
    allow_only = None

    def _post_init(self):
        #this insanity is caused by some weird python scoping.
        # see previous changesets for first attempts
        TableBaseClass = type('TableBaseClass', (self.layout.TableBase,), {})
        TableFillerClass = type('TableFillerClass', (self.layout.TableFiller,), {})
        EditableFormClass = type('EditableFormClass', (self.layout.EditableForm,), {})
        AddRecordFormClass = type('AddRecordFormClass', (self.layout.AddRecordForm,),{})

        if not hasattr(self, 'table_type'):
            class Table(TableBaseClass):
                __entity__=self.model
            self.table_type = Table

        if not hasattr(self, 'table_filler_type'):
            class MyTableFiller(TableFillerClass):
                __entity__ = self.model
            self.table_filler_type = MyTableFiller

        if not hasattr(self, 'edit_form_type'):
            class EditForm(EditableFormClass):
                __entity__ = self.model
            self.edit_form_type = EditForm

        if not hasattr(self, 'edit_filler_type'):
            class EditFiller(RecordFiller):
                __entity__ = self.model
            self.edit_filler_type = EditFiller

        if not hasattr(self, 'new_form_type'):
            class NewForm(AddRecordFormClass):
                __entity__ = self.model
            self.new_form_type = NewForm

        if not hasattr(self, 'new_filler_type'):
            class NewFiller(AddFormFiller):
                __entity__ = self.model
            self.new_filler_type = NewFiller


    def __init__(self, model, translations=None, **kw):
        super(CrudRestControllerConfig, self).__init__()

        self.model = model
        self._do_init_with_translations(translations)
        self._post_init()

    def _do_init_with_translations(self, translations):
        pass

provider_type_selector = ProviderTypeSelector()


class AdminConfig(object):
    DefaultControllerConfig = CrudRestControllerConfig
    layout = BasicAdminLayout

    default_index_template = None
    allow_only = None
    include_left_menu = True

    def __init__(self, models, translations=None):
        if translations is None:
            translations = {}

        if inspect.ismodule(models):
            models = [getattr(models, model) for model in dir(models) if inspect.isclass(getattr(models, model))]

        #purge all non-model objects
        try_models = models
        models = {}
        for model in try_models:
            try:
                provider_type_selector.get_selector(model)
                models[model.__name__.lower()] = model
            except ProviderTypeSelectorError:
                continue

        self.models = models
        self.translations = translations

    def lookup_controller_config(self, model_name):
        model_name_lower = model_name.lower()

        BaseControllerConfigClass = self.DefaultControllerConfig
        if hasattr(self, model_name_lower):
            BaseControllerConfigClass = getattr(self, model_name_lower)

        class ControllerConfigClass(BaseControllerConfigClass):
            layout = self.layout

        return ControllerConfigClass(self.models[model_name], self.translations)

