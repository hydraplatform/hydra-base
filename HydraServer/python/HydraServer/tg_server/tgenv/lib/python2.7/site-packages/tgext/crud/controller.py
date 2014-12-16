"""
"""
import tg
from tg import expose, flash, redirect, tmpl_context, request, abort
from tg.decorators import without_trailing_slash, with_trailing_slash, before_validate
from tg.controllers import RestController

from tgext.crud.decorators import (registered_validate, register_validators, catch_errors,
                                   optional_paginate)
from tgext.crud.utils import (SmartPaginationCollection, RequestLocalTableFiller, create_setter,
                              set_table_filler_getter, SortableTableBase, map_args_to_pks,
                              adapt_params_for_pagination, allow_json_parameters)
from sprox.providerselector import ProviderTypeSelector
from sprox.formbase import AddRecordForm, EditableForm
from sprox.fillerbase import RecordFiller, AddFormFiller
from .resources import CSSSource, crud_style, crud_script
import warnings

errors = ()
try:
    from sqlalchemy.exc import IntegrityError, DatabaseError, ProgrammingError
    errors =  (IntegrityError, DatabaseError, ProgrammingError)
except ImportError:
    pass

import cgi, inspect
from tgext.crud._compat import url_parse, string_type, unicode_text

class CrudRestControllerHelpers(object):
    def make_link(self, where, pk_count=0):
        if not where.startswith('/'):
            where = '../' * (1 + pk_count) + where
        return where

class CrudRestController(RestController):
    """
    :initialization options:

        **session**
              database session 

        **menu_items**
            Dictionary or mapping type of links to other CRUD sections.
            This is used to generate the links sidebar of the CRUD interface.
            Can be specified in the form ``model_items['lower_model_name'] = ModelClass`` or
            ``model_items['link'] = 'Name'``.

    :class attributes:

        **title**
            Title to be used for each page.  default: ``'Turbogears Admin System'``

        **model**
            Model this class is associated with.

        **remember_values**
            List of model attributes that need to keep previous value when not provided
            on submission instead of replacing the existing value with an empty one.
            It's commonly used with file fields to avoid having to reupload the file
            again when the model is edited.

        **keep_params**
            List of URL parameters that need to be kept around when redirecting between
            the various pages of the CRUD controller. Can be used to keep around filters
            or sorting when editing a subset of the models.

        **search_fields**
            Enables searching on some fields, can be ``True``, ``False`` or a list
            of fields for which searching should be enabled.

        **substring_filters**
            Enable substring filtering for some fields, by default is disabled.
            Pass ``True`` to enable it on all fields or pass a list of field
            names to enable it only on some fields.

        **json_dictify**
            ``True`` or ``False``, enables advanced dictification of retrieved models
            when providing JSON responses. This also enables JSON encoding of related entities
            for the returned model.

        **conditional_update_field**
            Name of the field used to perform conditional updates when ``PUT`` method is
            used as a REST API. ``None`` disables conditional updates (which is the default).

        **pagination**
            Dictionary of options for pagination. ``False`` disables pagination.
            By default ``{'items_per_page': 7}`` is provided.
            Currently the only supported option is ``items_per_page``.

        **resources**
            A list of CSSSource / JSSource that have to be injected inside CRUD
            pages when rendering. By default ``tgext.crud.resources.crud_style`` and
            ``tgext.crud.resources.crud_script`` are injected.

        **table**
            The ``sprox.tablebase.TableBase`` Widget instance used to display the table.
            By default ``tgext.crud.utils.SortableTableBase`` is used which permits to sort
            table by columns.

        **table_filler**
            The ``sprox.fillerbase.TableFiller`` instance used to retrieve data for the table.
            If you want to customize how data is retrieved override the 
            ``TableFiller._do_get_provider_count_and_objs`` method to return different entities and count.
            By default ``tgext.crud.utils.RequestLocalTableFiller`` is used which keeps
            track of the numer of entities retrieved during the current request to enable pagination.

        **edit_form**
            Form to be used for editing an existing model. 
            By default ``sprox.formbase.EditForm`` is used.

        **edit_filler**
            ``sprox.fillerbase.RecordFiller`` subclass used to load the values for
            an entity that need to be edited. Override the ``RecordFiller.get_value``
            method to provide custom values.

        **new_form**
            Form that defines how to create a new model.
            By default ``sprox.formbase.AddRecordForm`` is used.
    """

    title = "Turbogears Admin System"
    keep_params = None
    remember_values = []
    substring_filters = []
    search_fields = True  # True for automagic
    json_dictify = False # True is slower but provides relations
    conditional_update_field = None
    pagination = {'items_per_page': 7}
    resources = ( crud_style,
                  crud_script )

    def _before(self, *args, **kw):
        tmpl_context.title = self.title
        tmpl_context.menu_items = self.menu_items
        tmpl_context.kept_params = self._kept_params()
        tmpl_context.crud_helpers = self.helpers

        for resource in self.resources:
            resource.inject()

    __before__ = _before #This can be removed since 2.2

    def _mount_point(self):
        try:
            mount_point = self.mount_point
        except:
            mount_point = None

        if not mount_point:
            #non statically mounted or Old TurboGears, use relative URL
            mount_point = '.'

        return mount_point

    def _kept_params(self):
        if not self.keep_params:
            return {}

        if not request.referer:
            from_referer = {}
        else:
            parsed = url_parse(request.referer)
            from_referer = dict(cgi.parse_qsl(parsed.query))
        from_referer.update(request.params)

        pass_params = {}
        for k in self.keep_params:
            if k in from_referer:
                pass_params[k] = from_referer[k]
        return pass_params

    def _adapt_menu_items(self, menu_items):
        adapted_menu_items = type(menu_items)()

        for link, model in menu_items.items():
            if inspect.isclass(model):
                adapted_menu_items[link + 's'] = model.__name__
            else:
                adapted_menu_items[link] = model
        return adapted_menu_items

    def _get_search_fields(self, kw):
        if self.search_fields is True:
            return [
                (field, self.table.__headers__.get(field, field), kw.get(field, False))
                    for field in self.table.__fields__
                        if field != '__actions__'
                ]
        elif self.search_fields:
            # This allows for customizing the search fields to be shown in the table definition
            # search_fields can be either a list of tuples with (field, name) or just a string field = name
            search_fields = []
            for field in self.search_fields:
                if isinstance(field, string_type):
                    search_fields.append((field, field, kw.get(field, False)))
                else:
                    search_fields.append((field[0], field[1], kw.get(field[0], False)))
            return search_fields
        else:
            # This would be where someone explicitly disabled the search functionality
            return []

    def _get_current_search(self, search_fields):
        if not search_fields:
            return None

        for field, _, value in search_fields:
            if value is not False:
                return (field, value)
        return (search_fields[0][0], '')

    def _dictify(self, value, length=None):
        json_dictify = self.json_dictify
        if json_dictify is False:
            return value

        def _dictify(entity):
            if hasattr(entity, '__json__'):
                return entity.__json__()
            else:
                return self.provider.dictify(entity, **json_dictify)

        if length is not None:
            #return a generator, we don't want to consume the whole query if it is paginated
            return (_dictify(entity) for entity in value)
        else:
            return _dictify(value)

    def __init__(self, session, menu_items=None):
        if hasattr(self, 'style'):
            warnings.warn('style attribute is not supported anymore, '
                          'resources attribute replaces it', DeprecationWarning,
                          stacklevel=2)
            self.resources = (crud_script,
                              CSSSource(location='headbottom',
                                        src=self.style))

        if menu_items is None:
            menu_items = {}

        self.menu_items = self._adapt_menu_items(menu_items)
        self.helpers = CrudRestControllerHelpers()
        self.provider = ProviderTypeSelector().get_selector(self.model).get_provider(self.model, hint=session)
        self.session = session

        if self.json_dictify is True:
            self.json_dictify = {}

        #this makes crc declarative
        check_types = ['new_form', 'edit_form', 'table', 'table_filler', 'edit_filler']
        for type_ in check_types:
            if not hasattr(self, type_) and hasattr(self, type_+'_type'):
                setattr(self, type_, getattr(self, type_+'_type')(self.session))

        # Enable pagination only if table_filler has support for request local __count__
        self.pagination_enabled = (self.pagination and isinstance(self.table_filler, RequestLocalTableFiller))

        if hasattr(self, 'new_form'):
            #register the validators since they are none from the parent class
            register_validators(self, 'post', self.new_form)
        if hasattr(self, 'edit_form'):
            register_validators(self, 'put', self.edit_form)

    @with_trailing_slash
    @expose('genshi:tgext.crud.templates.get_all')
    @expose('mako:tgext.crud.templates.get_all')
    @expose('jinja:tgext.crud.templates.get_all')
    @expose('json:')
    @optional_paginate('value_list')
    def get_all(self, *args, **kw):
        """Return all records.
           Pagination is done by offset/limit in the filler method.
           Returns an HTML page with the records if not json.
        """
        if self.pagination:
            paginator = request.paginators['value_list']
            paginator.paginate_items_per_page = self.pagination['items_per_page']
        else:
            paginator = request.paginators['value_list']
            paginator.paginate_items_per_page = -1
            paginator.paginate_page = 0

        if tg.request.response_type == 'application/json':
            adapt_params_for_pagination(kw, self.pagination_enabled)
            try:
                count, values = self.table_filler._do_get_provider_count_and_objs(**kw)
            except Exception as e:
                abort(400, detail=unicode_text(e))
            values = self._dictify(values, length=count)
            if self.pagination_enabled:
                values = SmartPaginationCollection(values, count)
            return dict(value_list=values)

        if not getattr(self.table.__class__, '__retrieves_own_value__', False):
            kw.pop('substring_filters', None)
            if self.substring_filters is True:
                substring_filters = list(set(kw.keys()) - set(['limit', 'offset', 'order_by', 'desc']))
            else:
                substring_filters = self.substring_filters

            adapt_params_for_pagination(kw, self.pagination_enabled)
            try:
                values = self.table_filler.get_value(substring_filters=substring_filters, **kw)
            except Exception as e:
                flash('Invalid search query "%s": %s' % (request.query_string, e), 'warn')
                # Reset all variables to sane defaults
                kw = {}
                values = []
                self.table_filler.__count__ = 0
            if self.pagination_enabled:
                values = SmartPaginationCollection(values, self.table_filler.__count__)
        else:
            values = []

        tmpl_context.widget = self.table
        search_fields = self._get_search_fields(kw)
        current_search = self._get_current_search(search_fields)
        return dict(model=self.model.__name__, value_list=values,
                    mount_point=self._mount_point(),
                    headers=search_fields,  # Just for backwards compatibility
                    search_fields=search_fields, current_search=current_search)

    @expose('genshi:tgext.crud.templates.get_one')
    @expose('mako:tgext.crud.templates.get_one')
    @expose('jinja:tgext.crud.templates.get_one')
    @expose('json:')
    def get_one(self, *args, **kw):
        """get one record, returns HTML or json"""
        #this would probably only be realized as a json stream
        kw = map_args_to_pks(args, {})

        if tg.request.response_type == 'application/json':
            obj = self.provider.get_obj(self.model, kw)
            if obj is None:
                tg.response.status_code = 404
            elif self.conditional_update_field is not None:
                tg.response.last_modified = getattr(obj, self.conditional_update_field)

            return dict(model=self.model.__name__,
                        value=self._dictify(obj))

        tmpl_context.widget = self.edit_form
        value = self.edit_filler.get_value(kw)
        return dict(value=value, model=self.model.__name__)

    @expose('genshi:tgext.crud.templates.edit')
    @expose('mako:tgext.crud.templates.edit')
    @expose('jinja:tgext.crud.templates.edit')
    def edit(self, *args, **kw):
        """Display a page to edit the record."""
        pks = self.provider.get_primary_fields(self.model)
        kw = map_args_to_pks(args, {})

        tmpl_context.widget = self.edit_form
        value = self.edit_filler.get_value(kw)
        value['_method'] = 'PUT'
        return dict(value=value, model=self.model.__name__, pk_count=len(pks))

    @without_trailing_slash
    @expose('genshi:tgext.crud.templates.new')
    @expose('mako:tgext.crud.templates.new')
    @expose('jinja:tgext.crud.templates.new')
    def new(self, *args, **kw):
        """Display a page to show a new record."""
        tmpl_context.widget = self.new_form
        return dict(value=kw, model=self.model.__name__)

    @expose(content_type='text/html')
    @expose('json:', content_type='application/json')
    @before_validate(allow_json_parameters)
    @catch_errors(errors, error_handler=new)
    @registered_validate(error_handler=new)
    def post(self, *args, **kw):
        obj = self.provider.create(self.model, params=kw)

        if tg.request.response_type == 'application/json':
            if obj is not None and self.conditional_update_field is not None:
                tg.response.last_modified = getattr(obj, self.conditional_update_field)

            return dict(model=self.model.__name__,
                        value=self._dictify(obj))

        return redirect('./', params=self._kept_params())

    @expose(content_type='text/html')
    @expose('json:', content_type='application/json')
    @before_validate(allow_json_parameters)
    @before_validate(map_args_to_pks)
    @registered_validate(error_handler=edit)
    @catch_errors(errors, error_handler=edit)
    def put(self, *args, **kw):
        """update"""
        omit_fields = []
        if getattr(self, 'edit_form', None):
            omit_fields.extend(self.edit_form.__omit_fields__)

        for remembered_value in self.remember_values:
            value = kw.get(remembered_value)
            if value is None or value == '':
                omit_fields.append(remembered_value)

        obj = self.provider.get_obj(self.model, kw)

        #This should actually by done by provider.update to make it atomic
        can_modify = True
        if obj is not None and self.conditional_update_field is not None and \
           tg.request.if_unmodified_since is not None and \
           tg.request.if_unmodified_since < getattr(obj, self.conditional_update_field):
                can_modify = False

        if obj is not None and can_modify:
            obj = self.provider.update(self.model, params=kw, omit_fields=omit_fields)

        if tg.request.response_type == 'application/json':
            if obj is None:
                tg.response.status_code = 404
            elif can_modify is False:
                tg.response.status_code = 412
            elif self.conditional_update_field is not None:
                tg.response.last_modified = getattr(obj, self.conditional_update_field)

            return dict(model=self.model.__name__,
                        value=self._dictify(obj))

        pks = self.provider.get_primary_fields(self.model)
        return redirect('../' * len(pks), params=self._kept_params())

    @expose(content_type='text/html')
    @expose('json:', content_type='application/json')
    def post_delete(self, *args, **kw):
        """This is the code that actually deletes the record"""
        kw = map_args_to_pks(args, {})

        obj = None
        if kw:
            obj = self.provider.get_obj(self.model, kw)

        if obj is not None:
            self.provider.delete(self.model, kw)

        if tg.request.response_type == 'application/json':
            return dict()

        pks = self.provider.get_primary_fields(self.model)
        return redirect('./' + '../' * (len(pks) - 1), params=self._kept_params())

    @expose('genshi:tgext.crud.templates.get_delete')
    @expose('jinja:tgext.crud.templates.get_delete')
    def get_delete(self, *args, **kw):
        """This is the code that creates a confirm_delete page"""
        return dict(args=args)

class EasyCrudRestController(CrudRestController):
    def __init__(self, session, menu_items=None):
        if not (hasattr(self, 'table') or hasattr(self, 'table_type')):
            class Table(SortableTableBase):
                __entity__=self.model
            self.table = Table(session)

        if not (hasattr(self, 'table_filler') or hasattr(self, 'table_filler_type')):
            class MyTableFiller(RequestLocalTableFiller):
                __entity__ = self.model
            self.table_filler = MyTableFiller(session)

        if not (hasattr(self, 'edit_form') or hasattr(self, 'edit_form_type')):
            class EditForm(EditableForm):
                __entity__ = self.model
            self.edit_form = EditForm(session)

        if not (hasattr(self, 'edit_filler') or hasattr(self, 'edit_filler_type')):
            class EditFiller(RecordFiller):
                __entity__ = self.model
            self.edit_filler = EditFiller(session)

        if not (hasattr(self, 'new_form') or hasattr(self, 'new_form_type')):
            class NewForm(AddRecordForm):
                __entity__ = self.model
            self.new_form = NewForm(session)

        if not (hasattr(self, 'new_filler') or hasattr(self, 'new_filler_type')):
            class NewFiller(AddFormFiller):
                __entity__ = self.model
            self.new_filler = NewFiller(session)
        
        super(EasyCrudRestController, self).__init__(session, menu_items)

        #Permit to quickly customize form options
        if hasattr(self, '__form_options__'):
            for name, value in self.__form_options__.items():
                for form in (self.edit_form, self.new_form):
                    if form:
                        setattr(form, name, value)

        #Permit to quickly create custom actions to set values
        if hasattr(self, '__setters__'):
            for name, config in self.__setters__.items():
                setattr(self, name, create_setter(self, self.get_all, config))


        #Permit to quickly customize table options
        if hasattr(self, '__table_options__'):
            for name, value in self.__table_options__.items():
                if name.startswith('__') and name != '__actions__':
                    for table_object in (self.table_filler, self.table):
                        if table_object:
                            setattr(table_object, name, value)
                else:
                    if self.table_filler:
                        set_table_filler_getter(self.table_filler, name, value)
