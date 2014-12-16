"""
fillerbase Module

Classes to help fill widgets with data

Copyright (c) 2008-10 Christopher Perkins
Original Version by Christopher Perkins 2008
Released under MIT license.
"""

from .configbase import ConfigBase, ConfigBaseError
from .metadata import FieldsMetadata
import inspect
from datetime import datetime
from sprox._compat import string_type, byte_string, unicode_text
from markupsafe import Markup

encoding = 'utf-8'

class FillerBase(ConfigBase):
    """
    :Modifiers:

    see :mod:`sprox.configbase`.

    The base filler class.

    :Arguments:
      values
        pass through of values.  This is typically a set of default values that is updated by the
        filler.  This is useful when updating an existing form.
      kw
        Set of keyword arguments for assisting the fill.  This is for instance information like offset
        and limit for a TableFiller.

    :Usage:

    >>> filler = FillerBase()
    >>> filler.get_value()
    {}
    """

    def get_value(self, values=None, **kw):
        """
        The main function for getting data to fill widgets,
        """
        if values is None:
            values = {}
        return values

class ModelsFiller(FillerBase):
    pass
class ModelDefFiller(FillerBase):
    pass

class FormFiller(FillerBase):
    __metadata_type__ = FieldsMetadata

    def get_value(self, values=None, **kw):
        values = super(FormFiller, self).get_value(values)
        values['sprox_id'] =  self.__sprox_id__
        return values

class TableFiller(FillerBase):
    """
    This is the base class for generating table data for use in table widgets.  The TableFiller uses
    it's provider to obtain a dictionary of information about the __entity__ this Filler defines.
    This class is especially useful when you need to return a json stream, because it allows for
    customization of attributes.  A package which has similar functionality to this is TurboJson,
    but TurboJson is rules-based, where the semantics for generating dictionaries follows the same
    :mod:`sprox.configbase` methodology.

    Modifiers defined in this class

    +-----------------------------------+--------------------------------------------+------------------------------+
    | Name                              | Description                                | Default                      |
    +===================================+============================================+==============================+
    | __actions__                       | An overridable function to define how to   | a function that creates an   |
    |                                   | display action links in the form.          | edit and delete link.        |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __metadata_type__                 | How should we get data from the provider.  | FieldsMetadata               |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __possible_field_names__          | list or dict of names to use for discovery | None                         |
    |                                   | of field names for relationship columns.   | (See below.)                 |
    |                                   | (None uses the default list from           |                              |
    |                                   | :class:`sprox.configbase:ConfigBase`.)     |                              |
    |                                   | A dict provides field-level granularity    |                              |
    |                                   | (See also explanation below.)              |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __datetime_formatstr__            | format string for the strftime function of | '%Y-%m-%d %H:%M:%S'          |
    |                                   | datetime objects.                          | ("simplified" ISO-8601)      |
    |                                   | Classical american format would be         |                              |
    |                                   | '%m/%d/%Y %H:%M%p'.                        |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+

    see modifiers also in :mod:`sprox.configbase`.

    :Relations:

    By default, TableFiller will populate relations (join or foreign_key) with either the value
    from the related table, or a comma-separated list of values.  These values are derived from
    the related object given the field names provided by the __possible_field_names__ modifier.
    For instance, if you have a User class which is related to Groups, the groups item in the result
    dictionaries will be populated with Group.group_name.  The default field names are specified
    in __possible_field_name_defaults__: _name, name, description, title.

    :RESTful Actions:

    By default, Table filler provides an "__actions__" item in the resultant dictionary list.  This provides
    and edit, and (javascript) delete link which provide edit and DELETE functionality as HTML verbs in REST.
    For more information on developing RESTful URLs, please visit `http://microformats.org/wiki/rest/urls <http://microformats.org/wiki/rest/urls/>`_ .

    :Usage:

    Here is how we would get the values to fill up a user's table, minus the action column, and created date.

    >>> class UsersFiller(TableFiller):
    ...     __model__ = User
    ...     __actions__ = False
    ...     __omit_fields__ = ['created']
    >>> users_filler = UsersFiller(session)
    >>> value = users_filler.get_value(values={}, limit=20, offset=0)
    >>> print value #doctest: +IGNORE_WHITESPACE
    [{'town': u'Arvada', 'user_id': u'1', 'user_name': u'asdf',
    'town_id': u'1', 'groups': u'4', '_password': '******', 'password': '******',
    'email_address': u'asdf@asdf.com', 'display_name': u'None'}]
    """
    __actions__ = True
    __metadata_type__ = FieldsMetadata
    __possible_field_names__ = None
    __datetime_formatstr__ = '%Y-%m-%d %H:%M:%S'

    def _do_init_attrs(self):
        super(TableFiller, self)._do_init_attrs()
        if self.__possible_field_names__ is None:
            self.__possible_field_names__ = self.__possible_field_name_defaults__

    def _get_list_data_value(self, field, values):
        l = []

        if isinstance(self.__possible_field_names__, dict) and field in self.__possible_field_names__:
            view_names = self.__possible_field_names__[field]
            if not isinstance(view_names, list):
                view_names = [view_names]
        elif isinstance(self.__possible_field_names__, list):
            view_names = self.__possible_field_names__
        else:
            view_names = self.__possible_field_name_defaults__

        for value in values:
            if not isinstance(value, string_type):
                name = self.__provider__.get_view_field_name(value.__class__, view_names, value)
                l.append(unicode_text(getattr(value, name)))
            else:
                #this is needed for postgres to see array values
                return values
        return ', '.join(l)

    def _get_relation_value(self, field, value):
        #this may be needed for catwalk, but I am not sure what conditions cause it to be needed
        #if value is None:
        #    return None
        name = self.__provider__.get_view_field_name(value.__class__, self.__possible_field_names__)
        return getattr(value, name)

    def get_count(self):
        """Returns the total number of items possible for retrieval.  This can only be
        executed after a get_value() call.  This call is useful for creating pagination in the context
        of a user interface.
        """
        if not hasattr(self, '__count__'):
            raise ConfigBaseError('Count not yet set for filler.  try calling get_value() first.')
        return self.__count__

    def _do_get_fields(self):
        fields = super(TableFiller, self)._do_get_fields()
        if '__actions__' not in self.__omit_fields__ and '__actions__' not in fields:
            fields.insert(0, '__actions__')
        return fields


    def __actions__(self, obj):
        """Override this function to define how action links should be displayed for the given record."""
        primary_fields = self.__provider__.get_primary_fields(self.__entity__)
        pklist = '/'.join(map(lambda x: str(getattr(obj, x)), primary_fields))
        value = '<div><div>&nbsp;<a href="'+pklist+'/edit" style="text-decoration:none">edit</a>'\
              '</div><div>'\
              '<form method="POST" action="'+pklist+'" class="button-to">'\
            '<input type="hidden" name="_method" value="DELETE" />'\
            '<input class="delete-button" onclick="return confirm(\'Are you sure?\');" value="delete" type="submit" '\
            'style="background-color: transparent; float:left; border:0; color: #286571; display: inline; margin: 0; padding: 0;"/>'\
        '</form>'\
        '</div></div>'
        return value

    def _do_get_provider_count_and_objs(self, **kw):
        limit = kw.pop('limit', None)
        offset = kw.pop('offset', None)
        order_by = kw.pop('order_by', None)
        desc = kw.pop('desc', False)
        substring_filters = kw.pop('substring_filters', [])
        count, objs = self.__provider__.query(self.__entity__, limit, offset, self.__limit_fields__,
                                              order_by, desc, substring_filters=substring_filters,
                                              filters=kw)
        self.__count__ = count
        return count, objs

    def get_value(self, values=None, **kw):
        """
        Get the values to fill a form widget.

        :Arguments:
         offset
          offset into the records
         limit
          number of records to return
         order_by
          name of the column to the return values ordered by
         desc
          order the columns in descending order

        All the other arguments will be used to filter the result
        """
        count, objs = self._do_get_provider_count_and_objs(**kw)
        self.__count__ = count
        rows = []
        for obj in objs:
            row = {}
            for field in self.__fields__:
                field_method = getattr(self, field, None)
                if inspect.ismethod(field_method):
                    argspec = inspect.getargspec(field_method)
                    if argspec and (len(argspec[0])-2>=len(kw) or argspec[2]):
                        value = getattr(self, field)(obj, **kw)
                    else:
                        value = getattr(self, field)(obj)
                else:
                    value = getattr(obj, field)
                    if 'password' in field.lower():
                        row[field] = '******'
                        continue
                    elif isinstance(value, list) or self.__provider__.is_query(self.__entity__, value):
                        value = self._get_list_data_value(field, value)
                    elif isinstance(value, datetime):
                        value = value.strftime(self.__datetime_formatstr__)
                    elif self.__provider__.is_relation(self.__entity__, field) and value is not None:
                        value = self._get_relation_value(field, value)
                    elif self.__provider__.is_binary(self.__entity__, field) and value is not None:
                        value = Markup('&lt;file&gt;')
                if isinstance(value, byte_string):
                    value = unicode_text(value, encoding='utf-8')
                row[field] = unicode_text(value)
            rows.append(row)
        return rows

class EditFormFiller(FormFiller):
    """
    This class will help to return a single record for use within a form or otherwise.
    The values are returned in dictionary form.

    :Modifiers:

    see :mod:`sprox.configbase`.


    :Usage:

    >>> class UserFiller(EditFormFiller):
    ...     __model__ = User
    >>> users_filler = UsersFiller(session)
    >>> value = users_filler.get_value(values={'user_id':'1'})
    >>> value # doctest: +SKIP
    {'town': u'Arvada', 'user_id': u'1', 'created': u'2008-12-28 17:33:11.078931',
      'user_name': u'asdf', 'town_id': u'1', 'groups': u'4', '_password': '******',
      'password': '******', 'email_address': u'asdf@asdf.com', 'display_name': u'None'}

    """
    def get_value(self, values=None, **kw):
        obj = self.__provider__.get_obj(self.__entity__, params=values, fields=self.__fields__)
        values = self.__provider__.dictify(obj, self.__fields__, self.__omit_fields__)
        for key in self.__fields__:
            method = getattr(self, key, None)
            if method:
                if inspect.ismethod(method):
                    values[key] = method(obj, **kw)
        return values

class RecordFiller(EditFormFiller):pass


class AddFormFiller(FormFiller):
    def get_value(self, values=None, **kw):
        """xxx: get the server/entity defaults."""
        kw = super(AddFormFiller, self).get_value(values, **kw)
        return self.__provider__.get_default_values(self.__entity__, params=values)
