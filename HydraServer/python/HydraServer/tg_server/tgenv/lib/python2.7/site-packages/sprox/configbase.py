from formencode.validators import Validator
from sprox.providerselector import ProviderTypeSelector
import copy

class ConfigBaseError(Exception):pass

class ConfigBase(object):
    """
    Base class for all configurable classes in Sprox.

    :Modifiers:

    +-----------------------------------+--------------------------------------------+------------------------------+
    | Name                              | Description                                | Default                      |
    +===================================+============================================+==============================+
    | __entity__ (__model__)            | Entity used for metadata extraction.       | None                         |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __sprox_id_                       | Id for use in the widget.                  | None                         |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __provider_type_selector_type__   | A type for selecting the provider.         | ProviderTypeSelector         |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_order__                   | A list of ordered field names.             | None                         |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __hide_fields__                   | Fields marked as hidden.                   | []                           |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __add_fields__                    | Additional fields to add to the config.    | {}                           |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __disable_fields__                | Field marked as disabled.                  | []                           |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __omit_fields__                   | Fields removed from the field list.        | []                           |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __limit_fields__                  | Limit the field list to this.              | None                         |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __field_attrs__                   | attr parmater to set to the field.         | {}                           |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __metadata_type__                 | Metadata associated with this config.      | None                         |
    +-----------------------------------+--------------------------------------------+------------------------------+
    | __possible_field_name_defaults__  | Default foreign field name list for        | ['name', 'title', '_name',   |
    |                                   | relationship columns.                      | 'description',               |
    |                                   | Used when there is no entry in             | '_description']              |
    |                                   | __possible_field_names__.                  |                              |
    +-----------------------------------+--------------------------------------------+------------------------------+
    """
    # what object does will this object use for metadata extraction
    # model and entity are one in the same
    __model__ = __entity__  = None

    # this is used by catwalk's validate decorator to lookup the sprocket in the cache
    __sprox_id__    = None

    #How should we select a provider
    __provider_type_selector_type__ = ProviderTypeSelector

    # field overrides
    __field_order__        = None
    __hide_fields__        = None
    __disable_fields__     = None
    __omit_fields__        = None
    __add_fields__         = None
    __limit_fields__       = None
    __field_attrs__        = None
    __metadata_type__      = None

    __possible_field_name_defaults__ = ['name', 'title', '_name', 'description', '_description']

    def __init__(self, provider_hint=None, **provider_hints):

        #map __model__ to __entity__, this may be deprecated
        if self.__entity__ is None and self.__model__ is not None:
            self.__entity__ = self.__model__

        self.__provider_type_selector__ = self.__provider_type_selector_type__()
        self.provider_hint  = provider_hint
        self.provider_hints = provider_hints
        self._do_init_defaults_from_entity()
        self._do_init_attrs()

    def __remove_duplicates(self, l):
        l2 = []
        for i in l:
            if i not in l2 and i is not None:
                l2.append(i)
        return l2

    @property
    def __fields__(self):
        return self._do_get_fields()

    def _do_get_fields(self):
        fields = []
        if self.__field_order__ is not None:
            #this makes sure all the ordered fields bubble to the start of the list
            fields.extend(self.__field_order__)
        if self.__limit_fields__ is not None:
            fields.extend(self.__limit_fields__)
            fields.extend(self.__hide_fields__)
            fields.extend(list(self.__add_fields__.keys()))
            fields = self.__remove_duplicates(fields)
            return fields
        else:
            fields = list(self.__metadata__.keys())

        fields.extend(list(self.__add_fields__.keys()))
        fields.extend(self.__hide_fields__)

        if self.__field_order__ is not None:
            fields = set(fields)
            field_order = set(self.__field_order__)
            extra_fields = fields.difference(field_order)
            fields = self.__field_order__+list(extra_fields)

        for field in self.__omit_fields__:
            while field in fields:
                fields.remove(field)

        r = []
        for field in fields:
            if field not in r and field is not None:
                r.append(field)
        return r

    @property
    def __metadata__(self):
        if not hasattr(self, '___metadata__'):
            if self.__metadata_type__ is None:
                raise ConfigBaseError('You must define a __metadata_type__ attribute for this object')
            self.___metadata__=self.__metadata_type__(self.__provider__, self.__entity__)
        return self.___metadata__

    @property
    def __provider__(self):
        if self.__entity__ is None:
            raise ConfigBaseError('You must define a __entity__ attribute for this object')
        return self.__provider_type_selector__.get_selector(self.__entity__).get_provider(self.__entity__,
                                                                                          self.provider_hint,
                                                                                          **self.provider_hints)

    def _do_init_attrs(self):
        if self.__hide_fields__ is None:
            self.__hide_fields__ = []
        if self.__disable_fields__ is None:
            self.__disable_fields__ = []
        if self.__omit_fields__ is None:
            self.__omit_fields__ = []
        if self.__add_fields__ is None:
            self.__add_fields__ = {}
        if self.__field_attrs__ is None:
            self.__field_attrs__ = {}

    def _do_init_defaults_from_entity(self):
        """
        Users model __sprox__ attribute to
        setup some defaults. This is useful for
        TGAdmin to make possible for people to
        customize its behavior without having to
        write a custom AdminConfig
        """
        sprox_meta = getattr(self.__entity__, '__sprox__', None)
        if sprox_meta:
            for attr, value in list(vars(sprox_meta).items()):
                if not attr.startswith('_'):
                    setattr(self, '__'+attr+'__', copy.deepcopy(value))
