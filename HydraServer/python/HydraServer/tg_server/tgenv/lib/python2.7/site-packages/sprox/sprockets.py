"""
Sprockets Module

This is sort of like the central nervous system of sprox.  Views and
Sessions are collected in separate caches and served up as sprockets.
The cache objects may be solidified at some point with a parent class.
They work for right now.


Classes:
Name           Description
SprocketCache  A cache of Sprockets
Sprocket       A binding of Filler and View configs
ConfigCache    Individual configs cached

Functions:
None

Copyright (c) 2007 Christopher Perkins
Original Version by Christopher Perkins 2007
Released under MIT license.
"""

from .providerselector import SAORMSelector
from .formbase import FormBase, AddRecordForm, EditableForm
from .tablebase import TableBase
from .entitiesbase import EntitiesBase, EntityDefBase

from .fillerbase import ModelsFiller, ModelDefFiller, EditFormFiller, AddFormFiller, FormFiller, TableFiller

class ConfigCacheError(Exception): pass

class ConfigCache(object):
    default_configs = {}

    def __init__(self, session, metadata=None):
        self.session = session
        self.metadata = metadata
        self._provider = SAORMSelector.get_provider(hint=session.bind, metadata=self.metadata)

    separator = '__'
    def _split_identifiers(self, key):
        separator = '__'
        if self.separator not in key:
            identifier = None
            view_type = key
        else:
            view_type, identifier = key.split(self.separator)
        return view_type, identifier

    def _get(self, key):
        view_type, identifier = self._split_identifiers(key)
        if view_type not in self.default_configs:
            raise ConfigCacheError('view_type:%s not found in default bases'%view_type)

        base = self.default_configs[view_type]
        if key != 'model_view':
            base.__entity__   = self._provider.get_entity(identifier)
        base.__provider__ = self._provider
        base.__sprox_id__ = key
        return base(self.session)

    def __getitem__(self, key):
        #xxx: turn caching back on
        #if key in self.__dict__:
        #    return self.__dict__[key]
        view = self._get(key)
        #self.__dict__[key] = view
        return view

class ViewCache(ConfigCache):
    default_configs = { 'model_view'   : EntitiesBase,
                        'edit'         : EditableForm,
                        'add'          : AddRecordForm,
                        'listing'      : TableBase,
                        'metadata'     : EntityDefBase,
                        }
class FillerCache(ConfigCache):
    """
        Container for Fillers
    """

    default_configs = { 'model_view'   : ModelsFiller,
                        'metadata'     : ModelDefFiller,
                        'view'         : FormFiller,
                        'listing'      : TableFiller,
                        'edit'         : EditFormFiller,
                        'add'          : AddFormFiller,
                        }
class Sprocket:
    """Association between a view and a sessionConfig"""

    def __init__(self, view, filler):
        """Construct a Sprocket Object

        view
            a ``view`` object which has been instantiated from a ``ViewConfig``
        filler
            defines how the view should be filled
        """

        self.view      = view
        self.filler    = filler

class SprocketCache(object):
    """Set of Associations between widgets and the method to obtain their data
       caching is disabled for now

    """
    def __init__(self, session, metadata=None):
        self.views    = self.view_type(session, metadata=metadata)
        self.fillers  = self.filler_type(session, metadata=metadata)

    view_type   = ViewCache
    filler_type = FillerCache
    sprocket_type = Sprocket

    def __getitem__(self, key):
        """
        """
        #xxx: enable caching
        #if key in self.__dict__:
        #    import pdb; pdb.set_trace()
        #    return self.__dict__.__getitem__(key)
        sprocket = self._get_sprocket(key)
        #self.__dict__[key] = sprocket
        return sprocket

    def _get_sprocket(self, key):
        view = self.views[key]
        filler = self.fillers[key]
        return self.sprocket_type(view, filler)

    #xxx: enable caching
    #def __setitem__(self, key, item):
    #    return
        #if not isinstance(item, Sprocket):
        #    raise TypeError('item must be of type Sprocket')
        #return self.__dict__.__setitem__(key, item)

