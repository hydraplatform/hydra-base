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

from sprox.sprockets import ConfigCache, SprocketCache

from sprox.providerselector import SAORMSelector
from sprox.formbase import FormBase, AddRecordForm, EditableForm
from sprox.entitiesbase import EntitiesBase, EntityDefBase

from sprox.fillerbase import ModelsFiller, ModelDefFiller, EditFormFiller, AddFormFiller, FormFiller
from .fillerbase import DojoTableFiller
from .tablebase import DojoTableBase

class ViewCache(ConfigCache):
    default_configs = { 'model_view'   : EntitiesBase,
                        'edit'         : EditableForm,
                        'add'          : AddRecordForm,
                        'listing'      : DojoTableBase,
                        'metadata'     : EntityDefBase,
                        }

    json_url = 'data'

    def __getitem__(self, key):
        view = super(ViewCache, self).__getitem__(key)
        return view

class FillerCache(ConfigCache):
    default_configs = { 'model_view'   : ModelsFiller,
                        'metadata'     : ModelDefFiller,
                        'view'         : FormFiller,
                        'listing'      : DojoTableFiller,
                        'edit'         : EditFormFiller,
                        'add'          : AddFormFiller,
                        }

class DojoSprocketCache(SprocketCache):
    view_type   = ViewCache
    filler_type = FillerCache