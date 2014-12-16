"""
matadata Module

This contains the class which defines the generic interface for metadata.
Basically, it provides an interface for how data is extracted from the provider
for widget generation.

Copyright (c) 2008 Christopher Perkins
Original Version by Christopher Perkins 2007
Released under MIT license.
"""
from sprox.iprovider import IProvider

class MetadataError(Exception):pass
class NotFoundError(Exception):pass

class Metadata(dict):
    """Base Metadata class

    Metadatas are dictionary-like.  They map attributes
    of the entity they wrap, so that attributes of the entity
    can be examined without being explicitly set.  Elements
    of a metadata can be set if they are not already part of the
    wrapped entity.  This allows for customization of the
    metadata without modification to the wrapped metadata.
    """
    def __init__(self, provider, entity=None):
        self.provider = provider
        self.entity = entity

    def __setitem__(self, key, value):
        self._do_check_set_item(key, value)
        dict.__setitem__(self, key, value)

    def _do_get_item(self, item):
        raise NotImplementedError

    def _do_keys(sekf):
        raise NotImplementedError

    def _do_check_set_item(self, key, value):
        raise NotImplementedError

    def __getitem__(self, item):
        try:
            value = self._do_get_item(item)
            return value
        except NotFoundError:
            return dict.__getitem__(self, item)

    def keys(self):
        r = self._do_keys()
        r.extend(dict.keys(self))
        return r

class EntitiesMetadata(Metadata):
    """A class to extract entities from a database definition.
    """
    def _do_get_item(self, name):
        if name in self.provider.get_entities():
            return self.provider.get_entity(name)
        raise NotFoundError

    def _do_keys(self):
        entities = sorted(self.provider.get_entities())
        return entities

class FieldsMetadata(Metadata):
    """A class to extract fields from an entity.
    """
    def __init__(self, provider, entity):
        Metadata.__init__(self, provider, entity)
        self.provider = provider
        self.entity = entity

    def _do_check_set_item(self, key, value):
        if key in self.provider.get_fields(self.entity):
            raise MetadataError('%s is already found in entity: %s'%(key, self.entity))

    def _do_get_item(self, item):
        try:
            return self.provider.get_field(self.entity, item)
        except AttributeError:
            #XXX I'm not sure  if we should change the type,but we shouldn't swallow with except:
            if dict.__contains__(self, item):
                return dict.get(self, item)
            raise NotFoundError(self.entity,item)

    def _do_keys(self):
        return self.provider.get_fields(self.entity)

class FieldMetadata(Metadata):
    """In the future, if the Field attributes need to be extracted, this is where it will happen.
    """
    pass

