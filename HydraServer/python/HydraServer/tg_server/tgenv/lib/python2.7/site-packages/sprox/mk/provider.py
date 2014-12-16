"""
mongokitprovider Module

This contains the class which allows sprox to interface with any database.

Copyright &copy 2009 Jorge Vargas
Original Version by Jorge Vargas 2009
Released under MIT license.
"""
from sprox.iprovider import IProvider
from sprox.util import timestamp
from mongokit.pylons.document import MongoDocument
from pymongo.binary import Binary
import datetime

from .widgetselector import MongoKitWidgetSelector
from .validatorselector import MongoKitValidatorSelector

class MongoKitProvider(IProvider):
    
    default_widget_selector_type = MongoKitWidgetSelector
    default_validator_selector_type = MongoKitValidatorSelector
    
    def __init__(self,mapper):
        self.mapper = mapper

    def get_field(self, entity, name):
        """Get a field with the given field name."""
        return entity.structure[name]

    def get_fields(self, entity):
        """Get all of the fields for a given entity."""
        return list(entity.structure.keys())

    def get_entity(self, name):
        """Get an entity with the given name."""
        for entity in mapper:
            if name == entity.name:
                return entity
        raise KeyError('could not find model by the name %s'%(name))

    def get_entities(self):
        """Get all entities available for this provider."""
        return mapper

    def get_primary_fields(self, entity):
        """Get the fields in the entity which uniquely identifies a record."""
        return [self.get_primary_field(entity)]

    def get_primary_field(self, entity):
        """Get the single primary field for an entity"""
        #FIXME is this correct? it seems like a SA thing but in mongo _id is required
        return '_id'

    def get_view_field_name(self, entity, possible_names):
        """Get the name of the field which first matches the possible colums

        :Arguments:
          entity
            the entity where the field is located
          possible_names
            a list of names which define what the view field may contain.  This allows the first
            field that has a name in the list of names will be returned as the view field.
        """
        fields = self.get_fields(entity)
        view_field = None
        for column_name in possible_names:
            for actual_name in fields:
                if column_name == actual_name:
                    view_field = actual_name
                    break
            if view_field:
                break;
            for actual_name in fields:
                if column_name in actual_name:
                    view_field = actual_name
                    break
            if view_field:
                break;
        if view_field is None:
            view_field = fields[0]
        return view_field

    def get_dropdown_options(self, entity, field_name, view_names=None):
        """Get all dropdown options for a given entity field.

        :Arguments:
          entity
            the entity where the field is located
          field_name
            name of the field in the entity
          view_names
            a list of names which define what the view field may contain.  This allows the first
            field that has a name in the list of names will be returned as the view field.

        :Returns:
        A list of tuples with (id, view_value) as items.

        """
        raise NotImplementedError

    def get_relations(self, entity):
        """Get all of the field names in an enity which are related to other entities."""
#        return [fieldname for fieldname,field in entity.structure.items() if issubclass(field,Document)
        raise NotImplementedError

    def is_relation(self, entity, field_name):
        """Determine if a field is related to a field in another entity."""
        #TODO improve on this method to decouple from mongokit
        fieldtype = entity.structure[field_name]
        return issubclass(MongoDocument,fieldtype)

    def is_nullable(self, entity, field_name):
        """Determine if a field is nullable."""
        return field_name in entity.required_fields

    def get_field_default(self, field):
        return (False, None)

    def get_field_provider_specific_widget_args(self, field_name, field):
        return {}

    def get_default_values(self, entity, params):
        """Get the default values for form filling based on the database schema."""
        return entity.default_values

    def _cast_value(self, entity, key, value):
        if entity.structure[key] == datetime.datetime:
            return timestamp(value)
        return value
    
    def create(self, entity, params):
        """Create an entry of type entity with the given params."""
        obj = entity()
        for key,value in params.items():
            if key not in entity.structure:
                continue;
            value = self._cast_value(entity, key, value)
            if value is not None:
                setattr(obj,key,value)
        obj.save()
        return obj

    def get(self, entity, params):
        """Get a single entry of type entity which matches the params."""
#        if params:
#            raise NotImplementedError
        pk_name = self.get_primary_field(entity)
        return entity.get_from_id(params[pk_name])

    def update(self, entity, params):
        """Update an entry of type entity which matches the params."""
        pk_name = self.get_primary_field(entity)
        obj = entity.get_from_id(params[pk_name])
        params.pop('_id')
        params.pop('sprox_id')
        params.pop('_method')
        for key, value in params.items():
            if key not in entity.structure:
                continue;
            value = self._cast_value(entity, key, value)
            if value is not None:
                setattr(obj,key,value)
        obj.save()
        return obj

    def delete(self, entity, params):
        """Delete an entry of typeentity which matches the params."""
        pk_name = self.get_primary_field(entity)
        obj = entity.get_from_id(params[pk_name])
        obj.delete()
        return obj
        
    def query(self, entity, limit=None, offset=None, limit_fields=None, order_by=None, desc=False, **kw):
        query = entity.all()
        #XXX is count supposed to be ALL records or all records returned?
        count = query.count()
        if order_by is not None:
            pass #TODO
            if desc:
                pass #TODO
        if offset is not None:
            pass #TODO
        if limit is not None:
            query.limit(limit=int(limit))

        objs = entity.all()

        return count, objs

    def is_binary(self, entity, name):
        field = self.get_field(entity, name)
        return isinstance(field,Binary)
