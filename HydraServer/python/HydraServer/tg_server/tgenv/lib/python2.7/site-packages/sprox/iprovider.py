"""
iprovider Module

This contains the class which allows sprox to interface with any database
via an object-relational mapper (ORM) or object-document mapper (ODM).

Definitions:

    Mapped object
        An object which is implemented by the ORM/ODM and represents
        a record or document in the database.
    Entity
        An object that represents a set of mapped objects that are part
        of the same collection or table in the database.
    Field
        A definition of aproperty of a mapped object which is accessible
        using attribute notation. An entity contains a set of fields,
        and each field has unique name within the entity.
    Property field
        A field which contains a value which is presentable to the user.
    Foreign key field
        A field which contains a value that refers to another object in
        the database, and which is hidden from the user.
    Relation field
        A field whose value is another mapped object in the database, or a list
        or set of such objects. The referened objects are presented to the user
        using a selection or prompting widget.
    Primary key field
        A set of fields which identify the mapped object within the entity.
        Need not be user-presentable.
    Dictify
        An alternative representation of a mapped object as a dictionary; see
        the dictify method docstring for details.



Copyright &copy 2008 Christopher Perkins
Original Version by Christopher Perkins 2008
Released under MIT license.
"""

class IProvider:
    def __init__(self, hint=None, **hints):
        pass
    def get_field(self, entity, name):
        """Get a field with the given field name."""
        raise NotImplementedError

    def get_fields(self, entity):
        """Get all of the fields for a given entity, excluding foreign key fields.

        :Arguments:
          entity
            An entity or entity thunk.

        :Returns:
          A list of names of the fields that are not foreign keys in the entity.
        """
        raise NotImplementedError

    def get_entity(self, name):
        """Get an entity with the given name."""
        raise NotImplementedError

    def get_entities(self):
        """Get all entities available for this provider."""
        raise NotImplementedError

    def get_primary_fields(self, entity):
        """Get the fields in the entity which uniquely identifies a record.

        :Returns:
          A sequence of fields which when taken together uniquely identify a record.
        """
        raise NotImplementedError

    def get_primary_field(self, entity):
        """Get the single primary field for an entity. A single primary field is
        required for EditableForm and AddRecordForm-based forms.

        :Returns:
          A field which unique identifies a record. Raises an exception if there is no
          such field.
        """
        raise NotImplementedError

    def get_view_field_name(self, entity, possible_names):
        """Get the name of the field which first matches the possible colums

        :Arguments:
          entity
            the entity where the field is located
          possible_names
            a list of names which define what the view field may contain.  This allows the first
            field that has a name in the list of names will be returned as the view field.
        """
        raise NotImplementedError

    def get_dropdown_options(self, entity_or_field, field_name=None, view_names=None):
        """Get all dropdown options for a given entity field.

        :Arguments:
          entity_or_field
            either the entity where the field is located, or the field itself
          field_name
            if the entity is specified, name of the field in the entity. Otherwise, None
          view_names
            a list of names which define what the view field may contain.  This allows the first
            field that has a name in the list of names will be returned as the view field.

        :Returns:
          A list of tuples with (id, view_value) as items.

        """
        raise NotImplementedError

    def get_relations(self, entity):
        """Get all of the field names in an enity which are relation fields.

        :Returns:
          A list of names of relation fields in the entity.
          
        """
        raise NotImplementedError

    def is_relation(self, entity, field_name):
        """Determine if a field is a relation field."""
        raise NotImplementedError

    def is_nullable(self, entity, field_name):
        """Determine if a field is nullable."""
        raise NotImplementedError

    def get_default_values(self, entity, params):
        """Get the default values for form filling based on the database schema."""
        # XXX stubbed out in the SA provider and might not be a good API
        raise NotImplementedError

    def create(self, entity, params):
        """Create an entry of type entity with the given params."""
        raise NotImplementedError

    def get_obj(self, entity, params):
        """Get a single mapped object of type entity which matches the params."""
        raise NotImplementedError

    def get(self, entity, params, fields=None, omit_fields=None):
        """Get a single dictify of type entity which matches the params.

        Equivalent to dictify(get(entity, params), fields, omit_fields)."""
        raise NotImplementedError

    def update(self, entity, params):
        """Update an entry of type entity which matches the params."""
        raise NotImplementedError

    def delete(self, entity, params):
        """Delete an entry of typeentity which matches the params."""
        raise NotImplementedError

    def query(self,entity,limit=None,offset=None,limit_fields=None,order_by=None,desc=False):
        """Perform a query against this entity.

        :Arguments:
          entity
            the entity to be queried
          limit
            the maximum number of objects to return. If unspecified, returns all objects
          offset
            the number of objects at the start of the result set to skip. Defaults to 0
          limit_fields
            XXX
          order_by
            the name of a field to sort by. If unspecified, no sorting is done
          desc
            if true, the sort order is descending. Otherwise, it is ascending.

        :Returns:
          A tuple (count, iter) where iter is an iterator of mapped objects.
        """
        raise NotImplementedError

    def is_binary(self,entity,name):
        """Determine if the field in the entity is a binary field."""
        raise NotImplementedError

    def relation_fields(self, entity, field_name):
        """For the relation field with the given name, return the corresponding foreign key field(s) in the entity.

        :Returns:
          A list of the names of the foreign key fields.
        """
        raise NotImplementedError

    def get_field_widget_args(self, entity, field_name, field):
        """Return a dict with any additional arguments that should be passed for the widget for the field in the entity.

        :Returns:
          A dict of additional widget arguments."""
        return {}

    def is_unique(self, entity, field_name, value):
        """Return true if the value for field is not yet used within the entity."""
        # XXX rename this method to something better
        return True

    def is_unique_field(self, entity, field_name):
        """Return true if the field within the entity is a primary or alternate key."""
        return False

    def dictify(self, obj, fields=None, omit_fields=None):
        """Return a dictionary with keys being the names of fields in te object
        and values being the values of those fields, except that values that are
        mapped objects are replaced with the value of the corresponding primary
        key of the related object instead of the actual mapped object.

        :Arguments:
          obj
            a mapped object or None. If None, the return value is {}
          fields
            a container of field names or None. If None, all fields that are not
            in omit_fields are returned. Otherwise only those fields that are in
            fields and not in omit_fields are returned.
          omit_fields:
            a container of field names or None. If None, no fields are omitted.

        :Returns:
          A dictionary of {field_name: value} where field_name is the name of a
          property field or relation field, and value is the value of the property
          field or the related primary key value.
        """
        # XXX this api is going to have trouble with subdocuments, because
        # we may not necessary be able to derive a primary key reference to
        # a subdocument. (if they embedded in a non-relational db)
        raise NotImplementedError

