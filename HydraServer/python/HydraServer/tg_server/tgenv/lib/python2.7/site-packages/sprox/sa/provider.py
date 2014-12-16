"""
saprovider Module

this contains the class which allows dbsprockets to interface with sqlalchemy.

Classes:
Name                               Description
SAProvider                         sqlalchemy metadata/crud provider

Exceptions:
None

Functions:
None


Copyright (c) 2007 Christopher Perkins
Original Version by Christopher Perkins 2007
Released under MIT license.
"""
import inspect
import re
from sqlalchemy import and_, or_, DateTime, Date, Interval, Integer, MetaData, desc as _desc, func
from sqlalchemy import String
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy.orm.query import Query
from sqlalchemy.orm import Mapper, _mapper_registry, SynonymProperty, object_mapper, class_mapper
from sqlalchemy.orm.exc import UnmappedClassError, NoResultFound, UnmappedInstanceError
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.schema import Column
from sprox.sa.support import PropertyLoader, resolve_entity, Binary, LargeBinary

from sprox.iprovider import IProvider
from cgi import FieldStorage
from datetime import datetime, date, timedelta
from warnings import warn

from sprox.sa.widgetselector import SAWidgetSelector
from sprox.sa.validatorselector import SAValidatorSelector
from sprox._compat import string_type

class SAORMProviderError(Exception):pass

class SAORMProvider(IProvider):


    default_widget_selector_type = SAWidgetSelector
    default_validator_selector_type = SAValidatorSelector

    def __init__(self, hint=None, **hints):
        """
        initialize me with a engine, bound metadata or bound session object
        or a combination of the above.
        """
        #if hint is None and len(hints) == 0:
        #    raise SAORMProviderError('You must provide a hint to this provider')
        self.engine, self.session, self.metadata = self._get_engine(hint, hints)

    def _get_engine(self, hint, hints):
        metadata = hints.get('metadata', None)
        engine   = hints.get('engine', None)
        session  = hints.get('session', None)

        if isinstance(hint, Engine):
            engine=hint

        if isinstance(hint, MetaData):
            metadata=hint

        if isinstance(hint, (Session, ScopedSession)):
            session = hint

        if session is not None and engine is None:
            engine = session.bind

        if metadata is not None and engine is None:
            engine = metadata.bind

        return engine, session, metadata

    def get_fields(self, entity):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)
        field_names = list(mapper.c.keys())
        for prop in mapper.iterate_properties:
            try:
                getattr(mapper.c, prop.key)
                field_names.append(prop.key)
            except AttributeError:
                mapper.get_property(prop.key)
                field_names.append(prop.key)

        return field_names

    def get_entity(self, name):
        for mapper in _mapper_registry:
            if mapper.class_.__name__ == name:
                engine = mapper.tables[0].bind
                if engine is not None and mapper.tables[0].bind != self.engine:
                    continue
                return mapper.class_
        raise KeyError('could not find model by the name %s'%(name))

    def get_entities(self):
        entities = []
        for mapper in _mapper_registry:
            engine = mapper.tables[0].bind
            if engine is not None and mapper.tables[0].bind != self.engine:
                continue
            entities.append(mapper.class_.__name__)
        return entities

    def get_field(self, entity, name):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)
        try:
            return getattr(mapper.c, name)
        except (InvalidRequestError, AttributeError):
            try:
                return mapper.get_property(name)
            except InvalidRequestError:
                raise AttributeError



    def is_binary(self, entity, name):
        field = self.get_field(entity, name)
        if isinstance(field, PropertyLoader):
            field = self._relationship_local_side(field)[0]
        if isinstance(field, SynonymProperty):
            field = self.get_field(entity, field.name)
        return isinstance(field.type, (Binary, LargeBinary))

    def is_nullable(self, entity, name):
        field = self.get_field(entity, name)
        if isinstance(field, SynonymProperty):
            field = self.get_field(entity, field.name)
        if isinstance(field, PropertyLoader):
            return getattr(self._relationship_local_side(field)[0], 'nullable')
        return getattr(field, 'nullable', True)

    def is_string(self, entity, name):
        field = self.get_field(entity, name)
        if isinstance(field, PropertyLoader):
            field = self._relationship_local_side(field)[0]
        if isinstance(field, SynonymProperty):
            field = self.get_field(entity, field.name)

        #should cover Unicode, UnicodeText, Text and so on by inheritance
        return isinstance(field.type, String)

    def get_primary_fields(self, entity):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)
        fields = []

        for field_name in self.get_fields(entity):
            try:
                value = getattr(mapper.c, field_name)
            except AttributeError:
                # Relations won't be attributes, but can't be primary anyway.
                continue
            if value.primary_key and not field_name in fields:
                fields.append(field_name)
        return fields

    def get_primary_field(self, entity):
        fields = self.get_primary_fields(entity)
        assert len(fields) > 0
        return fields[0]

    def _find_title_column(self, entity):
        entity = resolve_entity(entity)
        for column in class_mapper(entity).columns:
            if 'title' in column.info and column.info['title']:
                return column.key
        return None

    def get_view_field_name(self, entity, possible_names, item=None):
        view_field = self._find_title_column(entity)

        fields = self.get_fields(entity)
        for column_name in possible_names:
            for actual_name in fields:
                if column_name == actual_name:
                    view_field = actual_name
                    break
            if view_field:
                break
            for actual_name in fields:
                if column_name in actual_name:
                    view_field = actual_name
                    break
            if view_field:
                break
        if view_field is None:
            view_field = fields[0]
        return view_field

    def get_dropdown_options(self, entity, field_name, view_names=None):
        if view_names is None:
            view_names = ['_name', 'name', 'description', 'title']
        if self.session is None:
            warn('No dropdown options will be shown for %s.  '
                 'Try passing the session into the initialization '
                 'of your form base object so that this sprocket '
                 'can have values in the drop downs'%entity)
            return []

        field = self.get_field(entity, field_name)

        target_field = entity
        if isinstance(field, PropertyLoader):
            target_field = field.argument
        target_field = resolve_entity(target_field)

        #some kind of relation
        if isinstance(target_field, Mapper):
            target_field = target_field.class_

        pk_fields = self.get_primary_fields(target_field)

        view_name = self.get_view_field_name(target_field, view_names)

        rows = self.session.query(target_field).all()

        if len(pk_fields) == 1:
            def build_pk(row):
                return getattr(row, pk_fields[0])
        else:
            def build_pk(row):
                return "/".join([str(getattr(row, pk)) for pk in pk_fields])

        return [ (build_pk(row), getattr(row, view_name)) for row in rows ]

    def get_relations(self, entity):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)
        return [prop.key for prop in mapper.iterate_properties if isinstance(prop, PropertyLoader)]

    def is_relation(self, entity, field_name):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)

        try:
            property = mapper.get_property(field_name)
        except InvalidRequestError:
            return False

        if isinstance(property, PropertyLoader):
            return True

    def relation_fields(self, entity, field_name):
        field = getattr(entity, field_name)
        return [ col.name for col in self._relationship_local_side(field.property)]

    def is_query(self, entity, value):
        """determines if a field is a query instead of actual list of data"""
        return isinstance(value, Query)

    def is_unique(self, entity, field_name, value):
        entity = resolve_entity(entity)
        field = getattr(entity, field_name)
        try:
            self.session.query(entity).filter(field==value).one()
        except NoResultFound:
            return True
        return False

    def get_synonyms(self, entity):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)
        return [prop.key for prop in mapper.iterate_properties if isinstance(prop, SynonymProperty)]

    def _adapt_type(self, value, primary_key):
        if isinstance(primary_key.type, Integer):
            value = int(value)
        return value

    def _relationship_local_side(self, relationship):
        if hasattr(relationship, 'local_columns'):
            return list(relationship.local_columns)
        return list(relationship.local_side) #pragma: no cover

    def _modify_params_for_relationships(self, entity, params, delete_first=True):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)
        relations = self.get_relations(entity)

        for relation in relations:
            if relation in params:
                prop = mapper.get_property(relation)
                target = prop.argument
                target = resolve_entity(target)
                value = params[relation]
                if value:
                    if prop.uselist and isinstance(value, list):
                        target_obj = []
                        for v in value:
                            try:
                                object_mapper(v)
                                target_obj.append(v)
                            except UnmappedInstanceError:
                                if hasattr(target, 'primary_key'):
                                    pk = target.primary_key
                                else:
                                    pk = class_mapper(target).primary_key
                                if isinstance(v, string_type) and "/" in v:
                                    v = map(self._adapt_type, v.split("/"), pk)
                                    v = tuple(v)
                                else:
                                    v = self._adapt_type(v, pk[0])
                                #only add those items that come back
                                new_v = self.session.query(target).get(v)
                                if new_v is not None:
                                    target_obj.append(new_v)
                    elif prop.uselist:
                        try:
                            object_mapper(value)
                            target_obj = [value]
                        except UnmappedInstanceError:
                            mapper = target
                            if not isinstance(target, Mapper):
                                mapper = class_mapper(target)
                            if isinstance(mapper.primary_key[0].type, Integer):
                                value = int(value)
                            target_obj = [self.session.query(target).get(value)]
                    else:
                        try:
                            object_mapper(value)
                            target_obj = value
                        except UnmappedInstanceError:
                            if isinstance(value, string_type) and "/" in value:
                                value = map(self._adapt_type, value.split("/"), prop.remote_side)
                                value = tuple(value)
                            else:
                                value = self._adapt_type(value, list(prop.remote_side)[0])
                            target_obj = self.session.query(target).get(value)
                    params[relation] = target_obj
                else:
                    del params[relation]
        return params

    def create(self, entity, params):
        entity = resolve_entity(entity)
        params = self._modify_params_for_dates(entity, params)
        params = self._modify_params_for_relationships(entity, params)
        obj = entity()
        

        relations = self.get_relations(entity)
        mapper = class_mapper(entity)
        for key, value in params.items():
            if value is not None:
                if isinstance(value, FieldStorage):
                    value = value.file.read()
                try:
                    if key not in relations and value and isinstance(mapper.columns[key].type, Integer):
                        value = int(value)
                except KeyError:
                    pass
                setattr(obj, key, value)

        self.session.add(obj)
        self.session.flush()
        return obj

    def flush(self):
        self.session.flush()

    def dictify(self, obj, fields=None, omit_fields=None):
        if obj is None:
            return {}
        r = {}
        mapper = class_mapper(obj.__class__)
        for prop in mapper.iterate_properties:
            if fields and prop.key not in fields:
                continue

            if omit_fields and prop.key in omit_fields:
                continue

            value = getattr(obj, prop.key)
            if value is not None:
                if isinstance(prop, PropertyLoader):
                    klass = prop.argument
                    if isinstance(klass, Mapper):
                        klass = klass.class_
                    pk_name = self.get_primary_field(klass)
                    if isinstance(value, list) or isinstance(value, Query):
                        #joins
                        value = [getattr(value, pk_name) for value in value]
                    else:
                        #fks
                        value = getattr(value, pk_name)
            r[prop.key] = value
        return r

    def get_field_default(self, field):
        if isinstance(field, Column) and field.default and getattr(field.default, 'arg', None) is not None:
            if isinstance(field.default.arg, (string_type, int, float)):
                return (True, field.default.arg)
        return (False, None)

    def get_field_provider_specific_widget_args(self, entity, field, field_name):
        args = {}
        if isinstance(field, PropertyLoader):
            args['provider'] = self
            args['nullable'] = self.is_nullable(entity, field_name)
        return args

    def get_default_values(self, entity, params):
        return params

    def get_obj(self, entity, params, fields=None, omit_fields=None):
        obj = self._get_obj(entity, params)
        return obj

    def get(self, entity, params, fields=None, omit_fields=None):
        obj = self.get_obj(entity, params, fields)
        return self.dictify(obj, fields, omit_fields)

    def query(self, entity, limit=None, offset=None, limit_fields=None,
            order_by=None, desc=False, field_names=[], filters={},
            substring_filters=[], **kw):
        entity = resolve_entity(entity)
        query = self.session.query(entity)

        filters = self._modify_params_for_dates(entity, filters)
        filters = self._modify_params_for_relationships(entity, filters)

        for field_name, value in filters.items():
            field = getattr(entity, field_name)
            if self.is_relation(entity, field_name) and isinstance(value, list):
                value = value[0]
                query = query.filter(field.contains(value))
            elif field_name in substring_filters and self.is_string(entity, field_name):
                escaped_value = re.sub('[\\\\%\\[\\]_]', '\\\\\g<0>', value.lower())
                query = query.filter(func.lower(field).contains(escaped_value, escape='\\'))
            else:
                query = query.filter(field==value) 

        count = query.count()

        if order_by is not None:
            if self.is_relation(entity, order_by):
                mapper = class_mapper(entity)
                class_ = None
                for prop in mapper.iterate_properties:
                    try:
                        class_ = prop.mapper.class_
                    except (AttributeError, KeyError):
                        pass
                query = self.session.query(entity).join(order_by)
                f = self.get_view_field_name(class_, field_names)
                field = self.get_field(class_, f)
            else:
                field = self.get_field(entity, order_by)

            if desc:
                field = _desc(field)
            query = query.order_by(field)

        if offset is not None:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)

        objs = query.all()

        return count, objs


    def _modify_params_for_dates(self, entity, params):
        entity = resolve_entity(entity)
        mapper = class_mapper(entity)
        for key, value in list(params.items()):
            if key in mapper.c and value is not None:
                field = mapper.c[key]
                if hasattr(field, 'type'):
                    if isinstance(field.type, DateTime):
                        if not isinstance(value, datetime):
                            dt = datetime.strptime(value[:19], '%Y-%m-%d %H:%M:%S')
                            params[key] = dt
                    elif isinstance(field.type, Date):
                        if not isinstance(value, date):
                            dt = datetime.strptime(value, '%Y-%m-%d').date()
                            params[key] = dt
                    elif isinstance(field.type, Interval):
                        if not isinstance(value, timedelta):
                            d = re.match(
                                r'((?P<days>\d+) days, )?(?P<hours>\d+):'
                                r'(?P<minutes>\d+):(?P<seconds>\d+)',
                                str(value)).groupdict(0)
                            dt = timedelta(**dict(( (key, int(value))
                                                    for key, value in list(d.items()) )))
                            params[key] = dt
        return params

    def _remove_related_empty_params(self, obj, params, omit_fields=None):
        entity = obj.__class__
        mapper = class_mapper(entity)
        relations = self.get_relations(entity)
        for relation in relations:
            if omit_fields and relation in omit_fields:
                continue

            #clear out those items which are not found in the params list.
            if relation not in params or not params[relation]:
                related_items = getattr(obj, relation)
                if related_items is not None:
                    if hasattr(related_items, '__iter__'):
                        setattr(obj, relation, [])
                    else:
                        setattr(obj, relation, None)

    def _get_obj(self, entity, pkdict):
        entity = resolve_entity(entity)
        pk_names = self.get_primary_fields(entity)
        pks = tuple([pkdict[n] for n in pk_names])
        return self.session.query(entity).get(pks)

    def update(self, entity, params, omit_fields=None):
        params = self._modify_params_for_dates(entity, params)
        params = self._modify_params_for_relationships(entity, params)
        obj = self._get_obj(entity, params)
        relations = self.get_relations(entity)
        mapper = object_mapper(obj)
        for key, value in params.items():
            if omit_fields and key in omit_fields:
                continue

            if isinstance(value, FieldStorage):
                value = value.file.read()
            # this is done to cast any integer columns into ints before they are
            # sent off to the interpreter.  Oracle really needs this.
            try:
                if key not in relations and value and isinstance(mapper.columns[key].type, Integer):
                    value = int(value)
            except KeyError:
                pass
            setattr(obj, key, value)

        self._remove_related_empty_params(obj, params, omit_fields)
        self.session.flush()
        return obj

    #this is hard to test because of some kind of rollback issue in the test framework
    def delete(self, entity, params):
        obj = self._get_obj(entity, params)  # pragma: no cover
        self.session.delete(obj)  # pragma: no cover
        return obj  # pragma: no cover

    def get_field_widget_args(self, entity, field_name, field):
        args = {}
        prop = getattr(field, 'property', None)
        if prop and isinstance(prop, PropertyLoader):
            args['provider'] = self
            args['nullable'] = self.is_nullable(entity, field_name)
        return args

    def is_unique_field(self, entity, field_name):
        field = self.get_field(entity, field_name)
        if hasattr(field, 'unique') and field.unique:
            return True
        return False
