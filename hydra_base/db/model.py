#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
from sqlalchemy import Column,\
ForeignKey,\
text,\
Integer,\
String,\
LargeBinary,\
TIMESTAMP,\
BIGINT,\
SMALLINT,\
Numeric,\
Text, \
DateTime,\
Unicode

from hydra_base.lib.objects import JSONObject

from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.ext.declarative import declared_attr

import datetime

from sqlalchemy import inspect, func

from ..exceptions import HydraError, PermissionError, ResourceNotFoundError

from sqlalchemy.orm import relationship, backref

from sqlalchemy.orm import noload, joinedload


from . import DeclarativeBase as Base, get_session

from ..util import generate_data_hash, get_val, get_json_as_string

from sqlalchemy.sql.expression import case
from sqlalchemy import UniqueConstraint, and_
from sqlalchemy.dialects import mysql

import pandas as pd

from sqlalchemy.orm import validates

import json
from .. import config
import logging
import bcrypt
log = logging.getLogger(__name__)


# Python 2 and 3 compatible string checking
# TODO remove this when Python2 support is dropped.
try:
    basestring
except NameError:
    basestring = str

def get_user_id_from_engine(ctx):
    """
        The session will have a user ID bound to it when checking for the permission.
    """
    if hasattr(get_session(), 'user_id'):
        return get_session().user_id
    else:
        return None

#***************************************************
#Data
#***************************************************

def _is_admin(user_id):
    """
        Is the specified user an admin
    """

    user = get_session().query(User).filter(User.id==user_id).one()

    if user.is_admin():
        return True
    else:
        return False


class Inspect(object):
    _parents = []
    _children = []

    def get_columns_and_relationships(self):
        return inspect(self).attrs.keys()

class AuditMixin(object):

    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    @declared_attr
    def created_by(cls):
        return Column(Integer, ForeignKey('tUser.id'), default=get_user_id_from_engine)

    @declared_attr
    def updated_by(cls):
        return Column(Integer, ForeignKey('tUser.id'), onupdate=get_user_id_from_engine)

    updated_at = Column(DateTime,  nullable=False, default=datetime.datetime.utcnow(), onupdate=datetime.datetime.utcnow())

class PermissionControlled(object):
    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = self.__ownerclass__()
            setattr(owner, self.__ownerfk__,  self.id)
            owner.user_id = int(user_id)
            self.owners.append(owner)

        owner.view  = read
        owner.edit  = write
        owner.share = share
        return owner

    def unset_owner(self, user_id):
        owner = None
        if str(user_id) == str(self.created_by):
            log.warning("Cannot unset %s as owner, as they created the dataset", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def _is_open(self):
        """
            Check to see whether this entity is visible globally, without any
            need for permission checking
        """
        pass

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this dataset
        """
        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True


        #Check if this entity is publicly open, therefore no need to check permissions.
        if self._is_open() == True:
            return True

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have read"
                             " access on dataset %s" %
                             (user_id, self.id))
            else:
                return False
        #Check that the user is in a group which can read this network
        self.check_group_read_permission(user_id)

        return True

    def check_group_read_permission(self, user_id):
        """
            1: Find which user groups a user is if
            2: Check if any of these groups has permission to read the object
        """

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this dataset
        """
        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have edit"
                             " access on dataset %s" %
                             (user_id, self.id))
            else:
                return False

        return True

    def check_share_permission(self, user_id, is_admin=None):
        """
            Check whether this user can write this dataset
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on dataset %s" %
                             (user_id, self.id))


#***************************************************
# Classes definition
#***************************************************

class DatasetOwner(Base, Inspect):
    """
    """

    __tablename__='tDatasetOwner'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User')
    dataset = relationship('Dataset', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

    _parents  = ['tDataset', 'tUser']
    _children = []

class Dataset(Base, Inspect, PermissionControlled, AuditMixin):
    """
        Table holding all the attribute values
    """
    __tablename__='tDataset'

    __ownerclass__ = DatasetOwner
    __ownerfk__    = 'dataset_id'

    id         = Column(Integer(), primary_key=True, index=True, nullable=False)
    name       = Column(String(200),  nullable=False)
    type       = Column(String(60),  nullable=False)
    unit_id    = Column(Integer(), ForeignKey('tUnit.id'),  nullable=True)
    hash       = Column(BIGINT(),  nullable=False, unique=True)
    cr_date    = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    hidden     = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    value = Column('value', Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
    unit = relationship('Unit', backref=backref("dataset_unit", order_by=unit_id))

    _parents  = ['tResourceScenario', 'tUnit']
    _children = ['tMetadata']

    def set_metadata(self, metadata_tree):
        """
            Set the metadata on a dataset

            **metadata_tree**: A dictionary of metadata key-vals.
            Transforms this dict into an array of metadata objects for
            storage in the DB.
        """
        if metadata_tree is None:
            return
        if isinstance(metadata_tree, str):
            metadata_tree = json.loads(metadata_tree)

        existing_metadata = []
        for m in self.metadata:
            existing_metadata.append(m.key)
            if m.key in metadata_tree:
                if m.value != metadata_tree[m.key]:
                    m.value = metadata_tree[m.key]


        for k, v in metadata_tree.items():
            if k not in existing_metadata:
                m_i = Metadata(key=str(k),value=str(v))
                self.metadata.append(m_i)

        metadata_to_delete =  set(existing_metadata).difference(set(metadata_tree.keys()))
        for m in self.metadata:
            if m.key in metadata_to_delete:
                get_session().delete(m)

    def get_val(self, timestamp=None):
        """
            If a timestamp is passed to this function,
            return the values appropriate to the requested times.

            If the timestamp is *before* the start of the timeseries data, return None
            If the timestamp is *after* the end of the timeseries data, return the last
            value.

            The raw flag indicates whether timeseries should be returned raw -- exactly
            as they are in the DB (a timeseries being a list of timeseries data objects,
            for example) or as a single python dictionary
        """
        val = get_val(self, timestamp)
        return val

    def set_hash(self,metadata=None):


        if metadata is None:
            metadata = self.get_metadata_as_dict()

        dataset_dict = dict(name      = self.name,
                           unit_id       = self.unit_id,
                           type       = self.type,
                           value      = self.value,
                           metadata   = metadata)

        data_hash = generate_data_hash(dataset_dict)

        self.hash = data_hash

        return data_hash

    def get_metadata_as_dict(self):
        metadata = {}
        for r in self.metadata:
            val = str(r.value)

            metadata[str(r.key)] = val

        return metadata


    def _is_open(self):
        """
            Check if this dataset is globally open (the default). This negates the
            need to check for ownership
        """
        if self.hidden == 'N':
            return True

        return False

class DatasetCollection(Base, Inspect):
    """
    """

    __tablename__='tDatasetCollection'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    _parents  = ['tDataset']
    _children = ['tDatasetCollectionItem']

class DatasetCollectionItem(Base, Inspect):
    """
    """

    __tablename__='tDatasetCollectionItem'

    collection_id = Column(Integer(), ForeignKey('tDatasetCollection.id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    collection = relationship('DatasetCollection', backref=backref("items", order_by=dataset_id, cascade="all, delete-orphan"))
    dataset = relationship('Dataset', backref=backref("collectionitems", order_by=dataset_id,  cascade="all, delete-orphan"))

    _parents  = ['tDatasetCollection']
    _children = []

class Metadata(Base, Inspect):
    """
    """

    __tablename__='tMetadata'

    dataset_id = Column(Integer(), ForeignKey('tDataset.id',  ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    key       = Column(String(60), primary_key=True, nullable=False)
    value     = Column(String(1000),  nullable=False)

    dataset = relationship('Dataset', backref=backref("metadata", order_by=dataset_id, cascade="all, delete-orphan"))


    _parents  = ['tDataset']
    _children = []
#********************************************************
#Attributes & Templates
#********************************************************

class Attr(Base, Inspect):
    """
    """

    __tablename__='tAttr'

    __table_args__ = (
        UniqueConstraint('name', 'dimension_id', name="unique name dimension_id"),
    )

    id           = Column(Integer(), primary_key=True, nullable=False)
    name         = Column(String(200),  nullable=False)
    dimension_id    = Column(Integer(), ForeignKey('tDimension.id'), nullable=True)
    description  = Column(String(1000))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    dimension = relationship('Dimension', backref=backref("attributes", uselist=True))

    _parents  = ['tDimension']
    _children = []

class AttrMap(Base, Inspect):
    """
    """

    __tablename__='tAttrMap'

    attr_id_a = Column(Integer(), ForeignKey('tAttr.id'), primary_key=True, nullable=False)
    attr_id_b = Column(Integer(), ForeignKey('tAttr.id'), primary_key=True, nullable=False)

    attr_a = relationship("Attr", foreign_keys=[attr_id_a], backref=backref('maps_to', order_by=attr_id_a))
    attr_b = relationship("Attr", foreign_keys=[attr_id_b], backref=backref('maps_from', order_by=attr_id_b))

class AttrGroup(Base, Inspect):

    """
        **exclusive** : If 'Y' then an attribute in this group cannot be in any other groups

    """

    __tablename__='tAttrGroup'

    __table_args__ = (
        UniqueConstraint('name', 'project_id', name="unique attr group name"),
    )

    id               = Column(Integer(), primary_key=True, nullable=False, index=True)
    name             = Column(String(200), nullable=False)
    description      = Column(String(1000))
    layout           = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
    exclusive        = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    project_id       = Column(Integer(), ForeignKey('tProject.id'), primary_key=False, nullable=False)
    cr_date          = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    project          = relationship('Project', backref=backref('attrgroups', uselist=True, cascade="all, delete-orphan"), lazy='joined')


    _parents  = ['tProject']
    _children = []

class AttrGroupItem(Base, Inspect):
    """
        Items within an attribute group. Groupings are network dependent, and you can't
        have an attribute in a group twice, or an attribute in two groups.
    """

    __tablename__='tAttrGroupItem'

    group_id    = Column(Integer(), ForeignKey('tAttrGroup.id'), primary_key=True, nullable=False)
    attr_id    = Column(Integer(), ForeignKey('tAttr.id'), primary_key=True, nullable=False)
    network_id    = Column(Integer(), ForeignKey('tNetwork.id'), primary_key=True, nullable=False)

    group = relationship('AttrGroup', backref=backref('items', uselist=True, cascade="all, delete-orphan"), lazy='joined')
    attr = relationship('Attr')
    network = relationship('Network', backref=backref('attrgroupitems', uselist=True, cascade="all, delete-orphan"), lazy='joined')


    _parents  = ['tAttrGroup']
    _children = []

class ResourceAttrMap(Base, Inspect):
    """
    """

    __tablename__ = 'tResourceAttrMap'

    network_a_id = Column(Integer(), ForeignKey('tNetwork.id'),
                          primary_key=True, nullable=False)
    network_b_id = Column(Integer(), ForeignKey('tNetwork.id'),
                          primary_key=True, nullable=False)
    resource_attr_id_a = Column(Integer(), ForeignKey('tResourceAttr.id'),
                                primary_key=True, nullable=False)
    resource_attr_id_b = Column(Integer(), ForeignKey('tResourceAttr.id'),
                                primary_key=True, nullable=False)

    resourceattr_a = relationship("ResourceAttr", foreign_keys=[resource_attr_id_a])
    resourceattr_b = relationship("ResourceAttr", foreign_keys=[resource_attr_id_b])

    network_a = relationship("Network", foreign_keys=[network_a_id])
    network_b = relationship("Network", foreign_keys=[network_b_id])

class Template(Base, Inspect):
    """
    Template
    """

    __tablename__ = 'tTemplate'

    id = Column(Integer(), primary_key=True, nullable=False)
    parent_id = Column(Integer(), ForeignKey('tTemplate.id'))
    name = Column(String(200), unique=True)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    description = Column(String(1000))
    created_by = Column(Integer(), ForeignKey('tUser.id'))
    project_id = Column(Integer(), ForeignKey('tProject.id', ondelete='CASCADE'))
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    layout  = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)

    parent = relationship('Template', remote_side=[id], backref=backref("children", order_by=id))

    _parents = []
    _children = ['tTemplateType']

    def set_inherited_columns(self, parent, child, table):
        """
            Set the value on the column of a target child.
            This checks if the value is null on the child, and sets it from
            the parent if so
        """
        inherited_columns = []
        for column in table.__table__.columns:

            colname = column.name

            if hasattr(table, '_protected_columns')\
               and colname in table._protected_columns:
                # as a child, you can't change stuff like IDs, cr dates etc.
                continue

            newval = getattr(parent, colname)

            if colname == 'layout':
                newval = get_json_as_string(newval)

            refval = getattr(child, colname)

            if refval is None:
                inherited_columns.append(colname)
                setattr(child, colname, newval)

        if hasattr(child, 'inherited_columns') and child.inherited_columns is not None:
            for c in inherited_columns:
                if c not in child.inherited_columns:
                    child.inherited_columns.append(c)
        else:
            child.inherited_columns = inherited_columns

        return child

    def get_typeattr(self, typeattr_id, child_typeattr=None, get_parent_types=True):
        """
            Return an individual type attribute.
            If this type attribute inherits from another, look up the tree to compile
            the inherited data, with the leaf node taking priority
        """

        #get all the type attrs for this type, and add any which are missing
        this_typeattr_i = get_session().query(TypeAttr)\
            .filter(TypeAttr.id == typeattr_id)\
            .options(joinedload('attr'))\
            .options(joinedload('default_dataset')).one()

        this_typeattr_i.properties = this_typeattr_i.properties or '{}'

        this_typeattr = JSONObject(this_typeattr_i)

        if child_typeattr is None:
            child_typeattr = this_typeattr
        else:
            child_typeattr = self.set_inherited_columns(this_typeattr, child_typeattr, this_typeattr_i)

        if this_typeattr.parent_id is not None and get_parent_types is True:
            return self.parent.get_typeattr(this_typeattr.parent_id,
                                            child_typeattr=child_typeattr,
                                            get_parent_types=get_parent_types)

        return child_typeattr

    def get_type(self, type_id, child_type=None, get_parent_types=True):
        """
            Return all the templatetypes relevant to this template.
            If this template inherits from another, look up the tree to compile
            an exhaustive list of types, removing any duplicates, prioritising
            the ones closest to this template (my immediate parent's values are used instead
            of its parents)

        """

        #Add resource attributes which are not defined already
        this_type_i = get_session().query(TemplateType).filter(
            TemplateType.id == type_id).options(noload('typeattrs')).one()

        this_type = JSONObject(this_type_i)

        this_type.typeattrs = []

        if not hasattr(this_type, 'ta_tree') or this_type.ta_tree is None:
            this_type.ta_tree = {}

        #get all the type attrs for this type, and add any which are missing
        typeattrs_i = get_session().query(TypeAttr)\
            .filter(TypeAttr.type_id == type_id)\
            .options(joinedload('attr'))\
            .options(joinedload('unit'))\
            .options(joinedload('default_dataset')).all()

        typeattrs = []
        for ta in typeattrs_i:
            ta.properties = ta.properties or '{}'  # add default properties
            typeattrs.append(JSONObject(ta))

        #Is this type the parent of a type. If so, we don't want to add a new type
        #we want to update an existing one with any data that it's missing
        if child_type is not None:
            child_type = self.set_inherited_columns(this_type, child_type, this_type_i)
            #check the child's typeattrs. If a typeattr exists on the parent, with the
            #same attr_id, then it should be ignore. THis can happen when adding a typeattr
            #to the child first, then the parent
            child_typeattrs = [ta.attr_id for ta in child_type.typeattrs]

            for i, typeattr in enumerate(typeattrs):
                if typeattr.attr_id in child_typeattrs:
                    log.debug("Found a typeattr for attribute %s on the "
                             "child type %s (%s). Ignoring",
                             typeattr.attr_id, child_type.name, child_type.id)
                    continue
                #Does this typeattr have a child?
                child_typeattr = child_type.ta_tree.get(typeattr.id)
                if child_typeattr is None:
                    #there is no child, so check if it is a child
                    if typeattr.parent_id is not None:
                        #it has a parent, so add it to the type's tree dict
                        #for processing farther up the tree
                        child_type.ta_tree[typeattr.parent_id] = typeattr
                    child_type.typeattrs.append(typeattr)
                else:
                    child_typeattr = self.set_inherited_columns(typeattr, child_typeattr, typeattrs_i[i])

        else:
            if not hasattr(this_type, 'typeattrs'):
                setattr(this_type, 'typeattrs', [])
            for typeattr in typeattrs:
                this_type.typeattrs.append(typeattr)
            child_type = this_type

        if this_type.parent_id is not None and get_parent_types is True:
            return self.parent.get_type(this_type.parent_id,
                                         child_type=child_type,
                                         get_parent_types=get_parent_types)
        child_type.ta_tree = None
        return child_type


    def get_types(self, type_tree={}, child_types=None, get_parent_types=True, child_template_id=None):
        """
            Return all the templatetypes relevant to this template.
            If this template inherits from another, look up the tree to compile
            an exhaustive list of types, removing any duplicates, prioritising
            the ones closest to this template (my immediate parent's values are used instead
            of its parents)

        """
        log.info("Getting Template Types..")

        #This avoids python's mutable keyword arguments causing child_data to keep its values between
        #function calls
        if child_types is None:
            child_types = []
            type_tree = {}

        #Add resource attributes which are not defined already
        types_i = get_session().query(TemplateType).filter(
            TemplateType.template_id == self.id).options(noload('typeattrs')).all()
        types = [JSONObject(t) for t in types_i]

        if child_template_id is None:
            child_template_id = self.id

        #TODO need to check here to see if there is a parent / child type
        #and then add or not add as approprioate
        for i, this_type in enumerate(types):
            this_type.child_template_id = child_template_id

            #This keeps track of which type attributes are currently associated
            #to this type. We'll use the data in this dict to set the 'typeattrs'
            #at the end
            if not hasattr(this_type, 'ta_tree') or this_type.ta_tree is None:
                this_type.ta_tree = {}

            #get all the type attrs for this type, and add any which are missing
            typeattrs_i = get_session().query(TypeAttr)\
                .filter(TypeAttr.type_id == this_type.id)\
                .options(joinedload('attr'))\
                .options(joinedload('default_dataset')).all()

            typeattrs = []
            for ta in typeattrs_i:
                ta.properties = ta.properties or '{}'  # add default properties
                typeattrs.append(JSONObject(ta))

            #Is this type the parent of a type. If so, we don't want to add a new type
            #we want to update an existing one with any data that it's missing
            if this_type.id in type_tree:
                #This is a deleted type, so ignore it in the parent
                if type_tree[this_type.id] is  None:
                    continue

                #Find the child type and update it.
                child_type = type_tree[this_type.id]

                child_type = self.set_inherited_columns(this_type, child_type, types_i[i])

                #check the child's typeattrs. If a typeattr exists on the parent, with the
                #same attr_id, then it should be ignore. THis can happen when adding a typeattr
                #to the child first, then the parent
                child_typeattrs = [ta.attr_id for ta in child_type.typeattrs]

                for typeattr in typeattrs:
                    if typeattr.attr_id in child_typeattrs:
                        log.debug("Found a typeattr for attribute %s on the "
                             "child type %s (%s). Ignoring",
                             typeattr.attr_id, child_type.name, child_type.id)
                        continue

                    #Does this typeattr have a child?
                    child_typeattr = type_tree[this_type.id].ta_tree.get(typeattr.id)
                    if child_typeattr is None:

                        #there is no child, so check if it is a child
                        if typeattr.parent_id is not None:
                            #it has a parent, so add it to the type's tree dict
                            #for processing farther up the tree
                            type_tree[this_type.id].ta_tree[typeattr.parent_id] = typeattr
                        child_type.typeattrs.append(typeattr)
                    else:
                        child_typeattr = self.set_inherited_columns(typeattr, child_typeattr, types_i[i])


                if this_type.parent_id is not None:
                    type_tree[this_type.parent_id] = child_type

            else:
                if not hasattr(this_type, 'typeattrs'):
                    setattr(this_type, 'typeattrs', [])
                for typeattr in typeattrs:
                    #is this a child? if so, register it as one
                    if typeattr.parent_id is not None:
                        this_type.ta_tree[typeattr.parent_id] = typeattr
                    this_type.typeattrs.append(typeattr)

                child_types.append(this_type)
                #set
                if this_type.parent_id is not None:
                    type_tree[this_type.parent_id] = this_type


        if self.parent is not None and get_parent_types is True:
            return self.parent.get_types(type_tree=type_tree,
                                         child_types=child_types,
                                         get_parent_types=get_parent_types,
                                        child_template_id=child_template_id)

        #clean up
        for child_type in child_types:
            child_type.ta_tree = None

        return child_types

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if str(user_id) == str(o.user_id):
                owner = o
                break
        else:
            owner = TemplateOwner()
            owner.template_id = self.id
            self.owners.append(owner)

        owner.user_id = int(user_id)
        owner.view  = read
        owner.edit  = write
        owner.share = share

        return owner

    def unset_owner(self, user_id):
        owner = None
        if str(user_id) == str(self.created_by):
            log.warning("Cannot unset %s as owner, as they created the template", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this template
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on template %s" %
                             (user_id, self.id))

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on template %s" %
                             (user_id, self.network_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this template
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id) or int(owner.user_id) == 1:
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on template %s" %
                             (user_id, self.template))

class TemplateOwner(Base, Inspect):
    """
    """

    __tablename__='tTemplateOwner'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    template_id = Column(Integer(), ForeignKey('tTemplate.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User')
    template = relationship('Template', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

    _parents = ['tTemplate', 'tUser']
    _children = []


class TemplateType(Base, Inspect):
    """
    Template Type
    """

    __tablename__ = 'tTemplateType'
    __table_args__ = (
        UniqueConstraint('template_id', 'name', 'resource_type', name="unique type name"),
    )

    #these are columns which are not allowed to be changed by a child type
    _protected_columns = ['id', 'template_id', 'parent_id', 'cr_date', 'updated_at']
    _hierarchy_columns = ['name', 'resource_type']

    id = Column(Integer(), primary_key=True, nullable=False)
    parent_id = Column(Integer(), ForeignKey('tTemplateType.id'))
    template_id = Column(Integer(), ForeignKey('tTemplate.id'), nullable=False)
    name = Column(String(200), nullable=True)
    description = Column(String(1000))
    resource_type = Column(String(200), nullable=True)
    alias = Column(String(100))
    status = Column(String(1),  nullable=True)
    layout = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'))
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    parent = relationship('TemplateType', remote_side=[id],
                          backref=backref("children", order_by=id))

    template = relationship('Template',
                            backref=backref("templatetypes",
                                            order_by=id,
                                            cascade="all, delete-orphan"))

    _parents = ['tTemplate']
    _children = ['tTypeAttr']

    def get_typeattrs(self, ta_tree={}, child_typeattrs=None, get_parent_types=True):
        """
            This is unfinished
        """
        #This avoids python's mutable keyword arguments causing child_data to keep its values between
        #function calls
        if child_typeattrs is None:
            child_typeattrs = []
            ta_tree = {}

        #get all the type attrs for this type, and add any which are missing
        typeattrs_i = get_session().query(TypeAttr)\
            .filter(TypeAttr.type_id == self.id)\
            .options(joinedload('default_dataset')).all()
        typeattrs = [JSONObject(ta) for ta in typeattrs_i]


        for i, typeattr in enumerate(typeattrs):

            #Does this typeattr have a child?
            child_typeattr = ta_tree.get(typeattr.id)
            if child_typeattr is None:
                #there is no child, so check if it is a child
                if typeattr.parent_id is not None:
                    #it has a parent, so add it to the tree dict
                    #for processing farther up the tree
                    ta_tree[typeattr.parent_id] = typeattr

            else:
                child_typeattr = self.template.set_inherited_columns(typeattr, child_typeattr, typeattrs_i[i])
                child_typeattrs.append(typeattr)



        if self.parent is not None and get_parent_types is True:
            return self.parent.get_typeattrs(ta_tree=ta_tree,
                                              child_typeattrs=child_typeattrs,
                                              get_parent_types=get_parent_types)
        return child_typeattrs

    def get_children(self):
        """
            Get the child types of a template type
        """

        child_types = get_session().query(TemplateType)\
                .filter(TemplateType.parent_id==self.id).all()
        return child_types

    def check_can_delete_resourcetypes(self, delete_resourcetypes=False):
        """
            Check if the delete operation will allow the deletion
            of resourcetypes. Default is NO
        """
        #Check if there are any resourcetypes associated to this type. If so,
        #don't delete it.
        resourcetype_count = get_session().query(ResourceType.id)\
                .filter(ResourceType.type_id == self.id).count()

        if resourcetype_count > 0 and delete_resourcetypes is False:
            raise HydraError(f"Unable to delete type. Template Type {self.id} has "
                             f"{resourcetype_count} resources associated to it. "
                             "Use the 'delete_resourcetypes' flag to delete these also.")

    def delete_children(self, delete_resourcetypes=False):
        """
            Delete the children associated to this type.
            THIS SHOULD BE DONE WITH EXTREME CAUTION.
            args:
                delete_resourcetypes (bool): If any resourcetypes are found to be
                associated to a child, throw an error to avoid leaving nodes with no types.
                If this flag is is set to true, then delete the resourcetypes

            This function works its way all the way down the tree to the leaf nodes
            and then deletes them from the leaf to the source
        """

        children = self.get_children()

        for child in children:
            child.delete_children(delete_resourcetypes=delete_resourcetypes)

            child.check_can_delete_resourcetypes(delete_resourcetypes=delete_resourcetypes)
            #delete all the resource types associated to this type
            if delete_resourcetypes is True:
                self.delete_resourcetypes()

            #delete the typeattrs
            for ta in child.typeattrs:
                get_session().delete(ta)

            get_session().delete(child)

    def delete_resourcetypes(self):
        """
        Delete the resourcetypes associated to a type
        """
        type_rs = get_session().query(ResourceType).filter(ResourceType.type_id==self.id).all()

        log.warn("Forcing the deletion of %s resource types from type %s",\
                 len(type_rs), self.id)

        for resource_type in type_rs:
            get_session().delete(resource_type)

class TypeAttr(Base, Inspect):
    """
        Type Attribute
    """

    __tablename__ = 'tTypeAttr'

    __table_args__ = (
        UniqueConstraint('type_id', 'attr_id', name='type_attr_1'),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    parent_id = Column(Integer(), ForeignKey('tTypeAttr.id'))
    attr_id = Column(Integer(), ForeignKey('tAttr.id'), nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.id', ondelete='CASCADE'),
                     nullable=False)
    default_dataset_id = Column(Integer(), ForeignKey('tDataset.id'))

    attr_is_var = Column(String(1), server_default=text(u"'N'"))
    data_type = Column(String(60))
    data_restriction = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'))
    unit_id = Column(Integer(), ForeignKey('tUnit.id'))
    description = Column(String(1000))
    properties = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), server_default='{}')
    status = Column(String(1),  nullable=True)
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))


    parent = relationship('TypeAttr', remote_side=[id], backref=backref("children", order_by=id))

    attr = relationship('Attr')
    #Don't use a cascade delete all here. Instead force the code to delete the typeattrs
    #manually, to avoid accidentally deleting them
    templatetype = relationship('TemplateType',
                                backref=backref("typeattrs",
                                                order_by=attr_id))
    unit = relationship('Unit',
                        backref=backref("typeattr_unit",
                                        order_by=unit_id))
    default_dataset = relationship('Dataset')

    _parents = ['tTemplateType', 'tUnit']
    _children = []

    def get_attr(self):
        """
            Get the attribute object
        """
        attr = None
        try:
            self.attr

            if self.attr is not None:
                attr = self.attr
        except:
            log.info("Unable to lazy-load attribute on typeattr %s", self.id)

        if attr is None:
            attr = get_session().query(Attr).filter(Attr.id == self.attr_id).first()

        return attr

    def get_unit(self):
        """
            Get the unit object
        """
        unit = None
        try:
            self.unit

            if self.unit is not None:
                unit = self.unit
        except:
            log.info("Unable to lazy-load unitibute on typeunit %s", self.id)

        if unit is None:
            unit = get_session().query(Unit).filter(unit.id == self.unit_id).first()

        return unit



    @property
    def is_var(self):
        return self.attr_is_var

    def get_properties(self):
        """
            This is here to match the JSONObject TypeAttr class which also
            has a get_properties, and which is required by some functions where
            both types can be validly passed in (like _set_typeattr)
        """
        return self.properties


class ResourceAttr(Base, Inspect):
    """
    """

    __tablename__ = 'tResourceAttr'

    __table_args__ = (
        UniqueConstraint('network_id', 'attr_id', name='net_attr_1'),
        UniqueConstraint('project_id', 'attr_id', name='proj_attr_1'),
        UniqueConstraint('node_id', 'attr_id', name='node_attr_1'),
        UniqueConstraint('link_id', 'attr_id', name='link_attr_1'),
        UniqueConstraint('group_id', 'attr_id', name='group_attr_1'),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    attr_id = Column(Integer(), ForeignKey('tAttr.id'), nullable=False)
    ref_key = Column(String(60), nullable=False, index=True)
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), index=True, nullable=True)
    project_id = Column(Integer(), ForeignKey('tProject.id'), index=True, nullable=True)
    node_id = Column(Integer(), ForeignKey('tNode.id'), index=True, nullable=True)
    link_id = Column(Integer(), ForeignKey('tLink.id'), index=True, nullable=True)
    group_id = Column(Integer(), ForeignKey('tResourceGroup.id'), index=True, nullable=True)
    attr_is_var = Column(String(1), nullable=False, server_default=text(u"'N'"))
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    attr = relationship('Attr')
    project = relationship('Project',
                           backref=backref('attributes',
                                           uselist=True,
                                           cascade="all, delete-orphan"),
                           uselist=False)
    network = relationship('Network',
                           backref=backref('attributes',
                                           uselist=True,
                                           cascade="all, delete-orphan"),
                           uselist=False)
    node = relationship('Node',
                        backref=backref('attributes',
                                        uselist=True,
                                        cascade="all, delete-orphan"),
                        uselist=False)
    link = relationship('Link',
                        backref=backref('attributes',
                                        uselist=True,
                                        cascade="all, delete-orphan"),
                        uselist=False)
    resourcegroup = relationship('ResourceGroup',
                                 backref=backref('attributes',
                                                 uselist=True,
                                                 cascade="all, delete-orphan"),
                                 uselist=False)

    _parents = ['tNode', 'tLink', 'tResourceGroup', 'tNetwork', 'tProject']
    _children = []

    def get_network(self):
        """
         Get the network that this resource attribute is in.
        """
        ref_key = self.ref_key
        if ref_key == 'NETWORK':
            return self.network
        elif ref_key == 'NODE':
            return self.node.network
        elif ref_key == 'LINK':
            return self.link.network
        elif ref_key == 'GROUP':
            return self.group.network
        elif ref_key == 'PROJECT':
            return None

    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'NETWORK':
            return self.network
        elif ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.resourcegroup
        elif ref_key == 'PROJECT':
            return self.project

    def get_resource_id(self):
        ref_key = self.ref_key
        if ref_key == 'NETWORK':
            return self.network_id
        elif ref_key == 'NODE':
            return self.node_id
        elif ref_key == 'LINK':
            return self.link_id
        elif ref_key == 'GROUP':
            return self.group_id
        elif ref_key == 'PROJECT':
            return self.project_id

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this resource attribute
        """
        return self.get_resource().check_read_permission(user_id, do_raise=do_raise, is_admin=is_admin)

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this node
        """
        return self.get_resource().check_write_permission(user_id, do_raise=do_raise, is_admin=is_admin)


class ResourceType(Base, Inspect):
    """
    """

    __tablename__='tResourceType'
    __table_args__ = (
        UniqueConstraint('network_id', 'type_id', name='net_type_1'),
        UniqueConstraint('node_id', 'type_id', name='node_type_1'),
        UniqueConstraint('link_id', 'type_id',  name = 'link_type_1'),
        UniqueConstraint('group_id', 'type_id', name = 'group_type_1'),

    )
    id = Column(Integer, primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.id'), primary_key=False, nullable=False)
    #This template id is used when the template and the type are not from the same template
    #i.e. the resource type is being used in a child template
    #If null, then the resources has either been created using a non-child template, or with a resource
    #type in a child template which has been entered to the DB, because the parent type has been altered in the child
    child_template_id = Column(Integer(), ForeignKey('tTemplate.id'), primary_key=False, nullable=True)
    ref_key = Column(String(60),nullable=False)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.id'), nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.id'), nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.id'), nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.id'), nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    #Don't used a delete cascade here because deleting the type accidentally can delete
    #this data. INstead the resource types should be deleted manually before the deletion
    #of the type
    templatetype = relationship('TemplateType', backref=backref('resourcetypes', uselist=True))

    network = relationship('Network', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    node = relationship('Node', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    link = relationship('Link', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    resourcegroup = relationship('ResourceGroup', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)

    _parents  = ['tNode', 'tLink', 'tResourceGroup', 'tNetwork', 'tProject']
    _children = []

    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'PROJECT':
            return self.project
        elif ref_key == 'NETWORK':
            return self.network
        elif ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.group

    def get_resource_id(self):
        ref_key = self.ref_key
        if ref_key == 'PROJECT':
            return self.project_id
        elif ref_key == 'NETWORK':
            return self.network_id
        elif ref_key == 'NODE':
            return self.node_id
        elif ref_key == 'LINK':
            return self.link_id
        elif ref_key == 'GROUP':
            return self.group_id

    def get_templatetype(self):
        """
            If this type is that of a child templatetype, then the full type
            needs to be constructed by the template. So instead of getting the
            template type directly, we get the template, then request the type.
        """

        #This resource was created using a child template
        if self.child_template_id is not None:
            template_i = get_session().query(Template)\
                .filter(Template.id == self.child_template_id).one()
        else:
            type_i = get_session().query(TemplateType).filter(TemplateType.id == self.type_id).one()

            if type_i.parent_id is None:
                return type_i

            template_i = get_session().query(Template)\
                    .filter(Template.id == type_i.template_id).one()

        type_i = template_i.get_type(self.type_id)

        return JSONObject(type_i)


#*****************************************************
# Topology & Scenarios
#*****************************************************

class Project(Base, Inspect):
    """
    """

    __tablename__='tProject'
    ref_key = 'PROJECT'


    __table_args__ = (
        UniqueConstraint('name', 'created_by', 'status', name="unique proj name"),
    )

    attribute_data = []

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200),  nullable=False, unique=False)
    description = Column(String(1000))
    layout = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)

    user = relationship('User', backref=backref("projects", order_by=id))

    _parents  = []
    _children = ['tNetwork']

    def get_name(self):
        return self.project_name

    def get_attribute_data(self):
        attribute_data_rs = get_session().query(ResourceScenario).join(ResourceAttr).filter(ResourceAttr.project_id==self.id).all()
        #lazy load datasets
        [rs.dataset.metadata for rs in attribute_data_rs]
        [rs.resourceattr.attr for rs in attribute_data_rs]
        self.attribute_data = attribute_data_rs
        return attribute_data_rs

    def add_attribute(self, attr_id, attr_is_var='N'):
        res_attr = ResourceAttr()
        res_attr.attr_id = attr_id
        res_attr.attr_is_var = attr_is_var
        res_attr.ref_key = self.ref_key
        res_attr.project_id  = self.id
        self.attributes.append(res_attr)

        return res_attr

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):

        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = ProjectOwner()
            owner.project_id = self.id
            owner.user_id = int(user_id)
            self.owners.append(owner)

        owner.view = read
        owner.edit = write
        owner.share = share

        return owner

    def unset_owner(self, user_id):
        owner = None
        if str(user_id) == str(self.created_by):
            log.warning("Cannot unset %s as owner, as they created the project", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this project
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == user_id:
                if owner.view == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have read"
                             " access on project %s" %
                             (user_id, self.id))
            else:
                return False

        return True

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this project
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have edit"
                                      " access on project %s" % (user_id, self.id))
            else:
                return False

        return True

    def check_share_permission(self, user_id, is_admin=None):
        """
            Check whether this user can write this project
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True
        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                                  " access on project %s" % (user_id, self.id))



class Network(Base, Inspect):
    """
    """

    __tablename__ = 'tNetwork'
    __table_args__ = (
        UniqueConstraint('name', 'project_id', name="unique net name"),
    )
    ref_key = 'NETWORK'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200), nullable=False)
    description = Column(String(1000))
    layout = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), nullable=True)
    appdata = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), nullable=True)
    project_id = Column(Integer(), ForeignKey('tProject.id'), nullable=False)
    status = Column(String(1), nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    projection = Column(String(200))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)

    project = relationship('Project',
                           backref=backref("networks",
                                           order_by="asc(Network.cr_date)",
                                           cascade="all, delete-orphan"))

    _parents = ['tNode', 'tLink', 'tResourceGroup']
    _children = ['tProject']

    def get_name(self):
        return self.name

    def add_attribute(self, attr_id, attr_is_var='N'):
        res_attr = ResourceAttr()
        res_attr.attr_id = attr_id
        res_attr.attr_is_var = attr_is_var
        res_attr.ref_key = self.ref_key
        res_attr.network_id  = self.id
        self.attributes.append(res_attr)

        return res_attr

    def add_link(self, name, desc, layout, node_1, node_2):
        """
            Add a link to a network. Links are what effectively
            define the network topology, by associating two already
            existing nodes.
        """

        existing_link = get_session().query(Link).filter(Link.name==name, Link.network_id==self.id).first()
        if existing_link is not None:
            raise HydraError("A link with name %s is already in network %s"%(name, self.id))

        l = Link()
        l.name        = name
        l.description = desc
        l.layout           = json.dumps(layout) if layout is not None else None
        l.node_a           = node_1
        l.node_b           = node_2

        get_session().add(l)

        self.links.append(l)

        return l


    def add_node(self, name, desc, layout, node_x, node_y):
        """
            Add a node to a network.
        """
        existing_node = get_session().query(Node).filter(Node.name==name, Node.network_id==self.id).first()
        if existing_node is not None:
            raise HydraError("A node with name %s is already in network %s"%(name, self.id))

        node = Node()
        node.name        = name
        node.description = desc
        node.layout      = json.dumps(layout) if layout is not None else None
        node.x           = node_x
        node.y           = node_y

        #Do not call save here because it is likely that we may want
        #to bulk insert nodes, not one at a time.

        get_session().add(node)

        self.nodes.append(node)

        return node

    def add_group(self, name, desc, status):
        """
            Add a new group to a network.
        """

        existing_group = get_session().query(ResourceGroup).filter(ResourceGroup.name==name, ResourceGroup.network_id==self.id).first()
        if existing_group is not None:
            raise HydraError("A resource group with name %s is already in network %s"%(name, self.id))

        group_i             = ResourceGroup()
        group_i.name        = name
        group_i.description = desc
        group_i.status      = status

        get_session().add(group_i)

        self.resourcegroups.append(group_i)


        return group_i

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if str(user_id) == str(o.user_id):
                owner = o
                break
        else:
            owner = NetworkOwner()
            owner.network_id = self.id
            self.owners.append(owner)

        owner.user_id = int(user_id)
        owner.view  = read
        owner.edit  = write
        owner.share = share

        return owner

    def unset_owner(self, user_id):

        owner = None
        if str(user_id) == str(self.created_by):
            log.warning("Cannot unset %s as owner, as they created the network", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this network
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have read"
                             " access on network %s" %
                             (user_id, self.id))
            else:
                return False

        return True

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this project
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have edit"
                             " access on network %s" %
                             (user_id, self.id))
            else:
                return False

        return True

    def check_share_permission(self, user_id, is_admin=None):
        """
            Check whether this user can write this project
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on network %s" %
                             (user_id, self.id))

class Link(Base, Inspect):
    """
    """

    __tablename__='tLink'

    __table_args__ = (
        UniqueConstraint('network_id', 'name', name="unique link name"),
    )
    ref_key = 'LINK'

    id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), nullable=False)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    node_1_id = Column(Integer(), ForeignKey('tNode.id'), nullable=False)
    node_2_id = Column(Integer(), ForeignKey('tNode.id'), nullable=False)
    name = Column(String(200))
    description = Column(String(1000))
    layout  = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    network = relationship('Network', backref=backref("links", order_by=network_id, cascade="all, delete-orphan"), lazy='joined')
    node_a = relationship('Node', foreign_keys=[node_1_id], backref=backref("links_to", order_by=id, cascade="all, delete-orphan"))
    node_b = relationship('Node', foreign_keys=[node_2_id], backref=backref("links_from", order_by=id, cascade="all, delete-orphan"))

    _parents  = ['tNetwork']
    _children = ['tResourceAttr', 'tResourceType']

    def get_name(self):
        return self.name

    #For backward compatibility
    @property
    def link_id(self):
        return self.id

    @property
    def link_name(self):
        return self.name

    @link_name.setter
    def link_name_setter(self, value):
        self.name = value

    @property
    def link_description(self):
        return self.description

    @link_description.setter
    def link_description_setter(self):
        self.description = self.link_description

    def add_attribute(self, attr_id, attr_is_var='N'):
        res_attr = ResourceAttr()
        res_attr.attr_id = attr_id
        res_attr.attr_is_var = attr_is_var
        res_attr.ref_key = self.ref_key
        res_attr.link_id  = self.id
        self.attributes.append(res_attr)

        return res_attr

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this link
        """
        return self.network.check_read_permission(user_id, do_raise=do_raise, is_admin=is_admin)

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this link
        """

        return self.network.check_write_permission(user_id, do_raise=do_raise, is_admin=is_admin)

class Node(Base, Inspect):
    """
    """

    __tablename__='tNode'
    __table_args__ = (
        UniqueConstraint('network_id', 'name', 'status', name="unique node name"),
    )
    ref_key = 'NODE'

    id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), nullable=False)
    description = Column(String(1000))
    name = Column(String(200),  nullable=False)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    x = Column(Numeric(precision=15, scale=10, asdecimal=True))
    y = Column(Numeric(precision=15, scale=10, asdecimal=True))
    layout  = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    network = relationship('Network', backref=backref("nodes", order_by=network_id, cascade="all, delete-orphan"), lazy='joined')

    _parents  = ['tNetwork']
    _children = ['tResourceAttr', 'tResourceType']

    def get_name(self):
        return self.name

    #For backward compatibility
    @property
    def node_id(self):
        return self.id

    @property
    def node_name(self):
        return self.name

    @node_name.setter
    def node_name_setter(self, value):
        self.name = value

    @property
    def node_description(self):
        return self.description

    @node_description.setter
    def node_description_setter(self):
        self.description = self.node_description

    def add_attribute(self, attr_id, attr_is_var='N'):
        res_attr = ResourceAttr()
        res_attr.attr_id = attr_id
        res_attr.attr_is_var = attr_is_var
        res_attr.ref_key = self.ref_key
        res_attr.node_id  = self.id
        self.attributes.append(res_attr)

        return res_attr

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this node
        """
        return self.network.check_read_permission(user_id, do_raise=do_raise, is_admin=is_admin)

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this node
        """

        return self.network.check_write_permission(user_id, do_raise=do_raise, is_admin=is_admin)

class ResourceGroup(Base, Inspect):
    """
    """

    __tablename__='tResourceGroup'
    __table_args__ = (
        UniqueConstraint('network_id', 'name', name="unique resourcegroup name"),
    )

    ref_key = 'GROUP'
    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200),  nullable=False)
    description = Column(String(1000))
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    network_id = Column(Integer(), ForeignKey('tNetwork.id'),  nullable=False)

    network = relationship('Network', backref=backref("resourcegroups", order_by=id, cascade="all, delete-orphan"), lazy='joined')

    _parents  = ['tNetwork']
    _children = ['tResourceAttr', 'tResourceType']

    def get_name(self):
        return self.group_name

    #For backward compatibility
    @property
    def group_id(self):
        return self.id

    @property
    def group_name(self):
        return self.name

    @group_name.setter
    def group_name_setter(self, value):
        self.name = value

    @property
    def group_description(self):
        return self.description

    @group_description.setter
    def group_description_setter(self):
        self.description = self.group_description

    def add_attribute(self, attr_id, attr_is_var='N'):
        res_attr = ResourceAttr()
        res_attr.attr_id = attr_id
        res_attr.attr_is_var = attr_is_var
        res_attr.ref_key = self.ref_key
        res_attr.group_id  = self.id
        self.attributes.append(res_attr)

        return res_attr

    def get_items(self, scenario_id):
        """
            Get all the items in this group, in the given scenario
        """
        items = get_session().query(ResourceGroupItem)\
                .filter(ResourceGroupItem.group_id==self.id).\
                filter(ResourceGroupItem.scenario_id==scenario_id).all()

        return items

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this group
        """
        return self.network.check_read_permission(user_id, do_raise=do_raise, is_admin=is_admin)

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this group
        """

        return self.network.check_write_permission(user_id, do_raise=do_raise, is_admin=is_admin)

class ResourceGroupItem(Base, Inspect):
    """
    """

    __tablename__='tResourceGroupItem'

    __table_args__ = (
        UniqueConstraint('group_id', 'node_id', 'scenario_id', name='node_group_1'),
        UniqueConstraint('group_id', 'link_id', 'scenario_id',  name = 'link_group_1'),
        UniqueConstraint('group_id', 'subgroup_id', 'scenario_id', name = 'subgroup_group_1'),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    ref_key = Column(String(60),  nullable=False)

    node_id     = Column(Integer(),  ForeignKey('tNode.id'))
    link_id     = Column(Integer(),  ForeignKey('tLink.id'))
    subgroup_id = Column(Integer(),  ForeignKey('tResourceGroup.id'))

    group_id = Column(Integer(), ForeignKey('tResourceGroup.id'))
    scenario_id = Column(Integer(), ForeignKey('tScenario.id'),  nullable=False, index=True)

    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    group = relationship('ResourceGroup', foreign_keys=[group_id], backref=backref("items", order_by=group_id))
    scenario = relationship('Scenario', backref=backref("resourcegroupitems", order_by=id, cascade="all, delete-orphan"))

    #These need to have backrefs to allow the deletion of networks & projects
    #--There needs to be a connection between the items & the resources to allow it
    node = relationship('Node', backref=backref("resourcegroupitems", order_by=id, cascade="all, delete-orphan"))
    link = relationship('Link', backref=backref("resourcegroupitems", order_by=id, cascade="all, delete-orphan"))
    subgroup = relationship('ResourceGroup', foreign_keys=[subgroup_id])

    _parents  = ['tResourceGroup', 'tScenario']
    _children = []

    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.subgroup

    def get_resource_id(self):
        ref_key = self.ref_key
        if ref_key == 'NODE':
            return self.node_id
        elif ref_key == 'LINK':
            return self.link_id
        elif ref_key == 'GROUP':
            return self.subgroup_id

class ResourceScenario(Base, Inspect):
    """
    """

    __tablename__='tResourceScenario'

    dataset_id = Column(Integer(), ForeignKey('tDataset.id'), nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.id'), primary_key=True, nullable=False, index=True)
    resource_attr_id = Column(Integer(), ForeignKey('tResourceAttr.id'), primary_key=True, nullable=False, index=True)
    source           = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    dataset      = relationship('Dataset', backref=backref("resourcescenarios", order_by=dataset_id))
    scenario     = relationship('Scenario', backref=backref("resourcescenarios", order_by=scenario_id, cascade="all, delete-orphan"))
    resourceattr = relationship('ResourceAttr', backref=backref("resourcescenarios", cascade="all, delete-orphan"), uselist=False)

    _parents  = ['tScenario', 'tResourceAttr']
    _children = ['tDataset']

    def get_dataset(self, user_id):
        dataset = get_session().query(Dataset.id,
                Dataset.type,
                Dataset.unit_id,
                Dataset.name,
                Dataset.hidden,
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.value).label('value')).filter(
                Dataset.id==self.id).outerjoin(DatasetOwner,
                                    and_(Dataset.id==DatasetOwner.dataset_id,
                                    DatasetOwner.user_id==user_id)).one()

        return dataset

    @property
    def value(self):
        return self.dataset

class Scenario(Base, Inspect):
    """
    """

    __tablename__='tScenario'
    __table_args__ = (
        UniqueConstraint('network_id', 'name', name="unique scenario name"),
    )

    id = Column(Integer(), primary_key=True, index=True, nullable=False)
    name = Column(String(200),  nullable=False)
    description = Column(String(2000))
    layout  = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), index=True)
    start_time = Column(String(60))
    end_time = Column(String(60))
    locked = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    time_step = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)
    parent_id = Column(Integer(), ForeignKey('tScenario.id'), nullable=True)

    network = relationship('Network', backref=backref("scenarios", order_by=id))
    parent = relationship('Scenario', remote_side=[id], backref=backref("children", order_by=id))

    _parents  = ['tNetwork']
    _children = ['tResourceScenario']

    def add_resource_scenario(self, resource_attr, dataset=None, source=None):
        rs_i = ResourceScenario()
        if resource_attr.id is None:
            rs_i.resourceattr = resource_attr
        else:
            rs_i.resource_attr_id = resource_attr.id

        if dataset.id is None:
            rs_i.dataset = dataset
        else:
            rs_i.dataset_id = dataset.id
        rs_i.source = source
        self.resourcescenarios.append(rs_i)

    def add_resourcegroup_item(self, ref_key, resource, group_id):
        group_item_i = ResourceGroupItem()
        group_item_i.group_id = group_id
        group_item_i.ref_key  = ref_key
        if ref_key == 'GROUP':
            group_item_i.subgroup = resource
        elif ref_key == 'NODE':
            group_item_i.node     = resource
        elif ref_key == 'LINK':
            group_item_i.link     = resource
        self.resourcegroupitems.append(group_item_i)

    def get_data(self, child_data=None, get_parent_data=False, ra_ids=None, include_results=True, include_only_results=False):
        """
            Return all the resourcescenarios relevant to this scenario.
            If this scenario inherits from another, look up the tree to compile
            an exhaustive list of resourcescnearios, removing any duplicates, prioritising
            the ones closest to this scenario (my immediate parent's values are used instead
            of its parents)

            If an explicit list of RAs is provided, only return data for these. This is used
            when requesting data for a specific resource, for example.
        """

        #This avoids python's mutable keyword argumets causing child_data to keep its values beween
        #function calls
        if child_data is None:
            child_data = []

        #Idenify all existing resource attr ids, which take priority over anything in here
        childrens_ras = []
        for child_rs in child_data:
            childrens_ras.append(child_rs.resource_attr_id)

        #Add resource attributes which are not defined already
        rs_query = get_session().query(ResourceScenario).filter(
            ResourceScenario.scenario_id == self.id).options(joinedload('dataset')).options(joinedload('resourceattr'))

        if ra_ids is not None:
            rs_query = rs_query.filter(ResourceScenario.resource_attr_id.in_(ra_ids))

        if include_results is False:
            rs_query = rs_query.outerjoin(ResourceAttr).filter(
                ResourceAttr.attr_is_var == 'N')

        if include_only_results is True:
            rs_query = rs_query.outerjoin(ResourceAttr).filter(
                ResourceAttr.attr_is_var == 'Y')

        resourcescenarios = rs_query.all()

        for this_rs in resourcescenarios:
            if this_rs.resource_attr_id not in childrens_ras:
                child_data.append(this_rs)

        if self.parent is not None and get_parent_data is True:
            return self.parent.get_data(child_data=child_data,
                                        get_parent_data=get_parent_data,
                                       ra_ids=ra_ids)

        return child_data


    def get_group_items(self, child_items=None, get_parent_items=False):
        """
            Return all the resource group items relevant to this scenario.
            If this scenario inherits from another, look up the tree to compile
            an exhaustive list of resource group items, removing any duplicates, prioritising
            the ones closest to this scenario (my immediate parent's values are used instead
            of its parents)
        """

        #This avoids python's mutable keyword argumets causing child_items to keep its values beween
        #function calls
        if child_items == None:
            child_items = []

        #Idenify all existing resource attr ids, which take priority over anything in here
        childrens_groups = []
        for child_rgi in child_items:
            childrens_groups.append(child_rgi.group_id)

        #Add resource attributes which are not defined already
        for this_rgi in self.resourcegroupitems:
            if this_rgi.group_id not in childrens_groups:
                child_items.append(this_rgi)

        if self.parent is not None and get_parent_items is True:
            return self.parent.get_group_items(child_items=child_items,
                                               get_parent_items=get_parent_items)

        return child_items

class RuleTypeDefinition(AuditMixin, Base, Inspect):
    """
        Describes the types of rules available in the system

        A rule type is a simple way of categorising rules. A rule may have no
        type or it may have 1. A rule type consists of a unique code and a name.

        In addition to separating rules, this enables rules to be searched more easily.
    """

    __tablename__='tRuleTypeDefinition'

    __table_args__ = (
        UniqueConstraint('code', name="Unique Rule Code"),
    )

    code = Column(String(200), nullable=False, primary_key=True)
    name = Column(String(200), nullable=False)


class RuleTypeLink(AuditMixin, Base, Inspect):
    """
        Links rules to type definitions.

        A rule type is a simple way of categorising rules. A rule may have no
        type or it may have 1. A rule type consists of a unique code and a name.

        In addition to separating rules, this enables rules to be searched more easily.
    """

    __tablename__='tRuleTypeLink'

    __table_args__ = (
        UniqueConstraint('code', 'rule_id', name="Unique Rule / Type"),
    )

    code    = Column(String(200), ForeignKey('tRuleTypeDefinition.code'), primary_key=True, nullable=False)
    rule_id = Column(Integer(), ForeignKey('tRule.id'), primary_key=True, nullable=False)

    #Set the backrefs so that when a type definition or a rule are deleted, so are the links.
    typedefinition = relationship('RuleTypeDefinition', uselist=False, lazy='joined',
                                  backref=backref('ruletypes', cascade="all, delete-orphan"))
    rule = relationship('Rule', backref=backref('types', order_by=code, uselist=True, cascade="all, delete-orphan"))

class RuleOwner(AuditMixin, Base, Inspect):
    """
        This table tracks the owners of rules, to ensure rules which contain confidential logic
        can be kept hidden
    """

    __tablename__='tRuleOwner'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    rule_id = Column(Integer(), ForeignKey('tRule.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User', foreign_keys=[user_id])
    rule = relationship('Rule', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

    _parents  = ['tRule', 'tUser']
    _children = []

class Rule(AuditMixin, Base, Inspect, PermissionControlled):
    """
        A rule is an arbitrary piece of text applied to resources
        within a scenario. A scenario itself cannot have a rule applied
        to it.
    """

    __tablename__ = 'tRule'
    __table_args__ = (
        UniqueConstraint('scenario_id', 'name', name="unique rule name"),
    )

    __ownerclass__ = RuleOwner
    __ownerfk__ = 'rule_id'

    id = Column(Integer(), primary_key=True, nullable=False)

    name = Column(String(200), nullable=False)
    description = Column(String(1000), nullable=True)

    format = Column(String(80), nullable=False, server_default='text')

    ref_key = Column(String(60), nullable=False, index=True)

    value = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), nullable=True)

    status = Column(String(1), nullable=False, server_default=text(u"'A'"))
    scenario_id = Column(Integer(), ForeignKey('tScenario.id'), nullable=True)

    network_id = Column(Integer(), ForeignKey('tNetwork.id'), index=True, nullable=True)
    node_id = Column(Integer(), ForeignKey('tNode.id'), index=True, nullable=True)
    link_id = Column(Integer(), ForeignKey('tLink.id'), index=True, nullable=True)
    group_id = Column(Integer(), ForeignKey('tResourceGroup.id'), index=True, nullable=True)

    scenario = relationship('Scenario',
                            backref=backref('rules',
                                            uselist=True,
                                            cascade="all, delete-orphan"),
                            lazy='joined')
    network = relationship('Network',
                           backref=backref("rules",
                                           order_by=network_id,
                                           cascade="all, delete-orphan"),
                           lazy='joined')
    node = relationship('Node',
                        backref=backref("rules",
                                        order_by=node_id,
                                        uselist=True,
                                        cascade="all, delete-orphan"),

                        lazy='joined')
    link = relationship('Link',
                        backref=backref("rules",
                                        order_by=link_id,
                                        uselist=True,
                                        cascade="all, delete-orphan"),
                        lazy='joined')
    group = relationship('ResourceGroup',
                         backref=backref("rules",
                                         order_by=group_id,#
                                         uselist=True,
                                         cascade="all, delete-orphan"),
                         lazy='joined')

    _parents = ['tScenario', 'tNode', 'tLink', 'tProject', 'tNetwork', 'tResourceGroup']
    _children = []


    def set_types(self, types):
        """
            Accepts a list of type JSONObjects or spyne objects and sets
            the type of the rule to be exactly this. This means deleting rules
            which are not in the list
        """

        #We take this to mean don't touch types.
        if types is None:
            return

        existingtypes = set([t.code for t in self.types])

        #Map a type code to a type object
        existing_type_map = dict((t.code, t) for t in self.types)

        newtypes = set([t.code for t in types])

        types_to_add = newtypes - existingtypes
        types_to_delete = existingtypes - newtypes

        for ruletypecode in types_to_add:

            self.check_type_definition_exists(ruletypecode)

            ruletypelink = RuleTypeLink()
            ruletypelink.code = ruletypecode
            self.types.append(ruletypelink)

        for type_to_delete in types_to_delete:
            get_session().delete(existing_type_map[type_to_delete])

    def check_type_definition_exists(self, code):
        """
        A convenience function to check if a rule type definition exists before trying to add a link to it
        """

        try:
            get_session().query(RuleTypeDefinition).filter(RuleTypeDefinition.code == code).one()
        except NoResultFound:
            raise ResourceNotFoundError("Rule type definition with code {} does not exist".format(code))

    def get_network(self):
        """
        Rules are associated with a network directly or nodes/links/groups in a network,
        so rules are always associated to one network.
        This function returns that network
        """
        rule_network = None
        if self.ref_key.upper() == 'NETWORK':
            rule_network = self.network
        elif self.ref_key.upper() == 'NODE':
            rule_network = self.node.network
        elif self.ref_key.upper() == 'LINK':
            rule_network = self.link.network
        elif self.ref_key.upper() == 'GROUP':
            rule_network = self.group.network

        return rule_network

class Note(Base, Inspect, PermissionControlled):
    """
        A note is an arbitrary piece of text which can be applied
        to any resource. A note is NOT scenario dependent. It is applied
        directly to resources. A note can be applied to a scenario.
    """

    __tablename__='tNote'

    id = Column(Integer(), primary_key=True, nullable=False)
    ref_key = Column(String(60),  nullable=False, index=True)
    value = Column(LargeBinary(),  nullable=True)
    created_by = Column(Integer(), ForeignKey('tUser.id'))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    scenario_id = Column(Integer(), ForeignKey('tScenario.id'),  index=True, nullable=True)
    project_id = Column(Integer(), ForeignKey('tProject.id'),  index=True, nullable=True)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.id'), index=True, nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.id'), index=True, nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.id'), index=True, nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.id'), index=True, nullable=True)

    scenario = relationship('Scenario', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    node = relationship('Node', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    link = relationship('Link', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    group = relationship('ResourceGroup', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    network = relationship('Network', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    project = relationship('Project', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')

    _parents  = ['tScenario', 'tNode', 'tLink', 'tProject', 'tNetwork', 'tResourceGroup']
    _children = []

    def set_ref(self, ref_key, ref_id):
        """
            Using a ref key and ref id set the
            reference to the appropriate resource type.
        """
        if ref_key == 'NETWORK':
            self.network_id = ref_id
        elif ref_key == 'NODE':
            self.node_id = ref_id
        elif ref_key == 'LINK':
            self.link_id = ref_id
        elif ref_key == 'GROUP':
            self.group_id = ref_id
        elif ref_key == 'SCENARIO':
            self.scenario_id = ref_id
        elif ref_key == 'PROJECT':
            self.project_id = ref_id

        else:
            raise HydraError("Ref Key %s not recognised."%ref_key)

    def get_ref_id(self):

        """
            Return the ID of the resource to which this not is attached
        """
        if self.ref_key == 'NETWORK':
            return self.network_id
        elif self.ref_key == 'NODE':
            return self.node_id
        elif self.ref_key == 'LINK':
            return self.link_id
        elif self.ref_key == 'GROUP':
            return self.group_id
        elif self.ref_key == 'SCENARIO':
            return self.scenario_id
        elif self.ref_key == 'PROJECT':
            return self.project_id

    def get_ref(self):
        """
            Return the ID of the resource to which this not is attached
        """
        if self.ref_key == 'NETWORK':
            return self.network
        elif self.ref_key == 'NODE':
            return self.node
        elif self.ref_key == 'LINK':
            return self.link
        elif self.ref_key == 'GROUP':
            return self.group
        elif self.ref_key == 'SCENARIO':
            return self.scenario
        elif self.ref_key == 'PROJECT':
            return self.project


#***************************************************
#Ownership & Permissions
#***************************************************
class ProjectOwner(Base, Inspect):
    """
    """

    __tablename__='tProjectOwner'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    project_id = Column(Integer(), ForeignKey('tProject.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User')
    project = relationship('Project', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

    _parents  = ['tProject', 'tUser']
    _children = []

class NetworkOwner(Base, Inspect):
    """
    """

    __tablename__='tNetworkOwner'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User')
    network = relationship('Network', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

    _parents  = ['tNetwork', 'tUser']
    _children = []

class Perm(Base, Inspect):
    """
    """

    __tablename__='tPerm'

    id = Column(Integer(), primary_key=True, nullable=False)
    code = Column(String(60),  nullable=False)
    name = Column(String(200),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    _parents  = ['tRole', 'tPerm']
    _children = []

    def __repr__(self):
        return "{0} ({1})".format(self.name, self.code)

class Role(Base, Inspect):
    """
    """

    __tablename__='tRole'

    id = Column(Integer(), primary_key=True, nullable=False)
    code = Column(String(60),  nullable=False)
    name = Column(String(200),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    _parents  = []
    _children = ['tRolePerm', 'tRoleUser']

    @property
    def permissions(self):
        return set([rp.perm for rp in self.roleperms])

    def __repr__(self):
        return "{0} ({1})".format(self.name, self.code)


class RolePerm(Base, Inspect):
    """
    """

    __tablename__='tRolePerm'

    perm_id = Column(Integer(), ForeignKey('tPerm.id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    perm = relationship('Perm', backref=backref('roleperms', uselist=True, lazy='joined'), lazy='joined')
    role = relationship('Role', backref=backref('roleperms', uselist=True, lazy='joined'), lazy='joined')

    _parents  = ['tRole', 'tPerm']
    _children = []

    def __repr__(self):
        return "{0}".format(self.perm)

class RoleUser(Base, Inspect):
    """
    """

    __tablename__='tRoleUser'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    role = relationship('Role', backref=backref('roleusers', uselist=True))
    user = relationship('User', backref=backref('roleusers', uselist=True))

    _parents  = ['tRole', 'tUser']
    _children = []

    def __repr__(self):
        return "{0}".format(self.role.name)


class User(Base, Inspect):
    """
    """

    __tablename__='tUser'

    id = Column(Integer(), primary_key=True, nullable=False)
    username = Column(String(60),  nullable=False, unique=True)
    password = Column(LargeBinary(),  nullable=False)
    display_name = Column(String(200),  nullable=False, server_default=text(u"''"))
    last_login = Column(TIMESTAMP())
    last_edit = Column(TIMESTAMP())
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    failed_logins = Column(SMALLINT, nullable=True, default=0)

    _parents  = []
    _children = ['tRoleUser']

    def validate_password(self, password):
        if bcrypt.hashpw(password.encode('utf-8'), self.password.encode('utf-8')) == self.password.encode('utf-8'):
            return True
        return False

    @property
    def permissions(self):
        """Return a set with all permissions granted to the user."""
        perms = set()
        for r in self.roles:
            perms = perms | set(r.permissions)
        return perms

    @property
    def roles(self):
        """Return a set with all roles granted to the user."""
        roles = []
        for ur in self.roleusers:
            roles.append(ur.role)
        return set(roles)

    def is_admin(self):
        """
            Check that the user has a role with the code 'admin'
        """
        for ur in self.roleusers:
            if ur.role.code == 'admin':
                return True

        return False

    def __repr__(self):
        return "{0}".format(self.username)


class Unit(Base, Inspect):
    """
    """

    __tablename__='tUnit'

    __table_args__ = (
        UniqueConstraint('abbreviation', name="unique abbreviation"),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    dimension_id = Column(Integer(), ForeignKey('tDimension.id'), nullable=False)

    # These lines are commented because sqllite seem not accepting utf8_bin. Find a solution
    #     name = Column(Unicode(60, collation='utf8_bin'),  nullable=False)
    #     abbreviation = Column(Unicode(60, collation='utf8_bin'),  nullable=False)
    #     lf = Column(Unicode(60, collation='utf8_bin'),  nullable=True)
    #     cf = Column(Unicode(60, collation='utf8_bin'),  nullable=True)
    #     description = Column(Unicode(1000, collation='utf8_bin'))
    name = Column(Unicode(60),  nullable=False)
    abbreviation = Column(Unicode(60).with_variant(mysql.VARCHAR(60, collation='utf8mb4_bin'), 'mysql'),  nullable=False)
    lf = Column(Unicode(60),  nullable=True)
    cf = Column(Unicode(60),  nullable=True)
    description = Column(Unicode(1000))

    project_id = Column(Integer(), ForeignKey('tProject.id'), index=True, nullable=True)

    dimension = relationship('Dimension', backref=backref("units", uselist=True, order_by=dimension_id, cascade="all, delete-orphan"), lazy='joined')
    project   = relationship('Project', backref=backref("units", order_by=dimension_id, cascade="all, delete-orphan"), lazy='joined')

    _parents  = ['tDimension', 'tProject']
    _children = ['tDataset', 'tTypeAttr']

    def __repr__(self):
        return "{0}".format(self.abbreviation)


class Dimension(Base, Inspect):
    """
    """

    __tablename__='tDimension'

    id = Column(Integer(), primary_key=True, nullable=False)

    # These lines are commented because sqllite seem not accepting utf8_bin. Find a solution
    # name = Column(Unicode(60, collation='utf8_bin'),  nullable=False, unique=True)
    # description = Column(Unicode(1000, collation='utf8_bin'))

    name = Column(Unicode(60),  nullable=False, unique=True)
    description = Column(Unicode(1000))

    project_id = Column(Integer(), ForeignKey('tProject.id'), index=True, nullable=True)

    _parents  = ['tProject']
    _children = ['tUnit', 'tAttr']

    def __repr__(self):
        return "{0}".format(self.name)

def create_resourcedata_view():
    #These are for creating the resource data view (see bottom of page)
    from sqlalchemy import select
    from sqlalchemy.schema import DDLElement
    from sqlalchemy.sql import table
    from sqlalchemy.ext import compiler

    class CreateView(DDLElement):
        def __init__(self, name, selectable):
            self.name = name
            self.selectable = selectable

    class DropView(DDLElement):
        def __init__(self, name):
            self.name = name

    @compiler.compiles(CreateView)
    def compile(element, compiler, **kw):
        return "CREATE VIEW %s AS %s" % (element.name, compiler.sql_compiler.process(element.selectable))

    @compiler.compiles(DropView)
    def compile(element, compiler, **kw):
        return "DROP VIEW %s" % (element.name)

    def view(name, metadata, selectable):
        t = table(name)

        for c in selectable.c:
            c._make_proxy(t)

        CreateView(name, selectable).execute_at('after-create', metadata)
        DropView(name).execute_at('before-drop', metadata)
        return t


    view_qry = select([
        ResourceAttr.id,
        ResourceAttr.attr_id,
        Attr.name,
        ResourceAttr.id,
        ResourceAttr.network_id,
        ResourceAttr.node_id,
        ResourceAttr.link_id,
        ResourceAttr.group_id,
        ResourceScenario.scenario_id,
        ResourceScenario.dataset_id,
        Dataset.unit_id,
        Dataset.name,
        Dataset.type,
        Dataset.value]).where(ResourceScenario.resource_attr_id==ResourceAttr.attr_id).where(ResourceAttr.attr_id==Attr.id).where(ResourceScenario.dataset_id==Dataset.id)

    stuff_view = view("vResourceData", Base.metadata, view_qry)
#TODO: Understand why this view is not being created.
#create_resourcedata_view()
