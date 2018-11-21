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
Float,\
Text

from sqlalchemy import inspect, func

from ..exceptions import HydraError, PermissionError

from sqlalchemy.orm import relationship, backref

from ..util.hydra_dateutil import ordinal_to_timestamp, get_datetime

from . import DeclarativeBase as Base, get_session

from ..util import generate_data_hash, get_val

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


def get_timestamp(ordinal):
    """
        Turn an ordinal timestamp into a datetime string.
    """
    if ordinal is None:
        return None
    timestamp = str(ordinal_to_timestamp(ordinal))
    return timestamp


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

class Dataset(Base, Inspect):
    """
        Table holding all the attribute values
    """
    __tablename__='tDataset'

    id         = Column(Integer(), primary_key=True, index=True, nullable=False)
    name       = Column(String(60),  nullable=False)
    type       = Column(String(60),  nullable=False)
    unit       = Column(String(60))
    hash       = Column(BIGINT(),  nullable=False, unique=True)
    cr_date    = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.id'))
    hidden     = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    value      = Column('value', Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)

    user = relationship('User', backref=backref("datasets", order_by=id))

    _parents  = ['tResourceScenario']
    _children = ['tMetadata']

    def set_metadata(self, metadata_dict):
        """
            Set the metadata on a dataset

            **metadata_dict**: A dictionary of metadata key-vals.
            Transforms this dict into an array of metadata objects for
            storage in the DB.
        """
        if metadata_dict is None:
            return

        existing_metadata = []
        for m in self.metadata:
            existing_metadata.append(m.key)
            if m.key in metadata_dict:
                if m.value != metadata_dict[m.key]:
                    m.value = metadata_dict[m.key]


        for k, v in metadata_dict.items():
            if k not in existing_metadata:
                m_i = Metadata(key=str(k),value=str(v))
                self.metadata.append(m_i)

        metadata_to_delete =  set(existing_metadata).difference(set(metadata_dict.keys()))
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
                           unit       = self.unit,
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

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = DatasetOwner()
            owner.dataset_id = self.id
            owner.user_id = int(user_id)
            self.owners.append(owner)

        owner.view  = read
        owner.edit  = write
        owner.share = share
        return owner

    def unset_owner(self, user_id):
        owner = None
        if str(user_id) == str(self.created_by):
            log.warn("Cannot unset %s as owner, as they created the dataset", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this dataset
        """

        if _is_admin(user_id):
            return

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on dataset %s" %
                             (user_id, self.id))

    def check_user(self, user_id):
        """
            Check whether this user can read this dataset
        """

        if self.hidden == 'N':
            return True

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    return True
        return False

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this dataset
        """
        if _is_admin(user_id):
            return

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on dataset %s" %
                             (user_id, self.id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this dataset
        """

        if _is_admin(user_id):
            return

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on dataset %s" %
                             (user_id, self.id))

class DatasetCollection(Base, Inspect):
    """
    """

    __tablename__='tDatasetCollection'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(60),  nullable=False)
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
        UniqueConstraint('name', 'dimension', name="unique name dimension"),
    )

    id           = Column(Integer(), primary_key=True, nullable=False)
    name         = Column(String(60),  nullable=False)
    dimension    = Column(String(60), server_default=text(u"'dimensionless'"))
    description  = Column(String(1000))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

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
    layout           = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
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

    __tablename__='tResourceAttrMap'

    network_a_id       = Column(Integer(), ForeignKey('tNetwork.id'), primary_key=True, nullable=False)
    network_b_id       = Column(Integer(), ForeignKey('tNetwork.id'), primary_key=True, nullable=False)
    resource_attr_id_a = Column(Integer(), ForeignKey('tResourceAttr.id'), primary_key=True, nullable=False)
    resource_attr_id_b = Column(Integer(), ForeignKey('tResourceAttr.id'), primary_key=True, nullable=False)

    resourceattr_a = relationship("ResourceAttr", foreign_keys=[resource_attr_id_a])
    resourceattr_b = relationship("ResourceAttr", foreign_keys=[resource_attr_id_b])

    network_a = relationship("Network", foreign_keys=[network_a_id])
    network_b = relationship("Network", foreign_keys=[network_b_id])

class Template(Base, Inspect):
    """
    """

    __tablename__='tTemplate'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(60),  nullable=False, unique=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    layout  = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)

    _parents  = []
    _children = ['tTemplateType']

class TemplateType(Base, Inspect):
    """
    """

    __tablename__='tTemplateType'
    __table_args__ = (
        UniqueConstraint('template_id', 'name', 'resource_type', name="unique type name"),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(60),  nullable=False)
    template_id = Column(Integer(), ForeignKey('tTemplate.id'), nullable=False)
    resource_type = Column(String(60))
    alias = Column(String(100))
    layout  = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    template = relationship('Template', backref=backref("templatetypes", order_by=id, cascade="all, delete-orphan"))


    _parents  = ['tTemplate']
    _children = ['tTypeAttr']

class TypeAttr(Base, Inspect):
    """
    """

    __tablename__='tTypeAttr'

    attr_id = Column(Integer(), ForeignKey('tAttr.id'), primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.id', ondelete='CASCADE'), primary_key=True, nullable=False)
    default_dataset_id = Column(Integer(), ForeignKey('tDataset.id'))
    attr_is_var        = Column(String(1), server_default=text(u"'N'"))
    data_type          = Column(String(60))
    data_restriction   = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
    unit               = Column(String(60))
    description        = Column(String(1000))
    properties         = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    attr = relationship('Attr')
    templatetype = relationship('TemplateType',  backref=backref("typeattrs", order_by=attr_id, cascade="all, delete-orphan"))
    default_dataset = relationship('Dataset')

    _parents  = ['tTemplateType']
    _children = []

    def get_attr(self):

        if self.attr is None:
            attr = get_session().query(Attr).filter(Attr.id==self.attr_id).first()
        else:
            attr = self.attr

        return attr


class ResourceAttr(Base, Inspect):
    """
    """

    __tablename__='tResourceAttr'

    __table_args__ = (
        UniqueConstraint('network_id', 'attr_id', name = 'net_attr_1'),
        UniqueConstraint('project_id', 'attr_id', name = 'proj_attr_1'),
        UniqueConstraint('node_id',    'attr_id', name = 'node_attr_1'),
        UniqueConstraint('link_id',    'attr_id', name = 'link_attr_1'),
        UniqueConstraint('group_id',   'attr_id', name = 'group_attr_1'),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    attr_id = Column(Integer(), ForeignKey('tAttr.id'),  nullable=False)
    ref_key = Column(String(60),  nullable=False, index=True)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.id'), index=True, nullable=True,)
    project_id  = Column(Integer(),  ForeignKey('tProject.id'), index=True, nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.id'), index=True, nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.id'), index=True, nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.id'), index=True, nullable=True)
    attr_is_var = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    attr = relationship('Attr')
    project = relationship('Project', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    network = relationship('Network', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    node = relationship('Node', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    link = relationship('Link', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    resourcegroup = relationship('ResourceGroup', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)

    _parents  = ['tNode', 'tLink', 'tResourceGroup', 'tNetwork', 'tProject']
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

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this resource attribute
        """
        self.get_resource().check_read_permission(user_id)

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this node
        """
        self.get_resource().check_write_permission(user_id)


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
    ref_key = Column(String(60),nullable=False)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.id'), nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.id'), nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.id'), nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.id'), nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))


    templatetype = relationship('TemplateType', backref=backref('resourcetypes', uselist=True, cascade="all, delete-orphan"))

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
    name = Column(String(60),  nullable=False, unique=False)
    description = Column(String(1000))
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
            log.warn("Cannot unset %s as owner, as they created the project", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this project
        """
        
        if _is_admin(user_id):
            return

        if str(user_id) == str(self.created_by):
            return

        for owner in self.owners:
            if owner.user_id == user_id:
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on project %s" %
                             (user_id, self.id))

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        if _is_admin(user_id):
            return

        if str(user_id) == str(self.created_by):
            return

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on project %s" %
                             (user_id, self.id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        if _is_admin(user_id):
            return

        if str(user_id) == str(self.created_by):
            return

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on project %s" %
                             (user_id, self.id))



class Network(Base, Inspect):
    """
    """

    __tablename__='tNetwork'
    __table_args__ = (
        UniqueConstraint('name', 'project_id', name="unique net name"),
    )
    ref_key = 'NETWORK'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200),  nullable=False)
    description = Column(String(1000))
    layout  = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
    project_id = Column(Integer(), ForeignKey('tProject.id'),  nullable=False)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    projection = Column(String(200))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)

    project = relationship('Project', backref=backref("networks", order_by="asc(Network.cr_date)", cascade="all, delete-orphan"))

    _parents  = ['tNode', 'tLink', 'tResourceGroup']
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
        node.layout      = str(layout) if layout is not None else None
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
            log.warn("Cannot unset %s as owner, as they created the network", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this network
        """
        if _is_admin(user_id):
            return

        if int(self.created_by) == int(user_id):
            return

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on network %s" %
                             (user_id, self.id))

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this project
        """
        if _is_admin(user_id):
            return

        if int(self.created_by) == int(user_id):
            return

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on network %s" %
                             (user_id, self.id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this project
        """
        
        if _is_admin(user_id):
            return

        if int(self.created_by) == int(user_id):
            return

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
    name = Column(String(60))
    description = Column(String(1000))
    layout  = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
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

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this link
        """
        self.network.check_read_permission(user_id)

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this link
        """

        self.network.check_write_permission(user_id)

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
    name = Column(String(60),  nullable=False)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    x = Column(Float(precision=10, asdecimal=True))
    y = Column(Float(precision=10, asdecimal=True))
    layout  = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
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

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this node
        """
        self.network.check_read_permission(user_id)

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this node
        """

        self.network.check_write_permission(user_id)

class ResourceGroup(Base, Inspect):
    """
    """

    __tablename__='tResourceGroup'
    __table_args__ = (
        UniqueConstraint('network_id', 'name', name="unique resourcegroup name"),
    )

    ref_key = 'GROUP'
    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(60),  nullable=False)
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

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this group
        """
        self.network.check_read_permission(user_id)

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this group
        """

        self.network.check_write_permission(user_id)

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
                Dataset.unit,
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
    description = Column(String(1000))
    layout  = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), index=True)
    start_time = Column(String(60))
    end_time = Column(String(60))
    locked = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    time_step = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)

    network = relationship('Network', backref=backref("scenarios", order_by=id))

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

class Rule(Base, Inspect):
    """
        A rule is an arbitrary piece of text applied to resources
        within a scenario. A scenario itself cannot have a rule applied
        to it.
    """

    __tablename__='tRule'
    __table_args__ = (
        UniqueConstraint('scenario_id', 'name', name="unique rule name"),
    )


    id = Column(Integer(), primary_key=True, nullable=False)

    name = Column(String(60), nullable=False)
    description = Column(String(1000), nullable=False)

    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    ref_key = Column(String(60),  nullable=False, index=True)

    value = Column(Text().with_variant(mysql.TEXT(4294967295), 'mysql'),  nullable=True)

    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    scenario_id = Column(Integer(), ForeignKey('tScenario.id'),  nullable=False)

    network_id  = Column(Integer(),  ForeignKey('tNetwork.id'), index=True, nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.id'), index=True, nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.id'), index=True, nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.id'), index=True, nullable=True)

    scenario = relationship('Scenario', backref=backref('rules', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')

    _parents  = ['tScenario', 'tNode', 'tLink', 'tProject', 'tNetwork', 'tResourceGroup']
    _children = []

class Note(Base, Inspect):
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

class Perm(Base, Inspect):
    """
    """

    __tablename__='tPerm'

    id = Column(Integer(), primary_key=True, nullable=False)
    code = Column(String(60),  nullable=False)
    name = Column(String(60),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    roleperms = relationship('RolePerm', lazy='joined')

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
    name = Column(String(60),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    roleperms = relationship('RolePerm', lazy='joined', cascade='all')
    roleusers = relationship('RoleUser', lazy='joined', cascade='all')

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

    perm = relationship('Perm', lazy='joined')
    role = relationship('Role', lazy='joined')

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

    user = relationship('User', lazy='joined')
    role = relationship('Role', lazy='joined')

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
    display_name = Column(String(60),  nullable=False, server_default=text(u"''"))
    last_login = Column(TIMESTAMP())
    last_edit = Column(TIMESTAMP())
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    roleusers = relationship('RoleUser', lazy='joined')

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


def create_resourcedata_view():
    #These are for creating the resource data view (see bottom of page)
    from sqlalchemy import select
    from sqlalchemy.schema import DDLElement
    from sqlalchemy.sql import table
    from sqlalchemy.ext import compiler
    from .model import ResourceAttr, ResourceScenario, Attr, Dataset

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
        Dataset.unit,
        Dataset.name,
        Dataset.type,
        Dataset.value]).where(ResourceScenario.resource_attr_id==ResourceAttr.attr_id).where(ResourceAttr.attr_id==Attr.id).where(ResourceScenario.dataset_id==Dataset.id)

    stuff_view = view("vResourceData", Base.metadata, view_qry)
