# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
from sqlalchemy import Column,\
ForeignKey,\
text,\
Integer,\
Numeric,\
String,\
LargeBinary,\
BigInteger,\
DateTime,\
TIMESTAMP,\
BIGINT,\
Float,\
Text

from HydraLib.HydraException import HydraError, PermissionError

from sqlalchemy.orm import relationship, backref

from HydraLib.dateutil import ordinal_to_timestamp, get_datetime

from HydraServer.db import DeclarativeBase as Base, DBSession

from HydraServer.util import generate_data_hash, get_val

from sqlalchemy.sql.expression import case
from sqlalchemy import UniqueConstraint, and_

import pandas as pd

import logging
import bcrypt
log = logging.getLogger(__name__)

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

class Dataset(Base):
    """
        Table holding all the attribute values
    """
    __tablename__='tDataset'

    dataset_id = Column(Integer(), primary_key=True, index=True, nullable=False)
    data_type = Column(String(60),  nullable=False)
    data_units = Column(String(60))
    data_dimen = Column(String(60), server_default='dimensionless')
    data_name = Column(String(60),  nullable=False)
    data_hash = Column(BIGINT(),  nullable=False, unique=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))
    hidden = Column(String(1),  nullable=False, server_default=text(u"'N'"))

    start_time = Column(String(60),  nullable=True)
    frequency = Column(String(10),  nullable=True)
    value = Column('value', LargeBinary(),  nullable=True)

    useruser = relationship('User', backref=backref("datasets", order_by=dataset_id))

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
            existing_metadata.append(m.metadata_name)
            if m.metadata_name in metadata_dict.keys():
                if m.metadata_val != metadata_dict[m.metadata_name]:
                    m.metadata_val = metadata_dict[m.metadata_name]

        for k, v in metadata_dict.items():
            if k not in existing_metadata:
                m_i = Metadata(metadata_name=str(k),metadata_val=str(v))
                self.metadata.append(m_i)

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

    def set_val(self, data_type, val):
        if data_type in ('descriptor','scalar','array'):
            self.value = str(val)
        elif data_type == 'eqtimeseries':
            self.start_time = str(val[0])
            self.frequency  = str(val[1])
            self.value      = str(val[2])
        elif data_type == 'timeseries':
            if type(val) == list:
                test_val_keys = []
                test_vals = []
                for time, value in val:
                    try:
                        v = eval(value)
                    except:
                        v = value
                    try:
                        test_val_keys.append(get_datetime(time))
                    except:
                        test_val_keys.append(time)
                    test_vals.append(v)

                timeseries_pd = pd.DataFrame(test_vals, index=pd.Series(test_val_keys))
                #Epoch doesn't work here because dates before 1970 are not supported
                #in read_json. Ridiculous.
                self.value =  timeseries_pd.to_json(date_format='iso', date_unit='ns')
            else:
                self.value = val
        else:
            raise HydraError("Invalid data type %s"%(data_type,))

    def set_hash(self,metadata=None):


        if metadata is None:
            metadata = self.get_metadata_as_dict()

        dataset_dict = dict(data_name = self.data_name,
                           data_units = self.data_units,
                           data_dimen = self.data_dimen,
                           data_type  = self.data_type,
                           value      = self.value,
                           metadata   = metadata)

        data_hash = generate_data_hash(dataset_dict)

        self.data_hash = data_hash

        return data_hash

    def get_metadata_as_dict(self):
        metadata = {}
        for r in self.metadata:
            val = str(r.metadata_val)

            metadata[str(r.metadata_name)] = val

        return metadata

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = DatasetOwner()
            owner.dataset_id = self.dataset_id
            owner.user_id = int(user_id)
            self.owners.append(owner)

        owner.view  = read
        owner.edit  = write
        owner.share = share
        return owner

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this dataset
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on dataset %s" %
                             (user_id, self.dataset_id))

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

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on dataset %s" %
                             (user_id, self.dataset_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this dataset
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on dataset %s" %
                             (user_id, self.dataset_id))

class DatasetCollection(Base):
    """
    """

    __tablename__='tDatasetCollection'

    collection_id = Column(Integer(), primary_key=True, nullable=False)
    collection_name = Column(String(60),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

class DatasetCollectionItem(Base):
    """
    """

    __tablename__='tDatasetCollectionItem'

    collection_id = Column(Integer(), ForeignKey('tDatasetCollection.collection_id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    collection = relationship('DatasetCollection', backref=backref("items", order_by=dataset_id, cascade="all, delete-orphan"))
    dataset = relationship('Dataset', backref=backref("collectionitems", order_by=dataset_id))


class Metadata(Base):
    """
    """

    __tablename__='tMetadata'

    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False, index=True)
    metadata_name = Column(String(60), primary_key=True, nullable=False)
    metadata_val = Column(LargeBinary(),  nullable=False)

    dataset = relationship('Dataset', backref=backref("metadata", order_by=dataset_id, cascade="all, delete-orphan"))



#********************************************************
#Attributes & Templates
#********************************************************

class Attr(Base):
    """
    """

    __tablename__='tAttr'

    __table_args__ = (
        UniqueConstraint('attr_name', 'attr_dimen', name="unique name dimension"),
    )

    attr_id = Column(Integer(), primary_key=True, nullable=False)
    attr_name = Column(String(60),  nullable=False)
    attr_dimen = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

class AttrMap(Base):
    """
    """

    __tablename__='tAttrMap'

    attr_id_a = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)
    attr_id_b = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)

    attr_a = relationship("Attr", foreign_keys=[attr_id_a], backref=backref('maps_to', order_by=attr_id_a))
    attr_b = relationship("Attr", foreign_keys=[attr_id_b], backref=backref('maps_from', order_by=attr_id_b))

class Template(Base):
    """
    """

    __tablename__='tTemplate'

    template_id = Column(Integer(), primary_key=True, nullable=False)
    template_name = Column(String(60),  nullable=False, unique=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    layout = Column(Text(1000))

class TemplateType(Base):
    """
    """

    __tablename__='tTemplateType'
    __table_args__ = (
        UniqueConstraint('template_id', 'type_name', name="unique type name"),
    )

    type_id = Column(Integer(), primary_key=True, nullable=False)
    type_name = Column(String(60),  nullable=False)
    template_id = Column(Integer(), ForeignKey('tTemplate.template_id'), nullable=False)
    resource_type = Column(String(60))
    alias = Column(String(100))
    layout = Column(Text(1000))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    template = relationship('Template', backref=backref("templatetypes", order_by=type_id, cascade="all, delete-orphan"))

class TypeAttr(Base):
    """
    """

    __tablename__='tTypeAttr'

    attr_id = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.type_id', ondelete='CASCADE'), primary_key=True, nullable=False)
    default_dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'))
    attr_is_var        = Column(String(1), server_default=text(u"'N'"))
    data_type          = Column(String(60))
    data_restriction   = Column(Text(1000))
    unit               = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    attr = relationship('Attr')
    templatetype = relationship('TemplateType',  backref=backref("typeattrs", order_by=attr_id, cascade="all, delete-orphan"))
    default_dataset = relationship('Dataset')

    def get_attr(self):

        if self.attr is None:
            attr = DBSession.query(Attr).filter(Attr.attr_id==self.attr_id).first()
        else:
            attr = self.attr

        return attr


class ResourceAttr(Base):
    """
    """

    __tablename__='tResourceAttr'

    resource_attr_id = Column(Integer(), primary_key=True, nullable=False)
    attr_id = Column(Integer(), ForeignKey('tAttr.attr_id'),  nullable=False)
    ref_key = Column(String(60),  nullable=False, index=True)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.network_id'), index=True, nullable=True,)
    project_id  = Column(Integer(),  ForeignKey('tProject.project_id'), index=True, nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'), index=True, nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'), index=True, nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.group_id'), index=True, nullable=True)
    attr_is_var = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    attr = relationship('Attr')
    project = relationship('Project', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    network = relationship('Network', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    node = relationship('Node', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    link = relationship('Link', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)
    resourcegroup = relationship('ResourceGroup', backref=backref('attributes', uselist=True, cascade="all, delete-orphan"), uselist=False)


    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'NETWORK':
            return self.network
        elif ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.group
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

class ResourceType(Base):
    """
    """

    __tablename__='tResourceType'
    __table_args__ = (
        UniqueConstraint('network_id', 'type_id', name='net_type_1'),
        UniqueConstraint('node_id', 'type_id', name='node_type_1'),
        UniqueConstraint('link_id', 'type_id',  name = 'link_type_1'),
        UniqueConstraint('group_id', 'type_id', name = 'group_type_1'),

    )
    resource_type_id = Column(Integer, primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.type_id'), primary_key=False, nullable=False)
    ref_key = Column(String(60),nullable=False)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.network_id'), nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'), nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'), nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.group_id'), nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))


    templatetype = relationship('TemplateType', backref=backref('resourcetypes', uselist=True, cascade="all, delete-orphan"))

    network = relationship('Network', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    node = relationship('Node', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    link = relationship('Link', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    resourcegroup = relationship('ResourceGroup', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)

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

class Project(Base):
    """
    """

    __tablename__='tProject'
    ref_key = 'PROJECT'

    attribute_data = []

    project_id = Column(Integer(), primary_key=True, nullable=False)
    project_name = Column(String(60),  nullable=False, unique=True)
    project_description = Column(String(1000))
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))

    user = relationship('User', backref=backref("projects", order_by=project_id))

    def get_name(self):
        return self.project_name

    def get_attribute_data(self):
        attribute_data_rs = DBSession.query(ResourceScenario).join(ResourceAttr).filter(ResourceAttr.project_id==1).all()
        self.attribute_data = attribute_data_rs
        return attribute_data_rs

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.project_id  = self.project_id
        self.attributes.append(attr)

        return attr

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = ProjectOwner()
            owner.project_id = self.project_id
            owner.user_id = int(user_id)
            self.owners.append(owner)

        owner.view  = read
        owner.edit  = write
        owner.share = share

        return owner

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this project
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on project %s" %
                             (user_id, self.project_id))

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
                             " access on project %s" %
                             (user_id, self.project_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on project %s" %
                             (user_id, self.project_id))



class Network(Base):
    """
    """

    __tablename__='tNetwork'
    __table_args__ = (
        UniqueConstraint('network_name', 'project_id', name="unique net name"),
    )
    ref_key = 'NETWORK'

    network_id = Column(Integer(), primary_key=True, nullable=False)
    network_name = Column(String(60),  nullable=False)
    network_description = Column(String(1000))
    network_layout = Column(Text(1000))
    project_id = Column(Integer(), ForeignKey('tProject.project_id'),  nullable=False)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    projection = Column(String(1000))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))

    project = relationship('Project', backref=backref("networks", order_by=network_id, cascade="all, delete-orphan"))

    def get_name(self):
        return self.network_name

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.network_id  = self.network_id
        self.attributes.append(attr)

        return attr

    def add_link(self, name, desc, layout, node_1, node_2):
        """
            Add a link to a network. Links are what effectively
            define the network topology, by associating two already
            existing nodes.
        """

        existing_link = DBSession.query(Link).filter(Link.link_name==name, Link.network_id==self.network_id).first()
        if existing_link is not None:
            raise HydraError("A link with name %s is already in network %s"%(name, self.network_id))

        l = Link()
        l.link_name        = name
        l.link_description = desc
        l.link_layout      = str(layout)
        l.node_a           = node_1
        l.node_b           = node_2

        DBSession.add(l)

        self.links.append(l)

        return l


    def add_node(self, name, desc, layout, node_x, node_y):
        """
            Add a node to a network.
        """

        existing_node = DBSession.query(Node).filter(Node.node_name==name, Node.network_id==self.network_id).first()
        if existing_node is not None:
            raise HydraError("A node with name %s is already in network %s"%(name, self.network_id))

        node = Node()
        node.node_name        = name
        node.node_description = desc
        node.node_layout      = str(layout)
        node.node_x           = node_x
        node.node_y           = node_y

        #Do not call save here because it is likely that we may want
        #to bulk insert nodes, not one at a time.

        DBSession.add(node)

        self.nodes.append(node)

        return node

    def add_group(self, name, desc, status):
        """
            Add a new group to a network.
        """

        existing_group = DBSession.query(ResourceGroup).filter(ResourceGroup.group_name==name, ResourceGroup.network_id==self.network_id).first()
        if existing_group is not None:
            raise HydraError("A resource group with name %s is already in network %s"%(name, self.network_id))

        group_i                      = ResourceGroup()
        group_i.group_name        = name
        group_i.group_description = desc
        group_i.status            = status

        DBSession.add(group_i)

        self.resourcegroups.append(group_i)


        return group_i

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = NetworkOwner()
            owner.network_id = self.network_id
            self.owners.append(owner)

        owner.user_id = int(user_id)
        owner.view  = read
        owner.edit  = write
        owner.share = share

        return owner

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this network
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on network %s" %
                             (user_id, self.network_id))

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
                             " access on network %s" %
                             (user_id, self.network_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on network %s" %
                             (user_id, self.network_id))

class Link(Base):
    """
    """

    __tablename__='tLink'

    __table_args__ = (
        UniqueConstraint('network_id', 'link_name', name="unique link name"),
    )
    ref_key = 'LINK'

    link_id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), nullable=False)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    node_1_id = Column(Integer(), ForeignKey('tNode.node_id'), nullable=False)
    node_2_id = Column(Integer(), ForeignKey('tNode.node_id'), nullable=False)
    link_name = Column(String(60))
    link_description = Column(String(1000))
    link_layout = Column(Text(1000))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    network = relationship('Network', backref=backref("links", order_by=network_id, cascade="all, delete-orphan"), lazy='joined')
    node_a = relationship('Node', foreign_keys=[node_1_id], backref=backref("links_to", order_by=link_id, cascade="all, delete-orphan"))
    node_b = relationship('Node', foreign_keys=[node_2_id], backref=backref("links_from", order_by=link_id, cascade="all, delete-orphan"))

    def get_name(self):
        return self.link_name

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.link_id  = self.link_id
        self.attributes.append(attr)

        return attr

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

class Node(Base):
    """
    """

    __tablename__='tNode'
    __table_args__ = (
        UniqueConstraint('network_id', 'node_name', name="unique node name"),
    )
    ref_key = 'NODE'

    node_id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), nullable=False)
    node_description = Column(String(1000))
    node_name = Column(String(60),  nullable=False)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    node_x = Column(Float(precision=10, asdecimal=True))
    node_y = Column(Float(precision=10, asdecimal=True))
    node_layout = Column(Text(1000))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    network = relationship('Network', backref=backref("nodes", order_by=network_id, cascade="all, delete-orphan"), lazy='joined')

    def get_name(self):
        return self.node_name

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.node_id  = self.node_id
        self.attributes.append(attr)

        return attr

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

class ResourceGroup(Base):
    """
    """

    __tablename__='tResourceGroup'
    __table_args__ = (
        UniqueConstraint('network_id', 'group_name', name="unique resourcegroup name"),
    )

    ref_key = 'GROUP'
    group_id = Column(Integer(), primary_key=True, nullable=False)
    group_name = Column(String(60),  nullable=False)
    group_description = Column(String(1000))
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'),  nullable=False)

    network = relationship('Network', backref=backref("resourcegroups", order_by=group_id, cascade="all, delete-orphan"), lazy='joined')

    def get_name(self):
        return self.group_name

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.group_id  = self.group_id
        self.attributes.append(attr)

        return attr

    def get_items(self, scenario_id):
        """
            Get all the items in this group, in the given scenario
        """
        items = DBSession.query(ResourceGroupItem)\
                .filter(ResourceGroupItem.group_id==self.group_id).\
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

class ResourceGroupItem(Base):
    """
    """

    __tablename__='tResourceGroupItem'

    item_id = Column(Integer(), primary_key=True, nullable=False)
    ref_key = Column(String(60),  nullable=False)

    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'))
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'))
    subgroup_id = Column(Integer(),  ForeignKey('tResourceGroup.group_id'))

    group_id = Column(Integer(), ForeignKey('tResourceGroup.group_id'))
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'),  nullable=False, index=True)

    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    
    group = relationship('ResourceGroup', foreign_keys=[group_id], backref=backref("items", order_by=group_id))
    scenario = relationship('Scenario', backref=backref("resourcegroupitems", order_by=item_id, cascade="all, delete-orphan"))

    #These need to have backrefs to allow the deletion of networks & projects
    #--There needs to be a connection between the items & the resources to allow it
    node = relationship('Node', backref=backref("resourcegroupitems", order_by=item_id, cascade="all, delete-orphan"))
    link = relationship('Link', backref=backref("resourcegroupitems", order_by=item_id, cascade="all, delete-orphan"))
    subgroup = relationship('ResourceGroup', foreign_keys=[subgroup_id])


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

class ResourceScenario(Base):
    """
    """

    __tablename__='tResourceScenario'

    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'), primary_key=True, nullable=False, index=True)
    resource_attr_id = Column(Integer(), ForeignKey('tResourceAttr.resource_attr_id'), primary_key=True, nullable=False, index=True)
    source           = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    dataset      = relationship('Dataset', backref=backref("resourcescenarios", order_by=dataset_id))
    scenario     = relationship('Scenario', backref=backref("resourcescenarios", order_by=resource_attr_id, cascade="all, delete-orphan"))
    resourceattr = relationship('ResourceAttr', backref=backref("resourcescenarios", cascade="all, delete-orphan"), uselist=False)

    def get_dataset(self, user_id):
        dataset = DBSession.query(Dataset.dataset_id,
                Dataset.data_type,
                Dataset.data_units,
                Dataset.data_dimen,
                Dataset.data_name,
                Dataset.hidden,
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.start_time).label('start_time'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.frequency).label('frequency'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.value).label('value')).filter(
                Dataset.dataset_id==self.dataset_id).outerjoin(DatasetOwner,
                                    and_(Dataset.dataset_id==DatasetOwner.dataset_id,
                                    DatasetOwner.user_id==user_id)).one()

        return dataset

class Scenario(Base):
    """
    """

    __tablename__='tScenario'
    __table_args__ = (
        UniqueConstraint('network_id', 'scenario_name', name="unique scenario name"),
    )

    scenario_id = Column(Integer(), primary_key=True, index=True, nullable=False)
    scenario_name = Column(String(60),  nullable=False)
    scenario_description = Column(String(1000))
    scenario_layout = Column(Text(1000))
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), index=True)
    start_time = Column(String(60))
    end_time = Column(String(60))
    locked = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    time_step = Column(String(60))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))

    network = relationship('Network', backref=backref("scenarios", order_by=scenario_id))

    def add_resource_scenario(self, resource_attr, dataset=None, source=None):
        rs_i = ResourceScenario()
        if resource_attr.resource_attr_id is None:
            rs_i.resourceattr = resource_attr
        else:
            rs_i.resource_attr_id = resource_attr.resource_attr_id

        if dataset.dataset_id is None:
            rs_i.dataset = dataset
        else:
            rs_i.dataset_id = dataset.dataset_id
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

class Rule(Base):
    """
        A rule is an arbitrary piece of text applied to resources
        within a scenario. A scenario itself cannot have a rule applied
        to it.
    """

    __tablename__='tRule'
    __table_args__ = (
        UniqueConstraint('scenario_id', 'rule_name', name="unique rule name"),
    )


    rule_id = Column(Integer(), primary_key=True, nullable=False)

    rule_name = Column(String(60), nullable=False)
    rule_description = Column(String(1000), nullable=False)

    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    ref_key = Column(String(60),  nullable=False, index=True)


    rule_text = Column('value', LargeBinary(),  nullable=True)

    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'),  nullable=False)

    network_id  = Column(Integer(),  ForeignKey('tNetwork.network_id'), index=True, nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'), index=True, nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'), index=True, nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.group_id'), index=True, nullable=True)

    scenario = relationship('Scenario', backref=backref('rules', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')

class Note(Base):
    """
        A note is an arbitrary piece of text which can be applied
        to any resource. A note is NOT scenario dependent. It is applied
        directly to resources. A note can be applied to a scenario.
    """

    __tablename__='tNote'

    note_id = Column(Integer(), primary_key=True, nullable=False)

    ref_key = Column(String(60),  nullable=False, index=True)

    note_text = Column('value', LargeBinary(),  nullable=True)

    created_by = Column(Integer(), ForeignKey('tUser.user_id'))

    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'),  index=True, nullable=True)
    project_id = Column(Integer(), ForeignKey('tProject.project_id'),  index=True, nullable=True)

    network_id  = Column(Integer(),  ForeignKey('tNetwork.network_id'), index=True, nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'), index=True, nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'), index=True, nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.group_id'), index=True, nullable=True)

    scenario = relationship('Scenario', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    node = relationship('Node', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    link = relationship('Link', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    group = relationship('ResourceGroup', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    network = relationship('Network', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')
    project = relationship('Project', backref=backref('notes', uselist=True, cascade="all, delete-orphan"), uselist=True, lazy='joined')

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
class ProjectOwner(Base):
    """
    """

    __tablename__='tProjectOwner'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    project_id = Column(Integer(), ForeignKey('tProject.project_id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User')
    project = relationship('Project', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

class NetworkOwner(Base):
    """
    """

    __tablename__='tNetworkOwner'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User')
    network = relationship('Network', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

class DatasetOwner(Base):
    """
    """

    __tablename__='tDatasetOwner'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)

    user = relationship('User')
    dataset = relationship('Dataset', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

class Perm(Base):
    """
    """

    __tablename__='tPerm'

    perm_id = Column(Integer(), primary_key=True, nullable=False)
    perm_code = Column(String(60),  nullable=False)
    perm_name = Column(String(60),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    roleperms = relationship('RolePerm', lazy='joined')

class Role(Base):
    """
    """

    __tablename__='tRole'

    role_id = Column(Integer(), primary_key=True, nullable=False)
    role_code = Column(String(60),  nullable=False)
    role_name = Column(String(60),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    roleperms = relationship('RolePerm', lazy='joined', cascade='all')
    roleusers = relationship('RoleUser', lazy='joined', cascade='all')

    @property
    def permissions(self):
        return set([rp.perm for rp in self.roleperms])


class RolePerm(Base):
    """
    """

    __tablename__='tRolePerm'

    perm_id = Column(Integer(), ForeignKey('tPerm.perm_id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.role_id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    perm = relationship('Perm', lazy='joined')
    role = relationship('Role', lazy='joined')

class RoleUser(Base):
    """
    """

    __tablename__='tRoleUser'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.role_id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    user = relationship('User', lazy='joined')
    role = relationship('Role', lazy='joined')

class User(Base):
    """
    """

    __tablename__='tUser'

    user_id = Column(Integer(), primary_key=True, nullable=False)
    username = Column(String(60),  nullable=False, unique=True)
    password = Column(String(1000),  nullable=False)
    display_name = Column(String(60),  nullable=False, server_default=text(u"''"))
    last_login = Column(TIMESTAMP())
    last_edit = Column(TIMESTAMP())
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    roleusers = relationship('RoleUser', lazy='joined')

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

from HydraServer.db import engine
Base.metadata.create_all(engine)
