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
from ..base import *

__all__ = ['ResourceAttr', 'ResourceAttrMap']

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