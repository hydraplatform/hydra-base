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

from .resourceattr import ResourceAttr
from .resource import Resource

__all__ = ['Link']

class Link(Base, Inspect, Resource):
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

