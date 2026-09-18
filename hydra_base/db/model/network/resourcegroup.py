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

__all__ = ['ResourceGroup']

class ResourceGroup(Base, Inspect, Resource):
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

