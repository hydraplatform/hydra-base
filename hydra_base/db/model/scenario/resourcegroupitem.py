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

__all__ = ['ResourceGroupItem']

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
