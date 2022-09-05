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
from .base import *

__all__ = ['Note']

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


