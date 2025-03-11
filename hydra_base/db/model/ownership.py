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

__all__ = ['ProjectOwner', 'NetworkOwner', 'DatasetOwner']

class ProjectOwner(Base, Inspect):
    """
    """

    __tablename__='tProjectOwner'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    project_id = Column(Integer(), ForeignKey('tProject.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False, default='Y')
    edit = Column(String(1),  nullable=False, default='N')
    share = Column(String(1),  nullable=False, default='N')

    user = relationship('User')
    project = relationship('Project', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

    _parents  = ['tProject', 'tUser']
    _children = []

    @property
    def read(self):
        return self.view

class NetworkOwner(Base, Inspect):
    """
    """

    __tablename__='tNetworkOwner'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False, default='Y')
    edit = Column(String(1),  nullable=False, default='N')
    share = Column(String(1),  nullable=False, default='N')

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
    view = Column(String(1),  nullable=False, default='Y')
    edit = Column(String(1),  nullable=False, default='N')
    share = Column(String(1),  nullable=False, default='N')

    user = relationship('User')
    dataset = relationship('Dataset', backref=backref('owners', order_by=user_id, uselist=True, cascade="all, delete-orphan"))

    _parents  = ['tDataset', 'tUser']
    _children = []