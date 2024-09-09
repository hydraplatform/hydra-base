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

__all__ = ["ProjectOwner", "NetworkOwner", "RuleOwner", "DatasetOwner"]

class OwnerMixin(object):

    user_id = Column(
        Integer(), ForeignKey("tUser.id"), primary_key=True, nullable=False, name="user_id"
    )

    @declared_attr
    def view(cls):
        return Column(String(1), nullable=False, default="Y")
    
    @declared_attr
    def edit(cls):
        return Column(String(1), nullable=False, default="N")
    
    @declared_attr
    def share(cls):
        return Column(String(1), nullable=False, default="N")

    @declared_attr
    def user(cls):
        return relationship("User", foreign_keys=[cls.user_id])


class ProjectOwner(Base, Inspect, AuditMixin, OwnerMixin):
    """
    """

    __tablename__='tProjectOwner'
    project_id = Column(
        Integer(), ForeignKey("tProject.id"), primary_key=True, nullable=False
    )

    project = relationship(
        "Project",
        backref=backref(
            "owners",
            order_by=OwnerMixin.user_id,
            uselist=True,
            cascade="all, delete-orphan",
        ),
    )

    _parents = ["tProject", "tUser"]
    _children = []

    @property
    def read(self):
        return self.view

class NetworkOwner(Base, Inspect, AuditMixin, OwnerMixin):
    """ """
    __tablename__='tNetworkOwner'

    network_id = Column(
        Integer(), ForeignKey("tNetwork.id"), primary_key=True, nullable=False
    )

    network = relationship(
        "Network",
        backref=backref(
            "owners",
            order_by=OwnerMixin.user_id,
            uselist=True,
            cascade="all, delete-orphan",
        ),
    )

    _parents = ["tNetwork", "tUser"]
    _children = []

class RuleOwner(Base, Inspect, AuditMixin, OwnerMixin):
    """
    This table tracks the owners of rules, to ensure rules which contain confidential logic
    can be kept hidden
    """

    __tablename__ = "tRuleOwner"

    rule_id = Column(
        Integer(), ForeignKey("tRule.id"), primary_key=True, nullable=False
    )

    rule = relationship(
        "Rule",
        backref=backref(
            "owners",
            order_by=OwnerMixin.user_id,
            uselist=True,
            cascade="all, delete-orphan",
        ),
    )

    _parents = ["tRule", "tUser"]
    _children = []


class DatasetOwner(Base, Inspect, AuditMixin, OwnerMixin):
    """ """

    __tablename__ = "tDatasetOwner"

    dataset_id = Column(
        Integer(), ForeignKey("tDataset.id"), primary_key=True, nullable=False
    )

    dataset = relationship(
        "Dataset",
        backref=backref(
            "owners",
            order_by=OwnerMixin.user_id,
            uselist=True,
            cascade="all, delete-orphan",
        ),
    )

    _parents = ["tDataset", "tUser"]
    _children = []
