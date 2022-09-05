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


__all__ = ['Unit', 'Dimension']

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
    abbreviation = Column(Unicode(60).with_variant(mysql.VARCHAR(60, collation='utf8_bin'), 'mysql'),  nullable=False)
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
    from .network import ResourceAttr, ResourceScenario
    from .dataset import Dataset
    from .attributes import Attr
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
