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
from . import *


__all__ = ['Attr', 'AttrMap', 'AttrGroup', 'AttrGroupItem']

class Attr(Base, Inspect):
    """
    An Attribute Definition
    """

    __tablename__='tAttr'

    __table_args__ = (
        UniqueConstraint('name', 'dimension_id', 'network_id', 'project_id', name="unique name dimension_id"),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200),  nullable=False)
    dimension_id = Column(Integer(), ForeignKey('tDimension.id'), nullable=True)
    description = Column(String(1000))
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    project_id = Column(Integer(), ForeignKey('tProject.id'), nullable=True)
    network_id = Column(Integer(), ForeignKey('tNetwork.id'), nullable=True)

    network = relationship('Network', foreign_keys=[network_id], backref=backref('scopedattributes', uselist=True, cascade="all, delete-orphan"), lazy='joined')
    project = relationship('Project', foreign_keys=[project_id], backref=backref('scopedattributes', uselist=True, cascade="all, delete-orphan"), lazy='joined')
    dimension = relationship('Dimension', foreign_keys=[dimension_id], backref=backref("attributes", uselist=True))

    _parents = ['tDimension']
    _children = []

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
    layout           = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)
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