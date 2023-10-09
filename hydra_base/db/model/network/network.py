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

from ..template import Template, TemplateType
from ..ownership import NetworkOwner
from .resourceattr import ResourceAttr
from ..scenario import ResourceGroupItem
from ..attributes import Attr

from . import Link
from . import Node
from . import ResourceGroup
from .resource import Resource

__all__ = ['Network']



class Network(Base, Inspect, PermissionControlled, Resource):
    """
    """

    __tablename__ = 'tNetwork'
    __table_args__ = (
        UniqueConstraint('name', 'project_id', name="unique net name"),
    )
    ref_key = 'NETWORK'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200), nullable=False)
    description = Column(String(1000))
    layout = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), nullable=True)
    appdata = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), nullable=True)
    project_id = Column(Integer(), ForeignKey('tProject.id'), nullable=False)
    status = Column(String(1), nullable=False, server_default=text(u"'A'"))
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    projection = Column(String(200))
    created_by = Column(Integer(), ForeignKey('tUser.id'), nullable=False)

    project = relationship('Project',
                           backref=backref("networks",
                                           order_by="asc(Network.cr_date)",
                                           cascade="all, delete-orphan"))

    _parents = ['tNode', 'tLink', 'tResourceGroup']
    _children = ['tProject']

    def is_owner(self, user_id):
        """
            Check whether this user is an owner of this project, either directly
            or by virtue of being an owner of a higher-up project
        """

        if self.check_read_permission(user_id, do_raise=False) is True:
            return True

        else:
            return self.project.is_owner(user_id)

    def get_name(self):
        return self.name

    def add_attribute(self, attr_id, attr_is_var='N'):
        res_attr = ResourceAttr()
        res_attr.attr_id = attr_id
        res_attr.attr_is_var = attr_is_var
        res_attr.ref_key = self.ref_key
        res_attr.network_id  = self.id
        self.attributes.append(res_attr)

        return res_attr

    def add_link(self, name, desc, layout, node_1, node_2):
        """
            Add a link to a network. Links are what effectively
            define the network topology, by associating two already
            existing nodes.
        """

        existing_link = get_session().query(Link).filter(Link.name==name, Link.network_id==self.id).first()
        if existing_link is not None:
            raise HydraError("A link with name %s is already in network %s"%(name, self.id))

        l = Link()
        l.name        = name
        l.description = desc
        l.layout           = json.dumps(layout) if layout is not None else None
        l.node_a           = node_1
        l.node_b           = node_2

        get_session().add(l)

        self.links.append(l)

        return l


    def add_node(self, name, desc, layout, node_x, node_y):
        """
            Add a node to a network.
        """
        existing_node = get_session().query(Node).filter(Node.name==name, Node.network_id==self.id).first()
        if existing_node is not None:
            raise HydraError("A node with name %s is already in network %s"%(name, self.id))

        node = Node()
        node.name        = name
        node.description = desc
        node.layout      = str(layout) if layout is not None else None
        node.x           = node_x
        node.y           = node_y

        #Do not call save here because it is likely that we may want
        #to bulk insert nodes, not one at a time.

        get_session().add(node)

        self.nodes.append(node)

        return node

    def add_group(self, name, desc, status):
        """
            Add a new group to a network.
        """

        existing_group = get_session().query(ResourceGroup).filter(ResourceGroup.name==name, ResourceGroup.network_id==self.id).first()
        if existing_group is not None:
            raise HydraError("A resource group with name %s is already in network %s"%(name, self.id))

        group_i             = ResourceGroup()
        group_i.name        = name
        group_i.description = desc
        group_i.status      = status

        get_session().add(group_i)

        self.resourcegroups.append(group_i)


        return group_i

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if str(user_id) == str(o.user_id):
                owner = o
                break
        else:
            owner = NetworkOwner()
            owner.network_id = self.id
            self.owners.append(owner)

        owner.user_id = int(user_id)
        owner.view  = read
        owner.edit  = write
        owner.share = share

        return owner

    def unset_owner(self, user_id):

        owner = None
        if str(user_id) == str(self.created_by):
            log.warning("Cannot unset %s as owner, as they created the network", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this network
        """

        can_read = super(Network, self).check_read_permission(user_id, do_raise=False, is_admin=is_admin)

        if can_read is True:
            return True

        can_read = self.project.check_read_permission(user_id, do_raise=False)

        if can_read is False and do_raise is True:
            raise PermissionError("Permission denied. User %s does not have read"
                         " access on network %s" %
                         (user_id, self.id))

        return can_read

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this project
        """
        can_write = super(Network, self).check_write_permission(user_id, do_raise=False, is_admin=is_admin)

        if can_write is True:
            return True

        can_write = self.project.check_write_permission(user_id)

        if can_write is False and do_raise is True:
            raise PermissionError("Permission denied. User %s does not have edit"
                         " access on network %s" %
                         (user_id, self.id))

        return can_write

    def get_owners(self):
        """
            Get all the owners of a network, both those which are applied directly
            to this network, but also who have been granted access via a project
        """

        owners = [JSONObject(o) for o in self.owners]
        owner_ids = [o.user_id for o in owners]

        project_owners = list(filter(lambda x:x.user_id not in owner_ids, self.project.get_owners()))

        for po in project_owners:
            po.source = f'Inherited from: {po.project_name} (ID:{po.project_id})'

        return owners + project_owners

    def get_scoped_attributes(self, include_hierarchy=False, name_match=None, return_json=True):
        """
            Get all the attributes scoped to this project, and to all projects above
            it in the project hierarchy (including global attributes if requested)
            args:
                include_hierarchy (Bool): Include attribtues from projects higher up in the 
                    project hierarchy
        """

        scoped_attrs_qry = get_session().query(Attr).filter(Attr.network_id==self.id)

        if name_match is not None:
            name_match = name_match.lower()
            scoped_attrs_qry = scoped_attrs_qry.filter(
                func.lower(Attr.name).like(f'%{name_match}%'))

        scoped_attrs = scoped_attrs_qry.all()

        if include_hierarchy is True:
            scoped_attrs.extend(self.project.get_scoped_attributes(
                include_hierarchy=True, name_match=name_match))

        if return_json is True:
            
            scoped_attrs_j = [JSONObject(a) for a in scoped_attrs]
            #This is for convenience to avoid having to do extra calls to get the network name
            for a in scoped_attrs_j:
                a.network_name = self.name
            return scoped_attrs_j
        else:
            return scoped_attrs