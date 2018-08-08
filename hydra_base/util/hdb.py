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

from ..db.model import Network, Scenario, Project, User, Role, Perm, RolePerm, RoleUser, ResourceAttr, ResourceType
from sqlalchemy.orm.exc import NoResultFound
from .. import db
import datetime
import random
import bcrypt
from ..exceptions import HydraError
import transaction
import logging
log = logging.getLogger(__name__)


def add_resource_types(resource_i, types):
    """
    Save a reference to the types used for this resource.

    @returns a list of type_ids representing the type ids
    on the resource.

    """
    if types is None:
        return []

    existing_type_ids = []
    if resource_i.types:
        for t in resource_i.types:
            existing_type_ids.append(t.type_id)

    new_type_ids = []
    for templatetype in types:

        if templatetype.id in existing_type_ids:
            continue

        rt_i = ResourceType()
        rt_i.type_id     = templatetype.id
        rt_i.ref_key     = resource_i.ref_key
        if resource_i.ref_key == 'NODE':
            rt_i.node_id      = resource_i.id
        elif resource_i.ref_key == 'LINK':
            rt_i.link_id      = resource_i.id
        elif resource_i.ref_key == 'GROUP':
            rt_i.group_id     = resource_i.id
        resource_i.types.append(rt_i)
        new_type_ids.append(templatetype.id)

    return new_type_ids

def add_resource_attributes(resource_i, attributes):
    if attributes is None:
        return {}
    resource_attrs = {}
    #ra is for ResourceAttr
    for ra in attributes:

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, ra.attr_is_var)
            db.DBSession.add(ra_i)
        else:
            ra_i = db.DBSession.query(ResourceAttr).filter(ResourceAttr.id==ra.id).one()
            ra_i.attr_is_var = ra.attr_is_var

        resource_attrs[ra.id] = ra_i
    return resource_attrs


def make_root_user():
    try:
        user = db.DBSession.query(User).filter(User.username=='root').one()
    except NoResultFound:

        root_pwd = bcrypt.hashpw(b'', bcrypt.gensalt())

        user = User(username='root',
                    password=root_pwd,
                    display_name='Root User')
        db.DBSession.add(user)

    try:
        role = db.DBSession.query(Role).filter(Role.code=='admin').one()
    except NoResultFound:
        raise HydraError("Admin role not found.")

    try:
        userrole = db.DBSession.query(RoleUser).filter(RoleUser.role_id==role.id,
                                                   RoleUser.user_id==user.id).one()
    except NoResultFound:
        userrole = RoleUser(role_id=role.id,user_id=user.id)
        user.roleusers.append(userrole)
        db.DBSession.add(userrole)


    db.DBSession.flush()

    user_id = user.id

    transaction.commit()

    return user_id


def login_user(username, password):
    try:
        user_i = db.DBSession.query(User).filter( User.username==username ).one()
    except NoResultFound:
        raise HydraError(username)

    userPassword = ""
    try:
        userPassword = user_i.password.encode('utf-8')
    except AttributeError:
        userPassword = user_i.password

    try:
        password = password.encode('utf-8')
    except AttributeError:
        pass

    if bcrypt.hashpw(password, userPassword) == userPassword:
        user_i.last_login = datetime.datetime.now()
        return user_i.id
    else:
        raise HydraError(username)

def create_default_net():
    try:
        net = db.DBSession.query(Network).filter(Network.id==1).one()
    except NoResultFound:
        project = Project(name="Project network", created_by=1)
        net = Network(name="Default network", created_by=1)
        scen = Scenario(name="Default network", created_by=1)
        project.networks.append(net)
        net.scenarios.append(scen)
        db.DBSession.add(net)
    db.DBSession.flush()
    transaction.commit()
    return net


def create_default_users_and_perms():

    perms = db.DBSession.query(Perm).all()
    if len(perms) > 0:
        return

    default_perms = ( ("add_user",   "Add User"),
                    ("edit_user",  "Edit User"),
                    ("add_role",   "Add Role"),
                    ("edit_role",  "Edit Role"),
                    ("add_perm",   "Add Permission"),
                    ("edit_perm",  "Edit Permission"),

                    ("add_network",    "Add network"),
                    ("edit_network",   "Edit network"),
                    ("delete_network", "Delete network"),
                    ("share_network",  "Share network"),
                    ("edit_topology",  "Edit network topology"),

                    ("add_project",    "Add Project"),
                    ("edit_project",   "Edit Project"),
                    ("delete_project", "Delete Project"),
                    ("share_project",  "Share Project"),

                    ("edit_data", "Edit network data"),
                    ("view_data", "View network data"),

                    ("add_template", "Add Template"),
                    ("edit_template", "Edit Template"))

    default_roles = (
                    ("admin",    "Administrator"),
                    ("dev",      "Developer"),
                    ("modeller", "Modeller / Analyst"),
                    ("manager",  "Manager"),
                    ("grad",     "Graduate"),
                    ("developer", "Developer"),
                    ("decision", "Decision Maker"),
                )

    roleperms = (
            ('admin', "add_user"),
            ('admin', "edit_user"),
            ('admin', "add_role"),
            ('admin', "edit_role"),
            ('admin', "add_perm"),
            ('admin', "edit_perm"),
            ('admin', "add_network"),
            ('admin', "edit_network"),
            ('admin', "delete_network"),
            ('admin', "share_network"),
            ('admin', "add_project"),
            ('admin', "edit_project"),
            ('admin', "delete_project"),
            ('admin', "share_project"),
            ('admin', "edit_topology"),
            ('admin', "edit_data"),
            ('admin', "view_data"),
            ('admin', "add_template"),
            ('admin', "edit_template"),

            ("developer", "add_network"),
            ("developer", "edit_network"),
            ("developer", "delete_network"),
            ("developer", "share_network"),
            ("developer", "add_project"),
            ("developer", "edit_project"),
            ("developer", "delete_project"),
            ("developer", "share_project"),
            ("developer", "edit_topology"),
            ("developer", "edit_data"),
            ("developer", "view_data"),
            ("developer", "add_template"),
            ("developer", "edit_template"),

            ("modeller", "add_network"),
            ("modeller", "edit_network"),
            ("modeller", "delete_network"),
            ("modeller", "share_network"),
            ("modeller", "edit_topology"),
            ("modeller", "add_project"),
            ("modeller", "edit_project"),
            ("modeller", "delete_project"),
            ("modeller", "share_project"),
            ("modeller", "edit_data"),
            ("modeller", "view_data"),

            ("manager", "edit_data"),
            ("manager", "view_data"),
    )

    perm_dict = {}
    for code, name in default_perms:
        perm = Perm(code=code, name=name)
        perm_dict[code] = perm
        db.DBSession.add(perm)
    role_dict = {}
    for code, name in default_roles:
        role = Role(code=code, name=name)
        role_dict[code] = role
        db.DBSession.add(role)

    for role_code, perm_code in roleperms:
        roleperm = RolePerm()
        roleperm.role = role_dict[role_code]
        roleperm.perm = perm_dict[perm_code]
        db.DBSession.add(roleperm)

    db.DBSession.flush()
