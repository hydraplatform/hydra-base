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

import os
import json
from ..db.model import Network, Scenario, Project, User, Role, Perm, RolePerm, RoleUser, ResourceAttr, ResourceType, Dimension, Unit
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from .. import db
import datetime
import random
import bcrypt
from ..exceptions import HydraError, HydraLoginUserNotFound, HydraLoginUserMaxAttemptsExceeded, HydraLoginUserPasswordWrong
from sqlalchemy.orm import load_only
from sqlalchemy import func
from ..lib.objects import JSONObject
from ..lib.users import get_remaining_login_attempts, inc_failed_login_attempts

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
        rt_i.template_id = resource_i.child_template_id
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
    except MultipleResultsFound:
        roles = list(db.DBSession.query(Role).all())
        log.info(roles)
        role = roles[0]
        #raise HydraError("Multiple rows found")

    try:
        userrole = db.DBSession.query(RoleUser).filter(RoleUser.role_id==role.id,
                                                       RoleUser.user_id==user.id).one()
    except NoResultFound:
        userrole = RoleUser(role_id=role.id,user_id=user.id)
        user.roleusers.append(userrole)
        db.DBSession.add(userrole)


    db.DBSession.flush()

    user_id = user.id

    # Do not remove!
    db.commit_transaction()

    return user_id


def login_user(username, password):

    try:
        user_i = db.DBSession.query(User).filter(User.username == username).one()
    except NoResultFound:
        """ The user has not been found in the DB """
        raise HydraLoginUserNotFound(username)
    except:
        """ Generic DB Error """
        raise HydraError(username)

    if get_remaining_login_attempts(username, user_id=user_i.id) <= 0:
        """  Account is not permitted to login """
        raise HydraLoginUserMaxAttemptsExceeded("Max login attempts exceeded for user {}".format(username))

    userPassword = ""
    try:
        userPassword = user_i.password.encode('utf-8')
    except (AttributeError, UnicodeEncodeError):
        userPassword = user_i.password

    try:
        password = password.encode('utf-8')
    except (AttributeError, UnicodeEncodeError):
        pass

    if bcrypt.checkpw(password, userPassword):
        user_i.last_login = datetime.datetime.now()
        user_i.failed_logins = 0
        user_id = user_i.id
        db.DBSession.flush()
        # Do not commit the transaction here because it is managed by HWI/Hydra-Server
        # db.commit_transaction()
        return user_id
    else:
        log.info("User {} now has {} failed logins".format(username, user_i.failed_logins+1))
        inc_failed_login_attempts(user_i.username, user_id=1)
        raise HydraLoginUserPasswordWrong(username)


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
    # Do not remove!
    db.commit_transaction()
    return net


def create_default_users_and_perms():
    """
        Adds the roles and perm to the DB. It adds only roles, perms and links between them that are not inside the db
        It is possible adding new role or perm and connecting them just modifiying the following lists
    """

    # perms = db.DBSession.query(Perm).all()
    # if len(perms) > 0:
    #     return

    log.info("Adding default users and permissions")

    default_perms = (
        ("get_user", "Get User"),
        ("add_user", "Add User"),
        ("edit_user", "Edit User"),
        ("get_role", "Get Role"),
        ("add_role", "Add Role"),
        ("edit_role", "Edit Role"),
        ("get_perm", "Get Permission"),
        ("add_perm", "Add Permission"),
        ("edit_perm", "Edit Permission"),

        ("add_attribute", "Add Attribute"),
        ("get_attribute", "Get Attribute"),
        ("edit_attribute", "Edit Attribute"),
        ("delete_attribute", "Delete Attribute"),

        ("add_network", "Add network"),
        ("edit_network", "Edit network"),
        ("get_network", "Get network"),
        ("delete_network", "Delete network"),
        ("share_network", "Share network"),
        ("edit_topology", "Edit network topology"),

        ("get_project", "Get Project"),
        ("add_project", "Add Project"),
        ("edit_project", "Edit Project"),
        ("delete_project", "Delete Project"),
        ("share_project", "Share Project"),

        ("edit_data", "Edit network data"),
        ("get_data", "View network data"),

        ("add_template", "Add Template"),
        ("edit_template", "Edit Template"),
        ("get_template", "Get Template"),
        ("delete_template", "Delete Template"),

        ("add_dimension", "Add Dimension"),
        ("update_dimension", "Update Dimension"),
        ("delete_dimension", "Delete Dimension"),

        ("add_unit", "Add Unit"),
        ("update_unit", "Update Unit"),
        ("delete_unit", "Delete Unit"),

        ('get_rules', "View Rules"),
        ('add_rules', "Add Rules"),
        ('update_rules', "Edit Rules"),
        ('share_rules', "Share Rules"),
        ('delete_rules', "Delete Rules")

    )

    default_roles = (
        ("admin", "Administrator"),
        ("dev", "Developer"),
        ("modeller", "Modeller / Analyst"),
        ("manager", "Manager"),
        ("grad", "Graduate"),
        ("developer", "Developer"),
        ("decision", "Decision Maker"),
    )

    roleperms = (
        # Admin permissions
        ('admin', "get_user"),
        ('admin', "add_user"),
        ('admin', "edit_user"),
        ('admin', "get_role"),
        ('admin', "add_role"),
        ('admin', "edit_role"),
        ('admin', "get_perm"),
        ('admin', "add_perm"),
        ('admin', "edit_perm"),
        ('admin', "add_attribute"),
        ('admin', "edit_attribute"),
        ('admin', "get_attribute"),
        ('admin', "delete_attribute"),
        ('admin', "add_network"),
        ('admin', "edit_network"),
        ('admin', "get_network"),
        ('admin', "delete_network"),
        ('admin', "share_network"),
        ('admin', "get_project"),
        ('admin', "add_project"),
        ('admin', "edit_project"),
        ('admin', "delete_project"),
        ('admin', "share_project"),
        ('admin', "edit_topology"),
        ('admin', "edit_data"),
        ('admin', "get_data"),
        ('admin', "add_template"),
        ('admin', "edit_template"),
        ('admin', "get_template"),
        ('admin', "delete_template"),

        ('admin', "add_dimension"),
        ('admin', "update_dimension"),
        ('admin', "delete_dimension"),

        ('admin', "add_unit"),
        ('admin', "update_unit"),
        ('admin', "delete_unit"),

        ('admin', 'get_rules'),
        ('admin', 'add_rules'),
        ('admin', 'update_rules'),
        ('admin', 'share_rules'),
        ('admin', 'delete_rules'),

        # Developer permissions
        ('developer', "add_attribute"),
        ('developer', "edit_attribute"),
        ('developer', "get_attribute"),
        ('developer', "delete_attribute"),
        ("developer", "add_network"),
        ("developer", "edit_network"),
        ("developer", "delete_network"),
        ("developer", "share_network"),
        ('developer', "get_project"),
        ("developer", "add_project"),
        ("developer", "edit_project"),
        ("developer", "delete_project"),
        ("developer", "share_project"),
        ("developer", "edit_topology"),
        ("developer", "edit_data"),
        ("developer", "get_data"),
        ("developer", "add_template"),
        ("developer", "edit_template"),

        ('developer', "add_dimension"),
        ('developer', "update_dimension"),
        ('developer', "delete_dimension"),

        ('developer', "add_unit"),
        ('developer', "update_unit"),
        ('developer', "delete_unit"),

        # modeller permissions
        ("modeller", "add_network"),
        ("modeller", "edit_network"),
        ("modeller", "delete_network"),
        ("modeller", "share_network"),
        ("modeller", "edit_topology"),
        ("modeller", "get_project"),
        ("modeller", "add_project"),
        ("modeller", "edit_project"),
        ("modeller", "delete_project"),
        ("modeller", "share_project"),
        ("modeller", "edit_data"),
        ("modeller", "get_data"),

        # Manager permissions
        ("manager", "edit_data"),
        ("manager", "get_data"),
    )

    # Map for code to ID
    id_maps_dict = {
        "perm": {},
        "role": {}
    }
    # Adding perms
    perm_dict = {}
    for code, name in default_perms:
        perm = Perm(code=code, name=name)
        perm_dict[code] = perm
        perms_by_name = db.DBSession.query(Perm).filter(Perm.code==code).all()
        if len(perms_by_name)==0:
            # Adding perm
            log.debug("# Adding PERM {}".format(code))
            db.DBSession.add(perm)
            db.DBSession.flush()

        perm_by_name = db.DBSession.query(Perm).filter(Perm.code==code).one()
        id_maps_dict["perm"][code] = perm_by_name.id

    # Adding roles
    role_dict = {}
    for code, name in default_roles:
        role = Role(code=code, name=name)
        role_dict[code] = role
        roles_by_name = db.DBSession.query(Role).filter(Role.code==code).all()
        if len(roles_by_name)==0:
            # Adding perm
            log.debug("# Adding ROLE {}".format(code))
            db.DBSession.add(role)
            db.DBSession.flush()

        role_by_name = db.DBSession.query(Role).filter(Role.code==code).one()
        id_maps_dict["role"][code] = role_by_name.id

    # Adding connections
    for role_code, perm_code in roleperms:
        #log.info("Link Role:{}({}) <---> Perm:{}({})".format(role_code, id_maps_dict["role"][role_code], perm_code, id_maps_dict["perm"][perm_code]))

        links_found = db.DBSession.query(RolePerm).filter(RolePerm.role_id==id_maps_dict["role"][role_code]).filter(RolePerm.perm_id==id_maps_dict["perm"][perm_code]).all()
        if len(links_found) == 0:
            # Adding link
            log.debug("# Adding link")
            roleperm = RolePerm()
            # roleperm.role = role_dict[role_code]
            # roleperm.perm = perm_dict[perm_code]
            roleperm.role_id = id_maps_dict["role"][role_code]
            roleperm.perm_id = id_maps_dict["perm"][perm_code]
            db.DBSession.add(roleperm)
            db.DBSession.flush()

    db.DBSession.flush()

def create_default_units_and_dimensions(update=True):
    """
        Adds the units and the dimensions reading a json file. It adds only dimensions and units that are not inside the db
        It is possible adding new dimensions and units to the DB just modifiyin the json file

        args:
            update (bool) default True: If there are existing units / dimensions in the DB, then update
            them with the defaults by adding any missing units. If False, do nothing if there are existing
            units.
    """

    log.info("Adding default units and dimensions.")

    if update is False:
        #if update is set to false, check if there are dimensions. If there are
        #any existing dimensions, then log it and return.
        num_dimensions = db.DBSession.query(func.count(Dimension.id)).scalar()
        if num_dimensions > 0:
            log.info("Existing dimensions found. Not creating defaults.")
            return


    default_units_file_location = os.path.realpath(\
        os.path.join(os.path.dirname(os.path.realpath(__file__)),
                     '../',
                     'static',
                     'default_units_and_dimensions.json'))


    d = None

    with open(default_units_file_location) as json_data:
        d = json.load(json_data)
        json_data.close()

    for json_dimension in d["dimension"]:
        new_dimension = None
        dimension_name = get_utf8_encoded_string(json_dimension["name"])

        db_dimensions_by_name = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).all()

        if len(db_dimensions_by_name) == 0:
            # Adding the dimension
            log.debug("Adding Dimension `{}`".format(dimension_name))
            new_dimension = Dimension()
            if "id" in json_dimension:
                # If ID is specified
                new_dimension.id = json_dimension["id"]

            new_dimension.name = dimension_name

            db.DBSession.add(new_dimension)
            db.DBSession.flush()

        # Get the dimension by name
        new_dimension = get_dimension_from_db_by_name(dimension_name)

        for json_unit in json_dimension["unit"]:
            db_units_by_name = db.DBSession.query(Unit).filter(Unit.abbreviation==get_utf8_encoded_string(json_unit['abbr'])).all()
            if len(db_units_by_name) == 0:
                # Adding the unit
                log.debug("Adding Unit %s in %s",json_unit['abbr'], json_dimension["name"])
                new_unit = Unit()
                if "id" in json_unit:
                    new_unit.id = json_unit["id"]
                new_unit.dimension_id   = new_dimension.id
                new_unit.name           = get_utf8_encoded_string(json_unit['name'])
                new_unit.abbreviation   = get_utf8_encoded_string(json_unit['abbr'])
                new_unit.lf             = get_utf8_encoded_string(json_unit['lf'])
                new_unit.cf             = get_utf8_encoded_string(json_unit['cf'])
                if "description" in json_unit:
                    # If Description is specified
                    new_unit.description = get_utf8_encoded_string(json_unit["description"])

                # Save on DB
                db.DBSession.add(new_unit)
                db.DBSession.flush()
            else:
                #log.critical("UNIT {}.{} EXISTANT".format(dimension_name,json_unit['abbr']))
                pass
    try:
        # Needed for test. on HWI it fails so we need to catch the exception and pass by
        db.DBSession.commit()
    except Exception as e:
        # Needed for HWI
        pass
    return


def get_utf8_encoded_string(string):
    try:
        return string.encode('utf-8').strip().replace('"','\\"')
    except Exception as e:
        return string

def get_dimension_from_db_by_name(dimension_name):
    """
        Gets a dimension from the DB table.
    """
    try:
        dimension = db.DBSession.query(Dimension).filter(Dimension.name==dimension_name).one()
        return JSONObject(dimension)
    except NoResultFound:
        raise ResourceNotFoundError("Dimension %s not found"%(dimension_name))
