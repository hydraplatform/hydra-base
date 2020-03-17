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

from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm.exc import NoResultFound

from hydra_base.db.models import UserGroup, UserGroupMember
from hydra_base.util.permissions import required_role
from hydra_base import db

import bcrypt

from hydra_base.exceptions import ResourceNotFoundError, HydraError
import logging

LOG = logging.getLogger(__name__)

IGNORE_COLUMNS = ['updated_at', 'updated_by', 'cr_date']


def _check_usergroup_exists(id, user_id):
    """
        Check if a usergroup exists. Raise a HydraError if not.
    """

    existing_group = db.DBSession.query(UserGroup)\
                             .filter(UserGroup.id == id).first()

    if existing_group is None:
         raise HydraError(f"User Group {id} does not exist.")

def _check_unique_usergroup(usergroup, user_id):
    """
    Check if a user has already created a user group with the same name, with the
    same parent. This effectively checks the DB unique constraint, but is used because
    parent_id is allowed to be null, and this constrait is not enforced in mysql, so a user
    can have multiple groups with the same name, where the parent_id is null
    """

    existing_groups = db.DBSession.query(UserGroup)\
                             .filter(UserGroup.name == usergroup.name)\
                             .filter(UserGroup.created_by == user_id)\
                             .filter(UserGroup.parent_id == usergroup.parent_id).all()

    if len(existing_groups) > 0:
         raise HydraError(f"User {user_id} has already created a group with the name {usergroup.name}.")

def _set_db_attrs(source, target):
    #this is a general way to set the attributes of an ORM object without having
    #to revisit it every time a new column is added. It relies on the JSONObjects
    #containing the correct attributes and values.
    for name, value in source.items():
        LOG.debug("[%s]: Setting %s : %s", target.__tablename__, name, value)
        if name not in IGNORE_COLUMNS:
            setattr(target, name, value)


def _get_usergroups_by_name(id, user_id, write=False):
    """
        Get a user group type by ID.
        args:
            id: The ID of the group
            user_id: THe user making the request
        returns:
            UserGroupType ORM object
        raises:
            ResourceNotFoundError if the type with specified ID does not exist
    """

    try:
        groups_i = db.DBSession.query(UserGroup).filter(UserGroup.name == name).all()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User Group with ID {id}")

    groups_to_return_i = []
    for group_i in groups_i:
        if group_i.check_read_permission(user_id, do_raise=False):
            groups_to_return_i.append(group_i)

    return groups_to_return_i

def _get_usergroup(id, user_id, write=False):
    """
        Get a user group type by ID.
        args:
            id: The ID of the group
            user_id: THe user making the request
            write: Whether to check for write permission (if it's to be updated or deleted)
        returns:
            UserGroupType ORM object
        raises:
            ResourceNotFoundError if the type with specified ID does not exist
    """

    try:
        group_i = db.DBSession.query(UserGroup).filter(UserGroup.id == id).one()
    except NoResultFound:
        raise ResourceNotFoundError(f"No User Group with ID {id}")

    group_i.check_read_permission(user_id)

    if write is True:
        group_i.check_write_permission(user_id)

    return group_i

@required_role('admin')
def add_usergroup(usergroup, **kwargs):
    """
    Add a new usergroup.

    args:
        usergroup (JSONObject): The name of the usergroup
    returns:
        An usergroup ORM object
    throws:
        HydraError if an usergroup or user exists with the same name

    a Group JSONObject is formatted as follows:
    {
    'name': 'My group',
    }
    """

    LOG.info("Adding user group with name %s", usergroup.name)

    user_id = kwargs.get('user_id')

    #check the user hasn't already added a group with this name.
    _check_unique_usergroup(usergroup, user_id)

    if usergroup.parent_id is not None:
        _check_usergroup_exists(usergroup.parent_id, user_id)

    new_usergroup_i = UserGroup()

    _set_db_attrs(usergroup, new_usergroup_i)

    db.DBSession.add(new_usergroup_i)

    db.DBSession.flush()

    #load any defaulted columns (like cr_date)
    db.DBSession.refresh(new_usergroup_i)

    LOG.info("Usergroup type %s added with ID %s", usergroup.name, new_usergroup_i.id)

    return new_usergroup_i

@required_role('admin')
def get_all_usergroups(**kwargs):
    """
    Get all usergroup types.
    args:

    returns:
        list(hydra_base.JSONObject) representing the usergroup types
    thows:
        HydraError if a group with this name already exists
    """
    LOG.info("Getting all user group types")

    usergrouptypes_i = db.DBSession.query(UserGroupType).all()

    LOG.info("Retrieved %s, group types", len(usergrouptypes_i))

    return usergrouptypes_i

@required_role('admin')
def get_usergroup(id, **kwargs):
    """
        Get an usergroup by id
        args:
            id (int): The ID of the usergroup
        returns:
            An usergroup ORM object
        throws:
            ResourceNotFoundError if no usergroup exists with the specified id
    """

    LOG.info("Getting user group [ %s ]", id)

    user_id = kwargs.get('user_id')

    usergrouptype_i = _get_usergroup(id, user_id, write=True)

    LOG.info("Retrieved group [ %s ] updated", id)

    return JSONObject(usergroup_i)

@required_role('admin')
def get_usergroups_by_name(name, **kwargs):
    """
        Get an usergroup by id
        args:
            name (string): The name of the usergroup
        returns:
            An usergroup ORM object
        throws:
            ResourceNotFoundError if no usergroup exists with the specified name
    """

    LOG.info("Getting user group [ %s ]", id)

    user_id = kwargs.get('user_id')

    usergrouptypes_i = _get_usergroupby_name(name, user_id, write=True)

    LOG.info("Retrieved %s groups with name [ %s ]", name)

    return [JSONObject(usergroup_i) for usergroup_i in usergroups_i]

@required_role('admin')
def get_all_usergroups(**kwargs):
    """
        Get all usergroups
        args:
        returns:
            list of usergroup ORM objects
        throws:
    """

    user_id=kwargs.get('user_id')
    #All groups of which the user is an owner
    created_groups = db.DBSession.query(UserGroup).filter(UserGroup.created_by==user_id).all()

    #Groups of which the user is a member but not the creator
    member_groups = db.DBSession.query(UserGroup).join(UserGroupMember).filter(UserGroupMember.user_id==user_id).all()

    #remove any duplicates from the above queries (such as ones which the user)
    #created and was also a member of.
    return_groups = created_groups
    return_group_ids = [g.id for g in created_groups]
    for member_group in member_groups:
        if member_group.id not in return_group_ids:
            return_groups.append(member_group)
            return_group_ids.append(member_group.id)

    return return_groups


@required_role('admin')
def delete_usergroup(id, **kwargs):
    """
        Remove a usergroup
        args:
            type_id (int): The ID of the user group to delete
        returns:
            None
        raises:
            ResourceNotFoundError if the group with specified ID does not exist
    """
    LOG.info("Deleting user group [ %s ]", id)
    user_id = kwargs.get('user_id')
    usergroup_i = _get_usergroup(id, user_id, write=True)
    db.DBSession.delete(usergroup_i)
    db.DBSession.flush()
    LOG.info("User group type [ %s ] deleted", id)
