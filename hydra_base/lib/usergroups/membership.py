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
from sqlalchemy.orm import joinedload


from hydra_base.db.model import UserGroup, UserGroup, UserGroupMember
from hydra_base.util.permissions import required_role, required_perms
from hydra_base.exceptions import ResourceNotFoundError, HydraError
from hydra_base.lib.objects import JSONObject
from hydra_base import db


import bcrypt

import logging

log = logging.getLogger(__name__)

def _get_usergroup(id):
    """
        Get a usergroup from the DB
    """
    usergroup_i = db.DBSession.query(UserGroup).filter(UserGroup.id == id).options(joinedload('members')).first()

    if usergroup_i is None:
        raise HydraError(f"User group with ID ({id}) not found")

    return usergroup_i

def _get_member(usergroup_id, user_id, do_raise=True):
    """
        Get the membership record of a user in a group
        args:
            usergroup_id
            user_id
        returns:
            UserGroupMember object
        raises
            ResourceNotFoundError if the member doesnt' exist
    """
    new_member = db.DBSession.query(UserGroupMember).filter(
        UserGroupMember.usergroup_id == usergroup_id,
        UserGroupMember.user_id == user_id
        ).first()

    if new_member is None and do_raise is True:
        raise ResourceNotFoundError(f"User {user_id} not found in usergroup {usergroup_id}")

    return new_member

#this user should be group admin, not a global admin
@required_role('admin')
def add_usergroup_member(usergroup_id, new_member_user_id, **kwargs):
    """
        Add a new member to a usergroup
        args:
            usergroup_id (int): THe ID of the group to add the member to
            user_id (int): THe ID of the user to add to the group
        returns:
            UserGroupMember object of the new membership entry, complete with unique ID
        raises
            ResourceNotFoundError if the target user group is not found.
    """
    user_id = kwargs.get('user_id')

    usergroup_i = _get_usergroup(usergroup_id)

    #check the user has permission to modify the group
    usergroup_i.check_write_permission(user_id)

    usergroup_i.add_member(new_member_user_id)

    db.DBSession.flush()

    #Do this because members have IDs, so this will return the newly created ID
    new_member_i = _get_member(usergroup_id, new_member_user_id)

    return new_member_i

@required_role('admin')
def remove_usergroup_member(usergroup_id, member_user_id, **kwargs):
    """
        Remove a member from a usergroup
        args:
            usergroup_id (int): THe ID of the group to remove the member from
            user_id (int): THe ID of the user to remove from the group
        returns:
            {'status': 'OK'}
        raises
            ResourceNotFoundError if the target user group is not found, or
            the user is not a member of the usergroup
    """
    user_id = kwargs.get('user_id')

    usergroup_i = _get_usergroup(usergroup_id)

    #check the user has permission to modify the group
    usergroup_i.check_write_permission(user_id)

    usergroup_i.remove_member(member_user_id)

    db.DBSession.flush()

    #Tell sqlalchemy to forget about this object, so it forces a reload
    db.DBSession.expunge(usergroup_i)

    return {'status': 'OK'}

@required_perms('get_users')
def get_usergroup_members(usergroup_id, **kwargs):
    user_id = kwargs.get('user_id')

    usergroup_i = _get_usergroup(usergroup_id)

    usergroup_i.check_read_permission(user_id)

    usergroup_members = usergroup_i.members

    return usergroup_members
