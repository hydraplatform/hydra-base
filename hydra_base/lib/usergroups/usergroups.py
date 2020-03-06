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

from ..db.model import UserGroup, usergroup, UserGroupMember
from ..util.permissions import required_role
from .. import db

import bcrypt

from ..exceptions import ResourceNotFoundError, HydraError
import logging

log = logging.getLogger(__name__)

def _get_usergroup(id):
    return None

@required_role('admin')
def add_usergroup(group, **kwargs):
    """
    Add a new usergroup.

    args:
        group (JSONObject): The name of the usergroup
        The name of an usergroup must not be the same as a user's username.
    returns:
        An usergroup ORM object
    throws:
        HydraError if an usergroup or user exists with the same name

    a Group JSONObject is formatted as follows:
    {
    'name': 'My group',
    }
    """

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

@required_role('admin')
def get_usergroup_ny_name(name, **kwargs):
    """
        Get an usergroup by id
        args:
            name (string): The name of the usergroup
        returns:
            An usergroup ORM object
        throws:
            ResourceNotFoundError if no usergroup exists with the specified name
    """

@required_role('admin')
def get_all_usergroups(**kwargs):
    """
        Get all usergroups
        args:
        returns:
            list of usergroup ORM objects
        throws:
    """
