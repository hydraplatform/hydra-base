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

import hydra_base
import hydra_base.exceptions
from hydra_base.lib.objects import JSONObject
from ..fixtures import *
import datetime
import bcrypt
import pytest

class TestUserGroup:
    """ A collection of tests which test the management of users within an organisation.
    """

    def test_add_usergroup(self, client, usergroupmaker):
        """Test adding a usergroup to an organisation"""

    def test_add_sub_usergroup(self, client, usergroupmaker):
        """Test adding a usergroup to an organisation"""

    def test_remove_usergroup(self, client, usergroupmaker):
        """Test removing a usergroup"""

    def test_add_usergroup_member(self, client, usergroupmaker):
        """Test adding a user to a user group"""

    def test_get_users(self, client, usergroupmaker):
        """ Test fetching all the users in an organisation"""

    def test_get_projects(self, client, usergroupmaker):
        """ Test fetching all the projects to which a user has access within an organisation"""

class TestUserGroupPermissions:
    """ A collection of tests which test the management of users permissions and roles
        within an organisation.
    """
    def test_add_user_role(self, client, usergroupmaker):
        """
            Test applying a role to a user within an organisation
        """

    def test_remove_user_role(self, client, usergroupmaker):
        """
            Test removing a role to a user within an organisation
        """

    def test_get_user_roles(self, client):
        """
            Test that a user with different roles in different organisations has
            the correct roles returned based on their organisation
        """


    def test_get_user_permissions(self, client):
        """
            Test that a user with different permissions in different organisations has
            the correct permissions returned based on their organisation
        """
