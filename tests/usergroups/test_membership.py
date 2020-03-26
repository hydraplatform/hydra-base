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
from hydra_base.exceptions import HydraError
from hydra_base.lib.objects import JSONObject
from ..fixtures import *
import datetime
import bcrypt
import pytest

class TestUserGroupMembership:
    """ A collection of tests which test the management of users
        within a user group.
    """

    def test_add_usergroup_member(self, session, client, usergroup):
        """Test adding a user to a user group"""

        user_id = pytest.user_a.id

        #Get the group's members top check the user's not already there
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert user_id not in [member.user_id for member in group_members]

        #add user_a to the usergroup
        client.add_usergroup_member(usergroup.id, user_id)

        #Get the group's members
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert user_id in [member.user_id for member in group_members]

        #add user_a to a group he's already in should error
        with pytest.raises(HydraError):
            client.add_usergroup_member(usergroup.id, user_id)

    def test_remove_usergroup_member(self, session, client, usergroup):
        """Test removing a user from a group"""

        user_id = pytest.user_a.id

        #Get the group's members top check the user's not already there
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert user_id not in [member.user_id for member in group_members]

        #add user_a to the usergroup
        client.add_usergroup_member(usergroup.id, user_id)

        #Get the group's members
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert user_id in [member.user_id for member in group_members]

        #remove user_a from the usergroup
        client.remove_usergroup_member(usergroup.id, user_id)

        #Get the group's members. The user should be removed
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been removed successfully
        assert user_id not in [member.user_id for member in group_members]

    def test_get_usergroup_members(self, session, client, usergroup):
        """ Test fetching all the users in a user group"""

        user_id = pytest.user_a.id

        #Get the group's members top check the user's not already there
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert len(group_members) == 0

        #add user_a to the usergroup
        client.add_usergroup_member(usergroup.id, user_id)

        #Get the group's members
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert len(group_members) == 1

        #remove user_a from the usergroup
        client.remove_usergroup_member(usergroup.id, user_id)

        #Get the group's members. The user should be removed
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been removed successfully
        assert len(group_members) == 0
