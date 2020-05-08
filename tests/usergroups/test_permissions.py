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

class TestUserGroupPermissions:
    """ A collection of tests which test the management of users permissions and roles
        within an organisation.
    """
    @pytest.mark.skip(reason="Not sure how / if this should be implemented")
    def test_add_user_role(self, session, client, usergroupmaker):
        """
            Test applying a role to a user within a group
        """

        user_id = pytest.user_a.id

        usergroup = usergroupmaker.create(client)

        #Get the group's members top check the user's not already there
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert user_id not in [member.user_id for member in group_members]

        #add user_a to the usergroup
        client.add_usergroup_member(usergroup.id, user_id)

        #Now set the role of the user within the group they are operating in....
        client.set_user_role()

        #Get the group's members
        group_members = client.get_usergroup_members(usergroup.id)

        #Check the user's been added successfully
        assert user_id in [member.user_id for member in group_members]

        #add user_a to a group he's already in should error
        with pytest.raises(HydraError):
            client.add_usergroup_member(usergroup.id, user_id)

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
