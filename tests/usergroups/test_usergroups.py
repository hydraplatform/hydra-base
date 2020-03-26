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

class TestUserGroup:
    """ A collection of tests which test the management of users within an organisation.
    """

    def test_add_usergroup(self, session, client, usergroupmaker, usergrouptype):
        """Test adding a usergroup to an organisation"""
        group_name = 'Test User Group'
        newgroup_j = usergroupmaker.create(client,
                                           name=group_name,
                                           type_id=usergrouptype.id)

        assert newgroup_j.id > 0
        assert newgroup_j.name == 'Test User Group'

        #Try adding a sub-group with the same name
        with pytest.raises(HydraError):
            usergroupmaker.create(client,
                                  name=group_name,
                                  type_id=usergrouptype.id)

    def test_add_sub_usergroup(self, session, client, usergroupmaker, usergrouptype):
        """Test adding a usergroup to an parent usergroup"""

        parentgroup_j = usergroupmaker.create(client, name='Parent Group')

        childgroup_j = usergroupmaker.create(client,
                                             name='Child Group',
                                             parentgroup_id=parentgroup_j.id,
                                             type_id=usergrouptype.id)

        assert childgroup_j.id > 0
        assert childgroup_j.name == 'Child Group'
        assert childgroup_j.parent_id == parentgroup_j.id

        #Try adding a sub-group with the same name
        with pytest.raises(HydraError):
            childgroup_j = usergroupmaker.create(client,
                                                 name='Child Group',
                                                 parentgroup_id=999,#This is wrong!
                                                 type_id=usergrouptype.id)



    def test_get_all_usergroups(self, session, client, usergroupmaker, usergrouptype):
        """Test removing a usergroup"""

        newgroup1_j = usergroupmaker.create(client, name='Test User Group 1',
                                            type_id=usergrouptype.id)
        newgroup2_j = usergroupmaker.create(client, name='Test User Group 2',
                                            type_id=usergrouptype.id)

        #Add a child group to show behaviour of 'get_all_usergroups' when there
        #is a tree structure
        childgroup_j = usergroupmaker.create(client, name='Child Group',
                                             parentgroup_id=newgroup1_j.id,
                                             type_id=usergrouptype.id)

        usergroups_j = client.get_all_usergroups()

        #This proves that the 'tree' structure is not preserved. All groups
        #are returned in a flat structure, to simplify searching
        assert len(usergroups_j) == 3

    def test_remove_usergroup(self, session, client, usergroupmaker, usergrouptype):
        """Test removing a usergroup"""

        newgroup1_j = usergroupmaker.create(client,
                                            name='Test User Group 1',
                                            type_id=usergrouptype.id)
        newgroup2_j = usergroupmaker.create(client,
                                            name='Test User Group 2',
                                            type_id=usergrouptype.id)

        #Add a child group to show behaviour of 'get_all_usergroups' when there
        #is a tree structure
        childgroup_j = usergroupmaker.create(client,
                                             name='Child Group',
                                             parentgroup_id=newgroup1_j.id,
                                             type_id=usergrouptype.id)

        usergroups_j = client.get_all_usergroups()

        #This proves that the 'tree' structure is not preserved. All groups
        #are returned in a flat structure, to simplify searching
        assert len(usergroups_j) == 3

        #removing the group removes the whole tree underneath
        client.delete_usergroup(newgroup1_j.id)

        usergroups_j = client.get_all_usergroups()
        #This proves that the 'tree' structure is not preserved. All groups
        #are returned in a flat structure, to simplify searching
        assert len(usergroups_j) == 1
