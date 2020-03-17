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
import datetime
import pytest

from hydra_base.lib.objects import JSONObject
from hydra_base.exceptions import ResourceNotFoundError, HydraError
from .fixtures import *

class TestUserGroupType:
    """ A collection of tests which test the management of users within an organisation.
    """

    def test_add_usergrouptype(self, session, client, grouptypemaker):
        """Test adding a usergroup type"""
        #This tests the grouptypemaker, which is almost identical to the add
        #call below. 
        newgroup_j = grouptypemaker.create(client)

        assert newgroup_j.id > 0

        groupname = 'My Group'
        newgroup_j = client.add_usergrouptype({'name':groupname})

        assert newgroup_j.id > 0
        assert newgroup_j.name == groupname

    def test_delete_usergrouptype(self, session, client, grouptypemaker):
        """Test removing a usergroup type"""

        grouptype_to_delete_j = grouptypemaker.create(client)

        client.delete_usergrouptype(grouptype_to_delete_j.id)

        with pytest.raises(ResourceNotFoundError):
            client.get_usergrouptype(grouptype_to_delete_j.id)

    def test_update_usergrouptype(self, session, client, grouptypemaker):
        """Test updating a usergroup type"""

        newgrouptype_j = grouptypemaker.create(client)

        newname = 'Updated Grouptype Name'

        newgrouptype_j.name = newname

        updated_grouptype_j = client.update_usergrouptype(newgrouptype_j)

        retrieved_grouptype_j = client.get_usergrouptype(updated_grouptype_j.id)

        assert retrieved_grouptype_j.name == newname
        assert retrieved_grouptype_j.updated_at != newgrouptype_j.updated_at
        assert retrieved_grouptype_j.cr_date == newgrouptype_j.cr_date
