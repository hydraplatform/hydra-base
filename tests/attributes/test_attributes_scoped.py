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
"""
    NOTE:
    All the methods that has SKELETON key in the doc, are not yet implemented partially/fully

"""
# Example of a SKELETON METHOD
# def test_(self, client):
#     """
#         SKELETON
#     """
#     pass


import logging
import copy
import json
import hydra_base as hb
from hydra_base.exceptions import HydraError
import datetime
import pytest
from hydra_base.lib.objects import JSONObject
log = logging.getLogger(__name__)

class TestScopedAttribute:
    """
        Test for attribute-based functionality
    """

    """
        TESTED
    """

    def test_add_network_scoped_attribute(self, client, network_with_data):
        """
            Test adding a network-scoped attributes.
            1: Test adding a global attribute.
            2: Test adding a network-scoped attribute.
            3: Test that the network-scoped attribute doesn't appear in the
            get_attributes() function but does appear in the get_attributes(network_id)
        """
        global_attr = JSONObject({
            "name": f'Global Attribute {datetime.datetime.now()}',
            "dimension_id": None
        })

        network_scoped_attr = JSONObject({
            "name": f'Network Attribute {datetime.datetime.now()}',
            "dimension_id": None,
            "network_id": network_with_data.id
        })

        previous_all_attributes = client.get_attributes()

        client.add_attribute(global_attr)

        all_global_attributes = client.get_attributes()

        assert len(all_global_attributes) == len(previous_all_attributes) + 1

        #try add it again. SHould have no effect
        client.add_attribute(network_scoped_attr)

        global_attributes_no_network = client.get_attributes()
        network_scoped_attributes = client.get_attributes(network_id=network_with_data.id)

        #This should not have changed
        assert len(global_attributes_no_network) == len(all_global_attributes)

        #It's 2 because there is one added by default to all new networks, plus the one we just added
        assert len(network_scoped_attributes) == 2


    def test_add_project_scoped_attribute(self, client, network_with_data):
        """
            Test adding a project-scoped attributes.
            1: Test adding a global attribute.
            2: Test adding a project-scoped attribute.
            3: Test that the project-scoped attribute doesn't appear in the
            get_attributes() function but does appear in the get_attributes(project_id)
        """
        global_attr = JSONObject({
            "name": f'Global Attribute {datetime.datetime.now()}',
            "dimension_id": None
        })

        project_scoped_attr = JSONObject({
            "name": f'Project Attribute {datetime.datetime.now()}',
            "dimension_id": None,
            "project_id": network_with_data.project_id
        })

        previous_all_attributes = client.get_attributes()

        client.add_attribute(global_attr)

        all_global_attributes = client.get_attributes()

        assert len(all_global_attributes) == len(previous_all_attributes) + 1

        #try add it again. SHould have no effect
        client.add_attribute(project_scoped_attr)

        global_attributes_no_project = client.get_attributes()
        project_scoped_attributes = client.get_attributes(project_id=network_with_data.project_id)

        #This should not have changed
        assert len(global_attributes_no_project) == len(all_global_attributes)
        
        #It's 2 because there is one added by default to all new projects, plus the one we just added
        assert len(project_scoped_attributes) == 2


    def test_add_network_and_project_scoped_attribute(self, client, network_with_data):
        """
            Test adding a network-scoped attributes.
            1: Test adding a global attribute.
            2: Test adding a network-scoped attribute.
            3: Test that the network-scoped attribute doesn't appear in the
            get_attributes() function but does appear in the get_attributes(network_id)
            4: Test adding a project-scoped attribute.
            5: Test that the project-scoped attribute doesn't appear in the
            get_attributes() function but does appear in the get_attributes(project_id)
        """
        global_attr = JSONObject({
            "name": f'Global Attribute {datetime.datetime.now()}',
            "dimension_id": None
        })

        network_scoped_attr = JSONObject({
            "name": f'Network Attribute {datetime.datetime.now()}',
            "dimension_id": None,
            "network_id": network_with_data.id
        })

        previous_all_attributes = client.get_attributes()

        client.add_attribute(global_attr)

        all_global_attributes = client.get_attributes()

        assert len(all_global_attributes) == len(previous_all_attributes) + 1

        #try add it again. SHould have no effect
        client.add_attribute(network_scoped_attr)

        global_attributes_no_network = client.get_attributes()
        network_scoped_attributes = client.get_attributes(network_id=network_with_data.id)

        #This should not have changed
        assert len(global_attributes_no_network) == len(all_global_attributes)

        #It's 2 because there is one added by default to all new networks and projects, plus the ones we just added
        assert len(network_scoped_attributes) == 2


        project_scoped_attr = JSONObject({
            "name": f'Project Attribute {datetime.datetime.now()}',
            "dimension_id": None,
            "project_id": network_with_data.project_id
        })

        #try add it again. SHould have no effect
        client.add_attribute(project_scoped_attr)

        global_attributes_no_project = client.get_attributes()
        project_scoped_attributes = client.get_attributes(project_id=network_with_data.project_id)

        #This should not have changed
        assert len(global_attributes_no_project) == len(all_global_attributes)

        #It's 2 because there is one added by default to all new projects, plus the one we just added
        assert len(project_scoped_attributes) == 2

        project_and_network_scoped_attributes = client.get_attributes(
            project_id=network_with_data.project_id,
            network_id=network_with_data.id)

        #It's 4 because there is one added by default to all new networks and projects, plus the ones we just added
        assert len(project_and_network_scoped_attributes) == 4


        global_and_project_and_network_scoped_attributes = client.get_attributes(
            project_id=network_with_data.project_id,
            network_id=network_with_data.id,
            include_global=True)
        
        #It's 4 because there is one added by default to all new networks and projects, plus the ones we just added
        assert len(global_and_project_and_network_scoped_attributes) == len(all_global_attributes) + 4

        #Now get project attributes, and include attributes from all networks within that project
        global_and_project_and_network_scoped_attributes = client.get_attributes(
            project_id=network_with_data.project_id,
            include_global=True,
            include_network_attributes=True)

        #It's 4 because there is one added by default to all new networks and projects, plus the ones we just added
        assert len(global_and_project_and_network_scoped_attributes) == len(all_global_attributes) + 4


    def test_add_duplicate_scoped_attribute(self, client, network_with_data):
        """
            Test adding a scoped attribute which exists at a higher level.
        """
        global_attr = JSONObject({
            "name": f'Duplicate Attribute',
            "dimension_id": None
        })

        network_scoped_attr = JSONObject({
            "name": f'Duplicate Attribute',
            "dimension_id": None,
            "network_id": network_with_data.id
        })

        client.add_attribute(global_attr)

        with pytest.raises(hb.HydraError):
            client.add_attribute(network_scoped_attr)

    def test_add_attribute_to_project_and_network(self, client, network_with_data):
        """
            Test adding a scoped attribute which exists at a higher level.
        """
        dual_scoped_attr = JSONObject({
            "name": f'Duplicate Attribute',
            "dimension_id": None,
            "network_id": network_with_data.id,
            "project_id": network_with_data.project_id
        })

        with pytest.raises(hb.HydraError):
            client.add_attribute(dual_scoped_attr)

    def test_add_attribute_without_permission(self, client):
        """
            Test to make sure a non-admin cannot add a global attribute
        """
        global_scoped_attr = JSONObject({
            "name": f'Global Attribute',
            "dimension_id": None,
        })
        client.user_id = pytest.user_c.id
        with pytest.raises(PermissionError):
            client.add_attribute(global_scoped_attr)
        client.user_id = 1 # reset the user ID

    def test_add_network_scoped_attribute_without_ownership(self, client, network_with_data):
        """
            Test to ensure only the owner of a network can add ann attribute scoped
            to it.
        """
        network_scoped_attr = JSONObject({
            "name": f'Net Attribute',
            "dimension_id": None,
            "network_id" : network_with_data.id
        })
        #User C does not own this network, so fail.
        client.user_id = pytest.user_c.id
        with pytest.raises(hb.PermissionError):
            client.add_attribute(network_scoped_attr)

        client.user_id = 1
        client.share_network(
            network_id=network_with_data.id,
            usernames=["UserC"],
            read_only=False,
            share=True)

        #User C is now an owner of the network, so it should succeedd
        client.user_id = pytest.user_c.id
        client.add_attribute(network_scoped_attr)
        client.user_id = 1 ## reset the user ID

    def test_add_project_scoped_attribute_without_ownership(self, client, network_with_data):
        """
            Test to ensure only the owner of a network can add ann attribute scoped
            to it.
        """
        project_scoped_attr = JSONObject({
            "name": f'Project Attribute',
            "dimension_id": None,
            "project_id" : network_with_data.project_id
        })
        user = client.testutils.create_user("UserD", role="developer")
        #User C does not own this network, so fail.
        client.user_id = user.id
        with pytest.raises(hb.PermissionError):
            client.add_attribute(project_scoped_attr)

        client.user_id = 1
        client.share_project(
            project_id=project_scoped_attr.project_id,
            usernames=["UserD"],
            read_only=False,
            share=True)

        #User C is now an owner of the network, so it should succeedd
        client.user_id = user.id
        client.add_attribute(project_scoped_attr)
        client.user_id = 1 ## reset the user ID
