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

        assert len(network_scoped_attributes) == 1


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

        assert len(project_scoped_attributes) == 1


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

        assert len(network_scoped_attributes) == 1


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

        assert len(project_scoped_attributes) == 1

        project_and_network_scoped_attributes = client.get_attributes(
            project_id=network_with_data.project_id,
            network_id=network_with_data.id)

        assert len(project_and_network_scoped_attributes) == 2


        global_and_project_and_network_scoped_attributes = client.get_attributes(
            project_id=network_with_data.project_id,
            network_id=network_with_data.id,
            include_global=True)

        assert len(global_and_project_and_network_scoped_attributes) == len(all_global_attributes) + 2

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


    def test_re_scope_network_attribute_to_global(self, client, network_with_data):
        """
            Test adding a global attribute when a scoped attribute already exist.
            In this case, the scoped attribute needs to be removed and all resource
            attributes need to be re-assigned to the higher-scoped attribute.
        """
        attr_name = f"Network Attribute {datetime.datetime.now()}"

        global_attr = JSONObject({
            "name": attr_name,
            "dimension_id": None
        })

        network_scoped_attr = JSONObject({
            "name": attr_name,
            "dimension_id": None,
            "network_id": network_with_data.id
        })

        #First add the network attribute
        network_attr = client.add_attribute(network_scoped_attr)


        test_link = network_with_data.links[0]
        client.add_resource_attribute('LINK', test_link.id, network_attr.id, False)

        test_link_updated = client.get_link(test_link.id)

        #Check that the attribute has been added to the link
        assert network_attr.id in [linkattr.attr_id for linkattr in test_link_updated.attributes]

        #The attribute is not global
        global_attributes_no_network = client.get_attributes()
        assert attr_name not in [a.name for a in global_attributes_no_network]

        #Then add the global attribute
        new_global_attr = client.add_attribute(global_attr)

        #The network attribute is no longer there
        with pytest.raises(HydraError):
            client.get_attribute_by_id(network_attr.id)

        global_attributes_no_network = client.get_attributes()

        network_scoped_attributes = client.get_attributes(network_id=network_with_data.id)


        #The attribute is now global, and not scoped to the network
        assert attr_name in [a.name for a in global_attributes_no_network]
        assert attr_name not in [a.name for a in network_scoped_attributes]

        #Now get the link again. Its attribute should have been updated
        test_link_updated = client.get_link(test_link.id)
        #Check that the resource attribute is using the global attribute and not the network scoped attribute
        assert network_attr.id not in [linkattr.attr_id for linkattr in test_link_updated.attributes]
        assert new_global_attr.id in [linkattr.attr_id for linkattr in test_link_updated.attributes]

    def test_re_scope_project_attribute_to_global(self, client, network_with_data):
        """
            Test adding a global attribute when a scoped attribute already exist.
            In this case, the scoped attribute needs to be removed and all resource
            attributes need to be re-assigned to the higher-scoped attribute.
        """
        attr_name = f"Project Attribute {datetime.datetime.now()}"

        global_attr = JSONObject({
            "name": attr_name,
            "dimension_id": None
        })

        project_scoped_attr = JSONObject({
            "name": attr_name,
            "dimension_id": None,
            "project_id": network_with_data.project_id
        })

        #First add the network attribute
        project_attr = client.add_attribute(project_scoped_attr)


        test_link = network_with_data.links[0]
        client.add_resource_attribute('LINK', test_link.id, project_attr.id, False)

        test_link_updated = client.get_link(test_link.id)

        #Check that the attribute has been added to the link
        assert project_attr.id in [linkattr.attr_id for linkattr in test_link_updated.attributes]

        #The attribute is not global
        global_attributes_no_project = client.get_attributes()
        assert attr_name not in [a.name for a in global_attributes_no_project]

        #Then add the global attribute
        new_global_attr = client.add_attribute(global_attr)

        #The network attribute is no longer there
        with pytest.raises(HydraError):
            client.get_attribute_by_id(project_attr.id)

        global_attributes_no_project = client.get_attributes()

        project_scoped_attributes = client.get_attributes(project_id=network_with_data.project_id)


        #The attribute is now global, and not scoped to the network
        assert attr_name in [a.name for a in global_attributes_no_project]
        assert attr_name not in [a.name for a in project_scoped_attributes]

        #Now get the link again. Its attribute should have been updated
        test_link_updated = client.get_link(test_link.id)
        #Check that the resource attribute is using the global attribute and not the network scoped attribute
        assert project_attr.id not in [linkattr.attr_id for linkattr in test_link_updated.attributes]
        assert new_global_attr.id in [linkattr.attr_id for linkattr in test_link_updated.attributes]


    def test_re_scope_network_attribute_to_project(self, client, network_with_data):
        """
            Test adding a project-level attribute when a scoped attribute already exist.
            In this case, the scoped attribute needs to be removed and all resource
            attributes need to be re-assigned to the higher-scoped attribute.
        """
        attr_name = f"Network Attribute {datetime.datetime.now()}"

        project_attr = JSONObject({
            "name": attr_name,
            "dimension_id": None,
            "project_id": network_with_data.project_id
        })

        network_scoped_attr = JSONObject({
            "name": attr_name,
            "dimension_id": None,
            "network_id": network_with_data.id
        })

        #First add the network attribute
        network_attr = client.add_attribute(network_scoped_attr)


        test_link = network_with_data.links[0]
        client.add_resource_attribute('LINK', test_link.id, network_attr.id, False)

        test_link_updated = client.get_link(test_link.id)

        #Check that the attribute has been added to the link
        assert network_attr.id in [linkattr.attr_id for linkattr in test_link_updated.attributes]

        #The attribute is not project
        project_attributes_no_network = client.get_attributes(project_id=network_with_data.project_id)
        assert attr_name not in [a.name for a in project_attributes_no_network]

        #Then add the project attribute
        new_project_attr = client.add_attribute(project_attr)

        #The network attribute is no longer there
        with pytest.raises(HydraError):
            client.get_attribute_by_id(network_attr.id)

        project_attributes_no_network = client.get_attributes(project_id=network_with_data.project_id)

        network_scoped_attributes = client.get_attributes(network_id=network_with_data.id)

        #The attribute is now project, and not scoped to the network
        assert attr_name in [a.name for a in project_attributes_no_network]
        assert attr_name not in [a.name for a in network_scoped_attributes]

        #Now get the link again. Its attribute should have been updated
        test_link_updated = client.get_link(test_link.id)
        #Check that the resource attribute is using the project attribute and not the network scoped attribute
        assert network_attr.id not in [linkattr.attr_id for linkattr in test_link_updated.attributes]
        assert new_project_attr.id in [linkattr.attr_id for linkattr in test_link_updated.attributes]
