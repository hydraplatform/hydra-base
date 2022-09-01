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
        Test for scoped-attribute-based functionality
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
        user = client.testutils.create_user("UserD", role="developer")
        #User D does not own this network, so fail.
        client.user_id = user.id
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

    def test_search_attribute_in_sub_project(self, client, projectmaker, networkmaker):
        """
           Test searching for an attribute defined on a parent project (p1) when searching from 
           network n1. This requires recursively looking up the project tree to collate
           all attributes available to network n1. Attributes defined on P3 should not
           be visible.
                                 p1
                                /  \
                               p2   p3
                              /
                             p4
                            /
                           n1
        """
        client.user_id = 1 # force current user to be 1 to avoid potential inconsistencies
        proj_user = client.user_id
        proj1 = projectmaker.create(share=False)
        proj2 = projectmaker.create(share=False, parent_id=proj1.id)
        proj3 = projectmaker.create(share=False, parent_id=proj1.id)
        proj4 = projectmaker.create(share=False, parent_id=proj2.id)

        net1 = networkmaker.create(project_id=proj4.id)

        client.user_id = pytest.user_c.id
        with pytest.raises(HydraError):
            client.get_project(proj4.id)

        with pytest.raises(HydraError):
            client.get_network(net1.id)

        #Now, as the main user, share P4 with user C
        client.user_id = proj_user
        client.share_network(net1.id, ['UserC'], False, False)

        client.add_attribute({'project_id': proj1.id,'name': 'p1_attr'})
        client.add_attribute({'project_id': proj3.id,'name': 'p3_attr'})
        client.add_attribute({'network_id': net1.id,'name': 'n1_attr'})

        matching_attributes = client.search_attributes('p1_', project_id=proj1.id)
        assert 'p1_attr' in [a.name for a in matching_attributes]
        matching_attributes = client.search_attributes('p1_', network_id=net1.id)
        assert 'p1_attr' in [a.name for a in matching_attributes]
        matching_attributes = client.search_attributes('n1_', network_id=net1.id)
        assert 'n1_attr' in [a.name for a in matching_attributes]
        matching_attributes = client.search_attributes('p3_', network_id=net1.id)
        assert 'p3_attr' not in [a.name for a in matching_attributes]

        #now do all the same things except with the shared user.
        client.user_id = pytest.user_c.id
        matching_attributes = client.search_attributes('p1_', project_id=proj1.id)
        assert 'p1_attr' in [a.name for a in matching_attributes]
        matching_attributes = client.search_attributes('p1_', network_id=net1.id)
        assert 'p1_attr' in [a.name for a in matching_attributes]
        matching_attributes = client.search_attributes('n1_', network_id=net1.id)
        assert 'n1_attr' in [a.name for a in matching_attributes]
        matching_attributes = client.search_attributes('p3_', network_id=net1.id)
        assert 'p3_attr' not in [a.name for a in matching_attributes]

        with pytest.raises(HydraError):
            client.search_attributes('p3_', project_id=proj3.id)