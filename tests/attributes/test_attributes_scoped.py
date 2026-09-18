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



    def test_rescope_attribute(self, client, projectmaker, networkmaker):
        """
           Test to make sure that when a scoped attribute is added to a network
           which has a sibling containing the same attribute, then rather than
           adding a new attribute, the current attribute is re-scoped to the project
        """
        client.user_id = 1 # force current user to be 1 to avoid potential inconsistencies
        proj_user = client.user_id
        proj1 = projectmaker.create(share=False)

        proj1net1 = networkmaker.create(project_id=proj1.id)
        proj1net2 = networkmaker.create(project_id=proj1.id)

        #Add a child project which is at the same level as proj1net1/
        childproj = projectmaker.create(share=False, parent_id=proj1.id)
        childprojnet1 = networkmaker.create(project_id=childproj.id)


        proj2 = projectmaker.create(share=False)

        proj2net1 = networkmaker.create(project_id=proj2.id)

        net_scoped_attr = client.add_attribute({'network_id': proj1net1.id,'name': 'test_scoped_attr'})
        childproj_scoped_attr = client.add_attribute({'project_id': childproj.id,'name': 'test_scoped_attr'})
        #safety net to make sure it's been added properly
        net_scoped_attr = client.get_attribute_by_id(attr_id=net_scoped_attr.id)
        childproj_scoped_attr = client.get_attribute_by_id(attr_id=childproj_scoped_attr.id)

        #Make sure they've been addded properly
        assert  net_scoped_attr.network_id == proj1net1.id
        assert  childproj_scoped_attr.project_id == childproj.id

        #Add a resource attribute to both the child network and child project
        new_network_ra = client.add_resource_attribute('NODE', proj1net1.nodes[0].id, net_scoped_attr.id, is_var=False)
        new_child_project_ra = client.add_resource_attribute('NODE', childprojnet1.nodes[0].id, childproj_scoped_attr.id, is_var=False)

        #now add an attribute with the same name at the project level, thus creating a potential
        #duplication
        newly_scoped_attr = client.add_attribute({'project_id': proj1.id,'name': 'test_scoped_attr'})

        with pytest.raises(HydraError):
            client.get_attribute_by_id(attr_id=net_scoped_attr.id)

        with pytest.raises(HydraError):
            _ = client.get_attribute_by_id(attr_id=childproj_scoped_attr.id)

        #check it has been rescoped
        assert newly_scoped_attr.project_id == proj1.id

        updated_network_ra = client.get_resource_attribute(new_network_ra.id)
        updated_child_project_ra = client.get_resource_attribute(new_child_project_ra.id)

        assert updated_network_ra.attr_id == newly_scoped_attr.id
        assert updated_child_project_ra.attr_id == newly_scoped_attr.id

        matching_attributes = client.search_attributes('test_scoped', network_id=proj1net1.id)

        assert len(matching_attributes) == 1 # the default scoped attrs plus this one.

        assert 'test_scoped_attr' in [a.name for a in matching_attributes]

        assert matching_attributes[0].id == newly_scoped_attr.id

        proj2_net_scoped_attr_ = client.add_attribute({'network_id': proj2net1.id,'name': 'test_scoped_attr'})
        #safety net to make sure it's been added properly
        proj2_net_scoped_attr = client.get_attribute_by_id(attr_id=proj2_net_scoped_attr_.id)
        new_ra = client.add_resource_attribute('NODE', proj2net1.nodes[0].id, proj2_net_scoped_attr.id, is_var=False)
        assert  proj2_net_scoped_attr.network_id == proj2net1.id

        #now add an attribute with the same name at the project level, thus creating a potential
        #duplication
        proj2_newly_scoped_attr = client.add_attribute({'project_id': proj2.id,'name': 'test_scoped_attr'})

        with pytest.raises(HydraError):
            _ = client.get_attribute_by_id(attr_id=proj2_net_scoped_attr.id)

        #check it has been rescoped
        assert proj2_newly_scoped_attr.project_id == proj2.id

        proj2_scoped_attr = client.get_attribute_by_id(attr_id=proj2_newly_scoped_attr.id)
        assert proj2_scoped_attr != newly_scoped_attr

    def test_different_projects_same_name_attributes(self, client, projectmaker, networkmaker):
        """
            Verifies that attribute deletion following rescoping occurs only within the
            correct bounds.
            Two same-name-and-dimension attrs added in peer projects should exist
            independently; the addition of the second should have no consequences for the first.
        """
        client.user_id = 1
        proj_user = client.user_id
        proj1 = projectmaker.create(name="Project 1", share=False)
        proj2 = projectmaker.create(name="Project 2", share=False)

        project1_scoped_attr = JSONObject({
            "name": f"Project Attribute",
            "dimension_id": None,
            "project_id": proj1.id
        })

        project2_scoped_attr = JSONObject({
            "name": f"Project Attribute",
            "dimension_id": None,
            "project_id": proj2.id
        })

        proj1_attr = client.add_attribute(project1_scoped_attr)
        net1 = networkmaker.create(project_id=proj1.id)
        proj1_ra = client.add_resource_attribute('NODE', net1.nodes[0].id, proj1_attr.id, is_var=False)
        proj1_attrs_before = client.get_attributes(project_id=proj1.id)

        proj2_attr = client.add_attribute(project2_scoped_attr)
        net2 = networkmaker.create(project_id=proj2.id)
        proj2_ra = client.add_resource_attribute('NODE', net2.nodes[0].id, proj2_attr.id, is_var=False)
        proj1_attrs_after = client.get_attributes(project_id=proj1.id)

        assert len(proj1_attrs_before) == len(proj1_attrs_after)
        assert proj1_attrs_before == proj1_attrs_after


    def test_bulk_add_network_and_project_scoped_attribute(self, client, network_with_data):
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

        project_scoped_attr = JSONObject({
            "name": f'Project Attribute {datetime.datetime.now()}',
            "dimension_id": None,
            "project_id": network_with_data.project_id
        })


        previous_all_attributes = client.get_attributes()

        new_attributes = client.add_attributes([global_attr,
                                                network_scoped_attr,
                                                project_scoped_attr])

        all_global_attributes = client.get_attributes()

        assert len(all_global_attributes) == len(previous_all_attributes) + 1

        #try add it again. SHould have no effect
        client.add_attributes([global_attr,
                               network_scoped_attr,
                               project_scoped_attr])

        global_attributes_no_network = client.get_attributes()
        network_scoped_attributes = client.get_attributes(
            network_id=network_with_data.id)

        #This should not have changed
        assert len(global_attributes_no_network) == len(all_global_attributes)

        #It's 2 because there is one added by default to all new
        #networks and projects, plus the ones we just added
        assert len(network_scoped_attributes) == 2

        #try add it again. SHould have no effect
        client.add_attribute(project_scoped_attr)

        global_attributes_no_project = client.get_attributes()
        project_scoped_attributes = client.get_attributes(
            project_id=network_with_data.project_id)

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


    def test_add_network_scoped_attribute_with_attribute_already_existing(self, client, network_with_data):
        """
            Test that when adding an attribute to a network, where that attribute is already
            scoped higher, then return the higher scoped attribute instead of failing
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

        project_attr_name =  f'Project Attribute {datetime.datetime.now()}'

        project_scoped_attr = JSONObject({
            "name": project_attr_name,
            "dimension_id": None,
            "project_id": network_with_data.project_id
        })


        previous_all_attributes = client.get_attributes()

        new_attributes = client.add_attributes([global_attr, network_scoped_attr, project_scoped_attr])

        new_project_attr = list(filter(lambda x:x.project_id==network_with_data.project_id, new_attributes))[0]

        scoped_ids = [a.id for a in new_attributes]

        #Now add the attribute which is scoped to the project, to the network
        duplicate_network_scoped_attr = JSONObject({
            "name": project_attr_name,
            "dimension_id": None,
            "network_id": network_with_data.id
        })


        new_attributes = client.add_attributes([duplicate_network_scoped_attr])

        #should return an existing scoped attribute (the project one)
        assert len(new_attributes) == 1
        assert new_attributes[0].id == new_project_attr.id

        new_attribute = client.add_attribute(duplicate_network_scoped_attr)

        assert new_attribute.id == new_project_attr.id
