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
# def test_(self, session):
#     """
#         SKELETON
#     """
#     pass


import logging
from hydra_base.util.testing import get_by_name
from .fixtures import *
import hydra_base as hb
from hydra_base.exceptions import ResourceNotFoundError
import pytest
from hydra_base.lib.objects import JSONObject
log = logging.getLogger(__name__)

class TestAttribute:
    """
        Test for attribute-based functionality
    """

    def test_add_attribute(self, session, projectmaker):
        test_attr = JSONObject({
            "name": 'Test Attribute 1',
            "dimension_id": None
        })
        new_attr = hb.add_attribute(test_attr, user_id=pytest.root_user_id)

        assert new_attr.name == test_attr.name, \
            "add_attribute didn't work"

        #make a new project so we can scope an attribute to it.
        project = projectmaker.create('scope-attributes')

        attr_with_project = JSONObject({
            "name": 'Project-Scoped Attribute',
            "dimension_id": None,
            "project_id":project.id
        })

        new_attr = hb.add_attribute(attr_with_project, user_id=pytest.root_user_id)

        all_attributes = hb.get_all_attributes(user_id=pytest.root_user_id)
        scoped_attributes = hb.get_all_attributes(project_id=project.id, user_id=pytest.root_user_id)

        assert len(all_attributes) == 2
        assert len(scoped_attributes) == 1

    def test_update_attribute(self, session, projectmaker):
        test_attr = JSONObject({
            "name": 'Test Attribute 1',
            "dimension_id": None
        })
        new_attr = hb.add_attribute(test_attr, user_id=pytest.root_user_id)

        new_attr.name = "Test attr updated"
        updated_attr = hb.update_attribute(new_attr, user_id=pytest.root_user_id)

        assert new_attr.id == updated_attr.id and \
                updated_attr.name == new_attr.name, \
                "update_attribute didn't work"

        #make a new project so we can scope an attribute to it.
        project = projectmaker.create('scope-attributes')
        updated_attr.project_id=project.id
        project_scoped_attr = hb.update_attribute(updated_attr, user_id=pytest.root_user_id)

        #As there's only 1 attribute in the system, check that it is included
        #in both the global request and the scoped request
        all_attributes = hb.get_all_attributes(user_id=pytest.root_user_id)
        scoped_attributes = hb.get_all_attributes(project_id=project.id, user_id=pytest.root_user_id)

        assert len(all_attributes) == 1
        assert len(scoped_attributes) == 1

    def test_delete_attribute(self, session, projectmaker):
        test_attr = JSONObject({
            "name": 'Test Attribute 1',
            "dimension_id": None
        })
        new_attr = hb.add_attribute(test_attr, user_id=pytest.root_user_id)

        result = hb.delete_attribute(new_attr.id, user_id=pytest.root_user_id)
        assert result == True, \
                "delete_attribute didn't work"


        with pytest.raises(ResourceNotFoundError):
            result = hb.delete_attribute(new_attr.id)

        #Add an attribute, scoped to a project. Check it is scoped correctly.
        #Then delete it and check the scoping has been removed also.
        project = projectmaker.create('scope-attributes')
        scoped_attr = JSONObject({
            "name": 'Scoped Attribute',
            "dimension_id": None,
            "project_id":project.id
        })

        scoped_attr_i = hb.add_attribute(scoped_attr, user_id=pytest.root_user_id)

        project_attrs_before_delete = \
                hb.get_project_attributes(project.id, user_id=pytest.root_user_id)

        #check the attribute is scoped to the project
        assert len(project_attrs_before_delete) == 1

        result = hb.delete_attribute(scoped_attr_i.id, user_id=pytest.root_user_id)

        project_attrs_after_delete = \
                hb.get_project_attributes(project.id, user_id=pytest.root_user_id)

        #double-check the scoping isn't there any more.
        assert len(project_attrs_after_delete) == 0

    def test_add_attributes(self, session, projectmaker):
        project = projectmaker.create('scope-attributes')
        test_attrs = [
            #a global attribute
            JSONObject({
                "name": 'Test Attribute 1',
                "dimension_id": None
            }),
            #another global attribute, but scoped to the project
            JSONObject({
                "name": 'Test Attribute 2',
                "dimension_id": 1,
                "project_id" : project.id
            }),
            None
        ]
        new_attrs_list = hb.add_attributes(test_attrs, user_id=pytest.root_user_id)


        #Ensure the attributes went in correctly
        new_attr_names = [a.name for a in new_attrs_list]

        for test_attr in test_attrs:
            if test_attr is None:
                continue
            assert test_attr.name in new_attr_names

        #Double check that there's 2 attributes in the system, and that 1 of them
        #is scoped to the project
        all_attributes = hb.get_all_attributes(user_id=pytest.root_user_id)
        scoped_attributes = hb.get_all_attributes(project_id=project.id, user_id=pytest.root_user_id)

        assert len(all_attributes) == 2
        assert len(scoped_attributes) == 1


    def test_get_attributes(self, session, projectmaker):
        """
            def get_attributes(**kwargs):
        """

        project = projectmaker.create('scope-attributes')

        unscoped_attr = JSONObject({
            "name": 'Unscoped Attribute',
            "dimension_id": None
        })
        scoped_attr = JSONObject({
            "name": 'Scoped Attribute',
            "dimension_id": None,
            "project_id": project.id
        })
        unscoped_attr_i = hb.add_attribute(unscoped_attr, user_id=pytest.root_user_id)
        scoped_attr_i = hb.add_attribute(scoped_attr, user_id=pytest.root_user_id)

        all_attributes = hb.get_attributes(user_id=pytest.root_user_id)
        assert len(all_attributes) == 2, "get_attributes didn't work as expected!"

        project_scoped_attributes = hb.get_attributes(project_id=project.id, user_id=pytest.root_user_id)
        assert len(project_scoped_attributes) == 1, "Getting scoped attributes didn't work"

    def test_add_project_attribute(self, session, projectmaker):

        #test that attributes can be sciped to more than 1 project
        project_1 = projectmaker.create("scope-attributes-1")
        project_2 = projectmaker.create("scope-attributes-2")

        unscoped_attr = JSONObject({
            "name": 'Unscoped Attribute',
            "dimension_id": None
        })
        scoped_attr = JSONObject({
            "name": 'Scoped Attribute',
            "dimension_id": None,
        })

        #Add two attributes, one of which will become scoped
        unscoped_attr_i = hb.add_attribute(unscoped_attr, user_id=pytest.root_user_id)
        scoped_attr_i = hb.add_attribute(scoped_attr, user_id=pytest.root_user_id)

        #scope scoped_attr_i to both projects
        hb.add_project_attribute(scoped_attr_i.id, project_1.id, 'Project1-Scoped Attr', user_id=pytest.root_user_id)
        hb.add_project_attribute(scoped_attr_i.id, project_2.id, user_id=pytest.root_user_id)

        #Make sure adding a duplicate raises an error
        with pytest.raises(hb.HydraError):
            hb.add_project_attribute(scoped_attr_i.id, project_2.id, user_id=pytest.root_user_id)

        all_attributes = hb.get_attributes(user_id=pytest.root_user_id)
        assert len(all_attributes) == 2, "get_attributes didn't work as expected!"

        """
        Test project 1 scoping
        """

        #These two queries should be equivalent -- requesting for attributes scoped to the project
        project_1_scoped_attributes = hb.get_attributes(project_id=project_1.id,
                                                        user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes) == 1

        project_1_scoped_attributes_1 = hb.get_project_attributes(project_1.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes_1) == 1

        assert project_1_scoped_attributes[0].project_info.display_name == 'Project1-Scoped Attr'


        """
        Test project 2 scoping
        """

        #These two queries should be equivalent -- requesting for attributes scoped to the project
        project_2_scoped_attributes = hb.get_attributes(project_id=project_2.id,
                                                        user_id=pytest.root_user_id)
        assert len(project_2_scoped_attributes) == 1

        project_2_scoped_attributes_1 = hb.get_project_attributes(project_2.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_2_scoped_attributes_1) == 1

        assert project_2_scoped_attributes[0].project_info.display_name == None


    def test_update_project_attribute(self, session, projectmaker):

        #test that attributes can be sciped to more than 1 project
        project_1 = projectmaker.create("scope-attributes-1")

        unscoped_attr = JSONObject({
            "name": 'Unscoped Attribute',
            "dimension_id": None
        })
        scoped_attr = JSONObject({
            "name": 'Scoped Attribute',
            "dimension_id": None,
        })

        #Add two attributes, one of which will become scoped
        unscoped_attr_i = hb.add_attribute(unscoped_attr, user_id=pytest.root_user_id)
        scoped_attr_i = hb.add_attribute(scoped_attr, user_id=pytest.root_user_id)

        #scope scoped_attr_i to both projects
        hb.add_project_attribute(scoped_attr_i.id, project_1.id, 'Project1-Scoped Attr', user_id=pytest.root_user_id)

        all_attributes = hb.get_attributes(user_id=pytest.root_user_id)
        assert len(all_attributes) == 2, "get_attributes didn't work as expected!"

        """
        Test project 1 scoping
        """

        #These two queries should be equivalent -- requesting for attributes scoped to the project
        project_1_scoped_attributes = hb.get_attributes(project_id=project_1.id,
                                                        user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes) == 1

        project_1_scoped_attributes_1 = hb.get_project_attributes(project_1.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes_1) == 1

        assert project_1_scoped_attributes[0].project_info.display_name == 'Project1-Scoped Attr'

        hb.update_project_attribute(scoped_attr_i.id, project_1.id, 'Updated Project1-Scoped Attr')

        project_1_scoped_attributes_1 = hb.get_project_attributes(project_1.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes_1) == 1

        assert project_1_scoped_attributes_1[0].project_info.display_name == 'Updated Project1-Scoped Attr'


    def test_remove_project_attribute(self, session, projectmaker):

        #test that attributes can be sciped to more than 1 project
        project_1 = projectmaker.create("scope-attributes-1")
        project_2 = projectmaker.create("scope-attributes-2")

        unscoped_attr = JSONObject({
            "name": 'Unscoped Attribute',
            "dimension_id": None
        })
        scoped_attr = JSONObject({
            "name": 'Scoped Attribute',
            "dimension_id": None,
        })
        unscoped_attr_i = hb.add_attribute(unscoped_attr, user_id=pytest.root_user_id)
        scoped_attr_i = hb.add_attribute(scoped_attr, user_id=pytest.root_user_id)

        #scope scoped_attr_i to both projects
        hb.add_project_attribute(scoped_attr_i.id, project_1.id, 'Project1-Scoped Attr', user_id=pytest.root_user_id)
        hb.add_project_attribute(scoped_attr_i.id, project_2.id, user_id=pytest.root_user_id)

        all_attributes = hb.get_attributes(user_id=pytest.root_user_id)
        assert len(all_attributes) == 2, "get_attributes didn't work as expected!"

        """
        Test project 1 scoping
        """

        #These two queries should be equivalent -- requesting for attributes scoped to the project
        project_1_scoped_attributes = hb.get_attributes(project_id=project_1.id,
                                                        user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes) == 1

        project_1_scoped_attributes_1 = hb.get_project_attributes(project_1.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes_1) == 1


        """
        Test project 2 scoping
        """

        #These two queries should be equivalent -- requesting for attributes scoped to the project
        project_2_scoped_attributes = hb.get_attributes(project_id=project_2.id,
                                                        user_id=pytest.root_user_id)
        assert len(project_2_scoped_attributes) == 1

        project_2_scoped_attributes_1 = hb.get_project_attributes(project_2.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_2_scoped_attributes_1) == 1


        """
        Remove scoping from 1 of the projects (project_2), then recheck scoping
        """
        hb.remove_project_attribute(scoped_attr_i.id, project_2.id, user_id=pytest.root_user_id)
        #Calling remove on a non-existent project attribute does nothing.
        hb.remove_project_attribute(scoped_attr_i.id, project_2.id, user_id=pytest.root_user_id)

        """
        Re-check  project 1 scoping (should still be scoped, 1 result).
        """

        #These two queries should be equivalent -- requesting for attributes scoped to the project
        project_1_scoped_attributes = hb.get_attributes(project_id=project_1.id,
                                                        user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes) == 1

        project_1_scoped_attributes_1 = hb.get_project_attributes(project_1.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_1_scoped_attributes_1) == 1


        """
        Re-check Test project 2 scoping -- should be unscoped (no results)
        """

        #These two queries should be equivalent -- requesting for attributes scoped to the project
        project_2_scoped_attributes = hb.get_attributes(project_id=project_2.id,
                                                        user_id=pytest.root_user_id)
        assert len(project_2_scoped_attributes) == 0

        project_2_scoped_attributes_1 = hb.get_project_attributes(project_2.id,
                                                                  user_id=pytest.root_user_id)
        assert len(project_2_scoped_attributes_1) == 0


    def test_get_template_attributes(self, session):
        """
            SKELETON
            def get_template_attributes(template_id, **kwargs):
        """
        pass

    def test_get_attribute_by_id(self, session, attribute):

        existing_attr = attribute

        retrieved_attr = hb.get_attribute_by_id(existing_attr.id, user_id=pytest.root_user_id)

        assert existing_attr.name           == retrieved_attr.name
        assert existing_attr.dimension_id   == retrieved_attr.dimension_id
        assert existing_attr.description    == retrieved_attr.description



    def test_get_all_attributes(self, session, attributes):

        all_attributes = hb.get_attributes(user_id=pytest.root_user_id)
        attribute_names = []
        for a in all_attributes:
            attribute_names.append(a.name)

        assert "Multi-added Attr 1" in attribute_names
        assert "Multi-added Attr 2" in attribute_names


    def test_get_attribute_by_name_and_dimension(self, session, attribute):
        existing_attr = attribute
        retrieved_attr = hb.get_attribute_by_name_and_dimension(
                                            existing_attr.name,
                                            existing_attr.dimension_id,
                                            user_id=pytest.root_user_id)

        assert existing_attr.id == retrieved_attr.id
        assert existing_attr.description == retrieved_attr.description

    def test_check_attr_dimension(self, session, new_dataset):
        """
            def check_attr_dimension(attr_id, **kwargs):
        """
        test_attr = JSONObject({
            "name": 'Test Attribute 1',
            "dimension_id": hb.get_dimension_by_unit_id(new_dataset.unit_id).id
        })
        new_attr = hb.add_attribute(test_attr)

        result = hb.check_attr_dimension(new_attr.id)
        log.info(result)
        assert result == 'OK', "check_attr_dimension didn't work as expected"


        pass

class TestResourceAttribute:
    def test_add_resource_attribute(self, session):
        """
            SKELETON
            def add_resource_attribute(resource_type, resource_id, attr_id, is_var, error_on_duplicate=True, **kwargs):
        """
        pass


    def test_update_resource_attribute(self, session):
        """
            SKELETON
            def update_resource_attribute(resource_attr_id, is_var, **kwargs):
        """
        pass

    def test_delete_resource_attribute(self, session):
        """
            SKELETON
            def delete_resource_attribute(resource_attr_id, **kwargs):
        """
        pass


    def test_add_resource_attrs_from_type(self, session):
        """
            SKELETON
            def add_resource_attrs_from_type(type_id, resource_type, resource_id,**kwargs):
        """
        pass

    def test_get_resource_attribute(self, session):
        """
            SKELETON
            def get_resource_attribute(resource_attr_id, **kwargs):
        """
        pass

    def test_get_all_resource_attributes(self, session):
        """
            SKELETON
            def get_all_resource_attributes(ref_key, network_id, template_id=None, **kwargs):
        """
        pass

    def test_get_resource_attributes(self, session):
        """
            SKELETON
            def get_resource_attributes(ref_key, ref_id, type_id=None, **kwargs):
        """
        pass





    def test_add_group_attribute(self, session, network_with_data, attribute):
        group = network_with_data.resourcegroups[0]
        hb.add_resource_attribute('GROUP', group.id, attribute.id, 'Y', user_id=pytest.root_user_id)
        group_attrs = hb.get_resource_attributes('GROUP', group.id, user_id=pytest.root_user_id)
        group_attr_ids = []
        for ga in group_attrs:
            group_attr_ids.append(ga.attr_id)
        assert attribute.id in group_attr_ids

    def test_get_all_group_attributes(self, session, network_with_data):

        #Get all the node attributes in the network
        group_attr_ids = []
        for g in network_with_data.resourcegroups:
            for ga in g.attributes:
                group_attr_ids.append(ga.id)

        group_attributes = hb.get_all_resource_attributes('GROUP', network_with_data.id, user_id=pytest.root_user_id)

        #Check that the retrieved attributes are in the list of group attributes
        retrieved_ras = []
        for ga in group_attributes:
            retrieved_ras.append(ga.id)
        assert set(group_attr_ids) == set(retrieved_ras)



    def test_add_link_attribute(self, session, network_with_data, attribute):
        link = network_with_data.links[0]
        hb.add_resource_attribute('LINK', link.id, attribute.id, 'Y')
        link_attributes = hb.get_resource_attributes('LINK', link.id, user_id=pytest.root_user_id)
        network_attr_ids = []

        for ra in link_attributes:
            network_attr_ids.append(ra.attr_id)
        assert attribute.id in network_attr_ids

    def test_get_all_link_attributes(self, session, network_with_data):

        #Get all the node attributes in the network
        link_attr_ids = []
        for l in network_with_data.links:
            for la in l.attributes:
                link_attr_ids.append(la.id)
        link_attributes = hb.get_all_resource_attributes('LINK', network_with_data.id, user_id=pytest.root_user_id)
        #Check that the retrieved attributes are in the list of node attributes
        retrieved_ras = []
        for la in link_attributes:
            retrieved_ras.append(la.id)
        assert set(link_attr_ids) == set(retrieved_ras)


    def test_add_node_attribute(self, session, network_with_data, attribute):
        node = network_with_data.nodes[0]
        hb.add_resource_attribute('NODE', node.id, attribute.id, 'Y', user_id=pytest.root_user_id)
        node_attributes = hb.get_resource_attributes('NODE', node.id, user_id=pytest.root_user_id)
        network_attr_ids = []
        for ra in node_attributes:
            network_attr_ids.append(ra.attr_id)
        assert attribute.id in network_attr_ids

    def test_add_duplicate_node_attribute(self, session, network_with_data, attribute):
        node = network_with_data.nodes[0]
        hb.add_resource_attribute('NODE', node.id, attribute.id, 'Y', user_id=pytest.root_user_id)
        node_attributes = hb.get_resource_attributes('NODE', node.id, user_id=pytest.root_user_id)
        node_attr_ids = []
        for ra in node_attributes:
            node_attr_ids.append(ra.attr_id)
        assert attribute.id in node_attr_ids

        with pytest.raises(hb.HydraError):
            hb.add_resource_attribute('NODE', node.id, attribute.id, 'Y', user_id=pytest.root_user_id)

    def test_get_all_node_attributes(self, session, network_with_data):

        #Get all the node attributes in the network
        node_attr_ids = []
        for n in network_with_data.nodes:
            for a in n.attributes:
                node_attr_ids.append(a.id)

        node_attributes = hb.get_all_resource_attributes('NODE', network_with_data.id, user_id=pytest.root_user_id)

        #Check that the retrieved attributes are in the list of node attributes
        retrieved_ras = []
        for na in node_attributes:
            retrieved_ras.append(na.id)

        assert set(node_attr_ids) == set(retrieved_ras)

        template_id = network_with_data.types[0].template_id

        node_attributes = hb.get_all_resource_attributes('NODE', network_with_data.id, template_id, user_id=pytest.root_user_id)

        #Check that the retrieved attributes are in the list of node attributes
        retrieved_ras = []
        for na in node_attributes:
            retrieved_ras.append(na.id)
        assert set(node_attr_ids) == set(retrieved_ras)



    def test_add_network_attribute(self, session, network_with_data, attribute):

        new_attr = attribute

        hb.add_resource_attribute('NETWORK', network_with_data.id, new_attr.id, 'Y', user_id=pytest.root_user_id)

        updated_network = hb.get_network(network_with_data.id, user_id=pytest.root_user_id)

        network_attr_ids = []

        for ra in updated_network.attributes:
            network_attr_ids.append(ra.attr_id)
        assert new_attr.id in network_attr_ids

    def test_add_network_attrs_from_type(self, session, network_with_data, attribute):
        """
            Recreate the situation where a template is updated, so the network needs.
            to be updated to reflect this change.
            Equivalent to 'apply_template_to_network', just performed differently.
        """

        network = network_with_data

        type_id = network.types[0].id

        #Get the template type, and add a new typeattr to it
        templatetype_j = JSONObject(hb.get_templatetype(type_id))

        new_typeattr = JSONObject({
            'attr_id' : attribute.id
        })

        templatetype_j.typeattrs.append(new_typeattr)

        hb.update_templatetype(templatetype_j)

        #Record the network's resource attributes before the update
        before_net_attrs = []
        for ra in network.attributes:
            before_net_attrs.append(ra.attr_id)
            log.info("old: %s",ra.attr_id)

        #Set any new resource attributes
        hb.add_resource_attrs_from_type(type_id, 'NETWORK', network.id, user_id=pytest.root_user_id)

        updated_network = hb.get_network(network.id, user_id=pytest.root_user_id)
        after_net_attrs = []
        for ra in updated_network.attributes:
            after_net_attrs.append(ra.attr_id)
            log.info("new: %s",ra.attr_id)

        assert len(after_net_attrs) == len(before_net_attrs) + 1

    def OLD_test_get_network_attrs(self, session, network_with_data):

        net_attrs = hb.get_resource_attributes('NETWORK', network_with_data.id, user_id=pytest.root_user_id)
        net_type_attrs = hb.get_resource_attributes('NETWORK', network_with_data.id,
                                                   network_with_data.types[0].id,
                                                  user_id=pytest.root_user_id)

        assert len(net_attrs) == 3
        assert len(net_type_attrs) == 2



class TestAttributeMap:

    def test_set_attribute_mapping(self, session, networkmaker):
        net1 = networkmaker.create()
        net2 = networkmaker.create()
        net3 = networkmaker.create()

        s1 = net1.scenarios[0]
        s2 = net2.scenarios[0]

        node_1 = net1.nodes[0]
        node_2 = net2.nodes[1]
        node_3 = net3.nodes[2]

        attr_1 = get_by_name('node_attr_a', node_1.attributes)
        attr_2 = get_by_name('node_attr_b', node_2.attributes)
        attr_3 = get_by_name('node_attr_c', node_3.attributes)

        rs_to_update_from = None
        for rs in s1.resourcescenarios:
            if rs.resource_attr_id == attr_1.id:
                rs_to_update_from = rs


        rs_to_change = None
        for rs in s2.resourcescenarios:
            if rs.resource_attr_id == attr_2.id:
                rs_to_change = rs

        hb.set_attribute_mapping(attr_1.id, attr_2.id, user_id=pytest.root_user_id)
        hb.set_attribute_mapping(attr_1.id, attr_3.id, user_id=pytest.root_user_id)


        all_mappings_1 = hb.get_mappings_in_network(net1.id, user_id=pytest.root_user_id)
        all_mappings_2 = hb.get_mappings_in_network(net2.id, net2.id, user_id=pytest.root_user_id)


        assert len(all_mappings_1) == 2
        assert len(all_mappings_2) == 1

        node_mappings_1 = hb.get_node_mappings(node_1.id, user_id=pytest.root_user_id)
        node_mappings_2 = hb.get_node_mappings(node_1.id, node_2.id, user_id=pytest.root_user_id)

        assert len(node_mappings_1) == 2
        assert len(node_mappings_2) == 1

        map_exists = hb.check_attribute_mapping_exists(attr_1.id, attr_2.id, user_id=pytest.root_user_id)
        assert map_exists == 'Y'
        map_exists = hb.check_attribute_mapping_exists(attr_2.id, attr_1.id, user_id=pytest.root_user_id)
        assert map_exists == 'N'
        map_exists = hb.check_attribute_mapping_exists(attr_2.id, attr_3.id, user_id=pytest.root_user_id)
        assert map_exists == 'N'


        updated_rs = hb.update_value_from_mapping(attr_1.id, attr_2.id, s1.id, s2.id, user_id=pytest.root_user_id)

        assert str(updated_rs.dataset.value) == str(rs_to_update_from.dataset.value)

        log.info("Deleting %s -> %s", attr_1.id, attr_2.id)
        hb.delete_attribute_mapping(attr_1.id, attr_2.id, user_id=pytest.root_user_id)
        all_mappings_1 = hb.get_mappings_in_network(net1.id, user_id=pytest.root_user_id)
        assert len(all_mappings_1) == 1

        hb.delete_mappings_in_network(net1.id, user_id=pytest.root_user_id)
        all_mappings_1 = hb.get_mappings_in_network(net1.id, user_id=pytest.root_user_id)
        assert len(all_mappings_1) == 0



    def test_delete_attribute_mapping(self, session):
        """
            SKELETON
            def delete_attribute_mapping(resource_attr_a, resource_attr_b, **kwargs):
        """
        pass
    def test_delete_mappings_in_network(self, session):
        """
            SKELETON
            def delete_mappings_in_network(network_id, network_2_id=None, **kwargs):
        """
        pass
    def test_get_mappings_in_network(self, session):
        """
            SKELETON
            def get_mappings_in_network(network_id, network_2_id=None, **kwargs):
        """
        pass
    def test_get_node_mappings(self, session):
        """
            SKELETON
            def get_node_mappings(node_id, node_2_id=None, **kwargs):
        """
        pass
    def test_get_link_mappings(self, session):
        """
            SKELETON
            def get_link_mappings(link_id, link_2_id=None, **kwargs):
        """
        pass
    def test_get_network_mappings(self, session):
        """
            SKELETON
            def get_network_mappings(network_id, network_2_id=None, **kwargs):
        """
        pass
    def test_check_attribute_mapping_exists(self, session):
        """
            SKELETON
            def check_attribute_mapping_exists(resource_attr_id_source, resource_attr_id_target, **kwargs):
        """
        pass
