
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
from .fixtures import *
import hydra_base as hb
import datetime
import pytest
from hydra_base.lib.objects import JSONObject
log = logging.getLogger(__name__)

class TestAttributeGroups:
    """
        Test for attribute Groups-based functionality
    """


    def test_add_attribute_group(self, session, projectmaker, attribute):
        project = projectmaker.create()

        newgroup = JSONObject({
            'project_id'  : project.id,
            'name'        : "Attribute Group %s" % (datetime.datetime.now(),),
            'description' : "A description of an attribute group",
            'layout'      : {"color": "green"},
            'exclusive'   : 'Y',
        })

        newgroup = hb.add_attribute_group(newgroup, user_id=pytest.root_user_id)

        retrieved_new_group = hb.get_attribute_group(newgroup.id, user_id=pytest.root_user_id)

        assert retrieved_new_group.name == newgroup.name

    def test_update_attribute_group(self, session, attributegroup):

        newname = attributegroup.name + " Updated"

        attributegroup.name = newname

        hb.update_attribute_group(attributegroup, user_id=pytest.root_user_id)

        retrieved_new_group = hb.get_attribute_group(attributegroup.id, user_id=pytest.root_user_id)

        assert retrieved_new_group.name == newname


    def test_delete_attribute_group(self, session, attributegroup):

        hb.delete_attribute_group(attributegroup.id, user_id=pytest.root_user_id)

        with pytest.raises(hb.HydraError):
            hb.get_attribute_group(attributegroup.id, user_id=pytest.root_user_id)

    def test_basic_add_attribute_group_items(self, session, projectmaker, network_with_data, attributegroupmaker):
        project = projectmaker.create()

        #convenience naming
        network = network_with_data

        #Create two groups -- attributes which are associated with a network,
        #and everything else.
        group_1 = attributegroupmaker.create(project.id, "Network Attributes")
        group_2 = attributegroupmaker.create(project.id, "Node Attributes")

        network_attributes = []
        for netattr in network.attributes:
            network_attributes.append(JSONObject({'attr_id'    : netattr.attr_id,
                                       'network_id' : netattr.network_id,
                                       'group_id'   : group_1.id}))

        node_attr_tracker = []
        node_attributes = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject({'attr_id'    : node_attr.attr_id,
                                            'network_id' : node.network_id,
                                            'group_id'   : group_2.id}))
                    node_attr_tracker.append(node_attr.attr_id)


        hb.add_attribute_group_items(network_attributes, user_id=pytest.root_user_id)

        hb.add_attribute_group_items(node_attributes, user_id=pytest.root_user_id)

        all_items_in_network = hb.get_network_attributegroup_items(network.id, user_id=pytest.root_user_id)


        assert len(all_items_in_network) == len(network_attributes)+len(node_attributes)

    def test_exclusive_add_attribute_group_items(self, session, projectmaker, network_with_data, attributegroupmaker):
        """
            add attributes to a group that are already in an exclusive group
        """

        project = projectmaker.create()

        #convenience naming
        network = network_with_data

        #Create two groups -- attributes which are associated with a network,
        #and everything else.
        group_1 = attributegroupmaker.create(project.id, "Network Attributes", 'Y')
        group_2 = attributegroupmaker.create(project.id, "Node Attributes")

        network_attributes = []
        node_attributes = []

        for netattr in network.attributes:
            network_attributes.append(JSONObject({'attr_id'    : netattr.attr_id,
                                       'network_id' : netattr.network_id,
                                       'group_id'   : group_1.id}))
            #Put these attributes into both groups. THis should fail, as group 1
            #is exclusive, and already has these attributes
            node_attributes.append(JSONObject({'attr_id'    : netattr.attr_id,
                                       'network_id' : netattr.network_id,
                                       'group_id'   : group_2.id}))


        node_attr_tracker = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject({'attr_id'    : node_attr.attr_id,
                                            'network_id' : node.network_id,
                                            'group_id'   : group_2.id}))
                    node_attr_tracker.append(node_attr.attr_id)


        log.info("Adding items to group 1 (network attributes)")
        hb.add_attribute_group_items(network_attributes, user_id=pytest.root_user_id)

        #add a group with attributes that are already in an exclusive group
        with pytest.raises(hb.HydraError):
            log.info("Adding items to group 2 (node attributes, plus network attributes)")
            hb.add_attribute_group_items(node_attributes, user_id=pytest.root_user_id)

    def test_reverse_exclusive_add_attribute_group_items(self, session, projectmaker, network_with_data, attributegroupmaker):
        """
            add attributes to an exclusive group that are already in another group
        """

        project = projectmaker.create()

        #convenience naming
        network = network_with_data

        #Create two groups -- attributes which are associated with a network,
        #and everything else.
        group_1 = attributegroupmaker.create(project.id, "Network Attributes", 'Y')
        group_2 = attributegroupmaker.create(project.id, "Node Attributes")

        network_attributes = []
        node_attributes = []

        for netattr in network.attributes:
            network_attributes.append(JSONObject({'attr_id'    : netattr.attr_id,
                                       'network_id' : netattr.network_id,
                                       'group_id'   : group_1.id}))
            #Put these attributes into both groups. THis should fail, as group 1
            #is exclusive, and already has these attributes
            node_attributes.append(JSONObject({'attr_id'    : netattr.attr_id,
                                       'network_id' : netattr.network_id,
                                       'group_id'   : group_2.id}))


        node_attr_tracker = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject({'attr_id'    : node_attr.attr_id,
                                            'network_id' : node.network_id,
                                            'group_id'   : group_2.id}))
                    node_attr_tracker.append(node_attr.attr_id)


        log.info("Adding items to group 2 (node attributes, plus network attributes)")
        hb.add_attribute_group_items(node_attributes, user_id=pytest.root_user_id)

        #add attributes to an exclusive group that are already in another group
        with pytest.raises(hb.HydraError):
            log.info("Adding items to group 1 (network attributes)")
            hb.add_attribute_group_items(network_attributes, user_id=pytest.root_user_id)

    def test_delete_attribute_group_items(self, session, projectmaker, network_with_data, attributegroupmaker):
        project = projectmaker.create()

        #convenience naming
        network = network_with_data

        #Create two groups -- attributes which are associated with a network,
        #and everything else.
        group_1 = attributegroupmaker.create(project.id, "Network Attributes")
        group_2 = attributegroupmaker.create(project.id, "Node Attributes")

        network_attributes = []
        for netattr in network.attributes:
            network_attributes.append(JSONObject({'attr_id'    : netattr.attr_id,
                                       'network_id' : netattr.network_id,
                                       'group_id'   : group_1.id}))

        node_attr_tracker = []
        node_attributes   = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject({'attr_id'    : node_attr.attr_id,
                                            'network_id' : node.network_id,
                                            'group_id'   : group_2.id}))
                    node_attr_tracker.append(node_attr.attr_id)


        hb.add_attribute_group_items(network_attributes, user_id=pytest.root_user_id)

        hb.add_attribute_group_items(node_attributes, user_id=pytest.root_user_id)

        all_items_in_network = hb.get_network_attributegroup_items(network.id, user_id=pytest.root_user_id)

        assert len(all_items_in_network) == len(network_attributes)+len(node_attributes)

        #Now remove all the node attributes
        hb.delete_attribute_group_items(node_attributes, user_id=pytest.root_user_id)

        all_items_in_network = hb.get_network_attributegroup_items(network.id, user_id=pytest.root_user_id)

        assert len(all_items_in_network) == len(network_attributes)

    def test_get_attribute_group(self, session):
        """
            SKELETON
            def get_attribute_group(group_id, **kwargs):
        """
        pass
    def test_get_network_attributegroup_items(self, session):
        """
            SKELETON
            def get_network_attributegroup_items(network_id, **kwargs):
        """
        pass
    def test_get_group_attributegroup_items(self, session):
        """
            SKELETON
            def get_group_attributegroup_items(network_id, group_id, **kwargs):
        """
        pass
    def test_get_attribute_item_groups(self, session):
        """
            SKELETON
            def get_attribute_item_groups(network_id, attr_id, **kwargs):
        """
        pass
    def test_add_attribute_group_items(self, session):
        """
            SKELETON
            def add_attribute_group_items(attributegroupitems, **kwargs):
        """
        pass
