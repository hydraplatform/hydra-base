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

import logging
from util import update_template, get_by_name
from fixtures import *
import copy
import json
import hydra_base as hb
from hydra_base.exceptions import ResourceNotFoundError
import datetime
import pytest
from hydra_base.lib.objects import JSONObject
log = logging.getLogger(__name__)

class TestAttribute:
    """
        Test for attribute-based functionality
    """
    def test_get_network_attrs(self, session, network_with_data):

        net_attrs = hb.get_resource_attributes('NETWORK', network_with_data.id, user_id=pytest.root_user_id)
        net_type_attrs = hb.get_resource_attributes('NETWORK', network_with_data.id,
                                                   network_with_data.types[0].id,
                                                  user_id=pytest.root_user_id)

        assert len(net_attrs) == 3
        assert len(net_type_attrs) == 2


    def test_get_all_attributes(self, session, attributes):

        all_attributes = hb.get_attributes(user_id=pytest.root_user_id)
        attribute_names = []
        for a in all_attributes:
            attribute_names.append(a.name)

        assert "Multi-added Attr 1" in attribute_names
        assert "Multi-added Attr 2" in attribute_names

    def test_get_attribute_by_id(self, session, attribute):

        existing_attr = attribute

        retrieved_attr = hb.get_attribute_by_id(existing_attr.id, user_id=pytest.root_user_id)

        assert existing_attr.name        == retrieved_attr.name
        assert existing_attr.dimension   == retrieved_attr.dimension
        assert existing_attr.description == retrieved_attr.description

    def test_get_attribute_by_name_and_dimension(self, session, attribute):
        existing_attr = attribute
        retrieved_attr = hb.get_attribute_by_name_and_dimension(
                                            existing_attr.name,
                                            existing_attr.dimension,
                                            user_id=pytest.root_user_id)

        assert existing_attr.id == retrieved_attr.id
        assert existing_attr.description == retrieved_attr.description

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


        #print all_mappings_1
        #print all_mappings_2
        assert len(all_mappings_1) == 2
        assert len(all_mappings_2) == 1

        node_mappings_1 = hb.get_node_mappings(node_1.id, user_id=pytest.root_user_id)
        node_mappings_2 = hb.get_node_mappings(node_1.id, node_2.id, user_id=pytest.root_user_id)
        #print "*"*100
        #print node_mappings_1
        #print node_mappings_2
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

class TestAttributeGroups:
    """
        Test for attribute-based functionality
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
