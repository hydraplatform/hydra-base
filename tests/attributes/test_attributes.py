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

class TestAttribute:
    """
        Test for attribute-based functionality
    """

    """
        TESTED
    """

    def test_add_attribute(self, client):
        test_attr = JSONObject({
            "name": f'Test Attribute {datetime.datetime.now()}',
            "dimension_id": None
        })

        previous_all_attributes = client.get_attributes()

        new_attr = client.add_attribute(test_attr)

        assert new_attr.name == test_attr.name, \
            "add_attribute didn't work"

        all_attributes = client.get_attributes()

        assert len(all_attributes) == len(previous_all_attributes) + 1

        #try add it again. SHould have no effect
        new_attr = client.add_attribute(test_attr)

        all_attributes_insert_2 = client.get_attributes()

        assert len(all_attributes) == len(all_attributes_insert_2)

        upper_case_attr = test_attr
        upper_case_attr['name'] = upper_case_attr['name'].upper()

        new_upper_attr = client.add_attribute(upper_case_attr)

        assert new_upper_attr['id'] == new_attr['id']


    def test_update_attribute(self, client):
        test_attr = JSONObject({
            "name": f'Test Attribute {datetime.datetime.now()}',
            "dimension_id": None
        })

        new_attr = client.add_attribute(test_attr)
        new_attr.name = f"Test attr updated {datetime.datetime.now()}"
        updated_attr = client.update_attribute(new_attr)
        assert new_attr.id == updated_attr.id and \
                updated_attr.name == new_attr.name, \
                "update_attribute didn't work"
        #Try update this again (should have no effect)
        updated_attr.description = "Updated description"
        updated_attr_1 = client.update_attribute(updated_attr)
        assert updated_attr_1.description == "Updated description"

        #Now add a new attribute which should fail when updated
        test_attr_fail = JSONObject({
            "name": f'Test Attribute {datetime.datetime.now()}',
            "dimension_id": None
        })

        new_attr_fail = client.add_attribute(test_attr_fail)
        #set the name to be the same as the other attribute
        new_attr_fail.name = new_attr.name

        #this should fail because there's already an attribute with this naem
        #and dimension (since we've just set it.)
        with pytest.raises(HydraError):
            client.update_attribute(new_attr_fail)



    def test_delete_attribute(self, client):
        test_attr = JSONObject({
            "name": 'Test Attribute 1',
            "dimension_id": None
        })
        new_attr = client.add_attribute(test_attr)

        attr = client.get_attribute_by_id(new_attr.id)

        result = client.delete_attribute(new_attr.id)

        with pytest.raises(HydraError):
            client.get_attribute_by_id(new_attr.id)

        with pytest.raises(HydraError):
            result = client.delete_attribute(new_attr.id)

    def test_add_attributes(self, client):
        test_attrs = [
            JSONObject({
                "name": 'Test Attribute 1',
                "dimension_id": None
            }),
            JSONObject({
                "name": 'Test Attribute 2',
                "dimension_id": 1
            })
        ]
        new_attrs_list_1 = client.add_attributes(test_attrs)

        all_attributes_after_add_1 = client.get_attributes()

        #test to make sure that the insert is not case-sensitive.
        upper_test_attrs = test_attrs
        for a in upper_test_attrs:
            a['name'] = a['name'].upper()

        #Try adding the attributes again. It should ignore them as theyr'e already there.
        new_attrs_list_2 = client.add_attributes(upper_test_attrs)

        all_attributes_after_add_2 = client.get_attributes()

        #This should have returned the attributes with the IDS from the first insert
        assert sorted([a.id for a in new_attrs_list_1]) == sorted([a.id for a in new_attrs_list_2])

        assert len(all_attributes_after_add_1) == len(all_attributes_after_add_2)

        attributeset = set([(a.name, a.dimension) for a in all_attributes_after_add_1])

        #Ensure that there are no duplicates by checking that the length of the set
        #of name/dimension pairs is the same as the length of all attributes
        assert len(attributeset) == len(all_attributes_after_add_2)


    def test_get_attributes(self, client):
        """
            def get_attributes(**kwargs):
        """
        test_attr = JSONObject({
            "name": 'Test Attribute 1',
            "dimension_id": None
        })
        new_attr = client.add_attribute(test_attr)
        attributes = client.get_attributes()
        assert len(attributes) > 0, "get_attributes didn't work as expected!"


    def test_get_template_attributes(self, client):
        """
            SKELETON
            def get_template_attributes(template_id, **kwargs):
        """
        pass

    def test_get_attribute_by_id(self, client, attribute):

        existing_attr = attribute

        retrieved_attr = client.get_attribute_by_id(existing_attr.id)

        assert existing_attr.name           == retrieved_attr.name
        assert existing_attr.dimension_id   == retrieved_attr.dimension_id
        assert existing_attr.description    == retrieved_attr.description

    def test_get_attributes_by_id(self, client):

        test_attrs = [
            JSONObject({
                "name": 'Test Attribute 1',
                "dimension_id": None
            }),
            JSONObject({
                "name": 'Test Attribute 2',
                "dimension_id": 1
            })
        ]
        new_attrs_list = client.add_attributes(test_attrs)

        retrieved_attrs = client.get_attributes_by_id([a.id for a in new_attrs_list])

        assert retrieved_attrs[0].name == 'Test Attribute 1'
        assert retrieved_attrs[1].name == 'Test Attribute 2'

        retrieved_attrs = client.get_attributes_by_id([])
        assert len(retrieved_attrs) == 0


    def test_get_all_attributes(self, client, attributes):

        all_attributes = client.get_attributes()
        attribute_names = []
        for a in all_attributes:
            attribute_names.append(a.name)

        assert "Multi-added Attr 1" in attribute_names
        assert "Multi-added Attr 2" in attribute_names


    def test_get_attribute_by_name_and_dimension(self, client, attribute):
        existing_attr = attribute
        retrieved_attr = client.get_attribute_by_name_and_dimension(
                                            existing_attr.name,
                                            existing_attr.dimension_id)

        assert existing_attr.id == retrieved_attr.id
        assert existing_attr.description == retrieved_attr.description

    def test_check_attr_dimension(self, client, new_dataset):
        """
            def check_attr_dimension(attr_id, **kwargs):
        """
        test_attr = JSONObject({
            "name": 'Test Attribute 1',
            "dimension_id": client.get_dimension_by_unit_id(new_dataset.unit_id).id
        })
        new_attr = client.add_attribute(test_attr)

        result = client.check_attr_dimension(new_attr.id)
        log.info(result)
        assert result == 'OK', "check_attr_dimension didn't work as expected"


        pass

class TestResourceAttribute:

    def test_add_resource_attributes(self,
                                     client,
                                     network_with_data,
                                     attribute):

        new_attr = attribute

        existing_attr = network_with_data.attributes[0]

        #add one new one, plus one existing one. This should result in only one being added
        newattributes = [
            {"attr_id": new_attr.id, "network_id": network_with_data.id, "attr_is_var": "Y"},
            existing_attr
        ]

        num_added = client.add_resource_attributes(newattributes)

        updated_network = client.get_network(network_with_data.id)

        assert len(updated_network.attributes) == len(network_with_data.attributes) + len(num_added)

        assert new_attr.id in [netattr.attr_id for netattr in updated_network.attributes]

    def test_update_resource_attribute(self, client):
        """
            SKELETON
            def update_resource_attribute(resource_attr_id, is_var, **kwargs):
        """
        pass

    def test_delete_resource_attribute(self, client):
        """
            SKELETON
            def delete_resource_attribute(resource_attr_id, **kwargs):
        """
        pass


    def test_add_resource_attrs_from_type(self, client):
        """
            SKELETON
            def add_resource_attrs_from_type(type_id, resource_type, resource_id,**kwargs):
        """
        pass

    def test_get_resource_attribute(self, client):
        """
            SKELETON
            def get_resource_attribute(resource_attr_id, **kwargs):
        """
        pass

    def test_get_all_resource_attributes(self, client):
        """
            SKELETON
            def get_all_resource_attributes(ref_key, network_id, template_id=None, **kwargs):
        """
        pass

    def test_get_resource_attributes(self, client):
        """
            SKELETON
            def get_resource_attributes(ref_key, ref_id, type_id=None, **kwargs):
        """
        pass

    def test_get_all_network_attributes(self, client, network_with_data):
        all_network_attributes = client.get_all_network_attributes(network_with_data.id)

        manual_all_network_attributes = [a.attr_id for a in network_with_data.attributes]
        for n in network_with_data.nodes:
            for a in n.attributes:
                if a.attr_id not in manual_all_network_attributes:
                    manual_all_network_attributes.append(a.attr_id)
        for n in network_with_data.links:
            for a in n.attributes:
                print(a.attr_id)
                if a.attr_id not in manual_all_network_attributes:
                    manual_all_network_attributes.append(a.attr_id)
        for n in network_with_data.resourcegroups:
            for a in n.attributes:
                if a.attr_id not in manual_all_network_attributes:
                    manual_all_network_attributes.append(a.attr_id)

        assert len(set([a.id for a in all_network_attributes])) == len(manual_all_network_attributes)


    def test_add_group_attribute(self, client, network_with_data, attribute):
        group = network_with_data.resourcegroups[0]
        client.add_resource_attribute('GROUP', group.id, attribute.id, 'Y')
        group_attrs = client.get_resource_attributes('GROUP', group.id)
        group_attr_ids = []
        for ga in group_attrs:
            group_attr_ids.append(ga.attr_id)
        assert attribute.id in group_attr_ids

    def test_get_all_group_attributes(self, client, network_with_data):

        #Get all the node attributes in the network
        group_attr_ids = []
        for g in network_with_data.resourcegroups:
            for ga in g.attributes:
                group_attr_ids.append(ga.id)

        group_attributes = client.get_all_resource_attributes('GROUP', network_with_data.id)

        #Check that the retrieved attributes are in the list of group attributes
        retrieved_ras = []
        for ga in group_attributes:
            retrieved_ras.append(ga.id)
        assert set(group_attr_ids) == set(retrieved_ras)



    def test_add_link_attribute(self, client, network_with_data, attribute):
        link = network_with_data.links[0]
        client.add_resource_attribute('LINK', link.id, attribute.id, 'Y')
        link_attributes = client.get_resource_attributes('LINK', link.id)
        network_attr_ids = []

        for ra in link_attributes:
            network_attr_ids.append(ra.attr_id)
        assert attribute.id in network_attr_ids

    def test_get_all_link_attributes(self, client, network_with_data):

        #Get all the node attributes in the network
        link_attr_ids = []
        for l in network_with_data.links:
            for la in l.attributes:
                link_attr_ids.append(la.id)
        link_attributes = client.get_all_resource_attributes('LINK', network_with_data.id)
        #Check that the retrieved attributes are in the list of node attributes
        retrieved_ras = []
        for la in link_attributes:
            retrieved_ras.append(la.id)
        assert set(link_attr_ids) == set(retrieved_ras)


    def test_add_node_attribute(self, client, network_with_data, attribute):
        node = network_with_data.nodes[0]
        client.add_resource_attribute('NODE', node.id, attribute.id, 'Y')
        node_attributes = client.get_resource_attributes('NODE', node.id)
        network_attr_ids = []
        for ra in node_attributes:
            network_attr_ids.append(ra.attr_id)
        assert attribute.id in network_attr_ids

    def test_add_duplicate_node_attribute(self, client, network_with_data, attribute):
        node = network_with_data.nodes[0]
        client.add_resource_attribute('NODE', node.id, attribute.id, 'Y')
        node_attributes = client.get_resource_attributes('NODE', node.id)
        node_attr_ids = []
        for ra in node_attributes:
            node_attr_ids.append(ra.attr_id)
        assert attribute.id in node_attr_ids

        with pytest.raises(hb.HydraError):
            client.add_resource_attribute('NODE', node.id, attribute.id, 'Y')

    def test_get_all_node_attributes(self, client, network_with_data):

        #Get all the node attributes in the network
        node_attr_ids = []
        for n in network_with_data.nodes:
            for a in n.attributes:
                node_attr_ids.append(a.id)

        node_attributes = client.get_all_resource_attributes('NODE', network_with_data.id)

        #Check that the retrieved attributes are in the list of node attributes
        retrieved_ras = []
        for na in node_attributes:
            retrieved_ras.append(na.id)

        assert set(node_attr_ids) == set(retrieved_ras)

        template_id = network_with_data.types[0].template_id

        node_attributes = client.get_all_resource_attributes('NODE', network_with_data.id, template_id)

        #Check that the retrieved attributes are in the list of node attributes
        retrieved_ras = []
        for na in node_attributes:
            retrieved_ras.append(na.id)
        assert set(node_attr_ids) == set(retrieved_ras)



    def test_add_network_attribute(self, client, network_with_data, attribute):

        new_attr = attribute

        client.add_resource_attribute('NETWORK', network_with_data.id, new_attr.id, 'Y')

        updated_network = client.get_network(network_with_data.id)

        network_attr_ids = []

        for ra in updated_network.attributes:
            network_attr_ids.append(ra.attr_id)
        assert new_attr.id in network_attr_ids

    def test_add_network_attrs_from_type(self, client, network_with_data, attribute):
        """
            Recreate the situation where a template is updated, so the network needs.
            to be updated to reflect this change.
            Equivalent to 'apply_template_to_network', just performed differently.
        """

        network = network_with_data

        type_id = network.types[0].id

        #Get the template type, and add a new typeattr to it
        templatetype_j = JSONObject(client.get_templatetype(type_id))

        new_typeattr = JSONObject({
            'attr_id' : attribute.id
        })

        templatetype_j.typeattrs.append(new_typeattr)

        client.update_templatetype(templatetype_j)

        #Record the network's resource attributes before the update
        before_net_attrs = []
        for ra in network.attributes:
            before_net_attrs.append(ra.attr_id)
            log.info("old: %s",ra.attr_id)

        #Set any new resource attributes
        client.add_resource_attrs_from_type(type_id, 'NETWORK', network.id)

        updated_network = client.get_network(network.id)
        after_net_attrs = []
        for ra in updated_network.attributes:
            after_net_attrs.append(ra.attr_id)
            log.info("new: %s",ra.attr_id)

        assert len(after_net_attrs) == len(before_net_attrs) + 1

    def OLD_test_get_network_attrs(self, client, network_with_data):

        net_attrs = client.get_resource_attributes('NETWORK', network_with_data.id)
        net_type_attrs = client.get_resource_attributes('NETWORK',
                                                        network_with_data.id,
                                                        network_with_data.types[0].id)

        assert len(net_attrs) == 3
        assert len(net_type_attrs) == 2

    def test_delete_all_duplicate_attributes(self, client, network_with_data):

        duplicate_attribute = JSONObject({'name': 'duplicate', 'dimension_id': None})

        #use dedicated testing function  which allows duplicates
        dupe_attr_1 = client.add_attribute(duplicate_attribute, check_existing=False)
        dupe_attr_2 = client.add_attribute(duplicate_attribute, check_existing=False)

        all_attrs = client.get_attributes()

        assert dupe_attr_1.id in [a.id for a in all_attrs]
        assert dupe_attr_2.id in [a.id for a in all_attrs]

        #add duplicate resource attributes
        client.add_resource_attribute('NETWORK', network_with_data.id, dupe_attr_1.id, 'Y')
        client.add_resource_attribute('NETWORK', network_with_data.id, dupe_attr_2.id, 'Y')

        #check the dupes are there
        updated_net = client.get_network(network_with_data.id)
        updated_net_ras = [ra.attr_id for ra in updated_net.attributes]
        assert dupe_attr_1.id in updated_net_ras
        assert dupe_attr_2.id in updated_net_ras

        #now add duplicate attrs to the template type
        templatetype_to_update = network_with_data.types[0].id
        client.add_typeattr(JSONObject({'type_id': templatetype_to_update,
                                        'attr_id': dupe_attr_1.id}))
        client.add_typeattr(JSONObject({'type_id': templatetype_to_update,
                                        'attr_id': dupe_attr_2.id}))

        #check the dupes are there
        updated_type = client.get_templatetype(templatetype_to_update)
        assert dupe_attr_1.id in [ta.attr_id for ta in updated_type.typeattrs]
        assert dupe_attr_1.id in [ta.attr_id for ta in updated_type.typeattrs]

        client.delete_all_duplicate_attributes()

        #check the dupes are gone
        updated_net = client.get_network(network_with_data.id)
        updated_net_ras = [ra.attr_id for ra in updated_net.attributes]
        assert dupe_attr_1.id in updated_net_ras
        assert dupe_attr_2.id not in updated_net_ras

        #check the dupes are gone
        updated_type = client.get_templatetype(templatetype_to_update)
        assert dupe_attr_1.id in [ta.attr_id for ta in updated_type.typeattrs]
        assert dupe_attr_2.id not in [ta.attr_id for ta in updated_type.typeattrs]

        reduced_attrs = client.get_attributes()

        #check that the first attr is there, but the dupe is not.
        #the one to keep should be the one with the lowest ID
        lowest_id = min(dupe_attr_1.id, dupe_attr_2.id)
        assert lowest_id in [a.id for a in reduced_attrs]
        assert dupe_attr_2.id not in [a.id for a in reduced_attrs]

    def test_delete_duplicate_resourceattributes(self, client, network_with_data):

        #first add a duplicate resourceattr to the network

        #find a resourceattr.
        network_with_data.attributes.sort(key=lambda x : x.attr_id)
        ra1 = network_with_data.attributes[0]
        ra2 = network_with_data.attributes[1]
        ra3 = network_with_data.attributes[-1] # not associated to a template

        ra_attribute1 = client.get_attribute_by_id(ra1.attr_id)
        ra_attribute2 = client.get_attribute_by_id(ra2.attr_id)
        ra_attribute3 = client.get_attribute_by_id(ra3.attr_id)

        #create an attribute with the same name but a different dimension
        duplicate_attribute1 = JSONObject({
            'name': ra_attribute1.name,
            'dimension_id': 1
        })
        duplicate_attribute2 = JSONObject({
            'name': ra_attribute2.name,
            'dimension_id': 1
        })

        duplicate_attribute3 = JSONObject({
            'name': ra_attribute3.name,
            'dimension_id': 1
        })
        dupeattr1 = client.add_attribute(duplicate_attribute1)
        dupeattr2 = client.add_attribute(duplicate_attribute2)
        dupeattr3 = client.add_attribute(duplicate_attribute3)

        dupe_ra1 = client.add_resource_attribute('NETWORK', network_with_data.id, dupeattr1.id, 'N')#
        #get an arbitrary dataset
        dataset = client.get_dataset(1)
        #set a value on the RA which sould get transferred in the deletion later
        new_rscen = client.add_data_to_attribute(network_with_data.scenarios[0].id,
                                                 dupe_ra1.id,
                                                 dataset)
        #add 2 more dupes but with no data associated to them
        dupe_ra2 = client.add_resource_attribute('NETWORK', network_with_data.id, dupeattr2.id, 'N')
        dupe_ra3 = client.add_resource_attribute('NETWORK', network_with_data.id, dupeattr3.id, 'N')

        updated_net = client.get_network(network_with_data.id, include_data=True)
        updated_net_ras = [ra.attr_id for ra in updated_net.attributes]
        assert dupeattr1.id in updated_net_ras
        assert dupeattr2.id in updated_net_ras
        assert dupeattr3.id in updated_net_ras

        #verify the data has been associated to the dupe RA
        original_rs = network_with_data.scenarios[0].resourcescenarios
        new_rs = updated_net.scenarios[0].resourcescenarios
        assert len(new_rs) == len(original_rs) + 1
        assert ra1.id not in [rs.resource_attr_id for rs in new_rs]
        assert dupe_ra1.id in [rs.resource_attr_id for rs in new_rs]

        #now that the new attribute is added, try to delete it.
        client.delete_duplicate_resourceattributes()

        updated_net = client.get_network(network_with_data.id, include_data=True)
        updated_net_ras = [ra.attr_id for ra in updated_net.attributes]
        #the number of network attributes has decreased because BOTH duplicates
        #of RA3 (which are not associated to a template) have been removed.
        #This means one of the original ones is now gone
        assert len(updated_net.attributes) == len(network_with_data.attributes) -1
        assert dupeattr1.id not in updated_net_ras
        assert dupeattr2.id not in updated_net_ras
        assert dupeattr3.id not in updated_net_ras

        #verify the data which was on the dupe has been remapped to the remaining, correct, attribute
        original_rs = network_with_data.scenarios[0].resourcescenarios
        new_rs = updated_net.scenarios[0].resourcescenarios
        assert len(new_rs) == len(original_rs) + 1
        assert ra1.id in [rs.resource_attr_id for rs in new_rs]
        assert dupe_ra1.id not in [rs.resource_attr_id for rs in new_rs]

class TestAttributeMap:

    def test_set_attribute_mapping(self, client, networkmaker):
        net1 = networkmaker.create()
        net2 = networkmaker.create()
        net3 = networkmaker.create()

        s1 = net1.scenarios[0]
        s2 = net2.scenarios[0]

        node_1 = net1.nodes[0]
        node_2 = net2.nodes[1]
        node_3 = net3.nodes[2]

        attr_1 = client.testutils.get_by_name('node_attr_a', node_1.attributes)
        attr_2 = client.testutils.get_by_name('node_attr_b', node_2.attributes)
        attr_3 = client.testutils.get_by_name('node_attr_c', node_3.attributes)

        rs_to_update_from = None
        for rs in s1.resourcescenarios:
            if rs.resource_attr_id == attr_1.id:
                rs_to_update_from = rs


        rs_to_change = None
        for rs in s2.resourcescenarios:
            if rs.resource_attr_id == attr_2.id:
                rs_to_change = rs

        client.set_attribute_mapping(attr_1.id, attr_2.id)
        client.set_attribute_mapping(attr_1.id, attr_3.id)


        all_mappings_1 = client.get_mappings_in_network(net1.id)
        all_mappings_2 = client.get_mappings_in_network(net2.id, net2.id)


        assert len(all_mappings_1) == 2
        assert len(all_mappings_2) == 1

        node_mappings_1 = client.get_node_mappings(node_1.id)
        node_mappings_2 = client.get_node_mappings(node_1.id, node_2.id)

        assert len(node_mappings_1) == 2
        assert len(node_mappings_2) == 1

        map_exists = client.check_attribute_mapping_exists(attr_1.id, attr_2.id)
        assert map_exists == 'Y'
        map_exists = client.check_attribute_mapping_exists(attr_2.id, attr_1.id)
        assert map_exists == 'N'
        map_exists = client.check_attribute_mapping_exists(attr_2.id, attr_3.id)
        assert map_exists == 'N'


        updated_rs = client.update_value_from_mapping(attr_1.id, attr_2.id, s1.id, s2.id)

        assert str(updated_rs.dataset.value) == str(rs_to_update_from.dataset.value)

        log.info("Deleting %s -> %s", attr_1.id, attr_2.id)
        client.delete_attribute_mapping(attr_1.id, attr_2.id)
        all_mappings_1 = client.get_mappings_in_network(net1.id)
        assert len(all_mappings_1) == 1

        client.delete_mappings_in_network(net1.id)
        all_mappings_1 = client.get_mappings_in_network(net1.id)
        assert len(all_mappings_1) == 0



    def test_delete_attribute_mapping(self, client):
        """
            SKELETON
            def delete_attribute_mapping(resource_attr_a, resource_attr_b, **kwargs):
        """
        pass
    def test_delete_mappings_in_network(self, client):
        """
            SKELETON
            def delete_mappings_in_network(network_id, network_2_id=None, **kwargs):
        """
        pass
    def test_get_mappings_in_network(self, client):
        """
            SKELETON
            def get_mappings_in_network(network_id, network_2_id=None, **kwargs):
        """
        pass
    def test_get_node_mappings(self, client):
        """
            SKELETON
            def get_node_mappings(node_id, node_2_id=None, **kwargs):
        """
        pass
    def test_get_link_mappings(self, client):
        """
            SKELETON
            def get_link_mappings(link_id, link_2_id=None, **kwargs):
        """
        pass
    def test_get_network_mappings(self, client):
        """
            SKELETON
            def get_network_mappings(network_id, network_2_id=None, **kwargs):
        """
        pass
    def test_check_attribute_mapping_exists(self, client):
        """
            SKELETON
            def check_attribute_mapping_exists(resource_attr_id_source, resource_attr_id_target, **kwargs):
        """
        pass











class TestAttributeGroups:
    """
        Test for attribute Groups-based functionality
    """


    def test_add_attribute_group(self, client, projectmaker, attribute):
        project = projectmaker.create()

        newgroup = JSONObject({
            'project_id'  : project.id,
            'name'        : "Attribute Group %s" % (datetime.datetime.now(),),
            'description' : "A description of an attribute group",
            'layout'      : {"color": "green"},
            'exclusive'   : 'Y',
        })

        newgroup = client.add_attribute_group(newgroup)

        retrieved_new_group = client.get_attribute_group(newgroup.id)

        assert retrieved_new_group.name == newgroup.name

    def test_update_attribute_group(self, client, attributegroup):

        newname = attributegroup.name + " Updated"

        attributegroup.name = newname

        client.update_attribute_group(attributegroup)

        retrieved_new_group = client.get_attribute_group(attributegroup.id)

        assert retrieved_new_group.name == newname


    def test_delete_attribute_group(self, client, attributegroup):

        client.delete_attribute_group(attributegroup.id)

        with pytest.raises(hb.HydraError):
            client.get_attribute_group(attributegroup.id)

    def test_basic_add_attribute_group_items(self, client, projectmaker, network_with_data, attributegroupmaker):
        project = projectmaker.create()

        #convenience naming
        network = network_with_data

        #Create two groups -- attributes which are associated with a network,
        #and everything else.
        group_1 = attributegroupmaker.create(project.id, "Network Attributes")
        group_2 = attributegroupmaker.create(project.id, "Node Attributes")

        network_attributes = []
        for netattr in network.attributes:
            network_attributes.append(JSONObject(
                {'attr_id' : netattr.attr_id,
                 'network_id' : network.id,
                 'group_id' : group_1.id
                 }))

        node_attr_tracker = []
        node_attributes = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject(
                        {'attr_id' : node_attr.attr_id,
                         'network_id' : network.id,
                         'group_id' : group_2.id
                         }))
                    node_attr_tracker.append(node_attr.attr_id)


        client.add_attribute_group_items(network_attributes)

        client.add_attribute_group_items(node_attributes)

        all_items_in_network = client.get_network_attributegroup_items(network.id)


        assert len(all_items_in_network) == len(network_attributes)+len(node_attributes)

    def test_exclusive_add_attribute_group_items(self, client, projectmaker, network_with_data, attributegroupmaker):
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
            network_attributes.append(JSONObject(
                {'attr_id' : netattr.attr_id,
                 'network_id' : network.id,
                 'group_id' : group_1.id}))
            #Put these attributes into both groups. THis should fail, as group 1
            #is exclusive, and already has these attributes
            node_attributes.append(JSONObject(
                {'attr_id' : netattr.attr_id,
                 'network_id' : network.id,
                 'group_id' : group_2.id}))


        node_attr_tracker = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject(
                        {'attr_id' : node_attr.attr_id,
                         'network_id' : network.id,
                         'group_id' : group_2.id}))
                    node_attr_tracker.append(node_attr.attr_id)


        log.info("Adding items to group 1 (network attributes)")
        client.add_attribute_group_items(network_attributes)

        #add a group with attributes that are already in an exclusive group
        with pytest.raises(hb.HydraError):
            log.info("Adding items to group 2 (node attributes, plus network attributes)")
            client.add_attribute_group_items(node_attributes)

    def test_reverse_exclusive_add_attribute_group_items(self, client, projectmaker, network_with_data, attributegroupmaker):
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
            network_attributes.append(JSONObject(
                {'attr_id' : netattr.attr_id,
                 'network_id' : network.id,
                 'group_id' : group_1.id}))
            #Put these attributes into both groups. THis should fail, as group 1
            #is exclusive, and already has these attributes
            node_attributes.append(JSONObject(
                {'attr_id' : netattr.attr_id,
                 'network_id' : network.id,
                 'group_id' : group_2.id}))


        node_attr_tracker = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject(
                        {'attr_id' : node_attr.attr_id,
                         'network_id' : network.id,
                         'group_id' : group_2.id}))
                    node_attr_tracker.append(node_attr.attr_id)


        log.info("Adding items to group 2 (node attributes, plus network attributes)")
        client.add_attribute_group_items(node_attributes)

        #add attributes to an exclusive group that are already in another group
        with pytest.raises(hb.HydraError):
            log.info("Adding items to group 1 (network attributes)")
            client.add_attribute_group_items(network_attributes)

    def test_delete_attribute_group_items(self, client, projectmaker, network_with_data, attributegroupmaker):
        project = projectmaker.create()

        #convenience naming
        network = network_with_data

        #Create two groups -- attributes which are associated with a network,
        #and everything else.
        group_1 = attributegroupmaker.create(project.id, "Network Attributes")
        group_2 = attributegroupmaker.create(project.id, "Node Attributes")

        network_attributes = []
        for netattr in network.attributes:
            network_attributes.append(JSONObject(
                {'attr_id' : netattr.attr_id,
                 'network_id' : network.id,
                 'group_id' : group_1.id}))

        node_attr_tracker = []
        node_attributes   = []
        for node in network.nodes:
            for node_attr in node.attributes:
                if node_attr.attr_id not in node_attr_tracker:
                    node_attributes.append(JSONObject(
                        {'attr_id' : node_attr.attr_id,
                         'network_id' : network.id,
                         'group_id' : group_2.id}))
                    node_attr_tracker.append(node_attr.attr_id)


        client.add_attribute_group_items(network_attributes)

        client.add_attribute_group_items(node_attributes)

        all_items_in_network = client.get_network_attributegroup_items(network.id)

        assert len(all_items_in_network) == len(network_attributes)+len(node_attributes)

        #Now remove all the node attributes
        client.delete_attribute_group_items(node_attributes)

        all_items_in_network = client.get_network_attributegroup_items(network.id)

        assert len(all_items_in_network) == len(network_attributes)

    def test_get_attribute_group(self, client):
        """
            SKELETON
            def get_attribute_group(group_id, **kwargs):
        """
        pass
    def test_get_network_attributegroup_items(self, client):
        """
            SKELETON
            def get_network_attributegroup_items(network_id, **kwargs):
        """
        pass
    def test_get_group_attributegroup_items(self, client):
        """
            SKELETON
            def get_group_attributegroup_items(network_id, group_id, **kwargs):
        """
        pass
    def test_get_attribute_item_groups(self, client):
        """
            SKELETON
            def get_attribute_item_groups(network_id, attr_id, **kwargs):
        """
        pass
    def test_add_attribute_group_items(self, client):
        """
            SKELETON
            def add_attribute_group_items(attributegroupitems, **kwargs):
        """
        pass
