# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import server
import logging
log = logging.getLogger(__name__)

class AttributeTest(server.SoapServerTest):
    """
        Test for attribute-based functionality
    """
    def test_get_network_attrs(self):
        net = self.create_network_with_data()

        net_attrs = self.client.service.get_network_attributes(net.id)
        net_type_attrs = self.client.service.get_network_attributes(net.id, net.types.TypeSummary[0].id)

        assert len(net_attrs.ResourceAttr) == 2
        assert len(net_type_attrs.ResourceAttr) == 1

    def test_add_attribute(self):
        name = "Test add Attr"
        dimension = "Volumetric flow rate"
        attr = self.client.service.get_attribute(name, "Volumetric flow rate")
        if attr is None:
            attr = {'name'  : name,
                    'dimen' : dimension,
                    'description' : "Attribute description",
                   }
            attr = self.client.service.add_attribute(attr)

            assert attr.description == "Attribute description"
            
        return attr

    def test_add_attributes(self):

        name1 = "Multi-added Attr 1"
        name2 = "Multi-added Attr 2"
        dimension = "Volumetric flow rate"
        attrs = self.client.factory.create('hyd:AttrArray')
        attr1 = {'name'  : name1,
                'dimen' : dimension,
                'description' : "Attribute 1 from a test of adding multiple attributes",
                }
        attrs.Attr.append(attr1)
        attr2 = {'name' : name2,
                 'dimen' : dimension,
                'description' : "Attribute 2 from a test of adding multiple attributes",
                }
        attrs.Attr.append(attr2)

        existing_attrs = self.client.service.get_attributes(attrs)
        for a in existing_attrs.Attr:
            if a is not None:
                attrs = existing_attrs
                break
        else:
            attrs = self.client.service.add_attributes(attrs)
            assert len(attrs.Attr) == 2
            for a in attrs.Attr:
                assert a.id is not None

        assert attrs.Attr[0].description ==  "Attribute 1 from a test of adding multiple attributes"
        assert attrs.Attr[1].description ==  "Attribute 2 from a test of adding multiple attributes"

        return attrs.Attr

    def test_get_all_attributes(self):
        self.test_add_attributes()

        all_attributes = self.client.service.get_all_attributes()
        attribute_names = []
        for a in all_attributes.Attr:
            attribute_names.append(a.name)

        assert "Multi-added Attr 1" in attribute_names
        assert "Multi-added Attr 2" in attribute_names

    def test_get_attribute_by_id(self):
        existing_attr = self.test_add_attribute()
        retrieved_attr = self.client.service.get_attribute_by_id(existing_attr.id)
        assert existing_attr.name == retrieved_attr.name
        assert existing_attr.dimen == retrieved_attr.dimen
        assert existing_attr.description == retrieved_attr.description

    def test_get_attribute(self):
        existing_attr = self.test_add_attribute()
        retrieved_attr = self.client.service.get_attribute(existing_attr.name,
                                                           existing_attr.dimen)
        assert existing_attr.id == retrieved_attr.id
        assert existing_attr.description == retrieved_attr.description

    def test_get_attributes(self):
        existing_attrs = self.test_add_attributes()
        name1     = existing_attrs[0].name
        name2     = existing_attrs[1].name
        dimension = existing_attrs[0].dimen 
        attrs = self.client.factory.create('hyd:AttrArray')
        attr1 = {'name'  : name1,
                'dimen' : dimension,
                }
        attrs.Attr.append(attr1)
        attr2 = {'name' : name2,
                 'dimen' : dimension,
                }
        attrs.Attr.append(attr2)

        attrs = self.client.service.get_attributes(attrs)
        assert attrs.Attr[0].id == existing_attrs[0].id
        assert attrs.Attr[1].id == existing_attrs[1].id
        assert attrs.Attr[1].description == existing_attrs[1].description

#    def test_delete_attribute(self):
#        attr = self.create_attr("attr_to_delete", "Volume")
#        self.client.service.delete_attribute(attr.id)
#        self.assertRaises(WebFault, self.client.service.get_attribute_by_id, attr.id)

    def test_add_network_attribute(self):
        network = self.create_network_with_data()
        new_attr = self.create_attr("new network attr", dimension=None)
        new_ra = self.client.service.add_network_attribute(network.id, new_attr.id, 'Y')
        updated_network = self.client.service.get_network(network.id)
        network_attr_ids = []
        for ra in updated_network.attributes.ResourceAttr:
            network_attr_ids.append(ra.attr_id)
        assert new_attr.id in network_attr_ids

    def test_add_network_attrs_from_type(self):
        network = self.create_network_with_data()
        type_id = network.types.TypeSummary[0].id
        before_net_attrs = []
        for ra in network.attributes.ResourceAttr:
            before_net_attrs.append(ra.attr_id)
            
        self.client.service.add_network_attrs_from_type(type_id, network.id) 
        
        updated_network = self.client.service.get_network(network.id)
        after_net_attrs = []
        for ra in updated_network.attributes.ResourceAttr:
            after_net_attrs.append(ra.attr_id)

        t = self.client.service.get_templatetype(type_id) 

        assert len(after_net_attrs) == len(before_net_attrs) + 1

    def test_add_node_attribute(self):
        network = self.create_network_with_data()
        node = network.nodes.Node[0]
        new_attr = self.create_attr("new node attr", dimension=None)
        self.client.service.add_node_attribute(node.id, new_attr.id, 'Y')
        node_attributes = self.client.service.get_node_attributes(node.id)
        network_attr_ids = []
        for ra in node_attributes.ResourceAttr:
            network_attr_ids.append(ra.attr_id)
        assert new_attr.id in network_attr_ids

    def test_add_link_attribute(self):
        network = self.create_network_with_data()
        link = network.links.Link[0]
        new_attr = self.create_attr("new link attr", dimension=None)
        self.client.service.add_link_attribute(link.id, new_attr.id, 'Y')
        link_attributes = self.client.service.get_link_attributes(link.id)
        network_attr_ids = []
        for ra in link_attributes.ResourceAttr:
            network_attr_ids.append(ra.attr_id)
        assert new_attr.id in network_attr_ids

    def test_add_group_attribute(self):
        network = self.create_network_with_data()
        group = network.resourcegroups.ResourceGroup[0]
        new_attr = self.create_attr("new group attr", dimension=None)
        self.client.service.add_group_attribute(group.id, new_attr.id, 'Y')
        group_attrs = self.client.service.get_group_attributes(group.id)
        network_attr_ids = []
        for ra in group_attrs.ResourceAttr:
            network_attr_ids.append(ra.attr_id)
        assert new_attr.id in network_attr_ids

class AttributeMapTest(server.SoapServerTest):
    def test_set_attribute_mapping(self):
        net1 = self.create_network_with_data()
        net2 = self.create_network_with_data()
        net3 = self.create_network_with_data()

        s1 = net1.scenarios.Scenario[0]
        s2 = net2.scenarios.Scenario[0]

        node_1 = net1.nodes.Node[0]
        node_2 = net2.nodes.Node[0]
        node_3 = net3.nodes.Node[0]

        attr_1 = node_1.attributes.ResourceAttr[0]
        attr_2 = node_2.attributes.ResourceAttr[1]
        attr_3 = node_3.attributes.ResourceAttr[2]

        rs_to_update_from = None
        for rs in s1.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == attr_1.id:
                rs_to_update_from = rs


        rs_to_change = None
        for rs in s2.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == attr_2.id:
                rs_to_change = rs

        self.client.service.set_attribute_mapping(attr_1.id, attr_2.id)
        self.client.service.set_attribute_mapping(attr_1.id, attr_3.id)
        
        all_mappings_1 = self.client.service.get_mappings_in_network(net1.id)
        all_mappings_2 = self.client.service.get_mappings_in_network(net2.id, net2.id)
        #print all_mappings_1 
        #print all_mappings_2 
        assert len(all_mappings_1[0]) == 2
        assert len(all_mappings_2[0]) == 1

        node_mappings_1 = self.client.service.get_node_mappings(node_1.id)
        node_mappings_2 = self.client.service.get_node_mappings(node_1.id, node_2.id)
        #print "*"*100
        #print node_mappings_1 
        #print node_mappings_2 
        assert len(node_mappings_1[0]) == 2
        assert len(node_mappings_2[0]) == 1
       
        updated_rs = self.client.service.update_value_from_mapping(attr_1.id, attr_2.id, s1.id, s2.id)
    
        assert str(updated_rs.value) == str(rs_to_update_from.value)
       
        log.info("Deleting %s -> %s", attr_1.id, attr_2.id)
        self.client.service.delete_attribute_mapping(attr_1.id, attr_2.id)
        all_mappings_1 = self.client.service.get_mappings_in_network(net1.id)
        assert len(all_mappings_1[0]) == 1
        self.client.service.delete_mappings_in_network(net1.id)
        all_mappings_1 = self.client.service.get_mappings_in_network(net1.id)
        assert len(all_mappings_1) == 0


if __name__ == '__main__':
    server.run()
