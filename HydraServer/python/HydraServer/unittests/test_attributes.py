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
                    'dimen' : dimension
                   }
            attr = self.client.service.add_attribute(attr)
            
        return attr

    def test_add_attributes(self):

        name1 = "Multi-added Attr 1"
        name2 = "Multi-added Attr 2"
        dimension = "Volumetric flow rate"
        attrs = self.client.factory.create('hyd:AttrArray')
        attr1 = {'name'  : name1,
                'dimen' : dimension
                }
        attrs.Attr.append(attr1)
        attr2 = {'name' : name2,
                 'dimen' : dimension
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

    def test_get_attribute(self):
        existing_attr = self.test_add_attribute()
        retrieved_attr = self.client.service.get_attribute(existing_attr.name,
                                                           existing_attr.dimen)
        assert existing_attr.id == retrieved_attr.id

    def test_get_attributes(self):
        existing_attrs = self.test_add_attributes()
        name1     = existing_attrs[0].name
        name2     = existing_attrs[1].name
        dimension = existing_attrs[0].dimen 
        attrs = self.client.factory.create('hyd:AttrArray')
        attr1 = {'name'  : name1,
                'dimen' : dimension
                }
        attrs.Attr.append(attr1)
        attr2 = {'name' : name2,
                 'dimen' : dimension
                }
        attrs.Attr.append(attr2)

        attrs = self.client.service.get_attributes(attrs)
        assert attrs.Attr[0].id == existing_attrs[0].id
        assert attrs.Attr[1].id == existing_attrs[1].id

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

if __name__ == '__main__':
    server.run()
