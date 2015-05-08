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
import datetime
from lxml import etree
from HydraLib import config
import logging
from suds import WebFault
log = logging.getLogger(__name__)

class TemplatesTest(server.SoapServerTest):
    """
        Test for templates
    """
    def set_template(self, template):
        if template is None:
            self.template = self.test_add_template()
        else:
            self.template = template

    def get_template(self):
        if hasattr(self, 'template'):
            try:

                self.client.service.get_template(self.template.id)
                return self.template
            except:
                self.template = self.test_add_template()
        else:
            self.template = self.test_add_template()
        return self.template

    def test_add_xml(self):
        template_file = open('template.xml', 'r')

        file_contents = template_file.read()

        new_tmpl = self.client.service.upload_template_xml(file_contents)

        assert new_tmpl is not None, "Adding template from XML was not successful!"

        assert len(new_tmpl.types.TemplateType) == 2

        for ta in new_tmpl.types.TemplateType[0].typeattrs.TypeAttr:
            assert ta.data_type == 'scalar'

    def test_add_template(self):

        link_attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        link_attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        node_attr_1 = self.create_attr("node_attr_1", dimension='Volume')
        node_attr_2 = self.create_attr("node_attr_2", dimension='Speed')
        net_attr_1 = self.create_attr("net_attr_2", dimension='Speed')

        template = self.client.factory.create('hyd:Template')
        template.name = 'Test template @ %s'%datetime.datetime.now()
        
        layout = self.client.factory.create("xs:anyType")
        layout.groups = '<groups>...</groups>'

        template.layout = layout 

        types = self.client.factory.create('hyd:TemplateTypeArray')
        #**********************
        #type 1           #
        #**********************
        type1 = self.client.factory.create('hyd:TemplateType')
        type1.name = "Node type"
        type1.alias = "Node type alias"
        type1.resource_type = 'NODE'

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = node_attr_1.id
        tattr_1.data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1}
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = node_attr_2.id
        tattr_2.data_restriction = {'INCREASING': None}
        tattrs.TypeAttr.append(tattr_2)

        type1.typeattrs = tattrs

        types.TemplateType.append(type1)
        #**********************
        #type 2           #
        #**********************
        type2 = self.client.factory.create('hyd:TemplateType')
        type2.name = "Link type"
        type2.alias = "Link type alias"
        type2.resource_type = 'LINK'

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = link_attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = link_attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        type2.typeattrs = tattrs

        types.TemplateType.append(type2)

        type3 = self.client.factory.create('hyd:TemplateType')
        type3.name = "Network Type"
        type3.alias = "Network Type alias"
        type3.resource_type = 'NETWORK'
        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = net_attr_1.id
        tattrs.TypeAttr.append(tattr_1)
        types.TemplateType.append(type3)
      #  type3.typeattrs = tattrs

        template.types = types

        new_template = self.client.service.add_template(template)

        assert new_template.name == template.name, "Names are not the same!"
        assert str(new_template.layout) == str(template.layout), "Layouts are not the same!"
        assert new_template.id is not None, "New Template has no ID!"
        assert new_template.id > 0, "New Template has incorrect ID!"

        assert len(new_template.types) == 1, "Resource types did not add correctly"
        for t in new_template.types.TemplateType[0].typeattrs.TypeAttr:
            assert t.attr_id in (node_attr_1.id, node_attr_2.id);
            "Node types were not added correctly!"

        for t in new_template.types.TemplateType[1].typeattrs.TypeAttr:
            assert t.attr_id in (link_attr_1.id, link_attr_2.id);
            "Node types were not added correctly!"

        return new_template

    def test_update_template(self):


        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        template = self.client.factory.create('hyd:Template')

        template.name = 'Test Template @ %s'%datetime.datetime.now()

        types = self.client.factory.create('hyd:TemplateTypeArray')

        type_1 = self.client.factory.create('hyd:TemplateType')
        type_1.name = "Node type 2"
        type_1.alias = "Node type 2 alias"
        type_1.resource_type = 'NODE'

        type_2 = self.client.factory.create('hyd:TemplateType')
        type_2.name = "Link type 2"
        type_2.alias = "Link type 2 alias"
        type_2.resource_type = 'LINK'

        tattrs_1 = self.client.factory.create('hyd:TypeAttrArray')
        tattrs_2 = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattr_1.dimension = 'Pressure'
        tattr_1.unit      = 'bar'
        tattrs_1.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattr_2.dimension = 'Speed'
        tattr_2.unit = 'mph'
        tattrs_2.TypeAttr.append(tattr_2)

        types.TemplateType.append(type_1)
        types.TemplateType.append(type_2)

        type_1.typeattrs = tattrs_1
        type_2.typeattrs = tattrs_2

        template.types = types

        new_template = self.client.service.add_template(template)

        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.id is not None, "New Template has no ID!"
        assert new_template.id > 0, "New Template has incorrect ID!"

        assert len(new_template.types[0]) == 2, "Resource types did not add correctly"
        assert len(new_template.types[0][0].typeattrs[0]) == 1, "Resource type attrs did not add correctly"
        assert new_template.types[0][0].typeattrs[0][0].dimension == 'Pressure'
        assert new_template.types[0][0].typeattrs[0][0].unit == 'bar'

        #update the name of one of the types
        new_template.types[0][0].name = "Test type 3"
        updated_type_id = new_template.types[0][0].id

        #add an template attr to one of the types
        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        new_template.types[0][0].typeattrs.TypeAttr.append(tattr_3)

        updated_template = self.client.service.update_template(new_template)

        assert updated_template.name == template.name, "Names are not the same!"

        updated_type = None
        for tmpltype in new_template.types.TemplateType:
            if tmpltype.id == updated_type_id:
                updated_type = tmpltype
                break

        assert updated_type.name == "Test type 3", "Resource types did not update correctly"

        assert len(updated_type.typeattrs[0]) == 2, "Resource type template attr did not update correctly"


        updated_template.types[0][0].typeattrs[0][0].dimension = 'Volume'
        updated_template.types[0][0].typeattrs[0][0].unit = 'm^3'
        self.assertRaises(WebFault, self.client.service.update_template, updated_template)

        attr_name = updated_template.types[0][0].typeattrs[0][0].attr_name
        updated_template.types[0][0].typeattrs[0][0].attr_id = None
        updated_template.types[0][0].typeattrs[0][0].dimension = 'Volume'
        updated_template.types[0][0].typeattrs[0][0].unit = 'm^3'
        updated_attr_template = self.client.service.update_template(updated_template)
        newattr = self.client.service.get_attribute(attr_name, 'Volume')
        assert updated_attr_template.types[0][0].typeattrs[0][2].attr_id == newattr.id

    def test_delete_template(self):
        
        network = self.create_network_with_data()

        new_template = self.test_add_template()

        retrieved_template = self.client.service.get_template(new_template.id)
        assert retrieved_template is not None


        self.client.service.apply_template_to_network(retrieved_template.id,
                                                             network.id)
        
        updated_network = self.client.service.get_network(network.id)
        assert len(updated_network.types.TypeSummary) == 2
        
        expected_net_type = None
        for t in new_template.types.TemplateType:
            if t.resource_type == 'NETWORK':
                expected_net_type = t.id

        network_type = updated_network.types.TypeSummary[1].id

        assert expected_net_type == network_type

        self.client.service.delete_template(new_template.id)
    
        self.assertRaises(WebFault, self.client.service.get_template, new_template.id)
        
        network_deleted_tmpl = self.client.service.get_network(network.id)
        
        assert len(network_deleted_tmpl.types.TypeSummary) == 1

    def test_add_type(self):
        
        template = self.get_template()

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = "%s alias" % templatetype.name
        templatetype.resource_type = 'LINK'
        templatetype.template_id = template.id
        layout = self.client.factory.create("xs:anyType")
        layout.color = 'red'
        layout.shapefile = 'blah.shp'

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        tattrs.TypeAttr.append(tattr_3)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        assert new_type.name == templatetype.name, "Names are not the same!"
        assert new_type.alias == templatetype.alias, "Aliases are not the same!"
        assert repr(new_type.layout) == repr(templatetype.layout), "Layouts are not the same!"
        assert new_type.id is not None, "New type has no ID!"
        assert new_type.id > 0, "New type has incorrect ID!"

        assert len(new_type.typeattrs[0]) == 3, "Resource type attrs did not add correctly"

        return new_type 

    def test_update_type(self):
       

        template = self.get_template()

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s" % (datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = self.get_template().id
        templatetype.resource_type = 'NODE'
        templatetype.template_id = template.id

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        assert new_type.name == templatetype.name, "Names are not the same!"
        assert new_type.alias == templatetype.alias, "Aliases are not the same!"
        assert new_type.id is not templatetype, "New type has no ID!"
        assert new_type.id > 0, "New type has incorrect ID!"

        assert len(new_type.typeattrs[0]) == 2, "Resource type attrs did not add correctly"

        new_type.name = "Updated type name @ %s"%(datetime.datetime.now())
        new_type.alias = templatetype.name + " alias"
        new_type.resource_type = 'NODE'

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        tattrs.TypeAttr.append(tattr_3)

        new_type.typeattrs = tattrs

        updated_type = self.client.service.update_templatetype(new_type)

        assert new_type.name == updated_type.name, "Names are not the same!"
        assert new_type.alias == updated_type.alias, "Aliases are not the same!"
        assert new_type.id == updated_type.id, "type ids to not match!"
        assert new_type.id > 0, "New type has incorrect ID!"

        assert len(updated_type.typeattrs[0]) == 3, "Template type attrs did not update correctly"


    def test_delete_type(self):
        new_template = self.test_add_template()

        retrieved_template = self.client.service.get_template(new_template.id)
        assert retrieved_template is not None

        templatetype = new_template.types.TemplateType[0]
        self.client.service.delete_templatetype(templatetype.id)
    
        updated_template = self.client.service.get_template(new_template.id)

        for tmpltype in updated_template.types.TemplateType:
            assert tmpltype.id != templatetype.id



    def test_get_type(self):
        new_type = self.get_template().types.TemplateType[0]
        new_type = self.client.service.get_templatetype(new_type.id)
        assert new_type is not None, "Resource type attrs not retrived by ID!"

    def test_get_type_by_name(self):
        new_type = self.get_template().types.TemplateType[0]
        new_type = self.client.service.get_templatetype_by_name(new_type.template_id, new_type.name)
        assert new_type is not None, "Resource type attrs not retrived by name!"


    def test_add_typeattr(self):


        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = self.get_template().id
        templatetype.resource_type = 'NODE'

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        tattr_3.type_id = new_type.id

        updated_type = self.client.service.add_typeattr(tattr_3)

        assert len(updated_type.typeattrs[0]) == 3, "Resource type attr did not add correctly"


    def test_delete_typeattr(self):
        
        template = self.test_add_template()

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.resource_type = 'NODE'
        templatetype.template_id = template.id

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        tattr_2.type_id = new_type.id

        self.client.service.delete_typeattr(tattr_2)
        
        updated_type = self.client.service.get_templatetype(new_type.id)

        assert len(updated_type.typeattrs[0]) == 1, "Resource type attr did not add correctly"

    def test_get_templates(self):
        self.get_template()
        templates = self.client.service.get_templates()
        for t in templates.Template:
            for typ in t.types.TemplateType:
                assert typ.resource_type is not None
        assert len(templates) > 0, "Templates were not retrieved!"


    def test_get_template(self):
        template = self.get_template()
        new_template = self.client.service.get_template(template.id)

        assert new_template.name == template.name, "Names are not the same! Retrieval by ID did not work!"



    def test_get_template_by_name_good(self):
        template = self.get_template()
        new_template = self.client.service.get_template_by_name(template.name)

        assert new_template.name == template.name, "Names are not the same! Retrieval by name did not work!"

    def test_get_template_by_name_bad(self):
        new_template = self.client.service.get_template_by_name("Not a template!")

        assert new_template is None

    def test_add_resource_type(self):

        template = self.get_template()
        types = template.types.TemplateType
        type_name = types[0].name
        type_id   = types[0].id

        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            type_summary = self.client.factory.create('hyd:TypeSummary')
            type_summary.template_id = template.id
            type_summary.template_name = template.name
            type_summary.id = type_id
            type_summary.name = type_name

            node.types.TypeSummary.append(type_summary)

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        net_summary = self.client.service.add_network(network)
        new_net = self.client.service.get_network(net_summary.id)

        for node in new_net.nodes.Node:
            assert node.types is not None and node.types.TypeSummary[0].name == "Node type"; "type was not added correctly!"


    def test_find_matching_resource_types(self):

        network = self.create_network_with_data()

        node_to_check = network.nodes.Node[0]
        matching_types = self.client.service.get_matching_resource_types('NODE', 
                                                                         node_to_check.id)

        assert len(matching_types) > 0, "No types returned!"


        matching_type_ids = []
        for tmpltype in matching_types.TypeSummary:
            matching_type_ids.append(tmpltype.id)

        assert node_to_check.types.TypeSummary[0].id in matching_type_ids, "TemplateType ID not found in matching types!"

    def test_assign_type_to_resource(self):
        network = self.create_network_with_data()
        template = self.get_template()
        templatetype = template.types.TemplateType[0]

        node_to_assign = network.nodes.Node[0]

        result = self.client.service.assign_type_to_resource(templatetype.id,
                                                             'NODE',
                                                             node_to_assign.id)

        node = self.client.service.get_node(node_to_assign.id)


        assert node.types is not None, \
            'Assigning type did not work properly.'

        assert str(result) in [str(x) for x in node.types.TypeSummary]

    def test_remove_type_from_resource(self):
        network = self.create_network_with_data()
        template = self.get_template()
        templatetype = template.types.TemplateType[0]

        node_to_assign = network.nodes.Node[0]

        result = self.client.service.assign_type_to_resource(templatetype.id,
                                                             'NODE',
                                                             node_to_assign.id)

        
        node = self.client.service.get_node(node_to_assign.id)


        assert node.types is not None, \
            'Assigning type did not work properly.'

        assert str(result) in [str(x) for x in node.types.TypeSummary]

       
        result = self.client.service.remove_type_from_resource(templatetype.id,
                                                            'NODE',
                                                            node_to_assign.id) 
        assert result == 'OK'
        
        node = self.client.service.get_node(node_to_assign.id)
 
        assert node.types.TypeSummary is None or str(result) not in [str(x) for x in node.types.TypeSummary] 

    def test_create_template_from_network(self):
        network = self.create_network_with_data()

        net_template = self.client.service.get_network_as_xml_template(network.id)

        assert net_template is not None

        template_xsd_path = config.get('templates', 'template_xsd_path')
        xmlschema_doc = etree.parse(template_xsd_path)

        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_tree = etree.fromstring(net_template)

        xmlschema.assertValid(xml_tree)

    def test_apply_template_to_network(self):
        net_to_update = self.create_network_with_data()
        template = self.get_template()
       
        #Test the links as it's easier
        empty_links = []
        for l in net_to_update.links.Link:
            if l.types is None:
                empty_links.append(l.id)

        #Add the resource attributes to the links, so we can guarantee
        #that these links will match those in the template.
        for t in template.types.TemplateType:
            if t.resource_type == 'LINK':
                link_type = t
                break

        link_ra_1 = dict(
            attr_id=link_type.typeattrs.TypeAttr[0].attr_id
        )
        link_ra_2 = dict(
            attr_id=link_type.typeattrs.TypeAttr[1].attr_id
        )
        for link in net_to_update.links.Link:
            if link.types is None:
                link.attributes.ResourceAttr.append(link_ra_1)
                link.attributes.ResourceAttr.append(link_ra_2)

        network = self.client.service.update_network(net_to_update)

        for n in network.nodes.Node:
            assert len(n.types.TypeSummary) == 1
            assert n.types.TypeSummary[0].name == 'Default Node'

        self.client.service.apply_template_to_network(template.id,
                                                             network.id)

        network = self.client.service.get_network(network.id)
       
        assert len(network.types.TypeSummary) == 2
        assert network.types.TypeSummary[1].name == 'Network Type'
        for l in network.links.Link:
            if l.id in empty_links:
                assert l.types is not None
                assert len(l.types.TypeSummary) == 1
                assert l.types.TypeSummary[0].name == 'Link type'

        #THe assignment of the template hasn't affected the nodes
        #as they do not have the appropriate attributes.
        for n in network.nodes.Node:
            assert len(n.types.TypeSummary) == 1
            assert n.types.TypeSummary[0].template_name == 'Default Template'

    def test_apply_template_to_network_twice(self):
        net_to_update = self.create_network_with_data()
        template = self.get_template()
       
        #Test the links as it's easier
        empty_links = []
        for l in net_to_update.links.Link:
            if l.types is None:
                empty_links.append(l.id)

        #Add the resource attributes to the links, so we can guarantee
        #that these links will match those in the template.
        for t in template.types.TemplateType:
            if t.resource_type == 'LINK':
                link_type = t
                break

        link_ra_1 = dict(
            attr_id=link_type.typeattrs.TypeAttr[0].attr_id
        )
        link_ra_2 = dict(
            attr_id=link_type.typeattrs.TypeAttr[1].attr_id
        )
        for link in net_to_update.links.Link:
            if link.types is None:
                link.attributes.ResourceAttr.append(link_ra_1)
                link.attributes.ResourceAttr.append(link_ra_2)

        network = self.client.service.update_network(net_to_update)

        for n in network.nodes.Node:
            assert len(n.types.TypeSummary) == 1
            assert n.types.TypeSummary[0].name == 'Default Node'

        self.client.service.apply_template_to_network(template.id,
                                                             network.id)
        self.client.service.apply_template_to_network(template.id,
                                                             network.id)

        network = self.client.service.get_network(network.id)
       
        assert len(network.types.TypeSummary) == 2
        assert network.types.TypeSummary[1].name == 'Network Type'
        for l in network.links.Link:
            if l.id in empty_links:
                assert l.types is not None
                assert len(n.types.TypeSummary) == 1
                assert l.types.TypeSummary[0].name == 'Link type'

        for n in network.nodes.Node:
            assert len(n.types.TypeSummary) == 1
            assert n.types.TypeSummary[0].template_name == 'Default Template'

    def test_remove_template_from_network(self):
        network = self.create_network_with_data()
        template_id = network.types.TypeSummary[0].template_id
       
        #Test the links as it's easier
        empty_links = []
        for l in network.links.Link:
            if l.types is None:
                empty_links.append(l.id)

        for n in network.nodes.Node:
            assert len(n.types.TypeSummary) == 1
            assert n.types.TypeSummary[0].name == "Default Node"

        self.client.service.apply_template_to_network(template_id,
                                                             network.id)

        self.client.service.remove_template_from_network(network.id, template_id, 'N')

        network_2 = self.client.service.get_network(network.id)

        assert network_2.types is None
        for l in network_2.links.Link:
            if l.id in empty_links:
                assert l.types is None

        for n in network_2.nodes.Node:
            assert n.types is None

    def test_remove_template_and_attributes_from_network(self):
        network = self.create_network_with_data()
        template = self.get_template()
       
        #Test the links as it's easier
        empty_links = []
        for l in network.links.Link:
            if l.types is None:
                empty_links.append(l.id)

        for n in network.nodes.Node:
            assert len(n.types.TypeSummary) == 1
            assert n.types.TypeSummary[0].name == 'Default Node'

        self.client.service.apply_template_to_network(template.id,
                                                             network.id)

        self.client.service.remove_template_from_network(network.id, template.id, 'Y')

        network_2 = self.client.service.get_network(network.id)

        assert len(network_2.types) == 1
        
        link_attrs = []
        for tt in template['types'].TemplateType:
            if tt['resource_type'] != 'LINK':
                continue
            for ta in tt['typeattrs'].TypeAttr:
                attr_id = ta['attr_id']
                if attr_id not in link_attrs:
                    link_attrs.append(attr_id)
                link_attrs.append(ta['attr_id'])
        for l in network_2.links.Link:
            if l.id in empty_links:
                assert l.types is None
            if l.attributes is not None:
                for a in l.attributes.ResourceAttr:
                    assert a.attr_id not in link_attrs

        for tt in template['types'].TemplateType:
            if tt['resource_type'] != 'NODE':
                continue
            for ta in tt['typeattrs'].TypeAttr:
                attr_id = ta['attr_id']
                if attr_id not in link_attrs:
                    link_attrs.append(attr_id)
                link_attrs.append(ta['attr_id'])
        for n in network_2.nodes.Node:
            assert len(n.types.TypeSummary) == 1
            if n.attributes is not None:
                for a in n.attributes.ResourceAttr:
                    assert a.attr_id not in link_attrs

    def test_validate_data(self):
        network = self.create_network_with_data()
        
        scenario = network.scenarios.Scenario[0]
        rs_ids = [rs.resource_attr_id for rs in scenario.resourcescenarios.ResourceScenario]

        for n in network.nodes.Node:
            node_type = self.client.service.get_templatetype(n.types.TypeSummary[0].id)
            for ra in n.attributes.ResourceAttr:
                for attr in node_type.typeattrs.TypeAttr:
                    if ra.attr_id == attr.attr_id and ra.id in rs_ids and attr.data_restriction is not None:
                #        logging.info("Validating RA %s in scenario %s", ra.id, scenario.id)
                        self.assertRaises(WebFault, self.client.service.validate_attr, ra.id, scenario.id, None)

    def test_validate_network(self):
        network = self.create_network_with_data()
        scenario = network.scenarios.Scenario[0]
        template = network.nodes.Node[0].types.TypeSummary[0]
        #Validate the network without data: should pass as the network is built
        #based on the template in these unit tests
        errors1 = self.client.service.validate_network(network['id'],
                                                       template['template_id'])
        #The network should have an error, saying that the template has net_attr_c,
        #but the network does not
        assert len(errors1) == 1
        assert errors1.string[0].find('net_attr_c')  > 0

        #Validate the network with data. Should fail as one of the attributes (node_attr_3)
        #is specified as being a 'Cost' in the template but 'Speed' is the dimension
        #of the dataset used. In addition, node_attr_1 specified a unit of 'm^3'
        #whereas the timeseries in the data is in 'cm^3', so each will fail on unit
        #mismatch also.
        errors2 = self.client.service.validate_network(network['id'],
                                                       template['template_id'],
                                                       scenario['id'])

        assert len(errors2) > 0
        #every node should have an associated error, plus the network error from above
        assert len(errors2.string) == len(network['nodes'].Node * 2)+1
        for err in errors2.string[1:]:
            try:
                assert err.startswith("Unit mismatch")
            except AssertionError:
                assert err.startswith("Dimension mismatch")

    def test_type_compatibility(self): 
        """
            Check function that thests whether two types are compatible -- the
            overlapping attributes are specified with the same unit.
            Change the unit associated with an attribute for types in two idencical
            templates, and test. There should be 1 error returned.
            THen test comparison of identical types. No errors should return.
        """
        template_1 = self.test_add_template()
        template_2 = self.test_add_template()
        
        diff_type_1_id = None
        same_type_1_id = None
        for typ in template_1.types.TemplateType:
            if typ.typeattrs:
                for ta in typ.typeattrs.TypeAttr:
                    if ta.attr_name == 'node_attr_1':
                        diff_type_1_id = typ.id
                        ta.unit = "m^3"
                    elif ta.attr_name == 'link_attr_1':
                        same_type_1_id = typ.id
        template_1 = self.client.service.update_template(template_1)

        diff_type_2_id = None
        same_type_2_id = None
        for typ in template_2.types.TemplateType:
            if typ.typeattrs:
                for ta in typ.typeattrs.TypeAttr:
                    if ta.attr_name == 'node_attr_1':
                        diff_type_2_id = typ.id
                        ta.unit = "cm^3"
                    elif ta.attr_name == 'link_attr_1':
                        same_type_2_id = typ.id
        
        #Before updating template 2, check compatibility of types, where T1 has 
        #a unit, but t2 does not.
        errors_diff = self.client.service.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = self.client.service.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

        #Now update T2 so that the types have conflicting units.
        template_2 = self.client.service.update_template(template_2)

        errors_diff = self.client.service.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = self.client.service.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

        for typ in template_1.types.TemplateType:
            if typ.typeattrs:
                for ta in typ.typeattrs.TypeAttr:
                    if ta.attr_name == 'node_attr_1':
                        ta.unit = None

        #Update template 1 now so that it has no unit, but template 2 does.
        template_1= self.client.service.update_template(template_1)
        errors_diff = self.client.service.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = self.client.service.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

if __name__ == '__main__':
    server.run()
