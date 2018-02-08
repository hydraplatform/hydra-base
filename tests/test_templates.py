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

import server
from lxml import etree
from hydra_base import config
from hydra_base.lib.objects import JSONObject
import hydra_base as hb
import logging
from hydra_base.exceptions import HydraError
import util
import datetime
import json
log = logging.getLogger(__name__)


class TestTemplates(server.HydraBaseTest):
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

                hb.get_template(self.template.id)
                return self.template
            except:
                self.template = self.test_add_template()
        else:
            self.template = self.test_add_template()
        return self.template

    def test_add_xml(self):
        template_file = open('template.xml', 'r')

        file_contents = template_file.read()

        new_tmpl = JSONObject(hb.import_template_xml(file_contents))

        assert new_tmpl is not None, "Adding template from XML was not successful!"

        assert len(new_tmpl.templatetypes) == 2

        for tt in new_tmpl.templatetypes:
            if tt.name == 'Reservoir':
                for ta in tt.typeattrs:
                    assert ta.data_type == 'scalar'

                assert tt.typeattrs[-1].properties is not None
                assert eval(tt.typeattrs[-1].properties)['template_property'] == "Test property from template"

        return new_tmpl

    def test_get_xml(self):
        xml_tmpl = self.test_add_xml()
        
        db_template = hb.get_template_as_xml(xml_tmpl.id)

        assert db_template is not None

        template_xsd_path = config.get('templates', 'template_xsd_path')
        xmlschema_doc = etree.parse(template_xsd_path)

        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_tree = etree.fromstring(db_template)

        xmlschema.assertValid(xml_tree)

    def test_get_dict(self):

        log.info("Loading XML template")
        #Upload the xml file initally to avoid having to manage 2 template files
        xml_tmpl = self.test_add_xml()
        
        template_dict = hb.get_template_as_dict(xml_tmpl.id)
       
        #Error that there's already a template with this name.
        self.assertRaises(HydraError, hb.import_template_dict, template_dict, allow_update=False)

        typename = template_dict['template']['templatetypes'][0]['name']

        template_dict['template']['templatetypes'][0].name = typename + "_updated"

        #Finds a template with this name and updates it to mirror this dict.
        #This includes deleting types if they're not in this dict.
        #Changing the name of a type has this effect, as a new template does not have
        #any reference to existing types in Hydra.
        log.info("Loading JSON Template template")
        updated_template = JSONObject(hb.import_template_dict(template_dict))

        log.info("Updating a type's name")
        assert updated_template['templatetypes'][-1]['name'] == typename + "_updated"

        #Now put it back to the original name so other tests will work
        log.info("Reverting the type's name")
        template_dict['template']['templatetypes'][0].name = typename
        updated_template = JSONObject(hb.import_template_dict(template_dict))

        assert updated_template['templatetypes'][-1]['name'] == typename

        log.info("Checking to ensure Template has been updated correctly...")
        #one final check to ensure that the type has been deleted
        check_template_i = hb.get_template(updated_template.id)

        assert len(check_template_i.templatetypes) == 2


    def test_add_template(self):

        link_attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        link_attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        node_attr_1 = self.create_attr("node_attr_1", dimension='Volume')
        node_attr_2 = self.create_attr("node_attr_2", dimension='Speed')
        net_attr_1 = self.create_attr("net_attr_2", dimension='Speed')

        template = JSONObject()
        template.name = 'Test template @ %s'%datetime.datetime.now()
        
        layout = {}
        layout['groups'] = '<groups>...</groups>'

        template.layout = layout 

        template.templatetypes = []
        #**********************
        #type 1           #
        #**********************
        type1 = JSONObject()
        type1.name = "Node type"
        type1.alias = "Node type alias"
        type1.resource_type = 'NODE'

        type1.typeattrs = []

        tattr_1 = JSONObject()
        tattr_1.attr_id = node_attr_1.id
        tattr_1.description = "Type attribute 1 description"
        tattr_1.properties = {'test_property': "test property add type"}  
        tattr_1.data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1}
        type1.typeattrs.append(tattr_1)

        tattr_2 = JSONObject()
        tattr_2.attr_id = node_attr_2.id
        tattr_2.description = "Type attribute 2 description"
        tattr_2.data_restriction = {'INCREASING': None}
        type1.typeattrs.append(tattr_2)

        template.templatetypes.append(type1)
        #**********************
        #type 2           #
        #**********************
        type2 = JSONObject()
        type2.name = "Link type"
        type2.alias = "Link type alias"
        type2.resource_type = 'LINK'

        type2.typeattrs = []

        tattr_1 = JSONObject()
        tattr_1.attr_id = link_attr_1.id
        type2.typeattrs.append(tattr_1)

        tattr_2 = JSONObject()
        tattr_2.attr_id = link_attr_2.id
        type2.typeattrs.append(tattr_2)

        template.templatetypes.append(type2)

        #**********************
        #type 3           #
        #**********************
        type3 = JSONObject()
        type3.name = "Network Type"
        type3.alias = "Network Type alias"
        type3.resource_type = 'NETWORK'
        
        type3.typeattrs = []

        tattr_3 = JSONObject()
        tattr_3.attr_id = net_attr_1.id
        tattr_3.data_restriction = {}
        
        type3.typeattrs.append(tattr_3)

        template.templatetypes.append(type3)

        new_template_i = hb.add_template(template)
        #TODO: HACK to load the attr
        for tt in new_template_i.templatetypes:
            for ta in tt.typeattrs:
                ta.attr

        new_template_j = JSONObject(new_template_i)

        assert new_template_j.name == template.name, "Names are not the same!"
        assert json.dumps(new_template_j.layout) == json.dumps(template.layout), "Layouts are not the same!"
        assert new_template_j.id is not None, "New Template has no ID!"
        assert new_template_j.id > 0, "New Template has incorrect ID!"

        assert len(new_template_j.templatetypes) == 3, "Resource types did not add correctly"
        for t in new_template_j.templatetypes[0].typeattrs:
            assert t.attr_id in (node_attr_1.id, node_attr_2.id);
            "Node types were not added correctly!"

        for t in new_template_j.templatetypes[1].typeattrs:
            assert t.attr_id in (link_attr_1.id, link_attr_2.id);
            "Node types were not added correctly!"

        return new_template_j 

    def test_update_template(self):


        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        template = JSONObject()

        template.name = 'Test Template @ %s'%datetime.datetime.now()

        template.templatetypes = []

        type_1 = JSONObject()
        type_1.name = "Node type 2"
        type_1.alias = "Node type 2 alias"
        type_1.resource_type = 'NODE'
        type_1.typeattrs = []

        type_2 = JSONObject()
        type_2.name = "Link type 2"
        type_2.alias = "Link type 2 alias"
        type_2.resource_type = 'LINK'
        type_2.typeattrs = []
        
        tattr_1 = JSONObject()
        tattr_1.attr_id = attr_1.id
        tattr_1.unit      = 'bar'
        tattr_1.description = "typeattr description 1"
        tattr_1.properties = {"test_property": "property value"}
        type_1.typeattrs.append(tattr_1)

        tattr_2 = JSONObject()
        tattr_2.attr_id = attr_2.id
        tattr_2.unit = 'mph'
        tattr_2.description = "typeattr description 2"
        type_2.typeattrs.append(tattr_2)

        template.templatetypes.append(type_1)
        template.templatetypes.append(type_2)

        new_template_i = hb.add_template(template)
        new_template_j = JSONObject(new_template_i)

        assert new_template_j.name == template.name, "Names are not the same!"
        assert new_template_j.id is not None, "New Template has no ID!"
        assert new_template_j.id > 0, "New Template has incorrect ID!"

        assert len(new_template_j.templatetypes) == 2, "Resource types did not add correctly"
        assert len(new_template_j.templatetypes[0].typeattrs) == 1, "Resource type attrs did not add correctly"
        assert new_template_j.templatetypes[0].typeattrs[0].unit == 'bar'

        #update the name of one of the types
        new_template_j.templatetypes[0].name = "Test type 3"
        updated_type_id = new_template_j.templatetypes[0].id

        #add an template attr to one of the types
        tattr_3 = JSONObject()
        tattr_3.attr_id = attr_3.id
        tattr_3.description = "updated typeattr description 1"
        tattr_3.properties = {"test_property_of_added_type": "property value"}
        new_template_j.templatetypes[0].typeattrs.append(tattr_3)

        updated_template_i = hb.update_template(new_template_j)
        updated_template_j = JSONObject(updated_template_i)

        assert updated_template_j.name == template.name, "Names are not the same!"

        updated_type = None
        for tmpltype in new_template_j.templatetypes:
            if tmpltype.id == updated_type_id:
                updated_type = tmpltype
                break

        assert updated_type.name == "Test type 3", "Resource types did not update correctly"

        assert len(updated_type.typeattrs) == 2, "Resource type template attr did not update correctly"

        #Test that when setting a unit on a type attr, it matches the dimension of its attr
        #In this case, setting m^3(Volume) fails as the attr has a dimension of 'Pressure'
        updated_template_j.templatetypes[0].typeattrs[0].unit = 'm^3'
        self.assertRaises(HydraError, hb.update_template, updated_template_j)

    def test_delete_template(self):
        
        network = self.create_network_with_data()

        new_template = self.test_add_template()

        retrieved_template_i = hb.get_template(new_template.id)
        assert retrieved_template_i is not None

        retrieved_template_j = JSONObject(retrieved_template_i)

        hb.apply_template_to_network(retrieved_template_j.id, network.id)
        
        updated_network = hb.get_network(network.id, user_id=self.user_id)
        assert len(updated_network.types) == 2
        
        expected_net_type = None
        for t in new_template.templatetypes:
            if t.resource_type == 'NETWORK':
                expected_net_type = t.id

        network_type = updated_network.types[1].type_id
        
        assert expected_net_type == network_type

        hb.delete_template(new_template.id)

        self.assertRaises(HydraError, hb.get_template, new_template.id)
        
        network_deleted_templatetypes = hb.get_network(network.id, user_id=self.user_id)
        
        assert len(network_deleted_templatetypes.types) == 1

    def test_add_type(self):
        
        template = self.get_template()

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = JSONObject()
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = "%s alias" % templatetype.name
        templatetype.resource_type = 'LINK'
        templatetype.template_id = template.id
        templatetype.layout = {"color": "red", "shapefile": "blah.shp"}

        templatetype.typeattrs = []

        tattr_1 = JSONObject()
        tattr_1.attr_id = attr_1.id
        tattr_1.description = "added type description 1"
        tattr_1.properties = {"add_type_test_property": "property value"}
        templatetype.typeattrs.append(tattr_1)

        tattr_2 = JSONObject()
        tattr_2.attr_id = attr_2.id
        tattr_1.description = "added type description 2"
        templatetype.typeattrs.append(tattr_2)

        tattr_3 = JSONObject()
        tattr_3.attr_id = attr_3.id
        templatetype.typeattrs.append(tattr_3)


        new_type_i = hb.add_templatetype(templatetype)
        new_type_j = JSONObject(new_type_i)

        assert new_type_j.name == templatetype.name, "Names are not the same!"
        assert new_type_j.alias == templatetype.alias, "Aliases are not the same!"
        assert new_type_j.layout == templatetype.layout, "Layouts are not the same!"
        assert new_type_j.id is not None, "New type has no ID!"
        assert new_type_j.id > 0, "New type has incorrect ID!"

        assert len(new_type_j.typeattrs) == 3, "Resource type attrs did not add correctly"

        return new_type_j 

    def test_update_type(self):
       

        template = self.get_template()

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = JSONObject()
        templatetype.name = "Test type name @ %s" % (datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = self.get_template().id
        templatetype.resource_type = 'NODE'
        templatetype.template_id = template.id


        tattr_1 = JSONObject()
        tattr_1.attr_id = attr_1.id

        tattr_2 = JSONObject()
        tattr_2.attr_id = attr_2.id

        templatetype.typeattrs = [tattr_1, tattr_2]

        new_type_i = hb.add_templatetype(templatetype)
        new_type_j = JSONObject(new_type_i)

        assert new_type_j.name == templatetype.name, "Names are not the same!"
        assert new_type_j.alias == templatetype.alias, "Aliases are not the same!"
        assert new_type_j.id is not templatetype, "New type has no ID!"
        assert new_type_j.id > 0, "New type has incorrect ID!"

        assert len(new_type_j.typeattrs) == 2, "Resource type attrs did not add correctly"
        new_type_j.name = "Updated type name @ %s"%(datetime.datetime.now())
        new_type_j.alias = templatetype.name + " alias"
        new_type_j.resource_type = 'NODE'

        tattr_3 = JSONObject()
        tattr_3.attr_id = attr_3.id
        tattr_3.description = "Descripton of added typeattr"
        tattr_3.properties = {"update_type_test_property": "property value"}
        new_type_j.typeattrs.append(tattr_3)

        new_type_j.typeattrs[0].description = "Updated typeattr description"

        updated_type_i = hb.update_templatetype(new_type_j)
        updated_type_j = JSONObject(updated_type_i)

        assert new_type_j.name == updated_type_j.name, "Names are not the same!"
        assert new_type_j.alias == updated_type_j.alias, "Aliases are not the same!"
        assert new_type_j.id == updated_type_j.id, "type ids to not match!"
        assert new_type_j.id > 0, "New type has incorrect ID!"
        assert new_type_j.typeattrs[0].description == "Updated typeattr description"
        assert new_type_j.typeattrs[-1].properties['update_type_test_property'] == "property value"
 
        assert len(updated_type_j.typeattrs) == 3, "Template type attrs did not update correctly"


    def test_delete_type(self):
        new_template = self.test_add_template()

        retrieved_template = hb.get_template(new_template.id)
        assert retrieved_template is not None

        templatetype = new_template.templatetypes[0]
        hb.delete_templatetype(templatetype.id)
    
        updated_template = JSONObject(hb.get_template(new_template.id))

        for tmpltype in updated_template.templatetypes:
            assert tmpltype.id != templatetype.id

    def test_get_type(self):
        new_type = self.get_template().templatetypes[0]
        new_type = hb.get_templatetype(new_type.id)
        assert new_type is not None, "Resource type attrs not retrived by ID!"

    def test_get_type_by_name(self):
        new_type = self.get_template().templatetypes[0]
        new_type = hb.get_templatetype_by_name(new_type.template_id, new_type.name)
        assert new_type is not None, "Resource type attrs not retrived by name!"


    def test_add_typeattr(self):


        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = JSONObject()
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = self.get_template().id
        templatetype.resource_type = 'NODE'

        tattr_1 = JSONObject()
        tattr_1.attr_id = attr_1.id

        tattr_2 = JSONObject()
        tattr_2.attr_id = attr_2.id
        tattr_2.description = "Description of typeattr from test_add_typeattr"
        tattr_2.properties = {"test_property":"property value"}

        templatetype.typeattrs = [tattr_1, tattr_2]

        new_type = JSONObject(hb.add_templatetype(templatetype))

        tattr_3 = JSONObject()
        tattr_3.attr_id = attr_3.id
        tattr_3.type_id = new_type.id
        tattr_3.description = "Description of additional typeattr from test_add_typeattr"
        tattr_3.properties = {"add_typeattr_test_property": "property value"}

        log.info("Adding Test Type attribute")
        
        hb.add_typeattr(tattr_3)

        updated_type = JSONObject(hb.get_templatetype(new_type.id,user_id=self.user_id))

        assert len(updated_type.typeattrs) == 3, "Type attr did not add correctly"

        assert eval(updated_type.typeattrs[-1].properties)['add_typeattr_test_property'] == "property value"


    def test_delete_typeattr(self):
        
        template = self.test_add_template()

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')

        templatetype = JSONObject()
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.resource_type = 'NODE'
        templatetype.template_id = template.id


        tattr_1 = JSONObject()
        tattr_1.attr_id = attr_1.id

        tattr_2 = JSONObject()
        tattr_2.attr_id = attr_2.id

        templatetype.typeattrs = [tattr_1, tattr_2]

        new_type = JSONObject(hb.add_templatetype(templatetype))

        tattr_2.type_id = new_type.id

        hb.delete_typeattr(tattr_2)
        
        updated_type = JSONObject(hb.get_templatetype(new_type.id))

        log.info(len(updated_type.typeattrs))

        assert len(updated_type.typeattrs) == 1, "Resource type attr did not add correctly"

    def test_get_templates(self):
        self.get_template()
        templates = [JSONObject(t) for t in hb.get_templates()]
        for t in templates:
            for typ in t.templatetypes:
                assert typ.resource_type is not None
        assert len(templates) > 0, "Templates were not retrieved!"


    def test_get_template(self):
        template = self.get_template()
        new_template = JSONObject(hb.get_template(template.id))

        assert new_template.name == template.name, "Names are not the same! Retrieval by ID did not work!"


    def test_get_template_by_name_good(self):
        template = self.get_template()
        new_template = JSONObject(hb.get_template_by_name(template.name))

        assert new_template.name == template.name, "Names are not the same! Retrieval by name did not work!"

    def test_get_template_by_name_bad(self):
        new_template = hb.get_template_by_name("Not a template!")

        assert new_template is None

    def test_add_resource_type(self):

        template = self.get_template()
        types = template.templatetypes
        type_name = types[0].name
        type_id   = types[0].id

        project = self.create_project()
        network = JSONObject()

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        network.nodes = []
        network.links = []

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            type_summary = JSONObject()
            type_summary.template_id = template.id
            type_summary.template_name = template.name
            type_summary.id = type_id
            type_summary.name = type_name

            node.types = [type_summary]

            network.nodes.append(node)

        for i in range(nlinks):
            link = JSONObject()
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = network.nodes[i].id
            link.node_2_id = network.nodes[i + 1].id

            network.links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'

        net_summary = hb.add_network(network, user_id=self.user_id)
        new_net = hb.get_network(net_summary.id, user_id=self.user_id)

        for node in new_net.nodes:
            assert node.types is not None and node.types[0].type_name == "Node type"; "type was not added correctly!"


    def test_find_matching_resource_types(self):

        network = self.create_network_with_data()

        node_to_check = network.nodes[0]
        matching_types_i = hb.get_matching_resource_types('NODE', node_to_check.id)
        matching_types_j = [JSONObject(i) for i in matching_types_i]

        assert len(matching_types_j) > 0, "No types returned!"

        matching_type_ids = []
        for tmpltype in matching_types_j:
            matching_type_ids.append(tmpltype.id)

        assert node_to_check.types[0].id in matching_type_ids, "TemplateType ID not found in matching types!"

    def test_assign_type_to_resource(self):
        network = self.create_network_with_data()
        template = self.get_template()
        templatetype = template.templatetypes[0]

        node_to_assign = network.nodes[0]

        result = JSONObject(hb.assign_type_to_resource(templatetype.id, 'NODE', node_to_assign.id))

        node = hb.get_node(node_to_assign.id)


        assert node.types is not None, \
            'Assigning type did not work properly.'

        assert str(result.id) in [str(x.type_id) for x in node.types]

    def test_remove_type_from_resource(self):
        network = self.create_network_with_data()
        template = self.get_template()
        templatetype = template.templatetypes[0]

        node_to_assign = network.nodes[0]

        result1_i = hb.assign_type_to_resource(templatetype.id, 'NODE', node_to_assign.id)

        result1_j = JSONObject(result1_i)

        node_j = JSONObject(hb.get_node(node_to_assign.id))

        assert node_j.types is not None, \
            'Assigning type did not work properly.'

        assert str(result1_j.id) in [str(x.type_id) for x in node_j.types]

        remove_result = hb.remove_type_from_resource(templatetype.id,
                                                            'NODE',
                                                            node_to_assign.id) 
        print(remove_result)
        assert remove_result == 'OK'
        
        updated_node_j = JSONObject(hb.get_node(node_to_assign.id))
 
        assert updated_node_j.types is None or str(result1_j.id) not in [str(x.type_id) for x in updated_node_j.types] 

    def test_create_template_from_network(self):
        network = self.create_network_with_data()

        net_template = hb.get_network_as_xml_template(network.id)

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
        for l in net_to_update.links:
            if l.types is None:
                empty_links.append(l.id)

        #Add the resource attributes to the links, so we can guarantee
        #that these links will match those in the template.
        for t in template.templatetypes:
            if t.resource_type == 'LINK':
                link_type = t
                break

        link_ra_1 = JSONObject(dict(
            attr_id=link_type.typeattrs[0].attr_id
        ))
        link_ra_2 = JSONObject(dict(
            attr_id=link_type.typeattrs[1].attr_id
        ))
        for link in net_to_update.links:
            if link.types is None:
                link.attributes.append(link_ra_1)
                link.attributes.append(link_ra_2)

        network = hb.update_network(net_to_update, user_id=self.user_id)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == 'Default Node'

        hb.apply_template_to_network(template.id, network.id)

        network = hb.get_network(network.id, user_id=self.user_id)
       
        assert len(network.types) == 2
        assert network.types[1].type_name == 'Network Type'
        for l in network.links:
            if l.id in empty_links:
                assert l.types is not None
                assert len(l.types) == 1
                assert l.types[0].type_name == 'Link type'

        #THe assignment of the template hasn't affected the nodes
        #as they do not have the appropriate attributes.
        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].template_name == 'Default Template'

    def test_apply_template_to_network_twice(self):
        net_to_update = self.create_network_with_data()
        template = self.get_template()
       
        #Test the links as it's easier
        empty_links = []
        for l in net_to_update.links:
            if l.types is None:
                empty_links.append(l.id)

        #Add the resource attributes to the links, so we can guarantee
        #that these links will match those in the template.
        for t in template.templatetypes:
            if t.resource_type == 'LINK':
                link_type = t
                break

        link_ra_1 = JSONObject(dict(
            attr_id=link_type.typeattrs[0].attr_id
        ))
        link_ra_2 = JSONObject(dict(
            attr_id=link_type.typeattrs[1].attr_id
        ))
        for link in net_to_update.links:
            if link.types is None:
                link.attributes.append(link_ra_1)
                link.attributes.append(link_ra_2)

        network = hb.update_network(net_to_update, user_id=self.user_id)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == 'Default Node'

        hb.apply_template_to_network(template.id, network.id)
        hb.apply_template_to_network(template.id, network.id)

        network = hb.get_network(network.id, user_id=self.user_id)
       
        assert len(network.types) == 2
        assert network.types[1].name == 'Network Type'
        for l in network.links:
            if l.id in empty_links:
                assert l.types is not None
                assert len(n.types) == 1
                assert l.types[0].name == 'Link type'

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].template_name == 'Default Template'

    def test_remove_template_from_network(self):
        network = self.create_network_with_data()
        template_id = network.types[0].template_id
       
        #Test the links as it's easier
        empty_links = []
        for l in network.links:
            if l.types is None or len(l.types) == 0:
                empty_links.append(l.id)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].type_name == "Default Node"

        hb.apply_template_to_network(template_id, network.id)

        hb.remove_template_from_network(network.id, template_id, 'N')

        network_2 = hb.get_network(network.id, user_id=self.user_id)

        assert len(network_2.types) == 0
        for l in network_2.links:
            if l.id in empty_links:
                assert len(l.types) == 0

        for n in network_2.nodes:
            assert len(n.types) == 0

    def test_remove_template_and_attributes_from_network(self):
        network = self.create_network_with_data()
        template = self.get_template()
       
        #Test the links as it's easier
        empty_links = []
        for l in network.links:
            if l.types is None:
                empty_links.append(l.id)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].type_name == 'Default Node'


        network_1 = hb.get_network(network.id, user_id=self.user_id)
        assert len(network_1.types) == 1

        hb.apply_template_to_network(template.id, network.id)

        network_3 = hb.get_network(network.id, user_id=self.user_id)
        assert len(network_3.types) == 2

        hb.remove_template_from_network(network.id, template.id, 'Y')

        network_2 = hb.get_network(network.id, user_id=self.user_id)

        assert len(network_2.types) == 1
        
        link_attrs = []
        for tt in template.templatetypes:
            if tt.resource_type != 'LINK':
                continue
            for ta in tt.typeattrs:
                attr_id = ta.attr_id
                if attr_id not in link_attrs:
                    link_attrs.append(attr_id)
                link_attrs.append(ta.attr_id)
        for l in network_2.links:
            if l.id in empty_links:
                assert l.types is None
            if l.attributes is not None:
                for a in l.attributes:
                    assert a.attr_id not in link_attrs

        for tt in template.templatetypes:
            if tt.resource_type != 'NODE':
                continue
            for ta in tt.typeattrs:
                attr_id = ta.attr_id
                if attr_id not in link_attrs:
                    link_attrs.append(attr_id)
                link_attrs.append(ta.attr_id)

        for n in network_2.nodes:
            assert len(n.types) == 1
            if n.attributes is not None:
                for a in n.attributes:
                    assert a.attr_id not in link_attrs

    def test_validate_attr(self):
        network = self.create_network_with_data()
        
        scenario = network.scenarios[0]
        rs_ids = [rs.resource_attr_id for rs in scenario.resourcescenarios]
        template_id = network.nodes[0].types[0].template_id

        for n in network.nodes:
            node_type = hb.get_templatetype(n.types[0].id)
            for ra in n.attributes:
                for attr in node_type.typeattrs:
                    if ra.attr_id == attr.attr_id and ra.id in rs_ids and attr.data_restriction is not None:
                #        logging.info("Validating RA %s in scenario %s", ra.id, scenario.id)
                        error = hb.validate_attr(ra.id, scenario.id, template_id)
                        assert error.ref_id == n.id


    def test_validate_attrs(self):
        network = self.create_network_with_data()
        
        scenario = network.scenarios[0]

        ra_ids = []

        for rs in scenario.resourcescenarios:
            ra_ids.append(rs.resource_attr_id)

        template_id = network.types[0].template_id

        errors = hb.validate_attrs(ra_ids, scenario.id, template_id)

        assert len(errors) > 0

    def test_validate_scenario(self):
        network = self.create_network_with_data()
        
        scenario = network.scenarios[0]
        template_id = network.nodes[0].types[0].template_id

        errors = hb.validate_scenario(scenario.id,template_id)

        assert len(errors) > 0 

    def test_validate_network(self):
        network = self.create_network_with_data(use_existing_template=False)
        
        util.update_template(network.types[0].template_id)

        scenario = network.scenarios[0]
        template = network.nodes[0].types[0]
        #Validate the network without data: should pass as the network is built
        #based on the template in these unit tests
        errors1 = hb.validate_network(network['id'], template['template_id'])

        #The network should have an error, saying that the template has net_attr_c,
        #but the network does not
        assert len(errors1) == 1
        assert errors1[0].find('net_attr_d')  > 0

        #Validate the network with data. Should fail as one of the attributes (node_attr_3)
        #is specified as being a 'Cost' in the template but 'Speed' is the dimension
        #of the dataset used. In addition, node_attr_1 specified a unit of 'm^3'
        #whereas the timeseries in the data is in 'cm^3', so each will fail on unit
        #mismatch also.
        errors2 = hb.validate_network(network['id'], template['template_id'],
                                                  scenario['id'])

        assert len(errors2) > 0
        #every node should have an associated error, plus the network error from above
        assert len(errors2) == len(network['nodes'] * 2)+1
        for err in errors2[1:]:
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
        for typ in template_1.templatetypes:
            if typ.typeattrs:
                for ta in typ.typeattrs:
                    if ta.attr.name == 'node_attr_1':
                        diff_type_1_id = typ.id
                        ta.unit = "m^3"
                    elif ta.attr.name == 'link_attr_1':
                        same_type_1_id = typ.id

        updated_template_1 = JSONObject(hb.update_template(template_1))

        diff_type_2_id = None
        same_type_2_id = None
        for typ in template_2.templatetypes:
            if typ.typeattrs:
                for ta in typ.typeattrs:
                    if ta.attr.name == 'node_attr_1':
                        diff_type_2_id = typ.id
                        ta.unit = "cm^3"
                    elif ta.attr.name == 'link_attr_1':
                        same_type_2_id = typ.id
        
        #Before updating template 2, check compatibility of types, where T1 has 
        #a unit, but t2 does not.
        errors_diff = hb.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = hb.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

        #Now update T2 so that the types have conflicting units.
        updated_template_2 = JSONObject(hb.update_template(template_2))

        errors_diff = hb.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = hb.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

        print(updated_template_1)
        
        for typ in updated_template_1.templatetypes:
            if typ.typeattrs:
                for ta in typ.typeattrs:
                    if ta.attr.name == 'node_attr_1':
                        ta.unit = None

        #Update template 1 now so that it has no unit, but template 2 does.
        updated_template_1_a= hb.update_template(updated_template_1)
        errors_diff = hb.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = hb.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

if __name__ == '__main__':
    server.run()
