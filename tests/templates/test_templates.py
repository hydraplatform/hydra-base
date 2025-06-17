#s!/usr/bin/env python
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

from lxml import etree
from hydra_base import config
from hydra_base.lib.objects import JSONObject
import hydra_base as hb
import logging
from hydra_base.exceptions import HydraError
import datetime
import json
import os
import pytest
log = logging.getLogger(__name__)


@pytest.fixture()
def template():
    return os.path.join(os.path.dirname(__file__), 'template.xml')


@pytest.fixture()
def template_json_object(client, template):

    with open(template) as fh:
        file_contents = fh.read()

    return JSONObject(client.import_template_xml(file_contents))


@pytest.fixture()
def mock_template(client):
    link_attr_1 = client.testutils.create_attribute("link_attr_1", dimension='Pressure')
    link_attr_2 = client.testutils.create_attribute("link_attr_2", dimension='Speed')
    node_attr_1 = client.testutils.create_attribute("node_attr_1", dimension='Volume')
    node_attr_2 = client.testutils.create_attribute("node_attr_2", dimension='Speed')
    net_attr_1 = client.testutils.create_attribute("net_attr_2", dimension='Speed')

    template = JSONObject()
    template.name = 'Test template @ %s' % datetime.datetime.now()

    layout = {}
    layout['groups'] = '<groups>...</groups>'

    template.layout = layout

    template.templatetypes = []
    # **********************
    # type 1           #
    # **********************
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
    # **********************
    # type 2           #
    # **********************
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

    # **********************
    # type 3           #
    # **********************
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

    new_template_i = client.add_template(template)
    # TODO: HACK to load the attr
    for tt in new_template_i.templatetypes:
        for ta in tt.typeattrs:
            ta.attr

    new_template_j = JSONObject(new_template_i)
    return new_template_j

# mock_template_copy is used for some of compatiblity tests but is identical to the
# mock_template fixture
mock_template_copy = mock_template


class TestTemplates:
    """
        Test for templates
    """
    def test_add_xml(self, template_json_object):
        new_tmpl = template_json_object

        assert new_tmpl is not None, "Adding template from XML was not successful!"

        assert len(new_tmpl.templatetypes) == 2

        for tt in new_tmpl.templatetypes:
            if tt.name == 'Reservoir':
                for ta in tt.typeattrs:
                    assert ta.data_type == 'scalar'

                assert tt.typeattrs[-1].properties is not None
                assert eval(tt.typeattrs[-1].properties)['template_property'] == "Test property from template"

    def test_get_xml(self, client, template_json_object):
        xml_tmpl = template_json_object

        db_template = client.get_template_as_xml(xml_tmpl.id)


        assert db_template is not None

        template_xsd_path = config.get('templates', 'template_xsd_path')
        xmlschema_doc = etree.parse(template_xsd_path)

        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_tree = etree.fromstring(db_template)

        xmlschema.assertValid(xml_tree)

    def test_get_dict(self, client, template_json_object):

        # Upload the xml file initally to avoid having to manage 2 template files
        xml_tmpl = template_json_object

        template_dict = client.get_template_as_dict(xml_tmpl.id)

        # Error that there's already a template with this name.
        with pytest.raises(HydraError):
            client.import_template_dict(template_dict, allow_update=False)

        typename = template_dict['template']['templatetypes'][0]['name']

        template_dict['template']['templatetypes'][0].name = typename + "_updated"

        # Finds a template with this name and updates it to mirror this dict.
        # This includes deleting types if they're not in this dict.
        # Changing the name of a type has this effect, as a new template does not have
        # any reference to existing types in Hydra.

        updated_template = JSONObject(client.import_template_dict(template_dict))

        type_names = []

        for templatetype in updated_template.templatetypes:
            type_names.append(templatetype.name)

        assert len(type_names) == 2
        assert typename + "_updated" in type_names and typename not in type_names

        # Now put it back to the original name so other tests will work
        log.info("Reverting the type's name")
        template_dict['template']['templatetypes'][0].name = typename
        updated_template = JSONObject(client.import_template_dict(template_dict))

        #just double-check that the JSON import works also
        updated_template = JSONObject(client.import_template_json(json.dumps(template_dict)))

        type_names = []
        for templatetype in updated_template.templatetypes:
            type_names.append(templatetype.name)

        assert len(type_names) == 2
        assert typename in type_names and typename + "_updated" not in type_names

        log.info("Checking to ensure Template has been updated correctly...")
        # one final check to ensure that the type has been deleted
        check_template_i = client.get_template(updated_template.id)

        assert len(check_template_i.templatetypes) == 2


    """
        TEMPLATES Functions
    """

    def test_add_template(self, client, mock_template):

        link_attr_1 = client.testutils.create_attribute("link_attr_1", dimension='Pressure')
        link_attr_2 = client.testutils.create_attribute("link_attr_2", dimension='Speed')
        node_attr_1 = client.testutils.create_attribute("node_attr_1", dimension='Volume')
        node_attr_2 = client.testutils.create_attribute("node_attr_2", dimension='Speed')

        new_template_j = mock_template

        assert new_template_j.name == mock_template.name, "Names are not the same!"
        assert json.dumps(new_template_j.layout) == json.dumps(mock_template.layout), "Layouts are not the same!"
        assert new_template_j.id is not None, "New Template has no ID!"
        assert new_template_j.id > 0, "New Template has incorrect ID!"

        assert len(new_template_j.templatetypes) == 3, "Resource types did not add correctly"
        for t in new_template_j.templatetypes[0].typeattrs:
            assert t.attr_id in (node_attr_1.id, node_attr_2.id);
            "Node types were not added correctly!"

        for t in new_template_j.templatetypes[1].typeattrs:
            assert t.attr_id in (link_attr_1.id, link_attr_2.id);
            "Node types were not added correctly!"

    def test_update_template(self, client):

        #Only applicable for tests.
        client.template.ATTR_CACHE = {}

        # Defining attributes
        attribute_1 = client.testutils.create_attribute("link_attr_1", dimension='Pressure')
        attribute_2 = client.testutils.create_attribute("link_attr_2", dimension='Speed')
        attribute_3 = client.testutils.create_attribute("node_attr_1", dimension='Volume')

        # Defining template
        template = JSONObject()

        template.name = 'Test Template @ %s'%datetime.datetime.now()

        template.templatetypes = []

        template_type_1 = JSONObject({
            "name" : "Node type 2",
            "alias" : "Node type 2 alias",
            "resource_type" : 'NODE',
            "typeattrs" : []
        })

        template_type_2 = JSONObject({
            "name" : "Link type 2",
            "alias" : "Link type 2 alias",
            "resource_type" : 'LINK',
            "typeattrs" : []
        })

        # Definind a typeattr data structure
        type_attribute_1 = JSONObject({
            "attr_id" : attribute_1.id,
            "unit_id" : client.get_unit_by_abbreviation('bar').id,
            "description" : "typeattr description 1",
            "properties" : {"test_property": "property value"}
        })
        template_type_1.typeattrs.append(type_attribute_1)

        type_attribute_2 = JSONObject({
            "attr_id" : attribute_2.id,
            "unit_id" : client.get_unit_by_abbreviation('mph').id,
            "description" : "typeattr description 2"
        })
        template_type_2.typeattrs.append(type_attribute_2)

        template.templatetypes.append(template_type_1)
        template.templatetypes.append(template_type_2)

        new_template_i = client.add_template(template)
        new_template_j = JSONObject(new_template_i)

        assert new_template_j.name == template.name, "Names are not the same!"
        assert new_template_j.id is not None, "New Template has no ID!"
        assert new_template_j.id > 0, "New Template has incorrect ID!"

        assert len(new_template_j.templatetypes) == 2, "Resource types did not add correctly"
        assert len(new_template_j.templatetypes[0].typeattrs) == 1, "Resource type attrs did not add correctly"
        assert new_template_j.templatetypes[0].typeattrs[0].unit_id == client.get_unit_by_abbreviation('bar').id

        #update the name of one of the types
        new_template_j.templatetypes[0].name = "Test type 3"
        updated_type_id = new_template_j.templatetypes[0].id

        #add a new type
        new_type = JSONObject({
            "name" : "New Node type",
            "alias" : "New Node type alias",
            "resource_type" : 'NODE',
            "typeattrs" : []
        })
        new_template_j.templatetypes.append(new_type)

        #add an template attr to one of the types
        type_attribute_3 = JSONObject({
            "attr_id" : attribute_3.id,
            "description" : "updated typeattr description 1",
            "properties" : {"test_property_of_added_type": "property value"}
        })
        new_template_j.templatetypes[0].typeattrs.append(type_attribute_3)
        updated_template_j = client.update_template(new_template_j)
        updated_template_retrieved_j = client.get_template(new_template_j.id)

        assert updated_template_retrieved_j.name == template.name, "Names are not the same!"

        updated_type = None
        for tmpltype in new_template_j.templatetypes:
            if tmpltype.id == updated_type_id:
                updated_type = tmpltype
                break

        assert updated_type.name == "Test type 3", "Resource types did not update correctly"

        assert len(updated_type.typeattrs) == 2, "Resource type template attr did not update correctly"

        """
            Test that when setting a unit on a type attr, it matches the dimension of its attr
            In this case, setting m^3(Volume) fails as the attr has a dimension of 'Pressure'
        """
        old_attribute_type_to_restore = updated_template_j.templatetypes[0].typeattrs[0].unit_id
        updated_template_j.templatetypes[0].typeattrs[0].unit_id = client.get_unit_by_abbreviation('m^3').id
        with pytest.raises(HydraError):
            client.update_template(updated_template_j)
        # Restoring the old value to not fail later for this reason
        updated_template_j.templatetypes[0].typeattrs[0].unit_id = old_attribute_type_to_restore


        """
            Testing the case in which the attribute has no dimension while the typeattr has UNIT_ID that is not none
        """
        attribute_dimension_none = client.testutils.create_attribute("node_attr_dimension_none", dimension=None)
        template_type_dimension_none = JSONObject({
            "name" : "Node type dimension_none",
            "alias" : "Node type dimension_none alias",
            "resource_type" : 'NODE',
            "typeattrs" : []
        })
        type_attribute_dimension_none = JSONObject({
            "attr_id" : attribute_dimension_none.id,
            "unit_id" : client.get_unit_by_abbreviation('bar').id,
            "description" : "type_attribute_dimension_none description 1",
            "properties" : {"test_property": "property value"}
        })
        template_type_dimension_none.typeattrs.append(type_attribute_dimension_none)
        updated_template_j.templatetypes.append(template_type_dimension_none)

        with pytest.raises(HydraError):
            client.update_template(updated_template_j) # It MUST fail because the unit_id is not consistent to dimension_id none

    def test_delete_template(self, client, network_with_data, mock_template):

        #Only applicable for tests. TODO: make this not rubbish.
        client.template.ATTR_CACHE = {}

        network = network_with_data
        new_template = mock_template

        retrieved_template_j = client.get_template(new_template.id)
        assert retrieved_template_j is not None

        client.apply_template_to_network(retrieved_template_j.id, network.id)

        updated_network = client.get_network(network.id)
        assert len(updated_network.types) == 2

        expected_net_type = None
        for t in new_template.templatetypes:
            if t.resource_type == 'NETWORK':
                expected_net_type = t.id

        network_types = [t.id for t in updated_network.types]

        assert expected_net_type in network_types

        #can't delete this because there's a resource type associated to one
        #of its types
        with pytest.raises(HydraError):
            client.delete_template(new_template.id)

        client.delete_template(new_template.id, delete_resourcetypes=True)

        with pytest.raises(HydraError):
            client.get_template(new_template.id)

        network_deleted_templatetypes = client.get_network(network.id)

        assert len(network_deleted_templatetypes.types) == 1

    def test_get_types_by_attr(self, client, network_with_data, mock_template):

        #Only applicable for tests. TODO: make this not rubbish.
        client.template.ATTR_CACHE = {}

        network = network_with_data
        new_template = mock_template

        retrieved_template_j = client.get_template(new_template.id)
        assert retrieved_template_j is not None

        client.apply_template_to_network(retrieved_template_j.id, network.id)

        updated_network = client.get_network(network.id)
        assert len(updated_network.types) == 2


        node_types = client.get_types_by_attr(updated_network.nodes[0], 'NODE')

        assert len(node_types) > 0

        #there could be lots of templates in the system, so the node should
        #match *at least* the number of node types, but may not be equal
        assert len(node_types) >= len(updated_network.nodes[0].types)



    """
        TEMPLATE TYPES Functions
    """

    def test_add_type(self, client, mock_template):

        template = mock_template

        templatetype = client.testutils.create_templatetype(template.id)

        new_type_i = client.add_templatetype(templatetype)
        new_type_j = JSONObject(new_type_i)

        assert new_type_j.name == templatetype.name, "Names are not the same!"
        assert new_type_j.alias == templatetype.alias, "Aliases are not the same!"
        assert new_type_j.layout == templatetype.layout, "Layouts are not the same!"
        assert new_type_j.id is not None, "New type has no ID!"
        assert new_type_j.id > 0, "New type has incorrect ID!"

        assert len(new_type_j.typeattrs) == 3, "Resource type attrs did not add correctly"

    def test_update_type(self, client, mock_template):

        template = mock_template

        attr_1 = client.testutils.create_attribute("link_attr_1", dimension='Pressure')
        attr_2 = client.testutils.create_attribute("link_attr_2", dimension='Speed')
        attr_3 = client.testutils.create_attribute("node_attr_1", dimension='Volume')

        templatetype = JSONObject()
        templatetype.name = "Test type name @ %s" % (datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = template.id
        templatetype.resource_type = 'NODE'

        tattr_1 = JSONObject()
        tattr_1.attr_id = attr_1.id

        tattr_2 = JSONObject()
        tattr_2.attr_id = attr_2.id

        templatetype.typeattrs = [tattr_1, tattr_2]

        new_type_i = client.add_templatetype(templatetype)
        type_to_update_j = JSONObject(new_type_i)

        assert type_to_update_j.name == templatetype.name, "Names are not the same!"
        assert type_to_update_j.alias == templatetype.alias, "Aliases are not the same!"
        assert type_to_update_j.id is not templatetype, "New type has no ID!"
        assert type_to_update_j.id > 0, "New type has incorrect ID!"

        assert len(type_to_update_j.typeattrs) == 2, "Resource type attrs did not add correctly"
        type_to_update_j.name = "Updated type name @ %s"%(datetime.datetime.now())
        type_to_update_j.alias = templatetype.name + " alias"
        type_to_update_j.resource_type = 'NODE'

        tattr_3 = JSONObject()
        tattr_3.attr_id = attr_3.id
        tattr_3.description = "Descripton of added typeattr"
        tattr_3.properties = {"update_type_test_property": "property value"}
        type_to_update_j.typeattrs.append(tattr_3)

        type_to_update_j.typeattrs[0].description = "Updated typeattr description"

        updated_type_i = client.update_templatetype(type_to_update_j)
        updated_type_j = JSONObject(updated_type_i)

        assert type_to_update_j.name == updated_type_j.name, "Names are not the same!"
        assert type_to_update_j.alias == updated_type_j.alias, "Aliases are not the same!"
        assert type_to_update_j.id == updated_type_j.id, "type ids to not match!"
        assert type_to_update_j.id > 0, "New type has incorrect ID!"
        assert type_to_update_j.typeattrs[0].description == "Updated typeattr description"
        assert type_to_update_j.typeattrs[-1].properties['update_type_test_property'] == "property value"

        assert len(updated_type_j.typeattrs) == 3, "Template type attrs did not update correctly"

    def test_delete_type(self, client, mock_template):
        new_template = mock_template

        retrieved_template = client.get_template(new_template.id)
        assert retrieved_template is not None

        templatetype = new_template.templatetypes[0]
        client.delete_templatetype(templatetype.id)

        updated_template = JSONObject(client.get_template(new_template.id))

        for tmpltype in updated_template.templatetypes:
            assert tmpltype.id != templatetype.id

    def test_get_type(self, client, mock_template):
        new_type = mock_template.templatetypes[0]
        new_type = client.get_templatetype(new_type.id)
        assert new_type is not None, "Resource type attrs not retrived by ID!"

    def test_get_type_by_name(self, client, mock_template):
        up_to_date_template = client.get_template(mock_template.id)
        #Get this in case the mock template has been updated since creation
        type = up_to_date_template.templatetypes[0]
        new_type = client.get_templatetype_by_name(type.template_id, type.name)
        assert new_type is not None, "Resource type attrs not retrived by name!"


    """
        Type Attributes Functions
    """


    def test_add_typeattr(self, client, mock_template):

        attr_1 = client.testutils.create_attribute("link_attr_1", dimension='Pressure')
        attr_2 = client.testutils.create_attribute("link_attr_2", dimension='Speed')
        attr_3 = client.testutils.create_attribute("node_attr_1", dimension='Volume')

        templatetype = JSONObject()
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = mock_template.id
        templatetype.resource_type = 'NODE'

        tattr_1 = JSONObject()
        tattr_1.attr_id = attr_1.id

        tattr_2 = JSONObject()
        tattr_2.attr_id = attr_2.id
        tattr_2.description = "Description of typeattr from test_add_typeattr"
        tattr_2.properties = {"test_property":"property value"}

        templatetype.typeattrs = [tattr_1, tattr_2]

        new_type = JSONObject(client.add_templatetype(templatetype))

        tattr_3 = JSONObject()
        tattr_3.attr_id = attr_3.id
        tattr_3.type_id = new_type.id
        tattr_3.description = "Description of additional typeattr from test_add_typeattr"
        tattr_3.properties = {"add_typeattr_test_property": "property value"}

        log.info("Adding Test Type attribute")

        client.add_typeattr(tattr_3)

        updated_type = JSONObject(client.get_templatetype(new_type.id))

        assert len(updated_type.typeattrs) == 3, "Type attr did not add correctly"

        assert eval(updated_type.typeattrs[-1].properties)['add_typeattr_test_property'] == "property value"

    def test_delete_typeattr(self, client, mock_template):

        template = mock_template

        attr_1 = client.testutils.create_attribute("link_attr_1", dimension='Pressure')
        attr_2 = client.testutils.create_attribute("link_attr_2", dimension='Speed')

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

        new_type = JSONObject(client.add_templatetype(templatetype))

        assert len(new_type.typeattrs) == 2

        typeattr_to_delete = new_type.typeattrs[0]

        client.delete_typeattr(typeattr_to_delete.id)

        updated_type = JSONObject(client.get_templatetype(new_type.id))


        assert len(updated_type.typeattrs) == 1, "Resource type attr did not add correctly"

    def test_get_templates(self, client, mock_template):

        templates = [JSONObject(t) for t in client.get_templates()]
        for t in templates:
            for typ in t.templatetypes:
                assert typ.resource_type is not None
        assert len(templates) > 0, "Templates were not retrieved!"

    def test_get_template(self, client, mock_template):
        new_template = JSONObject(client.get_template(mock_template.id))

        assert new_template.name == mock_template.name, "Names are not the same! Retrieval by ID did not work!"

    def test_get_template_by_name_good(self, client, mock_template):
        new_template = JSONObject(client.get_template_by_name(mock_template.name))

        assert new_template.name == mock_template.name, "Names are not the same! Retrieval by name did not work!"

    def test_get_template_by_name_bad(self, client):

        with pytest.raises(HydraError):
            new_template = client.get_template_by_name("Not a template!")

    def test_set_template_status(self, client, network_with_data,  mock_template):

        all_templates = client.get_templates(load_all=False)
        assert len(all_templates) > 1

        tmpl_to_deactivate = all_templates[0].id

        client.deactivate_template(tmpl_to_deactivate)

        #this should return one fewer
        filtered_templates = client.get_templates(load_all=False)
        assert len(filtered_templates) == len(all_templates) - 1

        unfiltered_templates = client.get_templates(load_all=False, include_inactive=True)
        assert len(unfiltered_templates) == len(all_templates)

        client.activate_template(tmpl_to_deactivate)

        unfiltered_templates = client.get_templates(load_all=False)
        assert len(unfiltered_templates) == len(all_templates)

    """
        Resource Types Functions
    """

    def test_add_resource_type(self, client, mock_template):

        template = mock_template
        types = template.templatetypes
        type_name = types[0].name
        type_id   = types[0].id

        project = client.testutils.create_project()
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
            type_summary.id = template.id
            type_summary.name = template.name
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

        net_summary = client.add_network(network)
        new_net = client.get_network(net_summary.id)

        for node in new_net.nodes:
            assert node.types is not None and node.types[0].name == "Node type"; "type was not added correctly!"

    def test_find_matching_resource_types(self, client, network_with_data):

        network = network_with_data

        node_to_check = network.nodes[0]
        matching_types_i = client.get_matching_resource_types('NODE', node_to_check.id)
        matching_types_j = [JSONObject(i) for i in matching_types_i]

        assert len(matching_types_j) > 0, "No types returned!"

        matching_type_ids = []
        for tmpltype in matching_types_j:
            matching_type_ids.append(tmpltype.id)

        assert node_to_check.types[0].id in matching_type_ids, "TemplateType ID not found in matching types!"

    def test_assign_type_to_resource(self, client, mock_template, network_with_data):
        network = network_with_data
        template = mock_template
        templatetype = template.templatetypes[0]

        node_to_assign = network.nodes[0]

        result = JSONObject(client.assign_type_to_resource(templatetype.id, 'NODE', node_to_assign.id))

        node = client.get_node(node_to_assign.id)

        assert len(node.types) == 2, 'Assigning type did not work properly.'

        assert str(result.id) in [str(x.type_id) for x in node.types]


    def test_assign_types_to_resources(self, client, mock_template, network_with_data):
        network = network_with_data
        template = mock_template
        templatetype = template.templatetypes[0]

        node_to_assign = network.nodes[0]

        resource_types = [JSONObject({
            'ref_key': 'NODE',
            'ref_id':node_to_assign.id,
            'type_id':templatetype.id
            })]
        results = client.assign_types_to_resources(resource_types)

        node = client.get_node(node_to_assign.id)

        assert len(node.types) == 2, 'Assigning type did not work properly.'

        assert str(results[0].id) in [str(x.type_id) for x in node.types]

    def test_remove_type_from_resource(self, client, mock_template, network_with_data):
        network = network_with_data
        template = mock_template
        templatetype = template.templatetypes[0]

        node_to_assign = network.nodes[0]

        result1_i = client.assign_type_to_resource(templatetype.id, 'NODE', node_to_assign.id)

        result1_j = JSONObject(result1_i)

        node_j = JSONObject(client.get_node(node_to_assign.id))

        assert node_j.types is not None, \
            'Assigning type did not work properly.'

        assert str(result1_j.id) in [str(x.type_id) for x in node_j.types]

        remove_result = client.remove_type_from_resource(templatetype.id, 'NODE', node_to_assign.id)
        assert remove_result == 'OK'

        updated_node_j = JSONObject(client.get_node(node_to_assign.id))

        assert updated_node_j.types is None or str(result1_j.id) not in [str(x.type_id) for x in updated_node_j.types]


    def test_create_template_from_network(self, client, network_with_data):
        network = network_with_data

        net_template = client.get_network_as_xml_template(network.id)


        assert net_template is not None

        template_xsd_path = config.get('templates', 'template_xsd_path')
        xmlschema_doc = etree.parse(template_xsd_path)

        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_tree = etree.fromstring(net_template)

        xmlschema.assertValid(xml_tree)

    def test_apply_template_to_network(self, client, mock_template, network_with_data):
        net_to_update = network_with_data
        template = mock_template

        # Test the links as it's easier
        empty_links = []
        for l in net_to_update.links:
            if l.types is None:
                empty_links.append(l.id)

        # Add the resource attributes to the links, so we can guarantee
        # that these links will match those in the template.
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

        network = client.update_network(net_to_update)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == 'Default Node'

        client.apply_template_to_network(template.id, network.id)

        network = client.get_network(network.id)

        assert len(network.types) == 2
        assert 'Network Type' in [t.name for t in network.types]
        for l in network.links:
            if l.id in empty_links:
                assert l.types is not None
                assert len(l.types) == 1
                assert l.types[0].name == 'Link type'

        # THe assignment of the template hasn't affected the nodes
        # as they do not have the appropriate attributes.
        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == 'Default Node'

    def test_apply_template_to_network_twice(self, client, mock_template, network_with_data):
        net_to_update = network_with_data
        template = mock_template

        # Test the links as it's easier
        empty_links = []
        for l in net_to_update.links:
            if l.types is None:
                empty_links.append(l.id)

        # Add the resource attributes to the links, so we can guarantee
        # that these links will match those in the template.
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

        network = client.update_network(net_to_update)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == 'Default Node'

        client.apply_template_to_network(template.id, network.id)

        client.apply_template_to_network(template.id, network.id)

        network = client.get_network(network.id)

        assert len(network.types) == 2

        assert 'Network Type' in [t.name for t in network.types]

        for l in network.links:
            if l.id in empty_links:
                assert l.types is not None
                assert len(n.types) == 1
                assert l.types[0].name == 'Link type'

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == 'Default Node'


    def test_remove_template_from_network(self, client, network_with_data):
        network = network_with_data
        template_id = network.types[0].template_id

        # Test the links as it's easier
        empty_links = []
        for l in network.links:
            if l.types is None or len(l.types) == 0:
                empty_links.append(l.id)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == "Default Node"

        client.apply_template_to_network(template_id, network.id)

        client.remove_template_from_network(network.id, template_id, 'N')

        network_2 = client.get_network(network.id)

        assert len(network_2.types) == 0
        for l in network_2.links:
            if l.id in empty_links:
                assert len(l.types) == 0

        for n in network_2.nodes:
            assert len(n.types) == 0

    def test_remove_template_and_attributes_from_network(self, client, mock_template, network_with_data):
        network = network_with_data
        template = mock_template

        # Test the links as it's easier
        empty_links = []
        for l in network.links:
            if l.types is None:
                empty_links.append(l.id)

        for n in network.nodes:
            assert len(n.types) == 1
            assert n.types[0].name == 'Default Node'

        network_1 = client.get_network(network.id)
        assert len(network_1.types) == 1

        client.apply_template_to_network(template.id, network.id)

        network_3 = client.get_network(network.id)
        assert len(network_3.types) == 2

        client.remove_template_from_network(network.id, template.id, 'Y')

        network_2 = client.get_network(network.id)

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
                for la in l.attributes:
                    assert la.attr_id not in link_attrs

        node_attrs = []
        for tt in template.templatetypes:
            if tt.resource_type != 'NODE':
                continue
            for ta in tt.typeattrs:
                attr_id = ta.attr_id
                if attr_id not in node_attrs:
                    node_attrs.append(attr_id)
                node_attrs.append(ta.attr_id)

        for n in network_2.nodes:
            assert len(n.types) == 1
            if n.attributes is not None:
                for node_attr in n.attributes:
                    assert node_attr.attr_id not in node_attrs

    def test_validate_attr(self, client, network_with_data):
        network = network_with_data

        scenario = network.scenarios[0]
        rs_ids = [rs.resource_attr_id for rs in scenario.resourcescenarios]
        template_id = network.nodes[0].types[0].template_id

        for n in network.nodes:
            node_type = client.get_templatetype(n.types[0].id)
            for ra in n.attributes:
                for type_attr in node_type.typeattrs:
                    if ra.attr_id == type_attr.attr_id and ra.id in rs_ids and type_attr.data_restriction is not None:
                #        logging.info("Validating RA %s in scenario %s", ra.id, scenario.id)
                        error = client.validate_attr(ra.id, scenario.id, template_id)
                        if error not in (None, {}):
                            assert error.ref_id == n.id


    def test_validate_attrs(self, client, network_with_data):
        network = network_with_data

        scenario = network.scenarios[0]

        ra_ids = []

        for rs in scenario.resourcescenarios:
            ra_ids.append(rs.resource_attr_id)

        template_id = network.types[0].template_id

        errors = client.validate_attrs(ra_ids, scenario.id, template_id)

        assert len(errors) > 0

    def test_validate_scenario(self, client, network_with_data):
        network = network_with_data

        scenario = network.scenarios[0]
        template_id = network.nodes[0].types[0].template_id

        errors = client.validate_scenario(scenario.id,template_id)

        assert len(errors) > 0

    def test_validate_network(self, client, network_with_data):
        network = network_with_data

        client.testutils.update_template(network.types[0].template_id)

        scenario = network.scenarios[0]
        networktype = network.nodes[0].types[0]
        # Validate the network without data: should pass as the network is built
        # based on the template in these unit tests
        errors1 = client.validate_network(network.id, networktype.template_id)
        # The network should have an error, saying that the template has net_attr_c,
        # but the network does not
        assert len(errors1) == 1
        assert errors1[0].find('net_attr_d') > 0

        # Validate the network with data. Should fail as one of the attributes (node_attr_3)
        # is specified as being a 'Cost' in the template but 'Speed' is the dimension
        # of the dataset used. In addition, node_attr_1 specified a unit of 'm^3'
        # whereas the timeseries in the data is in 'cm^3', so each will fail on unit
        # mismatch also.
        errors2 = client.validate_network(network.id, networktype.template_id,
                                                  scenario.id)

        assert len(errors2) > 0
        # every node should have an associated error, plus the network error from above
        assert len(errors2) == len(network['nodes'] * 2)+1
        for err in errors2[1:]:
            try:
                assert err.startswith("Unit mismatch")
            except AssertionError:
                assert err.startswith("Dimension mismatch")

    def test_type_compatibility(self, client, mock_template, mock_template_copy):
        """
            Check function that thests whether two types are compatible -- the
            overlapping attributes are specified with the same unit.
            Change the unit associated with an attribute for types in two idencical
            templates, and test. There should be 1 error returned.
            THen test comparison of identical types. No errors should return.
        """
        template_1 = mock_template
        template_2 = mock_template_copy

        diff_type_1_id = None
        same_type_1_id = None
        for typ in template_1.templatetypes:
            if typ.typeattrs:
                for ta in typ.typeattrs:
                    if ta.attr.name == 'node_attr_1':
                        diff_type_1_id = typ.id
                        ta.unit_id = client.get_unit_by_abbreviation("m^3").id
                    elif ta.attr.name == 'link_attr_1':
                        same_type_1_id = typ.id

        updated_template_1 = JSONObject(client.update_template(template_1))

        diff_type_2_id = None
        same_type_2_id = None
        for typ in template_2.templatetypes:
            if typ.typeattrs:
                for ta in typ.typeattrs:
                    if ta.attr.name == 'node_attr_1':
                        diff_type_2_id = typ.id
                        ta.unit_id = client.get_unit_by_abbreviation("cm^3").id
                    elif ta.attr.name == 'link_attr_1':
                        same_type_2_id = typ.id

        # Before updating template 2, check compatibility of types, where T1 has
        # a unit, but t2 does not.
        errors_diff = client.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = client.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

        # Now update T2 so that the types have conflicting units.
        updated_template_2 = JSONObject(client.update_template(template_2))

        errors_diff = client.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = client.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0

        for typ in updated_template_1.templatetypes:
            if typ.typeattrs:
                for ta in typ.typeattrs:
                    if ta.attr.name == 'node_attr_1':
                        ta.unit_id = None

        # Update template 1 now so that it has no unit, but template 2 does.
        updated_template_1_a = client.update_template(updated_template_1)
        errors_diff = client.check_type_compatibility(diff_type_1_id, diff_type_2_id)
        assert len(errors_diff) == 1
        errors_same = client.check_type_compatibility(same_type_1_id, same_type_2_id)
        assert len(errors_same) == 0
