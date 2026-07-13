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
import json
import datetime
import logging

import hydra_base
from hydra_base.util.hydra_dateutil import get_datetime
from hydra_base.lib.objects import JSONObject, Dataset
from hydra_base import config

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


class TestUtil:
    def __init__(self, client):
        self.client = client
        #avoid calling the 'get unit function a million times'
        self.unitmap = {}

    def get_unit(self, abbreviation):
        """
            Get the ID of a unit from its abbreviation
        """
        if self.unitmap.get(abbreviation) is None:
            unit = self.client.get_unit_by_abbreviation(abbreviation)
            self.unitmap[abbreviation] = unit
            return unit

        return self.unitmap[abbreviation]

    def create_user(self, name, role='admin'):

        existing_user = self.client.get_user_by_name(name)
        if existing_user not in (None, {}):
            return existing_user

        user = JSONObject(dict(
            username=name,
            password="password",
            display_name="test user",
        ))

        new_user = JSONObject(self.client.add_user(user))

        role = JSONObject(self.client.get_role_by_code(role))

        self.client.set_user_role(new_user_id=new_user.id, role_id=role.id)

        return new_user

    def update_template(self, template_id):

        template = JSONObject(self.client.get_template(template_id))
        new_net_attr = self.create_attribute("net_attr_d", dimension='Monetary Value')

        for tmpltype in template.templatetypes:
            if tmpltype.resource_type == 'NETWORK':
                typeattr_1 = JSONObject(dict(
                    attr_id = new_net_attr.id,
                    data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1},
                    unit_id = self.get_unit('USD').id,
                ))
                tmpltype.typeattrs.append(typeattr_1)
                break

        template = self.client.update_template(template)


    def create_template(self):

        net_attr1 = self.create_attribute("net_attr_a", dimension='Volume')
        net_attr2 = self.create_attribute("net_attr_c", dimension=None)
        link_attr_1 = self.create_attribute("link_attr_a", dimension='Pressure')
        link_attr_2 = self.create_attribute("link_attr_b", dimension='Speed')
        link_attr_3 = self.create_attribute("link_attr_c", dimension='Length')
        node_attr_1 = self.create_attribute("node_attr_a", dimension='Volume')
        node_attr_2 = self.create_attribute("node_attr_b", dimension='Speed')
        node_attr_3 = self.create_attribute("node_attr_c", dimension='Monetary value')
        node_attr_4 = self.create_attribute("node_attr_d", dimension='Volumetric flow rate')
        group_attr_1 = self.create_attribute("grp_attr_1", dimension='Monetary value')
        group_attr_2 = self.create_attribute("grp_attr_2", dimension=None)

        template = JSONObject()
        template['name'] = 'Default Template ' + str(datetime.datetime.now())


        types = []
        #**********************
        #network type         #
        #**********************
        net_type = JSONObject()
        net_type.name = "Default Network"
        net_type.alias = "Test type alias"
        net_type.resource_type = 'NETWORK'

        typeattrs = []

        typeattr_1 = JSONObject()
        typeattr_1.attr_id = net_attr1.id
        typeattr_1.data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1}
        typeattr_1.unit_id = self.get_unit('m^3').id
        typeattrs.append(typeattr_1)

        typeattr_2 = JSONObject()
        typeattr_2.attr_id = net_attr2.id
        typeattrs.append(typeattr_2)

        net_type.typeattrs = typeattrs

        types.append(net_type)
        #**********************
        # node type           #
        #**********************
        node_type = JSONObject()
        node_type.name = "Default Node"
        node_type.alias = "Test type alias"
        node_type.resource_type = 'NODE'

        typeattrs = []

        typeattr_1 = JSONObject()
        typeattr_1.attr_id = node_attr_1.id
        typeattr_1.data_restriction = {'LESSTHAN': 10, 'NUMPLACES': 1}
        typeattr_1.unit_id = self.get_unit('m^3').id
        typeattrs.append(typeattr_1)

        typeattr_2 = JSONObject()
        typeattr_2.attr_id = node_attr_2.id
        typeattr_2.data_restriction = {'INCREASING': None}
        typeattrs.append(typeattr_2)

        typeattr_3 = JSONObject()
        typeattr_3.attr_id = node_attr_3.id
        typeattrs.append(typeattr_3)

        typeattr_4 = JSONObject()
        typeattr_4.attr_id = node_attr_4.id
        typeattr_4.unit_id = self.get_unit("m^3 s^-1").id
        typeattrs.append(typeattr_4)

        node_type.typeattrs = typeattrs

        types.append(node_type)
        #**********************
        #link type            #
        #**********************
        link_type = JSONObject()
        link_type.name = "Default Link"
        link_type.resource_type = 'LINK'

        typeattrs = []

        typeattr_1 = JSONObject()
        typeattr_1.attr_id = link_attr_1.id
        typeattrs.append(typeattr_1)

        typeattr_2 = JSONObject()
        typeattr_2.attr_id = link_attr_2.id
        typeattrs.append(typeattr_2)

        typeattr_3 = JSONObject()
        typeattr_3.attr_id = link_attr_3.id
        typeattrs.append(typeattr_3)

        link_type.typeattrs = typeattrs

        types.append(link_type)

        #**********************
        #group type           #
        #**********************
        group_type = JSONObject()
        group_type.name = "Default Group"
        group_type.resource_type = 'GROUP'

        typeattrs = []

        typeattr_1 = JSONObject()
        typeattr_1.attr_id = group_attr_1.id
        typeattrs.append(typeattr_1)

        typeattr_2 = JSONObject()
        typeattr_2.attr_id = group_attr_2.id
        typeattrs.append(typeattr_2)

        group_type.typeattrs = typeattrs

        types.append(group_type)

        template.templatetypes = types

        new_template_i = self.client.add_template(template)
        new_template = JSONObject(new_template_i)

        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.id is not None, "New Template has no ID!"
        assert new_template.id > 0, "New Template has incorrect ID!"

        assert len(new_template.templatetypes) == len(types), "Resource types did not add correctly"
        for t in new_template.templatetypes[1].typeattrs:
            assert t.attr_id in (node_attr_1.id, node_attr_2.id, node_attr_3.id, node_attr_4.id),\
                "Node types were not added correctly!"

        for t in new_template.templatetypes[2].typeattrs:
            assert t.attr_id in (link_attr_1.id, link_attr_2.id, link_attr_3.id),\
                "Link types were not added correctly!"

        return new_template

    def create_templatetype(self, template_id):
        """
            Create a template type object (but don't add it to the DB)
            Args:
                template_id: The ID of the template to which to add this type
            Returns:
                JSONObject with some default values and the correct template ID
        """
        attr_1 = self.create_attribute("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attribute("link_attr_2", dimension='Speed')
        attr_3 = self.create_attribute("link_attr_3", dimension='Volume')

        templatetype = JSONObject()
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = "%s alias" % templatetype.name
        templatetype.resource_type = 'LINK'
        templatetype.template_id = template_id
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

        return templatetype

    def create_child_template(self, parent_id):
        """
            Make a template which is a child of a parent template.
        """

        net_attr_child = self.create_attribute("net_attr_child", dimension=None)
        link_attr_child = self.create_attribute("link_attr_child", dimension='Length')
        node_attr_child = self.create_attribute("node_attr_child", dimension='Volumetric flow rate')
        group_attr_child = self.create_attribute("grp_attr_child", dimension='Volume')

        name = 'Child Template ' + str(datetime.datetime.now())

        child_template_j = self.client.add_child_template(parent_id, name=name)

        return child_template_j

    def create_project(self, name=None, share=True, parent_id=None):

        if name is None:
            name = "Unittest Project"

        user_projects = self.client.get_project_by_name(name)

        if len(user_projects) == 0:
            project = JSONObject()
            project.name = name
            project.description = "Project which contains all unit test networks"
            project.parent_id = parent_id
            project = JSONObject(self.client.add_project(project))
            if share is True:
                self.client.share_project(
                    project.id,
                    ["UserA", "UserB", "UserC"], 'N', 'Y')

            proj_scoped_attr = self.create_attribute("Project Scoped Attr", dimension=None, project_id=project.id)


            return project
        else:
            return user_projects[0]


    def create_link(self, link_id, node_1_name, node_2_name, node_1_id, node_2_id):

        ra_array = []

        link = JSONObject({
            'id'          : link_id,
            'name'        : "%s_to_%s"%(node_1_name, node_2_name),
            'description' : 'A test link between two nodes.',
            'layout'      : None,
            'node_1_id'   : node_1_id,
            'node_2_id'   : node_2_id,
            'attributes'  : ra_array,
        })

        return link

    def create_node(self, node_id, attributes=None, node_name="Test Node Name"):

        if attributes is None:
            attributes = []
        #turn 0 into 1, -1 into 2, -2 into 3 etc..
        coord = (node_id * -1) + 1
        node = JSONObject({
            'id' : node_id,
            'name' : node_name,
            'description' : "A node representing a water resource",
            'layout'      : None,
            'x' : 10 * coord,
            'y' : 10 * coord -1,
            'attributes' : attributes,
        })

        return node

    def build_network(self, project_id=None, num_nodes=10, new_proj=True, map_projection='EPSG:4326'):
        start = datetime.datetime.now()
        if project_id is None:
            proj_name = None

            if new_proj is True:
                proj_name = "Test Project @ %s"%(datetime.datetime.now())
                project_id = self.create_project(name=proj_name).id
            else:
                project_id = project_id

        LOG.debug("Project creation took: %s"%(datetime.datetime.now()-start))
        start = datetime.datetime.now()

        template = self.create_template()

        LOG.debug("Attribute creation took: %s"%(datetime.datetime.now()-start))
        start = datetime.datetime.now()

        # Put an attribute on a group
        group_ra = JSONObject(dict(
            ref_id = None,
            ref_key = 'GROUP',
            attr_is_var = 'N',
            attr_id = template.templatetypes[2].typeattrs[0].attr_id,
            id = -1
        ))
        group_attrs = [group_ra]

        nodes = []
        links = []

        prev_node = None
        ra_index = 2

        network_type = template.templatetypes[0]
        node_type = template.templatetypes[1]
        link_type = template.templatetypes[2]
        for n in range(num_nodes):
            node = self.create_node(n*-1, node_name="Node %s"%(n))

            #From our attributes, create a resource attr for our node
            #We don't assign data directly to these resource attributes. This
            #is done when creating the scenario -- a scenario is just a set of
            #data for a given list of resource attributes.
            node_ra1 = JSONObject(dict(
                ref_key = 'NODE',
                ref_id = None,
                attr_id = node_type.typeattrs[0].attr_id,
                id = ra_index * -1,
                attr_is_var = 'N',
            ))
            ra_index = ra_index + 1
            node_ra2 = JSONObject(dict(
                ref_key = 'NODE',
                ref_id = None,
                attr_id = node_type.typeattrs[1].attr_id,
                id = ra_index * -1,
                attr_is_var = 'Y',
            ))
            ra_index = ra_index + 1
            node_ra3 = JSONObject(dict(
                ref_key = 'NODE',
                ref_id = None,
                attr_id = node_type.typeattrs[2].attr_id,
                id = ra_index * -1,
                attr_is_var = 'N',
            ))
            ra_index = ra_index + 1
            node_ra4 = JSONObject(dict(
                ref_key = 'NODE',
                ref_id = None,
                attr_id = node_type.typeattrs[3].attr_id,
                id = ra_index * -1,
                attr_is_var = 'N',
            ))
            ra_index = ra_index + 1

            node.attributes = [node_ra1, node_ra2, node_ra3, node_ra4]

            type_summary = JSONObject(dict(
                template_id = template.id,
                template_name = template.name,
                id = node_type.id,
                name = node_type.name
            ))

            type_summary_arr = [type_summary]

            node.types = type_summary_arr

            nodes.append(node)

            if prev_node is not None:
                #Connect the two nodes with a link
                link = self.create_link(
                    n*-1,
                    node['name'],
                    prev_node['name'],
                    node['id'],
                    prev_node['id'])

                link_ra1 = JSONObject(dict(
                    ref_id = None,
                    ref_key = 'LINK',
                    id = ra_index * -1,
                    attr_id = link_type.typeattrs[0].attr_id,
                    attr_is_var = 'N',
                ))
                ra_index = ra_index + 1
                link_ra2 = JSONObject(dict(
                    ref_id = None,
                    ref_key = 'LINK',
                    attr_id = link_type.typeattrs[1].attr_id,
                    id = ra_index * -1,
                    attr_is_var = 'N',
                ))
                ra_index = ra_index + 1
                link_ra3 = JSONObject(dict(
                    ref_id = None,
                    ref_key = 'LINK',
                    attr_id = link_type.typeattrs[2].attr_id,
                    id = ra_index * -1,
                    attr_is_var = 'N',
                ))
                ra_index = ra_index + 1

                link.attributes = [link_ra1, link_ra2, link_ra3]
                if link['id'] % 2 == 0:
                    type_summary_arr = []
                    type_summary = JSONObject()
                    type_summary.template_id = template.id
                    type_summary.template_name = template.name
                    type_summary.id = link_type.id
                    type_summary.name = link_type.name

                    type_summary_arr.append(type_summary)

                    link.types = type_summary_arr
                links.append(link)

            prev_node = node

        #A network must contain an array of links. In this case, the array

        LOG.debug("Making nodes & links took: %s"%(datetime.datetime.now()-start))
        start = datetime.datetime.now()

        #Create the scenario
        scenario = JSONObject()
        scenario.id = -1
        scenario.name = 'Scenario 1'
        scenario.description = 'Scenario Description'
        scenario.layout = json.dumps({'app': ["Unit Test1", "Unit Test2"]})
        scenario.start_time = datetime.datetime.now()
        scenario.end_time = scenario.start_time + datetime.timedelta(hours=1)
        scenario.time_step = 1 # one second intervals.

        #Multiple data (Called ResourceScenario) means an array.
        scenario_data = []

        group_array = []
        group = JSONObject(dict(
            id = -1,
            name = "Test Group",
            description = "Test group description"
        ))

        group.attributes = group_attrs

        group_array.append(group)

        group_item_array = []
        group_item_1 = JSONObject(dict(
            ref_key = 'NODE',
            ref_id = nodes[0]['id'],
            group_id = group['id'],
        ))
        group_item_2 = JSONObject(dict(
            ref_key = 'NODE',
            ref_id = nodes[1]['id'],
            group_id = group['id'],
        ))
        group_item_array = [group_item_1, group_item_2]

        scenario.resourcegroupitems = group_item_array

        #This is an example of 3 diffent kinds of data

        #For Links use the following:
        #A simple string (Descriptor)
        #A multi-dimensional array.

        #For nodes, use the following:
        #A time series, where the value may be a 1-D array

        nodes[0].attributes
        for n in nodes:
            for na in n.attributes:
                if na.get('attr_is_var', 'N') == 'N':
                    if na['attr_id'] == node_type.typeattrs[0].attr_id:
                        #less than 10 and with 1 decimal place,
                        #as per the restriction in the template
                        dataset = self.create_scalar(na, 1.1, unit='cm^3')
                    elif na['attr_id'] == node_type.typeattrs[2].attr_id:
                        #incorrect unit to test the validation
                        dataset = self.create_timeseries(na, 'cm^3')
                    elif na['attr_id'] == node_type.typeattrs[3].attr_id:
                        dataset = self.create_dataframe(na, unit='m^3 s^-1')
                elif na.get('attr_is_var', 'Y') == 'Y':
                    if na['attr_id'] == node_type.typeattrs[1].attr_id:
                        # correct unit (speed)
                        dataset = self.create_scalar(na, unit='m s^-1')
                scenario_data.append(dataset)
        count = 0
        for l in links:
            for na in l.attributes:
                if na['attr_id'] == link_type.typeattrs[0].attr_id:
                    array = self.create_array(na)
                    scenario_data.append(array)
                    count = count + 1
                elif na['attr_id'] == link_type.typeattrs[1].attr_id:
                    descriptor = self.create_descriptor(na)
                    scenario_data.append(descriptor)
                    count = count + 1

        grp_timeseries = self.create_timeseries(group_attrs[0], 'cm^3')#incorrect unit to test validation

        scenario_data.append(grp_timeseries)

        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios = scenario_data

        #A network can have multiple scenarios, so they are contained in
        #a scenario array
        scenario_array = []
        scenario_array.append(scenario)

        LOG.debug("Scenario definition took: %s"%(datetime.datetime.now()-start))
        #This can also be defined as a simple dictionary, but I do it this
        #way so I can check the value is correct after the network is created.
        layout = dict(
            color = 'red',
            shapefile = 'blah.shp'
        )

        node_array = nodes
        link_array = links

        net_attr = self.create_attribute("net_attr_b", dimension='Pressure')

        ra_index = ra_index + 1
        net_ra_notmpl = JSONObject(dict(
            ref_id = None,
            ref_key = 'NETWORK',
            attr_is_var = 'N',
            attr_id = net_attr.id,
            id = ra_index*-1
        ))
        ra_index = ra_index + 1
        net_ra_tmpl = JSONObject(dict(
            ref_id = None,
            ref_key = 'NETWORK',
            attr_is_var = 'N',
            attr_id = network_type.typeattrs[0].attr_id,
            id = ra_index*-1
        ))
        net_attrs = [net_ra_notmpl, net_ra_tmpl]

        net_type_summary_arr = []
        net_type_summary = JSONObject(dict(
            template_id = template.id,
            template_name = template.name,
            id = network_type.id,
            name = network_type.name,
        ))
        net_type_summary_arr.append(net_type_summary)

        network = JSONObject(dict(
            name = 'Network @ %s'%datetime.datetime.now(),
            description = 'Test network with 2 nodes and 1 link',
            project_id = project_id,
            links = link_array,
            nodes = node_array,
            layout = layout,
            scenarios = scenario_array,
            resourcegroups = group_array,
            projection = map_projection,
            attributes = net_attrs,
            types = net_type_summary_arr,
        ))

        return network

    def create_network_with_data(self, project_id=None, num_nodes=10,
                                 ret_full_net=True, new_proj=False,
                                 map_projection='EPSG:4326'):
        """
            Test adding data to a network through a scenario.
            This test adds attributes to one node and then assigns data to them.
            It assigns a descriptor, array and timeseries to the
            attributes node.
        """

        network = self.build_network(project_id, num_nodes, new_proj=new_proj,
                                   map_projection=map_projection)

        #LOG.debug(network)
        start = datetime.datetime.now()
        LOG.info("Creating network...")
        response_network_summary = JSONObject(self.client.add_network(network))

        #Add an attribute scoped to this network, and an associated resource attribute
        net_scoped_attr = self.create_attribute("Network Scoped Attr", dimension=None, network_id=response_network_summary.id)
        self.client.add_resource_attribute('NETWORK', response_network_summary.id, net_scoped_attr.id, False)

        LOG.info("Network Creation took: %s"%(datetime.datetime.now()-start))
        if ret_full_net is True:
            LOG.info("Fetching new network...:")
            start = datetime.datetime.now()
            net = self.client.get_network(response_network_summary.id, include_data=True)
            response_net = JSONObject(net)
            LOG.info("Network Retrieval took: %s"%(datetime.datetime.now()-start))
            self.check_network(network, response_net)

            return response_net
        else:
           return response_network_summary

    def create_network_with_extra_group(self, project_id=None,
                                        num_nodes=10,
                                        ret_full_net=True,
                                        new_proj=False,
                                        map_projection='EPSG:4326'):
        """
            Test adding data to a network through a scenario.
            This test adds attributes to one node and then assigns data to them.
            It assigns a descriptor, array and timeseries to the
            attributes node.
        """
        network = self.create_network_with_data(
                                 project_id=project_id,
                                 num_nodes=num_nodes,
                                 ret_full_net=ret_full_net,
                                 new_proj=new_proj,
                                 map_projection=map_projection)

        group = JSONObject({})
        group.network_id = network.id
        group.id = -1
        group.name = 'test new group'
        group.description = 'test new group'

        template_id = network.types[0].template_id
        template = JSONObject(self.client.get_template(template_id))

        type_summary_arr = []

        type_summary = JSONObject({})
        type_summary.id = template.id
        type_summary.name = template.name
        type_summary.id = template.templatetypes[2].id
        type_summary.name = template.templatetypes[2].name

        type_summary_arr.append(type_summary)

        group.types = type_summary_arr

        new_group = self.client.add_group(network.id, group)

        group_attr_ids = []
        for resource_attr in new_group.attributes:
            group_attr_ids.append(resource_attr.attr_id)

        updated_network = self.client.get_network(network.id)

        return updated_network

    def create_network_with_child_scenario(self, project_id=None,
                                        num_nodes=10,
                                        ret_full_net=True,
                                        new_proj=False,
                                        map_projection='EPSG:4326',
                                        levels=2):
        """
            Create a network with two scenarios -- one with data in it, and its child containing
            none of its own data.
        """
        network = self.create_network_with_data(
                                 project_id=project_id,
                                 num_nodes=num_nodes,
                                 ret_full_net=ret_full_net,
                                 new_proj=new_proj,
                                 map_projection=map_projection)
        parent_scenario = network.scenarios[0]
        parent_scenario_id = parent_scenario.id
        scenario_count = len(network.scenarios) + 1
        for level in range(levels-1):
            new_scenario_j = self.client.create_child_scenario(parent_scenario_id, "Scenario {0}".format(scenario_count))
            parent_scenario_id = new_scenario_j.id
            scenario_count = scenario_count + 1


        updated_network = JSONObject(self.client.get_network(network.id, include_data=True))

        return updated_network



    def get_network(self, network_id=None):
        """
            Get a network with all data.
        """
        network = JSONObject(self.client.get_network(network_id))
        return network



    def check_network(self, request_net, response_net):
        assert response_net.layout == request_net['layout']

        assert response_net.scenarios[0].created_by is not None

        for n in response_net.nodes:
            assert n.x is not None
            assert n.y is not None
            assert len(n.attributes) > 0

        before_times = []

        s = request_net['scenarios'][0]
        print(s['resourcescenarios'])
        ordered_rs_request = sorted(s['resourcescenarios'], key=lambda x: str(x.dataset.value))
        for rs0 in ordered_rs_request:

            if rs0.dataset.type == 'timeseries':
                val = json.loads(rs0.dataset.value)
                before_ts_times = list(val.values())[0].keys()
                before_times = []
                for t in before_ts_times:
                    try:
                        before_times.append(get_datetime(t))
                    except Exception as err:
                        before_times.append(t)

        after_times = []
        s = response_net.scenarios[0]
        ordered_rs_response = sorted(s.resourcescenarios, key=lambda x: str(x.dataset.value))

        for rs0 in ordered_rs_response:
            if rs0.dataset.type == 'timeseries':
                val = json.loads(rs0.dataset.value)
                after_ts_times = list(val.values())[0].keys()
                after_times = []
                for t in after_ts_times:
                    try:
                        after_times.append(get_datetime(t))
                    except Exception as err:
                        after_times.append(t)

        for d in after_times:
            assert d in before_times, f"{d} is not in {before_times}"


    def create_scalar(self, resource_attr, val=1.234, unit='m^3'):
        #with a resource attribute.

        dataset = Dataset(dict(
            id=None,
            type = 'scalar',
            name = 'Flow speed',
            unit_id =  self.get_unit(unit).id,
            hidden = 'N',
            value = val,
        ))

        scenario_attr = JSONObject(dict(
            attr_id=resource_attr.attr_id,
            resource_attr_id=resource_attr.id,
            dataset=dataset,
        ))

        return scenario_attr

    def create_descriptor(self, resource_attr, val="test"):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        dataset = Dataset(dict(
            id=None,
            type = 'descriptor',
            name = 'Flow speed',
            unit_id = self.get_unit('m s^-1').id, # This does not match the type on purpose, to test validation
            hidden = 'N',
            value = val,
        ))

        scenario_attr = JSONObject(dict(
            attr_id = resource_attr.attr_id,
            resource_attr_id = resource_attr.id,
            dataset = dataset,
        ))

        return scenario_attr


    def create_timeseries(self, resource_attr, unit='m^3'):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        #[[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]]]

        fmt = hydra_base.config.get('DEFAULT', 'datetime_format', "%Y-%m-%dT%H:%M:%S.%f000Z")

        t1 = datetime.datetime.now(datetime.timezone.utc)
        t2 = t1+datetime.timedelta(hours=1)
        t3 = t1+datetime.timedelta(hours=2)

        val_1 = [[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]], [[9, 8, 7],[6, 5, 4]]]
        val_2 = [1.0, 2.0, 3.0]

        val_3 = [3.0, None, None]

        ts_val = {"test_column": {t1.strftime(fmt): val_1,
                      t2.strftime(fmt): val_2,
                      t3.strftime(fmt): val_3}}

        metadata = {'created_by': 'Test user'}

        dataset = Dataset(dict(
            id=None,
            type = 'timeseries',
            name = 'my time series',
            unit_id = self.get_unit(unit).id, # This does not match the type on purpose, to test validation
            hidden = 'N',
            value = json.dumps(ts_val),
            metadata = metadata
        ))

        scenario_attr = JSONObject(dict(
            attr_id = resource_attr.attr_id,
            resource_attr_id = resource_attr.id,
            dataset = dataset,
        ))

        return scenario_attr

    def create_dataframe(self, resource_attr, name='Test Data Frame', dataframe_value=None, unit='m^3 s^-1'):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        if dataframe_value is None:
            val_1 = 1
            val_2 = 2
            val_3 = 3

            dataframe_value = {"test_column": {'key1': val_1,
                        'key2': val_2,
                        'key3': val_3}}

        metadata = {'created_by': 'Test user'}

        dataset = Dataset(dict(
            id=None,
            type = 'dataframe',
            name = name,
            unit_id = self.get_unit(unit).id,
            hidden = 'N',
            value = json.dumps(dataframe_value),
            metadata = metadata
        ))

        scenario_attr = JSONObject(dict(
            attr_id = resource_attr.attr_id,
            resource_attr_id = resource_attr.id,
            dataset = dataset,
        ))

        return scenario_attr

    def create_array(self, resource_attr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        #[[1, 2, 3], [4, 5, 6], [7, 8, 9]]

        arr = json.dumps([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])

        metadata_array = json.dumps({'created_by': 'Test user'})

        dataset = Dataset(dict(
            id=None,
            type = 'array',
            name = 'my array',
            unit_id = self.get_unit('bar').id,
            hidden = 'N',
            value = arr,
            metadata = metadata_array,
        ))

        scenario_attr = JSONObject(dict(
            attr_id = resource_attr.attr_id,
            resource_attr_id = resource_attr.id,
            dataset = dataset,
        ))

        return scenario_attr


    def create_attributes(self):
        dimension = "Volumetric flow rate"
        attrs = []
        attrs.append(
            JSONObject({
                'name'  : "Multi-added Attr 1",
                'dimension_id' : self.client.get_dimension_by_name(dimension).id,
                'description' : "Attribute 1 from a test of adding multiple attributes",
            })
        )
        attrs.append(
            JSONObject({
                'name' : "Multi-added Attr 2",
                'dimension_id' : self.client.get_dimension_by_name(dimension).id,
                'description' : "Attribute 2 from a test of adding multiple attributes",
            })
        )

        existing_attrs = []
        new_attrs = []

        for a in attrs:
            LOG.info("Getting attribute %s, %s", a.name, a.dimension_id)

            attr = self.client.get_attribute_by_name_and_dimension(a.name,
                                                                   a.dimension_id)
            if attr is not None and attr != {}:
                # The attribute already exists
                existing_attrs.append(attr)
            else:
                # The attribute is new
                new_attrs.append(a)

        added_attrs = self.client.add_attributes(new_attrs)

        # The existing attributes summed with the inserted ones are the total of "attrs" array
        total_attrs = existing_attrs + added_attrs

        assert len(total_attrs) == len(attrs)

        for my_attr in total_attrs:
            assert len(list(filter(lambda x: x.description == my_attr.description, attrs))) > 0

        return attrs

    def create_attribute(self, name="Test attribute", dimension=None, network_id=None, project_id=None):
        dimension_id = None
        if dimension is not None:
            dimension_id = self.client.get_dimension_by_name(dimension).id

        attr_i = self.client.get_attribute_by_name_and_dimension(name, dimension_id, network_id, project_id)
        if attr_i is None or attr_i == {}:
            attr = JSONObject({
                'name'  : name,
                'dimension_id' : dimension_id,
                'description' : "Attribute description",
                'network_id' : network_id,
                'project_id': project_id
            })
            attr = JSONObject(self.client.add_attribute(attr))
        else:
            attr = JSONObject(attr_i)

        return attr

    def create_attributegroup(self, project_id, name=None, exclusive='N'):

        if name is None:
            name = "Attribute Group %s" % (datetime.datetime.now(),)

        newgroup = JSONObject({
            'project_id'  : project_id,
            'name'        : name,
            'description' : "A description of an attribute group",
            'layout'      : {"color": "green"},
            'exclusive'   : exclusive,
            })

        newgroup = self.client.add_attribute_group(newgroup)
        return newgroup

    def get_by_name(self, name, entity_list):
        """
            given a list of JSONObjects with a name attribute, return the JSONObject with the
            specified name
        """
        entity_dict = {}
        for e in entity_list:
            entity_dict[e.name] = e

        return entity_dict[name]


    def create_dataset(self):
        """
            Creates a dataset and adds it to the DB
        """
        arr = json.dumps([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
        metadata_array = {'created_by': 'Test user'}
        u = self.get_unit('bar')
        # dataset = Dataset(dict(
        #     id=None,
        #     type = 'array',
        #     name = 'my array',
        #     unit_id = u.id,
        #     hidden = 'N',
        #     value = arr,
        #     metadata = metadata_array,
        # ))
        return  self.client.add_dataset(
            'array',
            arr,
            u.id,
            metadata = json.dumps(metadata_array),
            name = 'my array',
            flush = True
        )
