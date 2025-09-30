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

import copy
import datetime
import pytest
import datetime
import json

import hydra_base as hb

import logging
log = logging.getLogger(__name__)

class TestNetwork:
    """
        Test for network-based functionality
    """

    def test_add_network_unknown_attribute(self, client, projectmaker):
        project = projectmaker.create('test')
        project_id = project.id

        net_ra_bad_attr_id = dict(
            ref_id = None,
            ref_key = 'NETWORK',
            attr_is_var = 'N',
            attr_id = 999, # an unknown attribute ID~
            id = -1
        )

        network = dict(
            name = 'Network @ %s'%datetime.datetime.now(),
            description = 'Test network with 2 nodes and 1 link',
            project_id = project_id,
            links = [],
            nodes = [],
            layout = {},
            scenarios = [],
            resourcegroups = [],
            projection = None,
            attributes = [net_ra_bad_attr_id],
        )


        with pytest.raises(hb.exceptions.HydraError):
            client.add_network(network)



    def test_get_network_with_template(self, client, network_with_data):
        """

        """
        net = network_with_data
        logging.info("%s nodes before", len(net.nodes))
        #All the nodes are in this template, so return them all
        assert len(net.nodes) == 10
        #The type has only 2 attributes, so these are the only
        #ones which should be returned.
        for n in net.nodes:
            assert len(n.attributes) == 4

        #only 4 of the links in the network have a type, so only these
        #4 should be returned.
        logging.info("%s links before", len(net.links))
        assert len(net.links) == 9
        #of the 4 links returned, ensure the two attributes are on each one.
        for l in net.links:
            if l.types is not None:
                assert len(l.attributes) == 3
            else:
                assert len(l.attributes) == 3
        assert len(net.resourcegroups) == 1

        template_id = net.nodes[0].types[0].template_id

        filtered_net = client.get_network(net.id, template_id=template_id)
        logging.info("%s nodes after",len(filtered_net.nodes))
        #All the nodes are in this template, so return them all
        assert len(filtered_net.nodes) == 10

        assert len(filtered_net.attributes) == 2

        #The type has only 2 attributes, so these are the only
        #ones which should be returned.
        for n in filtered_net.nodes:
            assert len(n.attributes) == 4
        #only 4 of the links in the network have a type, so only these
        #4 should be returned.
        logging.info("%s links after", len(filtered_net.links))
        assert len(filtered_net.links) == 4
        #of the 4 links returned, ensure the two attributes are on each one.
        for l in filtered_net.links:
            assert len(l.attributes) == 3

        assert len(filtered_net.resourcegroups) == 0

        unfiltered_net = client.get_network(net.id)

        assert len(unfiltered_net.attributes) == 4

        filtered_net_with_extra_attributes = client.get_network(
            net.id,
            template_id=template_id,
            include_non_template_attributes=True)

        assert len(filtered_net.attributes) == len(filtered_net_with_extra_attributes.attributes) - 2

    def test_get_resources_of_type(self, client, network_with_data):
        """
            Test for the retrieval of all the resources of a specified
            type within a network.
        """

        net = network_with_data
        link_ids = []
        type_id = None
        for l in net.links:
            if l.types:
                if type_id is None:
                    type_id = l.types[0].id
                link_ids.append(l.id)

        resources_of_type = list(client.get_resources_of_type(net.id, type_id))

        #this returns a tuple of the node, link and group types. Link types are at
        #index 1 of this tuple
        assert len(resources_of_type) == 4
        for r in list(resources_of_type):
            assert r.ref_key == 'LINK'
            assert r.id in link_ids

    def test_get_all_resource_attributes_in_network(self, client, network_with_data):
        """
            Test to retrieve all the resourceattributes relating to the specified
            attribute ID in the network, regardless of what node, link group etc
            they belong to.
        """
        test_attr_id = None
        for a in network_with_data.nodes[0].attributes:
            test_attr_id = a.attr_id
            break


        all_network_resource_attrs = client.get_all_resource_attributes_in_network(
            test_attr_id,
            network_with_data.id
        )

        #Find the attribute that ALL nodes have.
        assert len(all_network_resource_attrs) == len(network_with_data.nodes)


    def test_get_network_1(self, client, networkmaker):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = networkmaker.create(map_projection='EPSG:21781')
        scenario_id = net.scenarios[0].id

        clone = client.clone_scenario(scenario_id)
        new_scenario = client.get_scenario(clone.id)

        full_network = client.get_network(new_scenario.network_id, include_data=False)

        for s in full_network.scenarios:
            assert len(s.resourcescenarios) == 0

        scen_ids = [scenario_id]
        partial_network = client.get_network(new_scenario.network_id, include_attributes=True, include_data=True, include_results=True, scenario_ids=scen_ids)

        assert len(partial_network.scenarios) == 1
        assert len(full_network.scenarios) == 2

        for s in partial_network.scenarios:
            assert len(s.resourcescenarios) > 0

        network_with_results = client.get_network(new_scenario.network_id, include_attributes=True, include_data=True, scenario_ids=scen_ids)
        network_no_results = client.get_network(new_scenario.network_id, include_attributes=True, include_data=True, include_results=False, scenario_ids=scen_ids)

        sample_rs= network_with_results.scenarios[0].resourcescenarios[0]
        #there should be one more result in the
        assert len(network_with_results.scenarios[0].resourcescenarios) == len(network_no_results.scenarios[0].resourcescenarios) + 10
        metadata = json.loads(sample_rs.dataset.metadata) if isinstance(sample_rs.dataset.metadata, str) else sample_rs.dataset.metadata
        assert len(metadata) == 0

        network_with_results_and_metadata = client.get_network(new_scenario.network_id, include_attributes=True, include_data=True, scenario_ids=scen_ids, template_id=None, include_non_template_attributes=None, include_metadata=True)

        sample_rs= network_with_results_and_metadata.scenarios[0].resourcescenarios[0]
        metadata = json.loads(sample_rs.dataset.metadata) if isinstance(sample_rs.dataset.metadata, str) else sample_rs.dataset.metadata
        #there should be one more result in the
        assert len(metadata) > 0



        with pytest.raises(hb.exceptions.HydraError):
            client.get_network_by_name(net.project_id, "I am not a network")

        net_by_name = client.get_network_by_name(net.project_id, net.name)
        assert net_by_name.id == full_network.id

        no_net_exists = client.network_exists(net.project_id, "I am not a network")
        assert no_net_exists == 'N'
        net_exists = client.network_exists(net.project_id, net.name)
        assert net_exists == 'Y'
        assert full_network.projection == 'EPSG:21781'

    def test_get_extents(self, client, network_with_data):
        """
        Extents test: Test that the min X, max X, min Y and max Y of a
        network are retrieved correctly.
        """
        net = network_with_data

        extents = client.get_network_extents(net.id)

        assert extents.min_x == 10
        assert extents.max_x == 100
        assert extents.min_y == 9
        assert extents.max_y == 99

    def test_update_network(self, client, network_with_data):

        net = hb.JSONObject(client.get_network(network_with_data.id))

        link_id = net.links[1].id
        old_node_1_id = net.links[1].node_1_id
        old_node_2_id = net.links[1].node_2_id

        net.links[1].node_1_id = net.nodes[-1].id
        net.links[1].node_2_id = net.nodes[-2].id
        net.links[1].layout = {'color':'red'}

        net.nodes[1].layout = {'color':'green'}

        net.description = \
            'A different network for SOAP unit tests.'

        updated_network = client.update_network(net)

        assert net.id == updated_network.id, \
            'network_id has changed on update.'
        assert net.name == updated_network.name, \
            "network_name changed on update."
        assert updated_network.links[1].id == link_id
        assert updated_network.links[1].node_1_id != old_node_1_id
        assert updated_network.links[1].node_1_id == net.nodes[-1].id
        assert updated_network.links[1].layout['color'] == 'red'

        assert updated_network.links[1].node_2_id != old_node_2_id
        assert updated_network.links[1].node_2_id == net.nodes[-2].id

        assert updated_network.nodes[1].layout['color'] == 'green'


############################################################
    def test_add_links(self, client, projectmaker):

        project = projectmaker.create('test')
        network = hb.JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = hb.JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = hb.JSONObject()
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id

            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = client.add_network(network)
        links = []

        link = hb.JSONObject()
        link.id = i * -1
        link.name = 'New Link'
        link.description = 'Test link ' + str(i)
        link.node_1_id = network.nodes[0].id
        link.node_2_id = network.nodes[2].id
        links.append(link)

        link2 = hb.JSONObject()
        link2.id = i * -2
        link2.name = 'New Link_2'
        link2.description = 'Test link ' + str(i)
        link2.node_1_id = network.nodes[0].id
        link2.node_2_id = network.nodes[2].id
        links.append(link2)

        new_links=client.add_links(network.id, links)

        new_network = client.get_network(network.id)

        assert len(network.links)+len(links) == len(new_network.links); "new nodes were not added correctly_2",

############################################################
    def test_add_link(self, client, projectmaker, template):
        project = projectmaker.create('test')
        network = hb.JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = hb.JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = hb.JSONObject()
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id

            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = client.add_network(network)
        network = client.get_network(network.id)

        link = hb.JSONObject()
        link.id = i * -1
        link.name = 'New Link'
        link.description = 'Test link ' + str(i)
        link.node_1_id = network.nodes[0].id
        link.node_2_id = network.nodes[2].id

        tmpl = template

        type_summary_arr = []

        type_summary      = hb.JSONObject()
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.templatetypes[1].id
        type_summary.name = tmpl.templatetypes[1].name

        type_summary_arr.append(type_summary)

        link.types = type_summary_arr

        new_link = client.add_link(network.id, link)

        link_attr_ids = []
        for resource_attr in new_link.attributes:
            link_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.templatetypes[1].typeattrs:
            assert typeattr.attr_id in link_attr_ids

        new_network = client.get_network(network.id)

        assert len(new_network.links) == len(network.links)+1; "New node was not added correctly"

    def test_add_node(self, client, projectmaker, template):
        project = projectmaker.create('test')
        network = hb.JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = hb.JSONObject()
            node.id = i * -1
            node.name = 'node ' + str(i)
            node.description = 'test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = hb.JSONObject()
            link.id = i * -1
            link.name = 'link ' + str(i)
            link.description = 'test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id

            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'a network for soap unit tests.'
        network.nodes = nodes
        network.links = links

        network = client.add_network(network)
        network = client.get_network(network.id)

        node = hb.JSONObject()
        new_node_num = nnodes + 1
        node.id = new_node_num * -1
        node.name = 'node ' + str(new_node_num)
        node.description = 'test node ' + str(new_node_num)
        node.x = 100
        node.y = 101


        tmpl = template

        type_summary_arr = []

        type_summary      = hb.JSONObject()
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.templatetypes[0].id
        type_summary.name = tmpl.templatetypes[0].name

        type_summary_arr.append(type_summary)

        node.types = type_summary_arr

        new_node = client.add_node(network.id, node)

        node_attr_ids = []
        for resource_attr in new_node.attributes:
            node_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.templatetypes[0].typeattrs:
            assert typeattr.attr_id in node_attr_ids

        new_network = client.get_network(network.id)

        assert len(new_network.nodes) == len(network.nodes)+1; "new node was not added correctly"

    ######################################
    def test_add_nodes(self, client, projectmaker):
        """
        Test add new nodes to network
        """

        project = projectmaker.create('test')
        network = hb.JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = hb.JSONObject()
            node.id = i * -1
            node.name = 'node ' + str(i)
            node.description = 'test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = hb.JSONObject()
            link.id = i * -1
            link.name = 'link ' + str(i)
            link.description = 'test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id
            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'a network for soap unit tests.'
        network.nodes = nodes
        network.links = links

        network = client.add_network(network)
        network = client.get_network(network.id)
        nodes = []

        for i in range (1200):
             node1 = hb.JSONObject()
             new_node_num = nnodes + 1
             node1.id = new_node_num * -1
             node1.name = 'node1_'+str(i)
             node1.description = 'test node ' + str(new_node_num)
             node1.x = 100+i
             node1.y = 101+i
             nodes.append(node1)

        new_nodes=client.add_nodes(network.id, nodes)
        new_network = client.get_network(network.id)

        assert len(network.nodes)+len(nodes) == len(new_network.nodes); "new nodes were not added correctly_2",

    ########################################


    def test_update_node(self, client, network_with_data):
        network = network_with_data

        node_to_update = hb.JSONObject(network.nodes[0])
        node_to_update.name = "Updated Node Name"
        node_to_update.layout      = {'app': ["Unit Test1", "Unit Test2"]}

        new_node = client.update_node(node_to_update)

        new_network = hb.JSONObject(client.get_network(network.id))

        updated_node = None
        for n in new_network.nodes:
            if n.id == node_to_update.id:
                updated_node = n
        assert updated_node.layout is not None
        assert updated_node.layout['app']  == ["Unit Test1", "Unit Test2"]
        assert updated_node.name == "Updated Node Name"

    def test_set_node_status(self, client, network_with_data):
        network = network_with_data

        node_to_delete = network.nodes[0]

        client.set_node_status(node_to_delete.id, 'X')

        new_network = client.get_network(network.id)

        node_ids = []
        for n in new_network.nodes:
            node_ids.append(n.id)
        for l in new_network.links:
            node_ids.append(l.node_1_id)
            node_ids.append(l.node_2_id)
        assert node_to_delete.id not in node_ids

        client.set_node_status(node_to_delete.id, 'A')

        new_network = client.get_network(network.id)

        node_ids = []
        for n in new_network.nodes:
            node_ids.append(n.id)
        link_node_ids = []
        for l in new_network.links:
            link_node_ids.append(l.node_1_id)
            link_node_ids.append(l.node_2_id)
        assert node_to_delete.id in link_node_ids
        assert node_to_delete.id in link_node_ids
        assert node_to_delete.id in node_ids

    def test_update_link(self, client, network_with_data):
        network = network_with_data

        link_to_update = network.links[0]
        link_to_update.name = "Updated link Name"
        link_to_update.layout      = {'app': ["Unit Test1", "Unit Test2"]}

        new_link = client.update_link(link_to_update)

        new_network = client.get_network(network.id)

        updated_link = None
        for l in new_network.links:
            if l.id == link_to_update.id:
                updated_link = l
        assert updated_link.layout is not None

        assert updated_link.layout['app']  == ["Unit Test1", "Unit Test2"]
        assert updated_link.name == "Updated link Name"

    def test_set_link_status(self, client, network_with_data):
        network = network_with_data

        link_to_delete = network.links[0]

        client.set_link_status(link_to_delete.id, 'X')

        new_network = hb.JSONObject(client.get_network(network.id))

        link_ids = []
        for l in new_network.links:
            link_ids.append(l.id)
        assert link_to_delete.id not in link_ids

        client.set_link_status(link_to_delete.id, 'A')
        new_network = client.get_network(network.id)
        link_ids = []
        for l in new_network.links:
            link_ids.append(l.id)
        assert link_to_delete.id in link_ids


    def test_set_network_status(self, client, projectmaker):
        project = projectmaker.create('test')
        network = hb.JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = hb.JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = hb.JSONObject()
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id
            #link = client.add_link(link)

            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = client.add_network(network)

        client.set_network_status(network.id, 'X')

        assert client.get_network(network.id).status == 'X', \
            'Deleting network did not work correctly.'

        client.set_network_status(network.id, 'A')

        assert client.get_network(network.id).status == 'A', \
            'Reactivating network did not work correctly.'

    def test_delete_network(self, client, projectmaker, network_with_data):

        network = client.get_network(network_with_data.id)

        client.delete_network(network_with_data.id, 'N')

        with pytest.raises(hb.exceptions.HydraError):
            client.get_network(network.id)
        with pytest.raises(hb.exceptions.HydraError):
            client.get_node(network.nodes[0].id)

        network_attributes = client.get_resource_attributes('NETWORK', network.id)
        assert len(network_attributes) == 0

    def test_get_node(self, client, network_with_data):
        network = network_with_data
        n = network.nodes[0]
        s = network.scenarios[0]

        node_without_data = client.get_node(n.id)

        for ra in node_without_data.attributes:
            assert not hasattr(ra, 'resourcescenario') or ra.resourcescenario is None

        node_with_data = client.get_node(n.id, s.id)

        attrs_with_data = []
        for ra in node_with_data.attributes:
            if hasattr(ra, 'resourcescenario') and ra.resourcescenario is not None:
                if ra.resourcescenario:
                    assert isinstance(ra.resourcescenario, hb.JSONObject)
                    attrs_with_data.append(ra.id)
        assert len(attrs_with_data) == 4


    def test_get_link(self, client, network_with_data):
        network = network_with_data
        l = network.links[-1]
        s = network.scenarios[0]

        link_without_data = client.get_link(l.id)

        for ra in link_without_data.attributes:
            assert not hasattr(ra, 'resourcescenario') or ra.resourcescenario is None

        link_with_data = client.get_link(l.id, s.id)

        attrs_with_data = []
        for ra in link_with_data.attributes:
            if ra.resourcescenario is not None:
                attrs_with_data.append(ra.id)
        assert len(attrs_with_data) == 2

    def test_cleanup_network(self, client, network_with_data):
        network = network_with_data

        node_to_delete = network.nodes[0]

        link_ids = []
        for l in network.links:
            if l.node_1_id == node_to_delete.id:
                link_ids.append(l.id)
            if l.node_2_id == node_to_delete.id:
                link_ids.append(l.id)

        client.set_node_status(node_to_delete.id, 'X')

        client.clean_up_network(network.id)

        with pytest.raises(hb.exceptions.HydraError):
            client.get_node(node_to_delete.id)

        for l in link_ids:
            with pytest.raises(hb.exceptions.HydraError):
                client.get_link(l)

    def test_validate_topology(self, client, projectmaker):
        project = projectmaker.create('test')
        network = hb.JSONObject({})
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = hb.JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        #NOTE: NOT ADDING ENOUGH LINKS!!
        for i in range(nlinks-1):
            link = hb.JSONObject()
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id

            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = client.add_network(network)

        result = client.validate_network_topology(network.id)
        assert len(result) == 1#This means orphan nodes are present

    def test_consistency_of_update(self, client, network_with_data):
        """
            Test to ensure that updating a network which has not changed
            does not cause any changes to the network.
            Procedure:
            1 Create a network.
            2 Immediately update the network without changing it.
            3 Check that the original network and the updated network are identical.
        """
        net = network_with_data

        for node in net.nodes:
            assert node.types is not None and  len(node.types) > 0

        original_net = client.get_network(net.id)

        updated_net_summary = client.update_network(net)

        updated_net = client.get_network(updated_net_summary.id)

        for node in updated_net.nodes:
            assert node.types is not None and  len(node.types) > 0

        for attr in original_net.keys():
            a = original_net.get(attr)
            b = updated_net[attr]
            #assert str(a) == str(b)
            if attr == 'scenarios':
                for s0 in original_net.scenarios:
                    for s1 in updated_net.scenarios:
                        if s0.id == s1.id:
                            for rs0 in s0.resourcescenarios:
                                for rs1 in s1.resourcescenarios:
                                    if rs0.resource_attr_id == rs1.resource_attr_id:
                                        #Leave these logging in as they are used to test
                                        #whether dataset updates work correctly
                                        #logging.info("%s vs %s",rs0.value, rs1.value)
                                        #logging.info(rs0.value.value==rs1.value.value)
                                        assert str(rs0.value) == str(rs1.value)
            else:
                if str(a) != str(b):
                    logging.critical("%s vs %s",str(a), str(b))
                assert str(a) == str(b)

    def test_get_attribute_data(self, client, network_with_data):
        net = network_with_data
        s = net.scenarios[0]

        node_ras = []
        for node in net.nodes:
            for ra in node.attributes:
                node_ras.append(ra.id)

        link_ras = []
        for link in net.links:
            for ra in link.attributes:
                link_ras.append(ra.id)

        group_ras = []
        for group in net.resourcegroups:
            for ra in group.attributes:
                group_ras.append(ra.id)


        new_node_ras = client.get_all_node_data(net.id, s.id)
        for ra in new_node_ras:
            assert ra.resourcescenario is not None
            assert ra.id in node_ras


        node_id_filter = [net.nodes[0].id, net.nodes[1].id]
        new_node_ras = client.get_all_node_data(net.id, s.id, node_id_filter)
        for ra in new_node_ras:
            assert ra.resourcescenario is not None
            assert ra.id in node_ras

        new_link_ras = client.get_all_link_data(net.id, s.id)
        for ra in new_link_ras:
            assert ra.resourcescenario is not None
            assert ra.id in link_ras

        link_id_filter = [net.links[0].id, net.links[1].id]
        new_link_ras = client.get_all_link_data(net.id, s.id, link_id_filter)
        for ra in new_link_ras:
            assert ra.resourcescenario is not None
            assert ra.id in link_ras

        new_group_ras = client.get_all_group_data(net.id, s.id)
        for ra in new_group_ras:
            assert ra.resourcescenario is not None
            assert ra.id in group_ras

        group_id_filter = [net.resourcegroups[0].id]
        new_group_ras = client.get_all_group_data(net.id, s.id, group_id_filter)
        for ra in new_group_ras:
            assert ra.resourcescenario is not None
            assert ra.id in group_ras

    def test_get_all_resource_data(self, client, network_with_data):
        net = network_with_data
        s = net.scenarios[0]

        all_ras = []
        for node in net.nodes:
            for ra in node.attributes:
                all_ras.append(ra.id)

        for link in net.links:
            for ra in link.attributes:
                all_ras.append(ra.id)

        for group in net.resourcegroups:
            for ra in group.attributes:
                all_ras.append(ra.id)

        all_resource_data = client.get_all_resource_data(s.id, include_values=False)
        for rd in all_resource_data:
            assert rd.value is None
            assert int(rd.resource_attr_id) in all_ras

        all_resource_data = client.get_all_resource_data(s.id, include_values=True)
        for rd in all_resource_data:
            assert rd.value is not None
            assert int(rd.resource_attr_id) in all_ras


        truncated_resource_data = client.get_all_resource_data(s.id, include_values=True, include_metadata=True, page_start=0, page_end=1)
        assert len(truncated_resource_data) == 1

    def test_get_resource_data(self, client, network_with_data):
        net = network_with_data
        s = net.scenarios[0]
        node = net.nodes[0]

        node_ras = [a.id for a in node.attributes]

        all_resource_data = client.get_resource_data('NODE', node.id, s.id, include_values=False)
        all_node_types = []
        for rd in all_resource_data:
            assert rd.dataset.value is None
            assert int(rd.resource_attr_id) in node_ras
            all_node_types.append(rd.dataset.type)

        all_node_types = list(set(all_node_types))

        for datatype in all_node_types:

            all_resource_data = client.get_resource_data('NODE', node.id,s.id, include_values=True)
            for rd in all_resource_data:
                assert rd.dataset.value is not None
                assert int(rd.resource_attr_id) in node_ras

            all_resource_data = client.get_resource_data('NODE', node.id,s.id,
                                                        exclude_data_types=[datatype],
                                                        include_values=True)
            for rd in all_resource_data:
                assert rd.dataset.type != datatype
                assert rd.dataset.value is not None
                assert int(rd.resource_attr_id) in node_ras

            all_resource_data = client.get_resource_data('NODE', node.id,s.id,
                                                        include_data_types=[datatype],
                                                        include_values=True)
            for rd in all_resource_data:
                assert rd.dataset.type == datatype
                assert rd.dataset.value is not None
                assert int(rd.resource_attr_id) in node_ras

            all_resource_data = client.get_resource_data('NODE', node.id,s.id,
                                                        include_data_type_values=[datatype],
                                                        include_values=True)
            for rd in all_resource_data:
                if rd.dataset.type == datatype:
                    assert rd.dataset.value is not None
                else:
                    assert rd.dataset.value is None
                assert int(rd.resource_attr_id) in node_ras

            all_resource_data = client.get_resource_data('NODE', node.id,s.id,
                                                        exclude_data_type_values=[datatype],
                                                        include_values=True)
            for rd in all_resource_data:
                if rd.dataset.type == datatype:
                    # there should be no values returned with this type
                    assert rd.dataset.value is None
                else:
                    assert rd.dataset.value is not None
                assert int(rd.resource_attr_id) in node_ras

    def test_delete_node(self, client, network_with_data):
        net = network_with_data
        scenario_id = net.scenarios[0].id
        client.clone_scenario(scenario_id)

        node_id_to_delete = net.nodes[0].id

        node_datasets = client.get_resource_data('NODE', node_id_to_delete, scenario_id)
        log.info("Deleting node %s", node_id_to_delete)
        client.delete_node(node_id_to_delete, 'Y')

        updated_net = client.get_network(net.id, True)

        remaining_node_ids = [n.id for n in updated_net.nodes]

        assert node_id_to_delete not in remaining_node_ids

        for l in updated_net.links:
            assert l.node_1_id != node_id_to_delete
            assert l.node_2_id != node_id_to_delete

        for rs in node_datasets:
            #In these tests, all timeseries are unique to their resources,
            #so after removing the node no timeseries to which it was attached
            #should still exist.
            d = rs.dataset
            if d.type == 'timeseries':
                with pytest.raises(hb.exceptions.HydraError):
                    dataset = client.get_dataset(d.id)

    def test_delete_link(self, client, network_with_data):
        net = network_with_data
        scenario_id = net.scenarios[0].id
        link_id_to_delete = net.links[0].id

        link_datasets = client.get_resource_data('LINK', link_id_to_delete, scenario_id)
        log.info("Deleting link %s", link_id_to_delete)
        client.delete_link(link_id_to_delete, 'Y')

        updated_net = client.get_network(net.id, True)

        remaining_link_ids = [n.id for n in updated_net.links]

        assert link_id_to_delete not in remaining_link_ids

        with pytest.raises(hb.exceptions.HydraError):
            client.get_link(link_id_to_delete)

        for rs in link_datasets:
            #In these tests, all timeseries are unique to their resources,
            #so after removing the link no timeseries to which it was attached
            #should still exist.
            d = rs.dataset
            if d.type == 'timeseries':
                with pytest.raises(hb.exceptions.HydraError):
                    client.get_dataset(d.id)

    def test_get_all_network_owners(self, client, projectmaker, networkmaker):
        proj = projectmaker.create()

        net1 = networkmaker.create(project_id=proj.id)
        client.share_network(net1.id,
                                 ["UserB"],#Not an admin
                                 'N',
                                 'Y')

        networkowners = client.get_all_network_owners()

        assert len(networkowners) >= 2
        networkowners = client.get_all_network_owners([net1.id])
        assert len(networkowners) == 2

        client.login('UserC', 'password')
        #fails because user C is not an admin
        with pytest.raises(hb.exceptions.HydraError):
            networkowners = client.get_all_network_owners([net1.id])
        #log back in as root
        client.login('root', '')

    def test_bulk_set_network_owners(self, client, networkmaker):
        net = networkmaker.create()

        networkowners = client.get_all_network_owners([net.id])

        assert len(networkowners) == 1

        new_owner = hb.JSONObject(dict(
            network_id=net.id,
            user_id=2,
            view='Y',
            edit='Y',
            share='Y',
        ))
        client.bulk_set_network_owners([new_owner])

        networkowners = client.get_all_network_owners([net.id])

        assert len(networkowners) == 2

    def test_clone_network_into_existing_project(self, client, network_with_data):
        net = network_with_data

        recipient_user = client.get_user_by_name('UserA')

        cloned_network_id = client.clone_network(net.id,
                                          recipient_user_id=recipient_user.id,
                                          new_network_name=None,
                                          project_id=None,
                                          project_name=None,
                                          new_project=False)

        cloned_network = client.get_network(cloned_network_id, include_data=True)

        assert cloned_network.name == net.name + " (1)"
        assert len(net.nodes) == len(cloned_network.nodes)
        assert len(net.links) == len(cloned_network.links)
        assert len(net.resourcegroups) == len(cloned_network.resourcegroups)
        assert len(net.scenarios) == len(cloned_network.scenarios)
        assert len(net.scenarios[0].resourcescenarios) == len(cloned_network.scenarios[0].resourcescenarios) + 10 #this ignores results
        assert len(net.scenarios[0].resourcegroupitems) == len(cloned_network.scenarios[0].resourcegroupitems)


        cloned_network_id = client.clone_network(net.id,
                                          recipient_user_id=recipient_user.id,
                                          new_network_name='My New Name',
                                          project_id=None,
                                          project_name=None,
                                          new_project=False)

        cloned_network = client.get_network(cloned_network_id, include_data=True)
        assert cloned_network.name == 'My New Name'

    def test_clone_network_with_scoped_attributes(self, client, network_with_data):
        net = network_with_data

        network_scoped_attributes = client.get_attributes(network_id=net.id)
        assert len(network_scoped_attributes) == 1
        network_scoped_resource_attributes = list(filter(lambda x: x.attr_id==network_scoped_attributes[0].id, net.attributes))
        assert len(network_scoped_resource_attributes) == 1

        recipient_user = client.get_user_by_name('UserA')

        cloned_network_id = client.clone_network(net.id,
                                          recipient_user_id=recipient_user.id,
                                          new_network_name=None,
                                          project_id=None,
                                          project_name=None,
                                          new_project=False)

        cloned_network = client.get_network(cloned_network_id, include_data=True)

        assert net.project_id == cloned_network.project_id

        network_scoped_attributes = client.get_attributes(network_id=cloned_network.id)
        assert len(network_scoped_attributes) == 0

        project_scoped_attributes = client.get_attributes(project_id=cloned_network.project_id)
        #the attribute has been re-scoped to the project, so now there are 2 on the project. one
        #is the project's original scoped attribute, and the other is the one which has been rescoped.
        assert len(project_scoped_attributes) == 2


    def test_clone_network_into_new_project(self, client, network_with_data):
        net = network_with_data

        recipient_user = client.get_user_by_name('UserA')

        cloned_network_id = client.clone_network(net.id,
                                          recipient_user_id=recipient_user.id,
                                          new_network_name=None,
                                          project_id=None,
                                          project_name=None,
                                          new_project=True)

        cloned_network = client.get_network(cloned_network_id, include_data=True)
        #No need to assert that the clone itself worked, as the other test does that.
        assert cloned_network.project_id != net.project_id

    def test_clone_network_with_scoped_attributes_to_new_project(self, client, network_with_data):
        net = network_with_data

        network_scoped_attributes = client.get_attributes(network_id=net.id)
        assert len(network_scoped_attributes) == 1
        network_scoped_resource_attributes = list(filter(lambda x: x.attr_id==network_scoped_attributes[0].id, net.attributes))
        assert len(network_scoped_resource_attributes) == 1

        recipient_user = client.get_user_by_name('UserA')

        cloned_network_id = client.clone_network(net.id,
                                          recipient_user_id=recipient_user.id,
                                          new_network_name=None,
                                          project_id=None,
                                          project_name=None,
                                          new_project=True)

        cloned_network = client.get_network(cloned_network_id, include_data=True)

        assert net.project_id != cloned_network.project_id

        network_scoped_attributes = client.get_attributes(network_id=cloned_network.id)
        assert len(network_scoped_attributes) == 1

        project_scoped_attributes = client.get_attributes(project_id=cloned_network.project_id)
        #the attribute has not been re-scoped to the project, so the project should have no attributes.
        #this project (as it was created outside the project test suite) does not have a default scoped
        #attributes like the projectes created using the ProjectMaker, hence it will have 0
        assert len(project_scoped_attributes) == 0


    def test_clone_node(self, client, network_with_data):

        node_to_clone = network_with_data.nodes[0]

        cloned_node_id = client.clone_node(node_to_clone.id)

        cloned_node = client.get_node(cloned_node_id)

        assert cloned_node.name == f"{node_to_clone.name} (1)"

        assert len(cloned_node.attributes) == len(node_to_clone.attributes)

        scenario = client.get_scenario(network_with_data.scenarios[0].id)
        original_node_data = list(filter(lambda x: x.resource_attr_id in [a.id for a in node_to_clone.attributes],
                                         scenario.resourcescenarios))

        cloned_node_data = list(filter(lambda x: x.resource_attr_id in [a.id for a in cloned_node.attributes],
                                       scenario.resourcescenarios))

        assert len(cloned_node_data) == len(original_node_data)-1 #has no outputs, so has one less dataset


        cloned_node_id_2 = client.clone_node(node_to_clone.id, include_outputs=True)

        cloned_node_2 = client.get_node(cloned_node_id_2)

        assert cloned_node_2.name == f"{node_to_clone.name} (2)"

        assert len(cloned_node_2.attributes) == len(node_to_clone.attributes)

        scenario = client.get_scenario(network_with_data.scenarios[0].id)
        original_node_data = list(filter(lambda x: x.resource_attr_id in [a.id for a in node_to_clone.attributes],
                                         scenario.resourcescenarios))

        cloned_node_data = list(filter(lambda x: x.resource_attr_id in [a.id for a in cloned_node_2.attributes],
                                       scenario.resourcescenarios))

        assert len(cloned_node_data) == len(original_node_data)

        with pytest.raises(hb.exceptions.HydraError):
            cloned_node_id_3 = client.clone_node(node_to_clone.id, name=network_with_data.nodes[1].name)

        name = "Cloned node"
        x = 100
        y = -100
        cloned_node_id_3 = client.clone_node(node_to_clone.id, name=name, new_x=x, new_y=y)

        cloned_node_3 = client.get_node(cloned_node_id_3)

        assert cloned_node_3.name == name
        assert cloned_node_3.x == x
        assert cloned_node_3.y == y



    def test_clone_nodes(self, client, network_with_data):

        nodes_to_clone = network_with_data.nodes[0:1]

        cloned_node_ids = client.clone_nodes([n.id for n in nodes_to_clone])

        for i, cloned_node_id in enumerate(cloned_node_ids):
            cloned_node = client.get_node(cloned_node_id)
            node_to_clone = nodes_to_clone[i]

            assert cloned_node.name == f"{node_to_clone.name} (1)"

            assert len(cloned_node.attributes) == len(node_to_clone.attributes)

            scenario = client.get_scenario(network_with_data.scenarios[0].id)
            original_node_data = list(filter(lambda x: x.resource_attr_id in [a.id for a in node_to_clone.attributes],
                                            scenario.resourcescenarios))

            cloned_node_data = list(filter(lambda x: x.resource_attr_id in [a.id for a in cloned_node.attributes],
                                        scenario.resourcescenarios))

            assert len(cloned_node_data) == len(original_node_data)-1 #has no outputs, so has one less dataset


    def test_cloned_node_name_similarity(self, client, network_with_data):

        node_to_clone = network_with_data.nodes[0]

        updated_similar_name = node_to_clone.name + ' extratext'

        update_node = network_with_data.nodes[1]
        update_node.name = updated_similar_name

        client.update_node(update_node)

        cloned_node_ids = client.clone_nodes([node_to_clone.id])

        changed_name_node = client.get_node(update_node.id)

        assert changed_name_node.name ==  updated_similar_name

        cloned_node = client.get_node(cloned_node_ids[0])

        assert cloned_node.name == f"{node_to_clone.name} (1)"

        second_cloned_node_ids = client.clone_nodes([node_to_clone.id])

        second_cloned_node = client.get_node(second_cloned_node_ids[0])

        assert second_cloned_node.name == f"{node_to_clone.name} (2)"

        third_cloned_node_ids = client.clone_nodes([node_to_clone.id])

        third_cloned_node = client.get_node(third_cloned_node_ids[0])

        assert third_cloned_node.name == f"{node_to_clone.name} (3)"
