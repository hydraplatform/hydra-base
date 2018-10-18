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
import copy
import datetime
from fixtures import *
import util

import hydra_base as hb
from hydra_base.lib.objects import JSONObject

import logging
log = logging.getLogger(__name__)

class TestNetwork:
    """
        Test for network-based functionality
    """

    def test_get_resources_of_type(self, session, network_with_data):
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

        resources_of_type = hb.get_resources_of_type(net.id, type_id, user_id=pytest.root_user_id)

        assert len(resources_of_type[1]) == 4

        for r in resources_of_type[0]:
            assert r.ref_key == 'LINK'
            assert r.id in link_ids

    def test_get_all_resource_attributes_in_network(self, session, network_with_data):
        """
            Test to retrieve all the resourceattributes relating to the specified
            attribute ID in the network, regardless of what node, link group etc
            they belong to.
        """
        test_attr_id = None
        for a in network_with_data.nodes[0].attributes:
            test_attr_id = a.attr_id
            break


        all_network_resource_attrs = hb.get_all_resource_attributes_in_network(test_attr_id,
                                                                network_with_data.id,
                                                                user_id=pytest.root_user_id)

        #Find the attribute that ALL nodes have.
        assert len(all_network_resource_attrs) == len(network_with_data.nodes)




    def test_get_network_with_template(self, session, network_with_data):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = network_with_data
        logging.info("%s nodes before"%(len(net.nodes)))
        #All the nodes are in this template, so return them all
        assert len(net.nodes) == 10
        #The type has only 2 attributes, so these are the only
        #ones which should be returned.
        for n in net.nodes:
            assert len(n.attributes) == 4
        #only 4 of the links in the network have a type, so only these
        #4 should be returned.
        logging.info("%s links before"%(len(net.links)))
        assert len(net.links) == 9
        #of the 4 links returned, ensure the two attributes are on each one.
        for l in net.links:
            if l.types is not None:
                assert len(l.attributes) == 3
            else:
                assert len(l.attributes) == 3
        assert len(net.resourcegroups) == 1

        template_id = net.nodes[0].types[0].template_id

        filtered_net = hb.get_network(net.id, template_id=template_id, user_id=pytest.root_user_id)
        logging.info("%s nodes after"%(len(filtered_net.nodes)))
        #All the nodes are in this template, so return them all
        assert len(filtered_net.nodes) == 10
        #The type has only 2 attributes, so these are the only
        #ones which should be returned.
        for n in filtered_net.nodes:
            assert len(n.attributes) == 4
        #only 4 of the links in the network have a type, so only these
        #4 should be returned.
        logging.info("%s links after"%(len(filtered_net.links)))
        assert len(filtered_net.links) == 4
        #of the 4 links returned, ensure the two attributes are on each one.
        for l in filtered_net.links:
            assert len(l.attributes) == 3

        assert len(filtered_net.resourcegroups) == 0

    def test_get_network(self, session, networkmaker):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = networkmaker.create(map_projection='EPSG:21781')
        scenario_id = net.scenarios[0].id

        clone = hb.clone_scenario(scenario_id, user_id=pytest.root_user_id)
        new_scenario = hb.get_scenario(clone.id, user_id=pytest.root_user_id)

        full_network = hb.get_network(new_scenario.network_id, user_id=pytest.root_user_id)

        for s in full_network.scenarios:
            assert len(s.resourcescenarios) == 0

        scen_ids = []
        scen_ids.append(scenario_id)
        partial_network = hb.get_network(new_scenario.network_id, True, 'Y', scen_ids, user_id=pytest.root_user_id)

        assert len(partial_network.scenarios) == 1
        assert len(full_network.scenarios)    == 2

        for s in partial_network.scenarios:
            assert len(s.resourcescenarios) > 0

        with pytest.raises(hb.exceptions.HydraError):
            hb.get_network_by_name(net.project_id, "I am not a network", user_id=pytest.root_user_id)

        net_by_name = hb.get_network_by_name(net.project_id, net.name, user_id=pytest.root_user_id)
        assert net_by_name.id == full_network.id

        no_net_exists = hb.network_exists(net.project_id, "I am not a network", user_id=pytest.root_user_id)
        assert no_net_exists == 'N'
        net_exists = hb.network_exists(net.project_id, net.name, user_id=pytest.root_user_id)
        assert net_exists == 'Y'
        assert full_network.projection == 'EPSG:21781'

    def test_get_extents(self, session, network_with_data):
        """
        Extents test: Test that the min X, max X, min Y and max Y of a
        network are retrieved correctly.
        """
        net = network_with_data

        extents = hb.get_network_extents(net.id, user_id=pytest.root_user_id)

        assert extents.min_x == 10
        assert extents.max_x == 100
        assert extents.min_y == 9
        assert extents.max_y == 99

    def test_update_network(self, session, projectmaker):
        project = projectmaker.create('test')
        network = JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = JSONObject()
            link.id = 1 * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id

            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.projection = "EPSG:1234"
        network.nodes = nodes
        network.links = links

        new_net = hb.add_network(network, user_id=pytest.root_user_id)

        hb.db.DBSession.expunge_all()

        net = JSONObject(hb.get_network(new_net.id, user_id=pytest.root_user_id))

        new_network = copy.deepcopy(net)

        link_id = new_network.links[1].id
        old_node_1_id = new_network.links[1].node_1_id
        old_node_2_id = new_network.links[1].node_2_id

        new_network.links[1].node_1_id = net.nodes[2].id
        new_network.links[1].node_2_id = net.nodes[1].id
        new_network.links[1].layout = {'color':'red'}

        new_network.nodes[1].layout = {'color':'green'}

        new_network.description = \
            'A different network for SOAP unit tests.'

        updated_network = JSONObject(hb.update_network(new_network, user_id=pytest.root_user_id))

        assert net.id == updated_network.id, \
            'network_id has changed on update.'
        assert net.name == updated_network.name, \
            "network_name changed on update."
        assert updated_network.links[1].id == link_id
        assert updated_network.links[1].node_1_id != old_node_1_id
        assert updated_network.links[1].node_1_id == net.nodes[2].id
        assert updated_network.links[1].layout['color'] == 'red'

        assert updated_network.links[1].node_2_id != old_node_2_id
        assert updated_network.links[1].node_2_id == net.nodes[1].id

        assert updated_network.nodes[1].layout['color'] == 'green'


############################################################
    def test_add_links(self, session, projectmaker):

        project = projectmaker.create('test')
        network = JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = JSONObject()
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

        network = hb.add_network(network, user_id=pytest.root_user_id)
        links = []

        link = JSONObject()
        link.id = i * -1
        link.name = 'New Link'
        link.description = 'Test link ' + str(i)
        link.node_1_id = network.nodes[0].id
        link.node_2_id = network.nodes[2].id
        links.append(link)

        link2 = JSONObject()
        link2.id = i * -2
        link2.name = 'New Link_2'
        link2.description = 'Test link ' + str(i)
        link2.node_1_id = network.nodes[0].id
        link2.node_2_id = network.nodes[2].id
        links.append(link2)

        new_links=hb.add_links(network.id, links, user_id=pytest.root_user_id)

        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

        assert len(network.links)+len(links) == len(new_network.links); "new nodes were not added correctly_2",

############################################################
    def test_add_link(self, session, projectmaker, template):
        project = projectmaker.create('test')
        network = JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = JSONObject()
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

        network = hb.add_network(network, user_id=pytest.root_user_id)
        network = hb.get_network(network.id, user_id=pytest.root_user_id)

        link = JSONObject()
        link.id = i * -1
        link.name = 'New Link'
        link.description = 'Test link ' + str(i)
        link.node_1_id = network.nodes[0].id
        link.node_2_id = network.nodes[2].id

        tmpl = template

        type_summary_arr = []

        type_summary      = JSONObject()
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.templatetypes[1].id
        type_summary.name = tmpl.templatetypes[1].name

        type_summary_arr.append(type_summary)

        link.types = type_summary_arr

        new_link = hb.add_link(network.id, link, user_id=pytest.root_user_id)

        link_attr_ids = []
        for resource_attr in new_link.attributes:
            link_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.templatetypes[1].typeattrs:
            assert typeattr.attr_id in link_attr_ids

        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

        assert len(new_network.links) == len(network.links)+1; "New node was not added correctly"
        return new_network

    def test_add_node(self, session, projectmaker, template):
        project = projectmaker.create('test')
        network = JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'node ' + str(i)
            node.description = 'test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = JSONObject()
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

        network = hb.add_network(network, user_id=pytest.root_user_id)
        network = hb.get_network(network.id, user_id=pytest.root_user_id)

        node = JSONObject()
        new_node_num = nnodes + 1
        node.id = new_node_num * -1
        node.name = 'node ' + str(new_node_num)
        node.description = 'test node ' + str(new_node_num)
        node.x = 100
        node.y = 101


        tmpl = template

        type_summary_arr = []

        type_summary      = JSONObject()
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.templatetypes[0].id
        type_summary.name = tmpl.templatetypes[0].name

        type_summary_arr.append(type_summary)

        node.types = type_summary_arr

        new_node = hb.add_node(network.id, node, user_id=pytest.root_user_id)

        node_attr_ids = []
        for resource_attr in new_node.attributes:
            node_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.templatetypes[0].typeattrs:
            assert typeattr.attr_id in node_attr_ids

        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

        assert len(new_network.nodes) == len(network.nodes)+1; "new node was not added correctly"

        return new_network

    ######################################
    def test_add_nodes(self, session, projectmaker):
        """
        Test add new nodes to network
        """

        project = projectmaker.create('test')
        network = JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'node ' + str(i)
            node.description = 'test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = JSONObject()
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

        network = hb.add_network(network, user_id=pytest.root_user_id)
        network = hb.get_network(network.id, user_id=pytest.root_user_id)
        nodes = []

        for i in range (1200):
             node1 = JSONObject()
             new_node_num = nnodes + 1
             node1.id = new_node_num * -1
             node1.name = 'node1_'+str(i)
             node1.description = 'test node ' + str(new_node_num)
             node1.x = 100+i
             node1.y = 101+i
             nodes.append(node1)

        new_nodes=hb.add_nodes(network.id, nodes, user_id=pytest.root_user_id)
        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

        assert len(network.nodes)+len(nodes) == len(new_network.nodes); "new nodes were not added correctly_2",

        return  new_network
    ########################################


    def test_update_node(self, session, network_with_data):
        network = network_with_data

        node_to_update = JSONObject(network.nodes[0])
        node_to_update.name = "Updated Node Name"
        node_to_update.layout      = {'app': ["Unit Test1", "Unit Test2"]}

        new_node = hb.update_node(node_to_update, user_id=pytest.root_user_id)

        new_network = JSONObject(hb.get_network(network.id, user_id=pytest.root_user_id))

        updated_node = None
        for n in new_network.nodes:
            if n.id == node_to_update.id:
                updated_node = n
        assert updated_node.layout is not None
        assert updated_node.layout['app']  == ["Unit Test1", "Unit Test2"]
        assert updated_node.name == "Updated Node Name"

    def test_set_node_status(self, session, network_with_data):
        network = network_with_data

        node_to_delete = network.nodes[0]

        hb.set_node_status(node_to_delete.id, 'X', user_id=pytest.root_user_id)

        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

        node_ids = []
        for n in new_network.nodes:
            node_ids.append(n.id)
        for l in new_network.links:
            node_ids.append(l.node_1_id)
            node_ids.append(l.node_2_id)
        assert node_to_delete.id not in node_ids

        hb.set_node_status(node_to_delete.id, 'A', user_id=pytest.root_user_id)

        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

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

    def test_update_link(self, session, network_with_data):
        network = network_with_data

        link_to_update = network.links[0]
        link_to_update.name = "Updated link Name"
        link_to_update.layout      = {'app': ["Unit Test1", "Unit Test2"]}

        new_link = hb.update_link(link_to_update, user_id=pytest.root_user_id)

        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

        updated_link = None
        for l in new_network.links:
            if l.id == link_to_update.id:
                updated_link = l
        assert updated_link.layout is not None

        assert updated_link.layout['app']  == ["Unit Test1", "Unit Test2"]
        assert updated_link.name == "Updated link Name"

    def test_set_link_status(self, session, network_with_data):
        network = network_with_data

        link_to_delete = network.links[0]
        
        hb.set_link_status(link_to_delete.id, 'X', user_id=pytest.root_user_id)

        new_network = JSONObject(hb.get_network(network.id, user_id=pytest.root_user_id))

        link_ids = []
        for l in new_network.links:
            link_ids.append(l.id)
        assert link_to_delete.id not in link_ids

        hb.set_link_status(link_to_delete.id, 'A', user_id=pytest.root_user_id)
        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)
        link_ids = []
        for l in new_network.links:
            link_ids.append(l.id)
        assert link_to_delete.id in link_ids


    def test_delete_link(self, session, network_with_data):
        network = network_with_data

        link_to_delete = network.links[0]
        
        #'N' is for purge_data
        hb.delete_link(link_to_delete.id, 'N', user_id=pytest.root_user_id)

        new_network = hb.get_network(network.id, user_id=pytest.root_user_id)

        link_ids = []
        for l in new_network.links:
            link_ids.append(l.id)
        assert link_to_delete.id not in link_ids

        with pytest.raises(hb.exceptions.HydraError):
            hb.get_link(link_to_delete.id, user_id=pytest.root_user_id)

    def test_set_network_status(self, session, projectmaker):
        project = projectmaker.create('test')
        network = JSONObject()
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        for i in range(nlinks):
            link = JSONObject()
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes[i].id
            link.node_2_id = nodes[i + 1].id
            #link = hb.add_link(link)

            links.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = hb.add_network(network, user_id=pytest.root_user_id)

        hb.set_network_status(network.id, 'X', user_id=pytest.root_user_id)

        assert hb.get_network(network.id, user_id=pytest.root_user_id).status == 'X', \
            'Deleting network did not work correctly.'

        hb.set_network_status(network.id, 'A', user_id=pytest.root_user_id)

        assert hb.get_network(network.id, user_id=pytest.root_user_id).status == 'A', \
            'Reactivating network did not work correctly.'

    def test_delete_network(self, session, projectmaker, network_with_data):

        network = hb.get_network(network_with_data.id, user_id=pytest.root_user_id)

        hb.delete_network(network_with_data.id, 'N', user_id=pytest.root_user_id)

        with pytest.raises(hb.exceptions.HydraError):
            hb.get_network(network.id, user_id=pytest.root_user_id)

    def test_get_node(self, session, network_with_data):
        network = network_with_data
        n = network.nodes[0]
        s = network.scenarios[0]

        node_without_data = hb.get_node(n.id, user_id=pytest.root_user_id)

        for ra in node_without_data.attributes:
            assert not hasattr(ra, 'resourcescenario') or ra.resourcescenario is None

        node_with_data = hb.get_node(n.id, s.id, user_id=pytest.root_user_id)

        attrs_with_data = []
        for ra in node_with_data.attributes:
            if hasattr(ra, 'resourcescenario') and ra.resourcescenario is not None:
                if ra.resourcescenario:
                    attrs_with_data.append(ra.id)
        assert len(attrs_with_data) == 3

    def test_get_link(self, session, network_with_data):
        network = network_with_data
        l = network.links[-1]
        s = network.scenarios[0]

        link_without_data = hb.get_link(l.id, user_id=pytest.root_user_id)

        for ra in link_without_data.attributes:
            assert not hasattr(ra, 'resourcescenario') or ra.resourcescenario is None

        link_with_data = hb.get_link(l.id, s.id, user_id=pytest.root_user_id)

        attrs_with_data = []
        for ra in link_with_data.attributes:
            if ra.resourcescenario is not None:
                attrs_with_data.append(ra.id)
        assert len(attrs_with_data) == 2

    def test_cleanup_network(self, session, network_with_data):
        network = network_with_data

        node_to_delete = network.nodes[0]

        link_ids = []
        for l in network.links:
            if l.node_1_id == node_to_delete.id:
                link_ids.append(l.id)
            if l.node_2_id == node_to_delete.id:
                link_ids.append(l.id)

        hb.set_node_status(node_to_delete.id, 'X', user_id=pytest.root_user_id)

        hb.clean_up_network(network.id, user_id=pytest.root_user_id)

        with pytest.raises(hb.exceptions.HydraError):
            hb.get_node(node_to_delete.id, user_id=pytest.root_user_id)

        for l in link_ids:
            with pytest.raises(hb.exceptions.HydraError):
                hb.get_link(l, user_id=pytest.root_user_id)

    def test_validate_topology(self, session, projectmaker):
        project = projectmaker.create('test')
        network = JSONObject({})
        nodes = []
        links = []

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = JSONObject()
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.append(node)

        #NOTE: NOT ADDING ENOUGH LINKS!!
        for i in range(nlinks-1):
            link = JSONObject()
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

        network = hb.add_network(network, user_id=pytest.root_user_id)

        result = hb.validate_network_topology(network.id, user_id=pytest.root_user_id)
        assert len(result) == 1#This means orphan nodes are present

    def test_consistency_of_update(self, session, network_with_data):
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

        original_net = hb.get_network(net.id, user_id=pytest.root_user_id)

        updated_net_summary = hb.update_network(net, user_id=pytest.root_user_id)

        updated_net = hb.get_network(updated_net_summary.id, user_id=pytest.root_user_id)

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

    def test_get_attribute_data(self, session, network_with_data):
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


        new_node_ras = hb.get_all_node_data(net.id, s.id, user_id=pytest.root_user_id)
        for ra in new_node_ras:
            assert ra.resourcescenario is not None
            assert ra.id in node_ras


        node_id_filter = [net.nodes[0].id, net.nodes[1].id]
        new_node_ras = hb.get_all_node_data(net.id, s.id, node_id_filter, user_id=pytest.root_user_id)
        for ra in new_node_ras:
            assert ra.resourcescenario is not None
            assert ra.id in node_ras

        new_link_ras = hb.get_all_link_data(net.id, s.id, user_id=pytest.root_user_id)
        for ra in new_link_ras:
            assert ra.resourcescenario is not None
            assert ra.id in link_ras

        link_id_filter = [net.links[0].id, net.links[1].id]
        new_link_ras = hb.get_all_link_data(net.id, s.id, link_id_filter, user_id=pytest.root_user_id)
        for ra in new_link_ras:
            assert ra.resourcescenario is not None
            assert ra.id in link_ras

        new_group_ras = hb.get_all_group_data(net.id, s.id, user_id=pytest.root_user_id)
        for ra in new_group_ras:
            assert ra.resourcescenario is not None
            assert ra.id in group_ras

        group_id_filter = [net.resourcegroups[0].id]
        new_group_ras = hb.get_all_group_data(net.id, s.id, group_id_filter, user_id=pytest.root_user_id)
        for ra in new_group_ras:
            assert ra.resourcescenario is not None
            assert ra.id in group_ras

    def test_get_resource_data(self, session, network_with_data):
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


        all_resource_data = hb.get_all_resource_data(s.id, include_values='Y', user_id=pytest.root_user_id)
        log.info(all_resource_data[0])
        for rd in all_resource_data:
            assert int(rd.resource_attr_id) in all_ras

        truncated_resource_data = hb.get_all_resource_data(s.id, include_values='Y', include_metadata='Y', page_start=0, page_end=1, user_id=pytest.root_user_id)
        assert len(truncated_resource_data) == 1




    def test_delete_node(self, session, network_with_data):
        net = network_with_data
        scenario_id = net.scenarios[0].id
        hb.clone_scenario(scenario_id, user_id=pytest.root_user_id)

        node_id_to_delete = net.nodes[0].id

        node_datasets = hb.get_resource_data('NODE', node_id_to_delete, scenario_id)
        log.info("Deleting node %s", node_id_to_delete)
        hb.delete_node(node_id_to_delete, 'Y', user_id=pytest.root_user_id)

        updated_net = hb.get_network(net.id, 'Y', user_id=pytest.root_user_id)

        remaining_node_ids = [n.id for n in updated_net.nodes]

        assert node_id_to_delete not in remaining_node_ids

        for l in updated_net.links:
            assert l.node_1_id != node_id_to_delete
            assert l.node_2_id != node_id_to_delete

        for rs in node_datasets:
            #In these tests, all timeseries are unique to their resources,
            #so after removing the node no timeseries to which it was attached
            #should still exist.
            d = rs.value
            if d.type == 'timeseries':
                with pytest.raises(hb.exceptions.HydraError):
                    dataset = hb.get_dataset(d.id, user_id=pytest.root_user_id)

    def test_delete_link(self, session, network_with_data):
        net = network_with_data
        scenario_id = net.scenarios[0].id
        link_id_to_delete = net.links[0].id

        link_datasets = hb.get_resource_data('LINK', link_id_to_delete, scenario_id, user_id=pytest.root_user_id)
        log.info("Deleting link %s", link_id_to_delete)
        hb.delete_link(link_id_to_delete, 'N', user_id=pytest.root_user_id)

        updated_net = hb.get_network(net.id, 'Y', user_id=pytest.root_user_id)

        remaining_link_ids = [n.id for n in updated_net.links]

        assert link_id_to_delete not in remaining_link_ids

        for rs in link_datasets:
            #In these tests, all timeseries are unique to their resources,
            #so after removing the link no timeseries to which it was attached
            #should still exist.
            d = rs.value
            if d.type == 'timeseries':
                with pytest.raises(hb.exceptions.HydraError):
                    hb.get_dataset(d.id, user_id=pytest.root_user_id)

if __name__ == '__main__':
    server.run()
