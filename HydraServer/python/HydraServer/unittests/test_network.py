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
import copy
import logging
import suds
import datetime
log = logging.getLogger(__name__)

class NetworkTest(server.SoapServerTest):
    """
        Test for network-based functionality
    """

    def test_get_resources_of_type(self):
        """
            Test for the retrieval of all the resources of a specified
            type within a network.
        """

        net = self.create_network_with_data()
        link_ids = []
        type_id = None
        for l in net.links.Link:
            if l.types:
                if type_id is None:
                    type_id = l.types.TypeSummary[0].id
                link_ids.append(l.id)

        resources_of_type = self.client.service.get_resources_of_type(net.id, type_id)

        assert len(resources_of_type[0]) == 4

        for r in resources_of_type[0]:
            assert r.ref_key == 'LINK'
            assert r.id in link_ids


    def test_get_network_with_template(self):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = self.create_network_with_data()
        logging.info("%s nodes before"%(len(net.nodes.Node)))
        #All the nodes are in this template, so return them all
        assert len(net.nodes.Node) == 10
        #The type has only 2 attributes, so these are the only
        #ones which should be returned.
        for n in net.nodes.Node:
            assert len(n.attributes.ResourceAttr) == 3
        #only 4 of the links in the network have a type, so only these
        #4 should be returned.
        logging.info("%s links before"%(len(net.links.Link)))
        assert len(net.links.Link) == 9
        #of the 4 links returned, ensure the two attributes are on each one.
        for l in net.links.Link:
            if l.types is not None:
                assert len(l.attributes.ResourceAttr) == 3
            else:
                assert len(l.attributes.ResourceAttr) == 3
        assert len(net.resourcegroups.ResourceGroup) == 1

        template_id = net.nodes.Node[0].types.TypeSummary[0].template_id

        filtered_net = self.client.service.get_network(net.id, 'N', template_id=template_id)
        logging.info("%s nodes after"%(len(filtered_net.nodes.Node)))
        #All the nodes are in this template, so return them all
        assert len(filtered_net.nodes.Node) == 10
        #The type has only 2 attributes, so these are the only
        #ones which should be returned.
        for n in filtered_net.nodes.Node:
            assert len(n.attributes.ResourceAttr) == 3
        #only 4 of the links in the network have a type, so only these
        #4 should be returned.
        logging.info("%s links after"%(len(filtered_net.links.Link)))
        assert len(filtered_net.links.Link) == 4
        #of the 4 links returned, ensure the two attributes are on each one.
        for l in filtered_net.links.Link:
            assert len(l.attributes.ResourceAttr) == 3
        assert filtered_net.resourcegroups is None

    def test_get_network(self):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = self.create_network_with_data(map_projection='EPSG:21781')
        scenario_id = net.scenarios.Scenario[0].id

        clone = self.client.service.clone_scenario(scenario_id)
        new_scenario = self.client.service.get_scenario(clone.id)

        full_network = self.client.service.get_network(new_scenario.network_id, 'N')

        for s in full_network.scenarios.Scenario:
            assert s.resourcescenarios is None

        scen_ids = self.client.factory.create("integerArray")
        scen_ids.integer.append(scenario_id)
        partial_network = self.client.service.get_network(new_scenario.network_id, 'Y', None, scen_ids)

        assert len(partial_network.scenarios.Scenario) == 1
        assert len(full_network.scenarios.Scenario)    == 2
        for s in partial_network.scenarios.Scenario:
            assert len(s.resourcescenarios.ResourceScenario) > 0

        self.assertRaises(suds.WebFault, self.client.service.get_network_by_name, net.project_id, "I am not a network")
        net_by_name = self.client.service.get_network_by_name(net.project_id, net.name)
        assert net_by_name.id == full_network.id

        no_net_exists = self.client.service.network_exists(net.project_id, "I am not a network")
        assert no_net_exists == 'N'
        net_exists = self.client.service.network_exists(net.project_id, net.name)
        assert net_exists == 'Y'
        assert full_network.projection == 'EPSG:21781'

    def test_get_extents(self):
        """
        Extents test: Test that the min X, max X, min Y and max Y of a
        network are retrieved correctly.
        """
        net = self.create_network_with_data()

        extents = self.client.service.get_network_extents(net.id)

        assert extents.min_x == 10
        assert extents.max_x == 100
        assert extents.min_y == 9
        assert extents.max_y == 99

    def test_update(self):
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

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = 1 * -1
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

        new_net = self.client.service.add_network(network)
        net = self.client.service.get_network(new_net.id)

        new_network = copy.deepcopy(net)

        link_id = new_network.links.Link[1].id
        old_node_1_id = new_network.links.Link[1].node_1_id
        old_node_2_id = new_network.links.Link[1].node_2_id

        new_network.links.Link[1].node_1_id = net.nodes.Node[2].id
        new_network.links.Link[1].node_2_id = net.nodes.Node[1].id

        new_network.description = \
            'A different network for SOAP unit tests.'

        updated_network = self.client.service.update_network(new_network)

        assert net.id == updated_network.id, \
            'network_id has changed on update.'
        assert net.name == updated_network.name, \
            "network_name changed on update."
        assert updated_network.links.Link[1].id == link_id
        assert updated_network.links.Link[1].node_1_id != old_node_1_id
        assert updated_network.links.Link[1].node_1_id == net.nodes.Node[2].id

        assert updated_network.links.Link[1].node_2_id != old_node_2_id
        assert updated_network.links.Link[1].node_2_id == net.nodes.Node[1].id

       # assert net.description != updated_network.description,\
       #     "project_description did not update"
       # assert updated_network.description == \
       #     'A different network for SOAP unit tests.', \
       #     "Update did not work correctly."

############################################################
    def test_add_links(self):

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

        network = self.client.service.add_network(network)
        links_ = self.client.factory.create('hyd:LinkArray')

        link = self.client.factory.create('hyd:Link')
        link.id = i * -1
        link.name = 'New Link'
        link.description = 'Test link ' + str(i)
        link.node_1_id = network.nodes.Node[0].id
        link.node_2_id = network.nodes.Node[2].id
        links_.Link.append(link)

        link2 = self.client.factory.create('hyd:Link')
        link2.id = i * -2
        link2.name = 'New Link_2'
        link2.description = 'Test link ' + str(i)
        link2.node_1_id = network.nodes.Node[0].id
        link2.node_2_id = network.nodes.Node[2].id
        links_.Link.append(link2)
        new_links=self.client.service.add_links(network.id, links_)

        new_network = self.client.service.get_network(network.id)
        assert len(new_links.Link) == len(links_.Link); "new nodes were not added correctly",
        assert len(network.links.Link)+len(links_.Link) == len(new_network.links.Link); "new nodes were not added correctly_2",

############################################################
    def test_add_link(self):
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

        network = self.client.service.add_network(network)

        link = self.client.factory.create('hyd:Link')
        link.id = i * -1
        link.name = 'New Link'
        link.description = 'Test link ' + str(i)
        link.node_1_id = network.nodes.Node[0].id
        link.node_2_id = network.nodes.Node[2].id

        tmpl = self.create_template()

        type_summary_arr = self.client.factory.create('hyd:TypeSummaryArray')

        type_summary      = self.client.factory.create('hyd:TypeSummary')
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.types.TemplateType[1].id
        type_summary.name = tmpl.types.TemplateType[1].name

        type_summary_arr.TypeSummary.append(type_summary)

        link.types = type_summary_arr

        new_link = self.client.service.add_link(network.id, link)

        link_attr_ids = []
        for resource_attr in new_link.attributes.ResourceAttr:
            link_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.types.TemplateType[1].typeattrs.TypeAttr:
            assert typeattr.attr_id in link_attr_ids

        new_network = self.client.service.get_network(network.id)

        assert len(new_network.links.Link) == len(network.links.Link)+1; "New node was not added correctly"
        return new_network

    def test_add_node(self):
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
            node.name = 'node ' + str(i)
            node.description = 'test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'link ' + str(i)
            link.description = 'test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'a network for soap unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        node = self.client.factory.create('hyd:Node')
        new_node_num = nnodes + 1
        node.id = new_node_num * -1
        node.name = 'node ' + str(new_node_num)
        node.description = 'test node ' + str(new_node_num)
        node.x = 100
        node.y = 101


        tmpl = self.create_template()

        type_summary_arr = self.client.factory.create('hyd:TypeSummaryArray')

        type_summary      = self.client.factory.create('hyd:TypeSummary')
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.types.TemplateType[0].id
        type_summary.name = tmpl.types.TemplateType[0].name

        type_summary_arr.TypeSummary.append(type_summary)

        node.types = type_summary_arr

        new_node = self.client.service.add_node(network.id, node)

        node_attr_ids = []
        for resource_attr in new_node.attributes.ResourceAttr:
            node_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.types.TemplateType[0].typeattrs.TypeAttr:
            assert typeattr.attr_id in node_attr_ids

        new_network = self.client.service.get_network(network.id)

        assert len(new_network.nodes.Node) == len(network.nodes.Node)+1; "new node was not added correctly"

        return new_network

    ######################################
    def test_add_nodes(self):
        """
        Test add new nodes to network
        """

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
            node.name = 'node ' + str(i)
            node.description = 'test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'link ' + str(i)
            link.description = 'test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id
            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'a network for soap unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)
        nodes_ = self.client.factory.create('hyd:NodeArray')


        for i in range (1200):
             node1 = self.client.factory.create('hyd:Node')
             new_node_num = nnodes + 1
             node1.id = new_node_num * -1
             node1.name = 'node1_'+str(i)
             node1.description = 'test node ' + str(new_node_num)
             node1.x = 100+i
             node1.y = 101+i
             nodes_.Node.append(node1)

        new_nodes=self.client.service.add_nodes(network.id, nodes_)
        new_network = self.client.service.get_network(network.id)

        assert len(new_nodes.Node) == len(nodes_.Node); "new nodes were not added correctly",

        assert len(network.nodes.Node)+len(nodes_.Node) == len(new_network.nodes.Node); "new nodes were not added correctly_2",

        return  new_network
    ########################################


    def test_update_node(self):
        network = self.test_add_node()

        node_to_update = network.nodes.Node[0]
        node_to_update.name = "Updated Node Name"
        node_to_update.layout      = {'app': ["Unit Test1", "Unit Test2"]}

        new_node = self.client.service.update_node(node_to_update)

        new_network = self.client.service.get_network(network.id)

        updated_node = None
        for n in new_network.nodes.Node:
            if n.id == node_to_update.id:
                updated_node = n
        assert updated_node.layout is not None
        assert updated_node.layout['app']  == ["Unit Test1", "Unit Test2"]
        assert updated_node.name == "Updated Node Name"

    def test_delete_node(self):
        network = self.test_add_node()

        node_to_delete = network.nodes.Node[0]

        self.client.service.delete_node(node_to_delete.id)

        new_network = self.client.service.get_network(network.id)

        node_ids = []
        for n in new_network.nodes.Node:
            node_ids.append(n.id)
        for l in new_network.links.Link:
            node_ids.append(l.node_1_id)
            node_ids.append(l.node_2_id)
        assert node_to_delete.id not in node_ids

        self.client.service.activate_node(node_to_delete.id)

        new_network = self.client.service.get_network(network.id)

        node_ids = []
        for n in new_network.nodes.Node:
            node_ids.append(n.id)
        link_node_ids = []
        for l in new_network.links.Link:
            link_node_ids.append(l.node_1_id)
            link_node_ids.append(l.node_2_id)
        assert node_to_delete.id in link_node_ids
        assert node_to_delete.id in link_node_ids
        assert node_to_delete.id in node_ids

    def test_update_link(self):
        network = self.test_add_link()

        link_to_update = network.links.Link[0]
        link_to_update.name = "Updated link Name"
        link_to_update.layout      = {'app': ["Unit Test1", "Unit Test2"]}

        new_link = self.client.service.update_link(link_to_update)

        new_network = self.client.service.get_network(network.id)

        updated_link = None
        for l in new_network.links.Link:
            if l.id == link_to_update.id:
                updated_link = l
        assert updated_link.layout is not None
        assert updated_link.layout['app']  == ["Unit Test1", "Unit Test2"]
        assert updated_link.name == "Updated link Name"

    def test_delete_link(self):
        network = self.test_add_link()

        link_to_delete = network.links.Link[0]

        self.client.service.delete_link(link_to_delete.id)

        new_network = self.client.service.get_network(network.id)

        link_ids = []
        for l in new_network.links.Link:
            link_ids.append(l.id)
        assert link_to_delete.id not in link_ids

        self.client.service.activate_link(link_to_delete.id)
        new_network = self.client.service.get_network(network.id)
        link_ids = []
        for l in new_network.links.Link:
            link_ids.append(l.id)
        assert link_to_delete.id in link_ids

#    def test_update_node_aud(self):
#        network = self.test_add_node()
#
#        node_to_update = network.nodes.Node[0]
#        for i in range(104):
#            node_to_update.name = "Updated Node Name %s"%(i)
#
#            new_node = self.client.service.update_node(node_to_update)
#
#        assert open('~/.hydra/audit/tNodeaud', mode='r')

    def test_load(self):
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

        network = self.client.service.add_network(network)

        new_network = self.client.service.get_network(network.id)

        assert network.name == new_network.name, \
            "network_name has changed."
        assert network.description == new_network.description,\
            "project_description did not load correctly"

    def test_delete(self):
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

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id
            #link = self.client.service.add_link(link)

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test @ %s'%(datetime.datetime.now())
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        self.client.service.delete_network(network.id)

        assert self.client.service.get_network(network.id).status == 'X', \
            'Deleting network did not work correctly.'

        self.client.service.activate_network(network.id)

        assert self.client.service.get_network(network.id).status == 'A', \
            'Reactivating network did not work correctly.'

    def test_get_node(self):
        network = self.create_network_with_data()
        n = network.nodes.Node[0]
        s = network.scenarios.Scenario[0]

        node_without_data = self.client.service.get_node(n.id)

        for ra in node_without_data.attributes.ResourceAttr:
            assert not hasattr(ra, 'resourcescenario')

        node_with_data = self.client.service.get_node(n.id, s.id)

        attrs_with_data = []
        for ra in node_with_data.attributes.ResourceAttr:
            if hasattr(ra, 'resourcescenario'):
                if ra.resourcescenario:
                    attrs_with_data.append(ra.id)
        assert len(attrs_with_data) == 2

    def test_get_link(self):
        network = self.create_network_with_data()
        n = network.links.Link[0]
        s = network.scenarios.Scenario[0]

        link_without_data = self.client.service.get_link(n.id)

        for ra in link_without_data.attributes.ResourceAttr:
            assert not hasattr(ra, 'resourcescenario')

        link_with_data = self.client.service.get_link(n.id, s.id)

        attrs_with_data = []
        for ra in link_with_data.attributes.ResourceAttr:
            if hasattr(ra, 'resourcescenario'):
                if ra.resourcescenario:
                    attrs_with_data.append(ra.id)
        assert len(attrs_with_data) == 2

    def test_cleanup_network(self):
        network = self.test_add_node()

        node_to_delete = network.nodes.Node[0]

        link_ids = []
        for l in network.links.Link:
            if l.node_1_id == node_to_delete.id:
                link_ids.append(l.id)
            if l.node_2_id == node_to_delete.id:
                link_ids.append(l.id)

        self.client.service.delete_node(node_to_delete.id)

        self.client.service.clean_up_network(network.id)

        self.assertRaises(suds.WebFault, self.client.service.get_node, node_to_delete.id)
        for l in link_ids:
            self.assertRaises(suds.WebFault, self.client.service.get_link, l)

    def test_validate_topology(self):
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

            nodes.Node.append(node)

        #NOTE: NOT ADDING ENOUGH LINKS!!
        for i in range(nlinks-1):
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

        network = self.client.service.add_network(network)

        result = self.client.service.validate_network_topology(network.id)
        assert len(result.integer) == 1#This means orphan nodes are present

    def test_consistency_of_update(self):
        """
            Test to ensure that updating a network which has not changed
            does not cause any changes to the network.
            Procedure:
            1 Create a network.
            2 Immediately update the network without changing it.
            3 Check that the original network and the updated network are identical.
        """
        net = self.create_network_with_data()

        for node in net.nodes.Node:
            assert node.types is not None and  len(node.types) > 0

        updated_net_summary = self.client.service.update_network(net)

        updated_net = self.client.service.get_network(updated_net_summary.id)

        for node in updated_net.nodes.Node:
            assert node.types is not None and  len(node.types) > 0

        for attr in net.__keylist__:
            a = net.__getitem__(attr)
            b = updated_net.__getitem__(attr)
            #assert str(a) == str(b)
            if attr == 'scenarios':
                for s0 in net.scenarios.Scenario:
                    for s1 in updated_net.scenarios.Scenario:
                        if s0.id == s1.id:
                            for rs0 in s0.resourcescenarios.ResourceScenario:
                                for rs1 in s1.resourcescenarios.ResourceScenario:
                                    if rs0.resource_attr_id == rs1.resource_attr_id:
                                        #Leave these logging in as they are used to test
                                        #whether dataset updates work correctly
                                        #logging.info("%s vs %s",rs0.value, rs1.value)
                                        #logging.info(rs0.value.value==rs1.value.value)
                                        assert str(rs0.value) == str(rs1.value)
            else:
                if str(a) != str(b):
                    logging.info("%s vs %s",str(a), str(b))
                assert str(a) == str(b)

    def test_get_attribute_data(self):
        net = self.create_network_with_data()
        s = net.scenarios.Scenario[0]

        node_ras = []
        for node in net.nodes.Node:
            for ra in node.attributes.ResourceAttr:
                node_ras.append(ra.id)

        link_ras = []
        for link in net.links.Link:
            for ra in link.attributes.ResourceAttr:
                link_ras.append(ra.id)

        group_ras = []
        for group in net.resourcegroups.ResourceGroup:
            for ra in group.attributes.ResourceAttr:
                group_ras.append(ra.id)


        new_node_ras = self.client.service.get_all_node_data(net.id, s.id)
        for ra in new_node_ras.ResourceAttr:
            assert ra.resourcescenario is not None
            assert ra.id in node_ras


        node_id_filter = [net.nodes.Node[0].id, net.nodes.Node[1].id]
        new_node_ras = self.client.service.get_all_node_data(net.id, s.id, node_id_filter)
        for ra in new_node_ras.ResourceAttr:
            assert ra.resourcescenario is not None
            assert ra.id in node_ras

        new_link_ras = self.client.service.get_all_link_data(net.id, s.id)
        for ra in new_link_ras.ResourceAttr:
            assert ra.resourcescenario is not None
            assert ra.id in link_ras

        link_id_filter = [net.links.Link[0].id, net.links.Link[1].id]
        new_link_ras = self.client.service.get_all_link_data(net.id, s.id, link_id_filter)
        for ra in new_link_ras.ResourceAttr:
            assert ra.resourcescenario is not None
            assert ra.id in link_ras

        new_group_ras = self.client.service.get_all_group_data(net.id, s.id)
        for ra in new_group_ras.ResourceAttr:
            assert ra.resourcescenario is not None
            assert ra.id in group_ras

        group_id_filter = [net.resourcegroups.ResourceGroup[0].id]
        new_group_ras = self.client.service.get_all_group_data(net.id, s.id, group_id_filter)
        for ra in new_group_ras.ResourceAttr:
            assert ra.resourcescenario is not None
            assert ra.id in group_ras

    def test_get_resource_data(self):
        net = self.create_network_with_data()
        s = net.scenarios.Scenario[0]

        all_ras = []
        for node in net.nodes.Node:
            for ra in node.attributes.ResourceAttr:
                all_ras.append(ra.id)

        for link in net.links.Link:
            for ra in link.attributes.ResourceAttr:
                all_ras.append(ra.id)

        for group in net.resourcegroups.ResourceGroup:
            for ra in group.attributes.ResourceAttr:
                all_ras.append(ra.id)


        all_resource_data = self.client.service.get_all_resource_data(s.id, include_values='Y')
        log.info(all_resource_data.ResourceData[0])
        for ra in all_resource_data.ResourceData:
            assert int(ra.resource_attr_id) in all_ras 

        truncated_resource_data = self.client.service.get_all_resource_data(s.id, include_values='Y', include_metadata='Y', page_start=0, page_end=1)
        assert len(truncated_resource_data.ResourceData) == 1




    def test_purge_node(self):
        net = self.create_network_with_data()
        scenario_id = net.scenarios.Scenario[0].id
        self.client.service.clone_scenario(scenario_id)

        node_id_to_delete = net.nodes.Node[0].id

        node_datasets = self.client.service.get_node_data(node_id_to_delete, scenario_id)
        log.info("Deleting node %s", node_id_to_delete)
        self.client.service.purge_node(node_id_to_delete)

        updated_net = self.client.service.get_network(net.id, 'Y')

        remaining_node_ids = [n.id for n in updated_net.nodes.Node]

        assert node_id_to_delete not in remaining_node_ids 

        for l in updated_net.links.Link:
            assert l.node_1_id != node_id_to_delete
            assert l.node_2_id != node_id_to_delete

        for rs in node_datasets.ResourceScenario:
            #In these tests, all timeseries are unique to their resources,
            #so after removing the node no timeseries to which it was attached
            #should still exist.
            d = rs.value
            if d.type == 'timeseries':
                self.assertRaises(suds.WebFault, self.client.service.get_dataset, d.id)

    def test_purge_link(self):
        net = self.create_network_with_data()
        scenario_id = net.scenarios.Scenario[0].id
        link_id_to_delete = net.links.Link[0].id

        link_datasets = self.client.service.get_link_data(link_id_to_delete, scenario_id)
        log.info("Deleting link %s", link_id_to_delete)
        self.client.service.purge_link(link_id_to_delete)

        updated_net = self.client.service.get_network(net.id, 'Y')

        remaining_link_ids = [n.id for n in updated_net.links.Link]

        assert link_id_to_delete not in remaining_link_ids 

        for rs in link_datasets.ResourceScenario:
            #In these tests, all timeseries are unique to their resources,
            #so after removing the link no timeseries to which it was attached
            #should still exist.
            d = rs.value
            if d.type == 'timeseries':
                self.assertRaises(suds.WebFault, self.client.service.get_dataset, d.id)
  
if __name__ == '__main__':
    server.run()
