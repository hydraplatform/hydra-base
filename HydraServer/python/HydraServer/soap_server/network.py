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
from spyne.model.primitive import Unicode, Integer
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Network,\
    Node,\
    Link,\
    Scenario,\
    ResourceGroup,\
    NetworkExtents,\
    ResourceSummary,\
    ResourceAttr,\
    ResourceScenario,\
    ResourceData
from HydraServer.lib import network, scenario
from hydra_base import HydraService
import datetime
import logging
import json
log = logging.getLogger(__name__)

class NetworkService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Network, _returns=Network)
    def add_network(ctx, net):
        """
        Takes an entire network complex model and saves it to the DB.  This
        complex model includes links & scenarios (with resource data).  Returns
        the network's complex model.

        As links connect two nodes using the node_ids, if the nodes are new
        they will not yet have node_ids. In this case, use negative ids as
        temporary IDS until the node has been given an permanent ID.

        All inter-object referencing of new objects should be done using
        negative IDs in the client.

        The returned object will have positive IDS

        """
        net = network.add_network(net, **ctx.in_header.__dict__)
        ret_net = Network(net, summary=True)
        return ret_net

    @rpc(Integer,
         Unicode(pattern="[YN]", default='Y'),
         Integer(),
         SpyneArray(Integer()),
         Unicode(pattern="[YN]", default='N'),
         _returns=Network)
    def get_network(ctx, network_id, include_data, template_id, scenario_ids, summary):
        """
            Return a whole network as a complex model.
        """
        net  = network.get_network(network_id,
                                   True if summary=='Y' else False,
                                   include_data,
                                   scenario_ids,
                                   template_id,
                                   **ctx.in_header.__dict__)
        ret_net = Network(net, True if summary=='Y' else False)
        return ret_net

    @rpc(Integer,
         _returns=Unicode)
    def get_network_as_json(ctx, network_id):
        """
            Return a whole network as a complex model.
        """
        net  = network.get_network(network_id,
                                   False,
                                   'Y',
                                   [],
                                   None,
                                   **ctx.in_header.__dict__)

        return json.dumps(str(net))

    @rpc(Integer, Unicode, _returns=Network)
    def get_network_by_name(ctx, project_id, network_name):
        """
        Return a whole network as a complex model.
        """

        net = network.get_network_by_name(project_id, network_name, **ctx.in_header.__dict__)

        return Network(net)

    @rpc(Integer, Unicode, _returns=Unicode)
    def network_exists(ctx, project_id, network_name):
        """
        Return a whole network as a complex model.
        """

        net_exists = network.network_exists(project_id, network_name, **ctx.in_header.__dict__)

        return net_exists

    @rpc(Network, 
        Unicode(pattern="['YN']", default='Y'),
        Unicode(pattern="['YN']", default='Y'),
        Unicode(pattern="['YN']", default='Y'),
        Unicode(pattern="['YN']", default='Y'),
        _returns=Network)
    def update_network(ctx, net, update_nodes, update_links, update_groups, update_scenarios):
        """
            Update an entire network
        """
        upd_nodes = True if update_nodes == 'Y' else False
        upd_links = True if update_links == 'Y' else False
        upd_groups = True if update_groups == 'Y' else False
        upd_scenarios = True if update_scenarios == 'Y' else False

        net = network.update_network(net,
                                     upd_nodes,
                                     upd_links,
                                     upd_groups,
                                     upd_scenarios,
                                     **ctx.in_header.__dict__)
        return Network(net, summary=True)

    @rpc(Integer, Integer(min_occurs=0), _returns=Node)
    def get_node(ctx, node_id, scenario_id):
        """
            Get a node using the node_id.
            optionally, scenario_id can be included if data is to be included
        """
        node = network.get_node(node_id, **ctx.in_header.__dict__)

        if scenario_id is not None:
            ret_node = Node(node)

            res_scens = scenario.get_resource_data('NODE', node_id, scenario_id, None)

            rs_dict = {}
            for rs in res_scens:
                rs_dict[rs.resource_attr_id] = rs

            for ra in ret_node.attributes:
                if rs_dict.get(ra.id):
                    ra.resourcescenario = ResourceScenario(rs_dict[ra.id])

            return ret_node
        else:
            ret_node = Node(node)
            return ret_node

    @rpc(Integer, Integer, _returns=Link)
    def get_link(ctx, link_id, scenario_id):
        link = network.get_link(link_id, **ctx.in_header.__dict__)

        if scenario_id is not None:
            ret_link = Link(link)
            res_scens = scenario.get_resource_data('LINK', link_id, scenario_id, None)
            rs_dict = {}
            for rs in res_scens:
                rs_dict[rs.resource_attr_id] = rs

            for ra in ret_link.attributes:
                if rs_dict.get(ra.id):
                    ra.resourcescenario = ResourceScenario(rs_dict[ra.id])

            return ret_link
        else:
            ret_link = Link(link)
            return ret_link

    @rpc(Integer, Integer, _returns=ResourceGroup)
    def get_resourcegroup(ctx, group_id, scenario_id):
        group = network.get_resourcegroup(group_id, **ctx.in_header.__dict__)

        if scenario_id is not None:
            ret_group = ResourceGroup(group)
            res_scens = scenario.get_resource_data('GROUP', group_id, scenario_id, None)
            rs_dict = {}
            for rs in res_scens:
                rs_dict[rs.resource_attr_id] = rs

            for ra in ret_group.attributes:
                if rs_dict.get(ra.id):
                    ra.resourcescenario = ResourceScenario(rs_dict[ra.id])

            return ret_group
        else:
            ret_group = ResourceGroup(group)
            return ret_group

    @rpc(Integer, _returns=Unicode)
    def delete_network(ctx, network_id):
        """
        Set status of network for delete or un-delete
        """
        #check_perm('delete_network')
        network.set_network_status(network_id, 'X', **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Unicode(pattern="[YN]", default='Y'), _returns=Unicode)
    def purge_network(ctx, network_id, purge_data):
        """
        Remove a network from hydra platform completely.
        """
        #check_perm('delete_network')
        network.purge_network(network_id, purge_data, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=Unicode)
    def activate_network(ctx, network_id):
        """
        Deletes a network. This does not remove the network from the DB. It
        just sets the status to 'X', meaning it can no longer be seen by the
        user.
        """
        #check_perm('delete_network')
        network.set_network_status(network_id, 'A', **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=NetworkExtents)
    def get_network_extents(ctx, network_id):
        """
        Given a network, return its maximum extents.
        This would be the minimum x value of all nodes,
        the minimum y value of all nodes,
        the maximum x value of all nodes and
        maximum y value of all nodes.

        @returns NetworkExtents object
        """
        extents = network.get_network_extents(network_id, **ctx.in_header.__dict__)

        ne = NetworkExtents()
        ne.network_id = extents['network_id']
        ne.min_x = extents['min_x']
        ne.max_x = extents['max_x']
        ne.min_y = extents['min_y']
        ne.max_y = extents['max_y']

        return ne

    @rpc(Integer, Node, _returns=Node)
    def add_node(ctx, network_id, node):

        """
        Add a node to a network:

        .. code-block:: python

            (Node){
               id = 1027
               name = "Node 1"
               description = "Node Description"
               x = 0.0
               y = 0.0
               attributes =
                  (ResourceAttrArray){
                     ResourceAttr[] =
                        (ResourceAttr){
                           attr_id = 1234
                        },
                        (ResourceAttr){
                           attr_id = 4321
                        },
                  }
             }
        """

        node_dict = network.add_node(network_id, node, **ctx.in_header.__dict__)

        new_node = Node(node_dict)

        return new_node


    @rpc(Integer,  SpyneArray(Node), _returns=SpyneArray(Node))
    def add_nodes(ctx, network_id, nodes):

        """
        Add a nodes to a network
        """

        node_s = network.add_nodes(network_id, nodes, **ctx.in_header.__dict__)
        new_nodes=[]
        for node in nodes:
            for node_ in node_s:
                if(node.name==node_.node_name):
                    new_nodes.append(Node(node_, summary=True))
                    break

        return new_nodes

    @rpc(Integer,  SpyneArray(Link), _returns=SpyneArray(Link))
    def add_links(ctx, network_id, links):

        """
        Add a nodes to a network
        """
        link_s = network.add_links(network_id, links, **ctx.in_header.__dict__)

        new_links=[]
        for link in links:
            for link_ in link_s:
                if(link.name==link_.link_name):
                    new_links.append(Link(link_, summary=True))
                    break

        return new_links


    @rpc(Node, _returns=Node)
    def update_node(ctx, node):
        """
        Update a node.
        If new attributes are present, they will be added to the node.
        The non-presence of attributes does not remove them.

        .. code-block:: python

            (Node){
               id = 1039
               name = "Node 1"
               description = "Node Description"
               x = 0.0
               y = 0.0
               status = "A"
               attributes =
                  (ResourceAttrArray){
                     ResourceAttr[] =
                        (ResourceAttr){
                           id = 850
                           attr_id = 1038
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                        (ResourceAttr){
                           id = 852
                           attr_id = 1040
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                  }
             }

        """

        node_dict = network.update_node(node, **ctx.in_header.__dict__)
        updated_node = Node(node_dict)

        return updated_node


    @rpc(Integer, _returns=Unicode)
    def delete_node(ctx, node_id):
        """
            Set the status of a node to 'X'
        """
        #check_perm('edit_topology')
        network.set_node_status(node_id, 'X', **ctx.in_header.__dict__)
        return 'OK'
    @rpc(Integer, _returns=Unicode)
    def activate_node(ctx, node_id):
        """
            Set the status of a node to 'A'
        """
        #check_perm('edit_topology')
        network.set_node_status(node_id, 'A', **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Unicode(pattern="[YN]", default='Y'), _returns=Unicode)
    def purge_node(ctx, node_id, purge_data):
        """
            Remove node from DB completely
            If there are attributes on the node, use purge_data to try to
            delete the data. If no other resources link to this data, it
            will be deleted.

        """
        network.delete_node(node_id, purge_data, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Link, _returns=Link)
    def add_link(ctx, network_id, link):
        """
            Add a link to a network
        """

        link_dict = network.add_link(network_id, link, **ctx.in_header.__dict__)
        new_link = Link(link_dict)

        return new_link

    @rpc(Link, _returns=Link)
    def update_link(ctx, link):
        """
            Update a link.
        """
        link_dict = network.update_link(link, **ctx.in_header.__dict__)
        updated_link = Link(link_dict)

        return updated_link

    @rpc(Integer, _returns=Unicode)
    def delete_link(ctx, link_id):
        """
            Set the status of a link to 'X'
        """
        network.set_link_status(link_id, 'X', **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=Unicode)
    def activate_link(ctx, link_id):
        """
            Set the status of a link to 'X'
        """
        network.set_link_status(link_id, 'A', **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Unicode(pattern="[YN]", default='Y'), _returns=Unicode)
    def purge_link(ctx, link_id, purge_data):
        """
            Remove link from DB completely
            If there are attributes on the link, use purge_data to try to
            delete the data. If no other resources link to this data, it
            will be deleted.
        """
        network.delete_link(link_id, purge_data, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, ResourceGroup, _returns=ResourceGroup)
    def add_group(ctx, network_id, group):
        """
            Add a resourcegroup to a network
        """

        group_i = network.add_group(network_id, group, **ctx.in_header.__dict__)
        new_group = ResourceGroup(group_i)

        return new_group

    @rpc(Integer, _returns=Unicode)
    def delete_group(ctx, group_id):
        """
            Set the status of a group to 'X'
        """
        network.set_group_status(group_id, 'X', **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Unicode(pattern="[YN]", default='Y'), _returns=Unicode)
    def purge_group(ctx, group_id, purge_data):
        """
            Remove a resource group from the DB completely. If purge data is set 
            to 'Y', any data that is unconnected after the removal of the group
            will be removed also.
        """
        network.delete_group(group_id, purge_data, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=Unicode)
    def activate_group(ctx, group_id):
        """
            Set the status of a group to 'A'
        """
        network.set_group_status(group_id, 'A', **ctx.in_header.__dict__)
        return 'OK'


    @rpc(Integer, _returns=SpyneArray(Scenario))
    def get_scenarios(ctx, network_id):
        """
            Get all the scenarios in a given network.
        """
        scenarios_i = network.get_scenarios(network_id, **ctx.in_header.__dict__)

        scenarios = []
        for scen in scenarios_i:
            scen.load()
            s_complex = Scenario(scen)
            scenarios.append(s_complex)

        return scenarios

    @rpc(Integer, _returns=SpyneArray(Integer))
    def validate_network_topology(ctx, network_id):
        """
            Check for the presence of orphan nodes in a network.
        """
        return network.validate_network_topology(network_id, **ctx.in_header.__dict__)

    @rpc(Integer, Integer, _returns=SpyneArray(ResourceSummary))
    def get_resources_of_type(ctx, network_id, type_id):
        """
            Return a list of Nodes, Links or ResourceGroups
            which have the specified type.
            @returns list of ResourceSummary objects.
            These objects contain the attributes common to all resources, namely:
            type, id, name, description, attribues and types.
        """

        nodes, links, groups = network.get_resources_of_type(network_id, type_id, **ctx.in_header.__dict__)

        resources = []
        for n in nodes:
            resources.append(ResourceSummary(n))
        for l in links:
            resources.append(ResourceSummary(l))
        for g in groups:
            resources.append(ResourceSummary(g))

        return resources

    @rpc(Integer, _returns=Unicode)
    def clean_up_network(ctx, network_id):
        """
            Purge all nodes, links, groups and scenarios from a network which
            have previously been deleted.
        """
        return network.clean_up_network(network_id, **ctx.in_header.__dict__)

    @rpc(Integer, Integer, Integer(max_occurs="unbounded"), Unicode(pattern="['YN']", default='N'), _returns=SpyneArray(ResourceAttr))
    def get_all_node_data(ctx, network_id, scenario_id, node_ids, include_metadata):
        """
            Return all the attributes for all the nodes in a given network and a
            given scenario.
            Returns a list of ResourceAttr objects, each with a resourcescenario
            attribute, containing the actual value for the scenario specified.
        """
        start = datetime.datetime.now()

        node_resourcescenarios = network.get_attributes_for_resource(network_id, scenario_id, 'NODE', node_ids, include_metadata)

        log.info("Qry done in %s", (datetime.datetime.now() - start))
        start = datetime.datetime.now()

        return_ras = []
        for ns in node_resourcescenarios:
            ra = ResourceAttr(ns.resourceattr)
            x = ResourceScenario(ns, ra.attr_id)
            ra.resourcescenario = x
            return_ras.append(ra)

        log.info("Return vals built in %s", (datetime.datetime.now() - start))

        return return_ras

    @rpc(Integer, Unicode(pattern="['YN']", default='N'), Unicode(pattern="['YN']", default='N'), Integer(min_occurs=0, max_occurs=1), Integer(min_occurs=0, max_occurs=1), _returns=SpyneArray(ResourceData))
    def get_all_resource_data(ctx, scenario_id, include_values, include_metadata, page_start, page_end):
        """
        Return all the attributes for all the nodes in a given network and a
        given scenario.

        :returns: An array of soap_server.hydra_complexmodels.ResourceData

        In this function array data and timeseries data are returned as JSON strings.

        If your data structure looks like:

        +----+----+-----+
        | H1 | H2 | H3  |
        +====+====+=====+
        | 1  | 10 | 100 |
        +----+----+-----+
        | 2  | 20 | 200 |
        +----+----+-----+
        | 3  | 30 | 300 |
        +----+----+-----+
        | 4  | 40 | 400 |
        +----+----+-----+

        Then hydra will provide the data in the following format:

        '{
            "H1" : {"0":1, "1":2, "3":3, "4":4},\n
            "H2"  : {"0":10, "1":20, "3":30, "4":40},\n
            "H3"  : {"0":100, "1":200, "3":300, "4":400}\n
        }'

        For a timeseries:

        +-------------------------+----+----+-----+
        | Time                    | H1 | H2 | H3  |
        +=========================+====+====+=====+
        | 2014/09/04 16:46:12:00  | 1  | 10 | 100 |
        +-------------------------+----+----+-----+
        | 2014/09/05 16:46:12:00  | 2  | 20 | 200 |
        +-------------------------+----+----+-----+
        | 2014/09/06 16:46:12:00  | 3  | 30 | 300 |
        +-------------------------+----+----+-----+
        | 2014/09/07 16:46:12:00  | 4  | 40 | 400 |
        +-------------------------+----+----+-----+

        Then hydra will provide the data in the following format:

        '{
            "H1" : {\n
                    "2014/09/04 16:46:12:00":1,\n
                    "2014/09/05 16:46:12:00":2,\n
                    "2014/09/06 16:46:12:00":3,\n
                    "2014/09/07 16:46:12:00":4},\n

            "H2" : {\n
                    "2014/09/04 16:46:12:00":10,\n
                    "2014/09/05 16:46:12:00":20,\n
                    "2014/09/06 16:46:12:00":30,\n
                    "2014/09/07 16:46:12:00":40},\n

            "H3" :  {\n
                     "2014/09/04 16:46:12:00":100,\n
                     "2014/09/05 16:46:12:00":200,\n
                     "2014/09/06 16:46:12:00":300,\n
                     "2014/09/07 16:46:12:00":400}\n
        }'
        """
        start = datetime.datetime.now()

        log.info("Getting all resource data for scenario %s", scenario_id)
        node_resourcedata = network.get_all_resource_data(scenario_id,
                                                          include_metadata=include_metadata,
                                                          page_start=page_start,
                                                          page_end=page_end)

        log.info("Qry done in %s", (datetime.datetime.now() - start))

        start = datetime.datetime.now()

        return_ras = []
        for nodeattr in node_resourcedata:
            ra = ResourceData(nodeattr, include_values)
            return_ras.append(ra)

        log.info("%s return data found in %s", len(return_ras), (datetime.datetime.now() - start))

        return return_ras

    @rpc(Integer, Integer, Integer(max_occurs="unbounded"), Unicode(pattern="['YN']", default='N'), _returns=SpyneArray(ResourceAttr))
    def get_all_link_data(ctx, network_id, scenario_id, link_ids, include_metadata):
        """
            Return all the attributes for all the links in a given network and a
            given scenario.
            Returns a list of ResourceAttr objects, each with a resourcescenario
            attribute, containing the actual value for the scenario specified.
        """
        start = datetime.datetime.now()

        link_resourcescenarios = network.get_attributes_for_resource(network_id, scenario_id, 'LINK', link_ids, include_metadata)

        log.info("Qry done in %s", (datetime.datetime.now() - start))
        start = datetime.datetime.now()

        return_ras = []
        for linkrs in link_resourcescenarios:
            ra = ResourceAttr(linkrs.resourceattr)
            ra.resourcescenario = ResourceScenario(linkrs, ra.attr_id)
            return_ras.append(ra)

        log.info("Return vals built in %s", (datetime.datetime.now() - start))

        return return_ras

    @rpc(Integer, Integer, Integer(max_occurs="unbounded"), Unicode(pattern="['YN']", default='N'), _returns=SpyneArray(ResourceAttr))
    def get_all_group_data(ctx, network_id, scenario_id, group_ids, include_metadata):
        """
            Return all the attributes for all the groups in a given network and a
            given scenario.
            Returns a list of ResourceAttr objects, each with a resourcescenario
            attribute, containing the actual value for the scenario specified.
        """

        group_resourcescenarios = network.get_attributes_for_resource(network_id, scenario_id, 'GROUP', group_ids, include_metadata)
        return_ras = []
        for grouprs in group_resourcescenarios:
            ra = ResourceAttr(grouprs.resourceattr)
            ra.resourcescenario = ResourceScenario(grouprs, ra.attr_id)
            return_ras.append(ra)

        return return_ras
