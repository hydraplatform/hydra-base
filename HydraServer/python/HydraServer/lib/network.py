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
import logging
from HydraLib.HydraException import HydraError, ResourceNotFoundError
import scenario
import datetime
import data
import time

from HydraServer.util.permissions import check_perm
import template
from HydraServer.db.model import Project, Network, Scenario, Node, Link, ResourceGroup,\
        ResourceAttr, ResourceType, ResourceGroupItem, Dataset, Metadata, DatasetOwner,\
        ResourceScenario, TemplateType, TypeAttr, Template
from sqlalchemy.orm import noload, joinedload, joinedload_all
from HydraServer.db import DBSession
from sqlalchemy import func, and_, or_, distinct
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import aliased
from HydraLib.hydra_dateutil import timestamp_to_ordinal
from HydraServer.util.hdb import add_attributes, add_resource_types

from sqlalchemy import case
from sqlalchemy.sql import null, literal_column

log = logging.getLogger(__name__)

class dictobj(dict):
    def __init__(self, obj_dict, extras={}):
        for k, v in extras.items():
            self[k] = v
            setattr(self, k, v)

        for k, v in obj_dict.items():
            self[k] = v
            setattr(self, k, v)

    def __getattr__(self, name):
        return self.get(name, None)

    def __setattr__(self, name, value):
        self[name] = value

def _update_attributes(resource_i, attributes):
    if attributes is None:
        return dict()
    attrs = {}
    attr_id_map = dict([(ra_i.resource_attr_id, ra_i) for ra_i in resource_i.attributes])

    #ra is for ResourceAttr
    for ra in attributes:

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, ra.attr_is_var)
        else:
            ra_i = attr_id_map[ra.id]
            ra_i.attr_is_var = ra.attr_is_var
        attrs[ra.id] = ra_i

    return attrs

def get_scenario_by_name(network_id, scenario_name,**kwargs):
    try:
        scen = DBSession.query(Scenario).filter(and_(Scenario.network_id==network_id, func.lower(Scenario.scenario_id) == scenario_name.lower())).one()
        return scen.scenario_id
    except NoResultFound:
        log.info("No scenario in network %s with name %s"\
                     % (network_id, scenario_name))
        return None

def get_timing(time):
    return datetime.datetime.now() - time

def _get_all_attributes(network):
    """
        Get all the complex mode attributes in the network so that they
        can be used for mapping to resource scenarios later.
    """
    attrs = network.attributes
    for n in network.nodes:
        attrs.extend(n.attributes)
    for l in network.links:
        attrs.extend(l.attributes)
    for g in network.resourcegroups:
        attrs.extend(g.attributes)

    return attrs

def _bulk_add_resource_attrs(network_id, ref_key, resources, resource_name_map):
    start_time = datetime.datetime.now()

    #List of resource attributes
    resource_attrs = {}

    #First get all the attributes assigned from the csv files.
    t0 = time.time()
    for resource in resources:
        resource_i = resource_name_map[resource.name]
        resource_attrs[resource.id] = []
        if resource.attributes is not None:
            for ra in resource.attributes:
                resource_attrs[resource.id].append({
                    'ref_key'     : ref_key,
                    'node_id'     : resource_i.node_id    if ref_key=='NODE' else None,
                    'link_id'     : resource_i.link_id    if ref_key=='LINK' else None,
                    'group_id'    : resource_i.group_id   if ref_key=='GROUP' else None,
                    'network_id'  : resource_i.network_id if ref_key=='NETWORK' else None,
                    'attr_id'     : ra.attr_id,
                    'attr_is_var' : ra.attr_is_var,
                })

    #Now get all the attributes supposed to be on the resources based on the types.
    t0 = time.time()
    all_types = DBSession.query(TemplateType).options(joinedload('typeattrs')).all()
    type_dict = {}
    for t in all_types:
        type_dict[t.type_id] = t.typeattrs
    #Holds all the attributes supposed to be on a resource based on its specified
    #type
    resource_resource_types = []
    for resource in resources:
        resource_i = resource_name_map[resource.name]
        existing_attrs = [ra['attr_id'] for ra in resource_attrs[resource.id]]
        if resource.types is not None:
            for resource_type in resource.types:
                #Go through all the resource types and add the appropriate resource
                #type entries
                resource_resource_types.append(
                    {
                        'ref_key'     : ref_key,
                        'node_id'     : resource_i.node_id    if ref_key=='NODE' else None,
                        'link_id'     : resource_i.link_id    if ref_key=='LINK' else None,
                        'group_id'    : resource_i.group_id   if ref_key=='GROUP' else None,
                        'network_id'  : resource_i.network_id if ref_key=='NETWORK' else None,
                        'type_id'     : resource_type.id,
                    }
                )
                #Go through all types in the resource and add attributes from these types
                #which have not already been added.
                typeattrs = type_dict[resource_type.id]
                for ta in typeattrs:
                    if ta.attr_id not in existing_attrs:
                        resource_attrs[resource.id].append({
                            'ref_key'     : ref_key,
                            'node_id'     : resource_i.node_id    if ref_key=='NODE' else None,
                            'link_id'     : resource_i.link_id    if ref_key=='LINK' else None,
                            'group_id'    : resource_i.group_id   if ref_key=='GROUP' else None,
                            'network_id'  : resource_i.network_id if ref_key=='NETWORK' else None,
                            'attr_id' : ta.attr_id,
                            'attr_is_var' : ta.attr_is_var,
                        })

    if len(resource_resource_types) > 0:
        DBSession.execute(ResourceType.__table__.insert(), resource_resource_types)
    logging.info("%s ResourceTypes inserted in %s secs", len(resource_resource_types), str(time.time() - t0))

    logging.info("Resource attributes from types added in %s"%(datetime.datetime.now() - start_time))

    if len(resource_attrs) > 0:
        all_resource_attrs = []
        for na in resource_attrs.values():
            all_resource_attrs.extend(na)
        if len(all_resource_attrs) > 0:
            DBSession.execute(ResourceAttr.__table__.insert(), all_resource_attrs)
            logging.info("ResourceAttr insert took %s secs"% str(time.time() - t0))
        else:
            logging.warn("No attributes on any resource....")

    logging.info("Resource attributes insertion from types done in %s"%(datetime.datetime.now() - start_time))

    #Now that the attributes are in, we need to map the attributes in the DB
    #to the attributes in the incoming data so that the resource scenarios
    #know what to refer to.
    res_qry = DBSession.query(ResourceAttr)
    if ref_key == 'NODE':
        res_qry = res_qry.join(Node).filter(Node.network_id==network_id)
    elif ref_key == 'GROUP':
        res_qry = res_qry.join(ResourceGroup).filter(ResourceGroup.network_id==network_id)
    elif ref_key == 'LINK':
        res_qry = res_qry.join(Link).filter(Link.network_id==network_id)

    real_resource_attrs = res_qry.all()
    logging.info("retrieved %s entries in %s"%(len(real_resource_attrs), datetime.datetime.now() - start_time))

    resource_attr_dict = {}
    for resource_attr in real_resource_attrs:
        if ref_key == 'NODE':
            ref_id = resource_attr.node_id
        elif ref_key == 'GROUP':
            ref_id = resource_attr.group_id
        elif ref_key == 'LINK':
            ref_id = resource_attr.link_id
        resource_attr_dict[(ref_id, resource_attr.attr_id)] = resource_attr

    logging.info("Processing Query results took %s"%(datetime.datetime.now() - start_time))


    resource_attrs = {}
    for resource in resources:
        iface_resource = resource_name_map[resource.name]
        if ref_key == 'NODE':
            ref_id = iface_resource.node_id
        elif ref_key == 'GROUP':
            ref_id = iface_resource.group_id
        elif ref_key == 'LINK':
            ref_id = iface_resource.link_id
        if resource.attributes is not None:
            for ra in resource.attributes:
                resource_attrs[ra.id] = resource_attr_dict[(ref_id, ra.attr_id)]
    logging.info("Resource attributes added in %s"%(datetime.datetime.now() - start_time))
    print " resource_attrs   size: ", len(resource_attrs)
    return resource_attrs

def _add_nodes_to_database(net_i, nodes):
    #First add all the nodes
    log.info("Adding nodes to network")
    node_list = []
    for node in nodes:
        node_dict = {'network_id'   : net_i.network_id,
                    'node_name' : node.name,
                     'node_description': node.description,
                     'layout'     : node.get_layout(),
                     'node_x'     : node.x,
                     'node_y'     : node.y,
                    }
        node_list.append(node_dict)
    t0 = time.time()
    if len(node_list):
        DBSession.execute(Node.__table__.insert(), node_list)
    logging.info("Node insert took %s secs"% str(time.time() - t0))

def _add_nodes(net_i, nodes):

    #check_perm(user_id, 'edit_topology')

    start_time = datetime.datetime.now()

    #List of resource attributes
    node_attrs = {}

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    if nodes is None or len(nodes) == 0:
        return node_id_map, node_attrs

    _add_nodes_to_database(net_i, nodes)

    iface_nodes = dict()
    for n_i in net_i.nodes:
        if iface_nodes.get(n_i.node_name) is not None:
            raise HydraError("Duplicate Node Name: %s"%(n_i.node_name))

        iface_nodes[n_i.node_name] = n_i

    for node in nodes:
        node_id_map[node.id] = iface_nodes[node.name]

    node_attrs = _bulk_add_resource_attrs(net_i.network_id, 'NODE', nodes, iface_nodes)

    log.info("Nodes added in %s", get_timing(start_time))

    return node_id_map, node_attrs

def _add_links_to_database(net_i, links, node_id_map):
    log.info("Adding links to network")
    link_dicts = []
    for link in links:
        node_1 = node_id_map.get(link.node_1_id)
        node_2 = node_id_map.get(link.node_2_id)

        if node_1 is None or node_2 is None:
            raise HydraError("Node IDS (%s, %s)are incorrect!"%(node_1, node_2))

        link_dicts.append({'network_id' : net_i.network_id,
                           'link_name' : link.name,
                           'link_description' : link.description,
                           'layout' : link.get_layout(),
                           'node_1_id' : node_1.node_id,
                           'node_2_id' : node_2.node_id
                          })
    if len(link_dicts) > 0:
        DBSession.execute(Link.__table__.insert(), link_dicts)

def _add_links(net_i, links, node_id_map):

    #check_perm(user_id, 'edit_topology')

    start_time = datetime.datetime.now()

    #List of resource attributes
    link_attrs = {}
    #Map negative IDS to their new, positive, counterparts.
    link_id_map = dict()

    if links is None or len(links) == 0:
        return link_id_map, link_attrs

    #Then add all the links.
#################################################################
    _add_links_to_database(net_i, links, node_id_map)
###################################################################
    log.info("Links added in %s", get_timing(start_time))
    iface_links = {}

    for l_i in net_i.links:

        if iface_links.get(l_i.link_name) is not None:
            raise HydraError("Duplicate Link Name: %s"%(l_i.link_name))

        iface_links[l_i.link_name] = l_i

    for link in links:
        link_id_map[link.id] = iface_links[link.name]

    link_attrs = _bulk_add_resource_attrs(net_i.network_id, 'LINK', links, iface_links)
    log.info("Links added in %s", get_timing(start_time))

    return link_id_map, link_attrs

def _add_resource_groups(net_i, resourcegroups):
    start_time = datetime.datetime.now()
    #List of resource attributes
    group_attrs = {}
    #Map negative IDS to their new, positive, counterparts.
    group_id_map = dict()

    if resourcegroups is None or len(resourcegroups)==0:
        return group_id_map, group_attrs
    #Then add all the groups.
    log.info("Adding groups to network")
    group_dicts = []
    if resourcegroups:
        for group in resourcegroups:

            group_dicts.append({'network_id' : net_i.network_id,
                           'group_name' : group.name,
                           'group_description' : group.description,
                          })

    if len(group_dicts) > 0:
        DBSession.execute(ResourceGroup.__table__.insert(), group_dicts)
        log.info("Resource Groups added in %s", get_timing(start_time))

        iface_groups = {}
        for g_i in net_i.resourcegroups:

            if iface_groups.get(g_i.group_name) is not None:
                raise HydraError("Duplicate Resource Group: %s"%(g_i.group_name))

            iface_groups[g_i.group_name] = g_i

        for group in resourcegroups:
            if group.id not in group_id_map:
                group_i = iface_groups[group.name]
                group_attrs[group.id] = []
                for ra in group.attributes:
                    group_attrs[group.id].append({
                        'ref_key' : 'GROUP',
                        'group_id' : group_i.group_id,
                        'attr_id' : ra.attr_id,
                        'attr_is_var' : ra.attr_is_var,
                    })
                group_id_map[group.id] = group_i
            group_id_map[group.id] = group_i


        group_attrs = _bulk_add_resource_attrs(net_i.network_id, 'GROUP', resourcegroups, iface_groups)
    log.info("Groups added in %s", get_timing(start_time))

    return group_id_map, group_attrs


def add_network(network,**kwargs):
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
    DBSession.autoflush = False
    user_id = kwargs.get('user_id')

    #check_perm('add_network')

    start_time = datetime.datetime.now()
    log.debug("Adding network")

    insert_start = datetime.datetime.now()

    proj_i = DBSession.query(Project).filter(Project.project_id == network.project_id).first()
    if proj_i is None:
        raise HydraError("Project ID is none. A project ID must be specified on the Network")

    existing_net = DBSession.query(Network).filter(Network.project_id == network.project_id, Network.network_name==network.name).first()
    if existing_net is not None:
        raise HydraError("A network with the name %s is already in project %s"%(network.name, network.project_id))

    proj_i.check_write_permission(user_id)

    net_i = Network()
    net_i.project_id          = network.project_id
    net_i.network_name        = network.name
    net_i.network_description = network.description
    net_i.created_by          = user_id
    net_i.projection          = network.projection

    if network.layout is not None:
        net_i.layout = network.get_layout()

    network.network_id = net_i.network_id
    DBSession.add(net_i)
    DBSession.flush()
    #These two lists are used for comparison and lookup, so when
    #new attributes are added, these lists are extended.

    #List of all the resource attributes
    all_resource_attrs = {}

    network_attrs  = add_attributes(net_i, network.attributes)
    add_resource_types(net_i, network.types)

    all_resource_attrs.update(network_attrs)

    log.info("Network attributes added in %s", get_timing(start_time))
    node_id_map, node_attrs = _add_nodes(net_i, network.nodes)
    all_resource_attrs.update(node_attrs)

    link_id_map, link_attrs = _add_links(net_i, network.links, node_id_map)
    all_resource_attrs.update(link_attrs)

    grp_id_map, grp_attrs = _add_resource_groups(net_i, network.resourcegroups)
    all_resource_attrs.update(grp_attrs)

    start_time = datetime.datetime.now()

    scenario_names = []
    if network.scenarios is not None:
        log.info("Adding scenarios to network")
        for s in network.scenarios:

            if s.name in scenario_names:
                raise HydraError("Duplicate scenario name: %s"%(s.name))

            scen = Scenario()
            scen.scenario_name        = s.name
            scen.scenario_description = s.description
            scen.layout               = s.get_layout()
            scen.start_time           = str(timestamp_to_ordinal(s.start_time)) if s.start_time else None
            scen.end_time             = str(timestamp_to_ordinal(s.end_time)) if s.end_time else None
            scen.time_step            = s.time_step
            scen.created_by           = user_id

            scenario_names.append(s.name)

            #extract the data from each resourcescenario
            incoming_datasets = []
            scenario_resource_attrs = []
            for r_scen in s.resourcescenarios:
                ra = all_resource_attrs[r_scen.resource_attr_id]
                incoming_datasets.append(r_scen.value)
                scenario_resource_attrs.append(ra)

            data_start_time = datetime.datetime.now()

            datasets = data._bulk_insert_data(
                                              incoming_datasets,
                                              user_id,
                                              kwargs.get('app_name')
                                             )

            log.info("Data bulk insert took %s", get_timing(data_start_time))
            ra_start_time = datetime.datetime.now()
            for i, ra in enumerate(scenario_resource_attrs):
                scen.add_resource_scenario(ra, datasets[i], source=kwargs.get('app_name'))

            log.info("Resource scenarios added in  %s", get_timing(ra_start_time))

            item_start_time = datetime.datetime.now()
            if s.resourcegroupitems is not None:
                for group_item in s.resourcegroupitems:
                    group_item_i = ResourceGroupItem()
                    group_item_i.group = grp_id_map[group_item.group_id]
                    group_item_i.ref_key  = group_item.ref_key
                    if group_item.ref_key == 'NODE':
                        group_item_i.node = node_id_map[group_item.ref_id]
                    elif group_item.ref_key == 'LINK':
                        group_item_i.link = link_id_map[group_item.ref_id]
                    elif group_item.ref_key == 'GROUP':
                        group_item_i.subgroup = grp_id_map[group_item.ref_id]
                    else:
                        raise HydraError("A ref key of %s is not valid for a "
                                         "resource group item.",\
                                         group_item.ref_key)

                    scen.resourcegroupitems.append(group_item_i)
            log.info("Group items insert took %s", get_timing(item_start_time))
            net_i.scenarios.append(scen)

    log.info("Scenarios added in %s", get_timing(start_time))
    net_i.set_owner(user_id)

    DBSession.flush()
    log.info("Insertion of network took: %s",(datetime.datetime.now()-insert_start))

    return net_i

def _get_all_resource_attributes(network_id, template_id=None):
    """
        Get all the attributes for the nodes, links and groups of a network.
        Return these attributes as a dictionary, keyed on type (NODE, LINK, GROUP)
        then by ID of the node or link.
    """
    base_qry = DBSession.query(ResourceAttr.resource_attr_id.label('resource_attr_id'),
                               ResourceAttr.ref_key.label('ref_key'),
                               ResourceAttr.cr_date.label('cr_date'),
                               ResourceAttr.attr_is_var.label('attr_is_var'),
                               ResourceAttr.node_id.label('node_id'),
                               ResourceAttr.link_id.label('link_id'),
                               ResourceAttr.group_id.label('group_id'),
                               ResourceAttr.network_id.label('network_id'),
                               ResourceAttr.attr_id.label('attr_id'),
                              )
    

    all_node_attribute_qry = base_qry.join(Node).filter(Node.network_id==network_id)

    all_link_attribute_qry = base_qry.join(Link).filter(Link.network_id==network_id)

    all_group_attribute_qry = base_qry.join(ResourceGroup).filter(ResourceGroup.network_id==network_id)
    network_attribute_qry = base_qry.filter(ResourceAttr.network_id==network_id)


    #Filter the group attributes by template
    if template_id is not None:
        all_node_attribute_qry = all_node_attribute_qry.join(ResourceType).join(TemplateType).join(TypeAttr).filter(TemplateType.template_id==template_id).filter(ResourceAttr.attr_id==TypeAttr.attr_id)
        all_link_attribute_qry = all_link_attribute_qry.join(ResourceType).join(TemplateType).join(TypeAttr).filter(TemplateType.template_id==template_id).filter(ResourceAttr.attr_id==TypeAttr.attr_id)
        all_group_attribute_qry = all_group_attribute_qry.join(ResourceType).join(TemplateType).join(TypeAttr).filter(TemplateType.template_id==template_id).filter(ResourceAttr.attr_id==TypeAttr.attr_id)
        network_attribute_qry = network_attribute_qry.join(ResourceType, ResourceAttr.network_id==ResourceType.network_id).join(TemplateType).join(TypeAttr).filter(TemplateType.template_id==template_id).filter(ResourceAttr.attr_id==TypeAttr.attr_id)

    x = time.time()
    logging.info("Getting all attributes using execute")
    attribute_qry = all_node_attribute_qry.union(all_link_attribute_qry, all_group_attribute_qry, network_attribute_qry)
    all_attributes = DBSession.execute(attribute_qry.statement).fetchall()
    log.info("%s attrs retrieved in %s", len(all_attributes), time.time()-x)

    logging.info("Attributes retrieved. Processing results...")
    x = time.time()
    node_attr_dict = dict() 
    link_attr_dict = dict()
    group_attr_dict = dict()
    network_attr_dict = dict()

    for attr in all_attributes:
        if attr.ref_key == 'NODE':
            nodeattr = node_attr_dict.get(attr.node_id, [])
            nodeattr.append(attr)
            node_attr_dict[attr.node_id] = nodeattr
        elif attr.ref_key == 'LINK':
            linkattr = link_attr_dict.get(attr.link_id, [])
            linkattr.append(attr)
            link_attr_dict[attr.link_id] = linkattr
        elif attr.ref_key == 'GROUP':
            groupattr = group_attr_dict.get(attr.group_id, [])
            groupattr.append(attr)
            group_attr_dict[attr.group_id] = groupattr
        elif attr.ref_key == 'NETWORK':
            networkattr = network_attr_dict.get(attr.network_id, [])
            networkattr.append(attr)
            network_attr_dict[attr.network_id] = networkattr

    all_attributes = {
        'NODE' : node_attr_dict,
        'LINK' : link_attr_dict,
        'GROUP': group_attr_dict,
        'NETWORK': network_attr_dict,
    }

    logging.info("Attributes processed in %s", time.time()-x)
    return all_attributes

def _get_all_templates(network_id, template_id):
    """
        Get all the templates for the nodes, links and groups of a network.
        Return these templates as a dictionary, keyed on type (NODE, LINK, GROUP)
        then by ID of the node or link.
    """
    base_qry = DBSession.query(
                               ResourceType.ref_key.label('ref_key'),
                               ResourceType.node_id.label('node_id'),
                               ResourceType.link_id.label('link_id'),
                               ResourceType.group_id.label('group_id'),
                               ResourceType.network_id.label('network_id'),
                               Template.template_name.label('template_name'),
                               Template.template_id.label('template_id'),
                               TemplateType.type_id.label('type_id'),
                               TemplateType.layout.label('layout'),
                               TemplateType.type_name.label('type_name'),
                              ).filter(TemplateType.type_id==ResourceType.type_id,
                                       Template.template_id==TemplateType.template_id)
    

    all_node_type_qry = base_qry.filter(Node.node_id==ResourceType.node_id,
                                        Node.network_id==network_id)

    all_link_type_qry = base_qry.filter(Link.link_id==ResourceType.link_id,
                                        Link.network_id==network_id)

    all_group_type_qry = base_qry.filter(ResourceGroup.group_id==ResourceType.group_id,
                                         ResourceGroup.network_id==network_id)

    network_type_qry = base_qry.filter(ResourceType.network_id==network_id)

    #Filter the group attributes by template
    if template_id is not None:
        all_node_type_qry = all_node_type_qry.filter(Template.template_id==template_id)
        all_link_type_qry = all_link_type_qry.filter(Template.template_id==template_id)
        all_group_type_qry = all_group_type_qry.filter(Template.template_id==template_id)

    x = time.time()
    logging.info("Getting all types")
    type_qry = all_node_type_qry.union(all_link_type_qry, all_group_type_qry, network_type_qry)
    all_types = DBSession.execute(type_qry.statement).fetchall()
    log.info("%s types retrieved in %s", len(all_types), time.time()-x)


    logging.info("Attributes retrieved. Processing results...")
    x = time.time()
    node_type_dict = dict() 
    link_type_dict = dict()
    group_type_dict = dict()
    network_type_dict = dict()

    for t in all_types:

        resourcetype = dictobj({'type_id':t.type_id})
        resourcetype.templatetype = dictobj(t)
        rttemplate = dictobj({'template_name':t.template_name})
        resourcetype.templatetype.template =rttemplate 

        if t.ref_key == 'NODE':
            nodetype = node_type_dict.get(t.node_id, [])
            nodetype.append(resourcetype)
            node_type_dict[t.node_id] = nodetype 
        elif t.ref_key == 'LINK':
            linktype = link_type_dict.get(t.link_id, [])
            linktype.append(resourcetype)
            link_type_dict[t.link_id] = linktype
        elif t.ref_key == 'GROUP':
            grouptype = group_type_dict.get(t.group_id, [])
            grouptype.append(resourcetype)
            group_type_dict[t.group_id] = grouptype
        elif t.ref_key == 'NETWORK':
            nettype = network_type_dict.get(t.network_id, [])
            nettype.append(resourcetype)
            network_type_dict[t.network_id] = nettype


    all_types = {
        'NODE' : node_type_dict,
        'LINK' : link_type_dict,
        'GROUP': group_type_dict,
        'NETWORK': network_type_dict,
    }

    logging.info("Attributes processed in %s", time.time()-x)
    return all_types


def _get_all_group_items(network_id):
    """
        Get all the resource group items in the network, across all scenarios
        returns a dictionary of dict objects, keyed on scenario_id
    """
    base_qry = DBSession.query(ResourceGroupItem) 

    item_qry = base_qry.join(Scenario).filter(Scenario.network_id==network_id)

    x = time.time()
    logging.info("Getting all items")
    all_items = DBSession.execute(item_qry.statement).fetchall()
    log.info("%s groups jointly retrieved in %s", len(all_items), time.time()-x)


    logging.info("items retrieved. Processing results...")
    x = time.time()
    item_dict = dict() 
    for item in all_items:

        items = item_dict.get(item.scenario_id, [])
        items.append(item)
        item_dict[item.scenario_id] = items 

    logging.info("items processed in %s", time.time()-x)

    return item_dict

def _get_all_resourcescenarios(network_id, user_id):
    """
        Get all the resource scenarios in a network, across all scenarios
        returns a dictionary of dict objects, keyed on scenario_id
    """ 

    rs_qry = DBSession.query(
                Dataset.data_type,
                Dataset.data_units,
                Dataset.data_dimen,
                Dataset.data_name,
                Dataset.data_hash,
                Dataset.cr_date,
                Dataset.created_by,
                Dataset.hidden,
                Dataset.start_time,
                Dataset.frequency,
                Dataset.value,
                ResourceScenario.dataset_id,
                ResourceScenario.scenario_id,
                ResourceScenario.resource_attr_id,
                ResourceScenario.source,
                ResourceAttr.attr_id,
    ).outerjoin(DatasetOwner, and_(DatasetOwner.dataset_id==Dataset.dataset_id, DatasetOwner.user_id==user_id)).filter(
                or_(Dataset.hidden=='N', DatasetOwner.user_id != None),
                ResourceAttr.resource_attr_id == ResourceScenario.resource_attr_id,
                Scenario.scenario_id==ResourceScenario.scenario_id,
                Scenario.network_id==network_id,
                Dataset.dataset_id==ResourceScenario.dataset_id)

    x = time.time()
    logging.info("Getting all resource scenarios")
    all_rs = DBSession.execute(rs_qry.statement).fetchall()
    log.info("%s resource scenarios retrieved in %s", len(all_rs), time.time()-x)


    logging.info("resource scenarios retrieved. Processing results...")
    x = time.time()
    rs_dict = dict() 
    for rs in all_rs:
        rs_obj = dictobj(rs)
        rs_attr = dictobj({'attr_id':rs.attr_id})
        rs_dataset = dictobj({
            'dataset_id':rs.dataset_id,
            'data_type' : rs.data_type,
            'data_units':rs.data_units,
            'data_dimen':rs.data_dimen,
            'data_name':rs.data_name,
            'data_hash':rs.data_hash,
            'cr_date':rs.cr_date,
            'created_by':rs.created_by,
            'hidden':rs.hidden,
            'start_date':rs.start_time,
            'frequency':rs.frequency,
            'value':rs.value,
            'metadata':[],
        })
        rs_obj.resourceattr = rs_attr
        rs_obj.dataset = rs_dataset

        scenario_rs = rs_dict.get(rs.scenario_id, [])
        scenario_rs.append(rs_obj)
        rs_dict[rs.scenario_id] = scenario_rs 

    logging.info("resource scenarios processed in %s", time.time()-x)

    return rs_dict


def _get_metadata(network_id, user_id):
    """
        Get all the metadata in a network, across all scenarios
        returns a dictionary of dict objects, keyed on dataset ID 
    """
    
    dataset_qry = DBSession.query(
                Dataset
    ).outerjoin(DatasetOwner, and_(DatasetOwner.dataset_id==Dataset.dataset_id, DatasetOwner.user_id==user_id)).filter(
                or_(Dataset.hidden=='N', DatasetOwner.user_id != None),
                Scenario.scenario_id==ResourceScenario.scenario_id,
                Scenario.network_id==network_id,
                Dataset.dataset_id==ResourceScenario.dataset_id).distinct().subquery()

    rs_qry = DBSession.query(
                Metadata
    ).join(dataset_qry, Metadata.dataset_id==dataset_qry.c.dataset_id)

    x = time.time()
    logging.info("Getting all matadata")
    all_metadata = DBSession.execute(rs_qry.statement).fetchall()
    log.info("%s metadata jointly retrieved in %s",len(all_metadata), time.time()-x)

    logging.info("metadata retrieved. Processing results...")
    x = time.time()
    metadata_dict = dict() 
    for m in all_metadata:
        metadata = metadata_dict.get(m.dataset_id, [])
        metadata.append(m)
        metadata_dict[m.dataset_id] = metadata

    logging.info("metadata processed in %s", time.time()-x)

    return metadata_dict

def _get_nodes(network_id, template_id=None):
    """
        Get all the nodes in a network
    """
    extras = {'types':[], 'attributes':[]}

    node_qry = DBSession.query(Node).filter(
                        Node.network_id==network_id,
                        Node.status=='A').options(noload('network'))
    if template_id is not None:
        node_qry = node_qry.filter(ResourceType.node_id==Node.node_id, TemplateType.type_id==ResourceType.type_id, TemplateType.template_id==template_id)
    node_res = DBSession.execute(node_qry.statement).fetchall()
    
    nodes = []
    for n in node_res:
        nodes.append(dictobj(n, extras))

    return nodes

def _get_links(network_id, template_id=None):
    """
        Get all the links in a network
    """
    extras = {'types':[], 'attributes':[]}
    link_qry = DBSession.query(Link).filter(
                                        Link.network_id==network_id,
                                        Link.status=='A').options(noload('network'))
    if template_id is not None:
        link_qry = link_qry.filter(ResourceType.link_id==Link.link_id, TemplateType.type_id==ResourceType.type_id, TemplateType.template_id==template_id)

    link_res = DBSession.execute(link_qry.statement).fetchall()

    links = []
    for l in link_res:
        links.append(dictobj(l, extras))

    return links
    
def _get_groups(network_id, template_id=None):
    """
        Get all the resource groups in a network
    """
    extras = {'types':[], 'attributes':[]}
    group_qry = DBSession.query(ResourceGroup).filter(
                                        ResourceGroup.network_id==network_id,
                                        ResourceGroup.status=='A').options(noload('network'))
    if template_id is not None:
        group_qry = group_qry.filter(ResourceType.group_id==ResourceGroup.group_id, TemplateType.type_id==ResourceType.type_id, TemplateType.template_id==template_id)

    group_res = DBSession.execute(group_qry.statement).fetchall()
    groups = []
    for g in group_res:
        groups.append(dictobj(g, extras))

    return groups


def _get_scenarios(network_id, include_data, user_id, scenario_ids=None):
    """
        Get all the scenarios in a network
    """
    scen_qry = DBSession.query(Scenario).filter(
                    Scenario.network_id == network_id).options(
                        noload('network')).filter(
                        Scenario.status == 'A')

    if scenario_ids:
        logging.info("Filtering by scenario_ids %s",scenario_ids)
        scen_qry = scen_qry.filter(Scenario.scenario_id.in_(scenario_ids))
    extras = {'resourcescenarios': [], 'resourcegroupitems': []}
    scens = [dictobj(s,extras) for s in DBSession.execute(scen_qry.statement).fetchall()]
    
    all_resource_group_items = _get_all_group_items(network_id)

    if include_data == 'Y':
        all_rs = _get_all_resourcescenarios(network_id, user_id)
        metadata = _get_metadata(network_id, user_id)

    for s in scens:
        s.resourcegroupitems = all_resource_group_items.get(s.scenario_id, [])

        if include_data == 'Y':
            s.resourcescenarios  = all_rs.get(s.scenario_id, [])
    
            for rs in s.resourcescenarios:
                rs.dataset.metadata = metadata.get(rs.dataset_id, [])

    return scens

def get_network(network_id, summary=False, include_data='N', scenario_ids=None, template_id=None, **kwargs):
    """
        Return a whole network as a dictionary.
        network_id: ID of the network to retrieve
        include_data: 'Y' or 'N'. Indicate whether scenario data is to be returned.
                      This has a significant speed impact as retrieving large amounts
                      of data can be expensive.
        scenario_ids: list of IDS to be returned. Used if a network has multiple
                      scenarios but you only want one returned. Using this filter
                      will speed up this function call.
        template_id:  Return the network with only attributes associated with this
                      template on the network, groups, nodes and links.
    """
    log.debug("getting network %s"%network_id)
    user_id = kwargs.get('user_id')
   
    try:
        log.debug("Querying Network %s", network_id)
        net_i = DBSession.query(Network).filter(
                                Network.network_id == network_id).options(
                                noload('scenarios')).options(
                                noload('nodes')).options(
                                noload('links')).options(
                                noload('types')).options(
                                noload('attributes')).options(
                                noload('resourcegroups')).one()
        
        net_i.check_read_permission(user_id)
        
        net = dictobj(net_i.__dict__)

        net.nodes          = _get_nodes(network_id, template_id=template_id)
        net.links          = _get_links(network_id, template_id=template_id) 
        net.resourcegroups = _get_groups(network_id, template_id=template_id)

        if summary is False:
            all_attributes = _get_all_resource_attributes(network_id, template_id)
            log.info("Setting attributes")
            net.attributes = all_attributes['NETWORK'].get(network_id, [])
            for node in net.nodes:
                node.attributes = all_attributes['NODE'].get(node.node_id, [])
            log.info("Node attributes set")
            for link in net.links:
                link.attributes = all_attributes['LINK'].get(link.link_id, [])
            log.info("Link attributes set")
            for group in net.resourcegroups:
                group.attributes = all_attributes['GROUP'].get(group.group_id, [])
            log.info("Group attributes set")
    

        log.info("Setting types")
        all_types = _get_all_templates(network_id, template_id)
        net.types = all_types['NETWORK'].get(network_id, [])
        for node in net.nodes:
            node.types = all_types['NODE'].get(node.node_id, [])
        for link in net.links:
            link.types = all_types['LINK'].get(link.link_id, [])
        for group in net.resourcegroups:
            group.types = all_types['GROUP'].get(group.group_id, [])

        log.info("Getting scenarios")

        net.scenarios = _get_scenarios(network_id, include_data, user_id, scenario_ids)

    except NoResultFound:
        raise ResourceNotFoundError("Network (network_id=%s) not found." %
                                  network_id)

    return net

def get_node(node_id,**kwargs):
    try:
        n = DBSession.query(Node).filter(Node.node_id==node_id).options(joinedload_all('attributes.attr')).one()
        return n
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id,))

def get_link(link_id,**kwargs):
    try:
        l = DBSession.query(Link).filter(Link.link_id==link_id).one()
        return l
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id,))

def get_resourcegroup(group_id,**kwargs):
    try:
        rg = DBSession.query(ResourceGroup).filter(ResourceGroup.group_id==group_id).one()
        return rg
    except NoResultFound:
        raise ResourceNotFoundError("ResourceGroup %s not found"%(group_id,))

def get_network_by_name(project_id, network_name,**kwargs):
    """
    Return a whole network as a complex model.
    """

    try:
        res = DBSession.query(Network.network_id).filter(func.lower(Network.network_name).like(network_name.lower()), Network.project_id == project_id).one()
        net = get_network(res.network_id, 'Y', None, **kwargs)
        return net
    except NoResultFound:
        raise ResourceNotFoundError("Network with name %s not found"%(network_name))


def network_exists(project_id, network_name,**kwargs):
    """
    Return a whole network as a complex model.
    """
    try:
        DBSession.query(Network.network_id).filter(func.lower(Network.network_name).like(network_name.lower()), Network.project_id == project_id).one()
        return 'Y'
    except NoResultFound:
        return 'N'

def update_network(network,
    update_nodes=True,
    update_links=True,
    update_groups=True,
    update_scenarios=True,
    **kwargs):
    """
        Update an entire network
    """
    log.info("Updating Network %s", network.name)
    user_id = kwargs.get('user_id')
    #check_perm('update_network')

    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network.id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Network with id %s not found"%(network.id))

    net_i.project_id          = network.project_id
    net_i.network_name        = network.name
    net_i.network_description = network.description
    net_i.layout              = network.get_layout()

    all_resource_attrs = {}
    all_resource_attrs.update(_update_attributes(net_i, network.attributes))
    add_resource_types(net_i, network.types)

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()
    
    if network.nodes is not None and update_nodes is True:
        log.info("Updating nodes")
        t0 = time.time()
        #First add all the nodes
        node_id_map = dict([(n.node_id, n) for n in net_i.nodes])
        for node in network.nodes:
            #If we get a negative or null node id, we know
            #it is a new node.
            if node.id is not None and node.id > 0:
                n = node_id_map[node.id]
                n.node_name        = node.name
                n.node_description = node.description
                n.node_x           = node.x
                n.node_y           = node.y
                n.status           = node.status
            else:
                log.info("Adding new node %s", node.name)
                n = net_i.add_node(node.name,
                                   node.description,
                                   node.layout,
                                   node.x,
                                   node.y)
                net_i.nodes.append(n)
                node_id_map[n.id] = n

            all_resource_attrs.update(_update_attributes(n, node.attributes))
            add_resource_types(n, node.types)
        log.info("Updating nodes took %s", time.time() - t0)

    link_id_map = dict()
    if network.links is not None and update_links is True:
        log.info("Updating links")
        t0 = time.time()
        link_id_map = dict([(l.link_id, l) for l in net_i.links])
        for link in network.links:
            node_1 = node_id_map[link.node_1_id]

            node_2 = node_id_map[link.node_2_id]

            if link.id is None or link.id < 0:
                log.info("Adding new link %s", link.name)
                l = net_i.add_link(link.name,
                                   link.description,
                                   link.layout,
                                   node_1,
                                   node_2)
                net_i.links.append(l)
                link_id_map[link.id] = l
            else:
                l = link_id_map[link.id]
                l.link_name       = link.name
                l.link_descripion = link.description
                l.node_a          = node_1
                l.node_b          = node_2
                

            all_resource_attrs.update(_update_attributes(l, link.attributes))
            add_resource_types(l, link.types)
        log.info("Updating links took %s", time.time() - t0)

    group_id_map = dict()
    #Next all the groups
    if network.resourcegroups is not None and update_groups is True:
        log.info("Updating groups")
        t0 = time.time()
        group_id_map = dict([(g.group_id, g) for g in net_i.resourcegroups])
        for group in network.resourcegroups:
            #If we get a negative or null group id, we know
            #it is a new group.
            if group.id is not None and group.id > 0:
                g_i = group_id_map[group.id]
                g_i.group_name        = group.name
                g_i.group_description = group.description
                g_i.status           = group.status
            else:
                log.info("Adding new group %s", link.name)
                g_i = net_i.add_group(group.name,
                                   group.description,
                                   group.status)
                net_i.resourcegroups.append(net_i)
                group_id_map[g_i.group_id] = g_i

            all_resource_attrs.update(_update_attributes(g_i, group.attributes))
            add_resource_types(g_i, group.types)
            group_id_map[group.id] = g_i
        log.info("Updating groups took %s", time.time() - t0)

    errors = []
    if network.scenarios is not None and update_scenarios is True:
        for s in network.scenarios:
            add_scenario = False
            if s.id is not None:
                if s.id > 0:
                    try:
                        scen_i = DBSession.query(Scenario).filter(Scenario.scenario_id==s.id).one()
                        if scen_i.locked == 'Y':
                            errors.append('Scenario %s was not updated as it is locked'%(s.id))
                            continue

                        scenario.update_scenario(s, **kwargs)
                    except NoResultFound:
                        raise ResourceNotFoundError("Scenario %s not found"%(s.id))
                else:
                    add_scenario = True
            else:
                add_scenario = True

            if add_scenario is True:
                log.info("Adding new scenario %s to network", s.name)
                scenario.add_scenario(network.id, s, **kwargs)

    DBSession.flush()

    updated_net = get_network(network.id, summary=True, **kwargs)
    return updated_net

def set_network_status(network_id,status,**kwargs):
    """
    Activates a network by setting its status attribute to 'A'.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_network')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
        net_i.status = status
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    DBSession.flush()
    return 'OK'

def get_network_extents(network_id,**kwargs):
    """
    Given a network, return its maximum extents.
    This would be the minimum x value of all nodes,
    the minimum y value of all nodes,
    the maximum x value of all nodes and
    maximum y value of all nodes.

    @returns NetworkExtents object
    """
    rs = DBSession.query(Node.node_x, Node.node_y).filter(Node.network_id==network_id).all()
    x_values = []
    y_values = []
    for r in rs:
        x_values.append(r.node_x)
        y_values.append(r.node_y)

    x_values.sort()
    y_values.sort()

    ne = dict(
        network_id = network_id,
        min_x = x_values[0],
        max_x = x_values[-1],
        min_y = y_values[0],
        max_y = y_values[-1],
    )
    return ne



#########################################
def add_nodes(network_id, nodes,**kwargs):
    """
        Add nodes to network
    """
    start_time = datetime.datetime.now()

    names=[]        # used to check uniqueness of node name
    for n_i in nodes:
        if n_i.name in names:
            raise HydraError("Duplicate Node Name: %s"%(n_i.name))
        names.append(n_i.name)

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    _add_nodes_to_database(net_i, nodes)

    net_i.project_id=net_i.project_id
    DBSession.flush()

    node_s =  DBSession.query(Node).filter(Node.network_id==network_id).all()

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    iface_nodes = dict()
    for n_i in node_s:
        iface_nodes[n_i.node_name] = n_i

    for node in nodes:
        node_id_map[node.id] = iface_nodes[node.name]

    _bulk_add_resource_attrs(network_id, 'NODE', nodes, iface_nodes)

    log.info("Nodes added in %s", get_timing(start_time))
    return node_s

##########################################################################
def add_links(network_id, links,**kwargs):
    '''
    add links to network
    '''
    start_time = datetime.datetime.now()
    user_id = kwargs.get('user_id')
    names=[]        # used to check uniqueness of link name before saving links to database
    for l_i in links:
        if l_i.name in names:
            raise HydraError("Duplicate Link Name: %s"%(l_i.name))
        names.append(l_i.name)

    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    node_id_map=dict()
    for node in net_i.nodes:
       node_id_map[node.node_id]=node
    _add_links_to_database(net_i, links, node_id_map)

    net_i.project_id=net_i.project_id
    DBSession.flush()
    link_s =  DBSession.query(Link).filter(Link.network_id==network_id).all()
    iface_links = {}
    for l_i in link_s:
        iface_links[l_i.link_name] = l_i
    link_attrs = _bulk_add_resource_attrs(net_i.network_id, 'LINK', links, iface_links)
    log.info("Nodes added in %s", get_timing(start_time))
    return link_s
#########################################


def add_node(network_id, node,**kwargs):

    """
    Add a node to a network:
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    new_node = net_i.add_node(node.name, node.description, node.layout, node.x, node.y)

    add_attributes(new_node, node.attributes)

    DBSession.flush()
    
    if node.types is not None and len(node.types) > 0:
        res_types = []
        res_attrs = []
        for typesummary in node.types:
            ra, rt = template.set_resource_type(new_node,
                                            typesummary.id,
                                            **kwargs)
            res_types.append(rt)
            res_attrs.extend(ra)
        if len(res_types) > 0:
            DBSession.execute(ResourceType.__table__.insert(), res_types)
        if len(res_attrs) > 0:
            DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)

    DBSession.refresh(new_node)

    return new_node
#########################################################################

def update_node(node,**kwargs):
    """
    Update a node.
    If new attributes are present, they will be added to the node.
    The non-presence of attributes does not remove them.

    """
    user_id = kwargs.get('user_id')
    try:
        node_i = DBSession.query(Node).filter(Node.node_id == node.id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node.id))

    node_i.network.check_write_permission(user_id)

    node_i.node_name = node.name
    node_i.node_x    = node.x
    node_i.node_y    = node.y
    node_i.node_description = node.description
    node_i.layout           = node.get_layout()

    _update_attributes(node_i, node.attributes)

    add_resource_types(node_i, node.types)
    DBSession.flush()

    return node_i

def set_node_status(node_id, status, **kwargs):
    """
        Set the status of a node to 'X'
    """
    user_id = kwargs.get('user_id')
    try:
        node_i = DBSession.query(Node).filter(Node.node_id == node_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id))

    node_i.network.check_write_permission(user_id)

    node_i.status = status

    for link in node_i.links_to:
        link.status = status
    for link in node_i.links_from:
        link.status = status

    DBSession.flush()

    return node_i

def _unique_data_qry(count=1):
    rs = aliased(ResourceScenario)
       
    log.critical(count)
    subqry=DBSession.query(rs.resource_attr_id,
                           rs.dataset_id,
                           func.count(rs.dataset_id).label('dataset_count')).\
                                group_by(rs.dataset_id).\
                                having(func.count(rs.dataset_id) == count).\
                                subquery()

    unique_data = DBSession.query(rs).\
                        join(subqry,
                                and_(rs.resource_attr_id==subqry.c.resource_attr_id, 
                                rs.dataset_id==subqry.c.dataset_id)
                            ).\
                    filter(
                        rs.resource_attr_id == ResourceAttr.resource_attr_id
                    )
    return unique_data


def delete_node(node_id, purge_data,**kwargs):
    """
        Remove node from DB completely
        If there are attributes on the node, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.

    """
    user_id = kwargs.get('user_id')
    try:
        node_i = DBSession.query(Node).filter(Node.node_id == node_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id))
    
    group_items = DBSession.query(ResourceGroupItem).filter(
                                                    ResourceGroupItem.node_id==node_id).all()
    for gi in group_items:
        DBSession.delete(gi)

    if purge_data == 'Y':
        #Find the number of times a a resource and dataset combination
        #occurs. If this equals the number of times the dataset appears, then
        #we can say this datset is unique to this node.
        count = DBSession.query(ResourceScenario.dataset_id).distinct(
            ResourceScenario.resource_attr_id,
            ResourceScenario.dataset_id).filter(ResourceScenario.resource_attr_id==ResourceAttr.resource_attr_id,
                                               ResourceAttr.node_id==node_id).count()

        node_data_qry = _unique_data_qry(count)
        node_data_qry = node_data_qry.filter(ResourceAttr.node_id==node_id)
        node_data = node_data_qry.all()

        for node_datum in node_data:
            log.info("Deleting node dataset %s", node_datum.dataset_id)
            DBSession.delete(node_datum.dataset)

    log.info("Deleting node %s, id=%s", node_i.node_name, node_id)

    node_i.network.check_write_permission(user_id)
    DBSession.delete(node_i)
    DBSession.flush()
    return 'OK'

def add_link(network_id, link,**kwargs):
    """
        Add a link to a network
    """
    user_id = kwargs.get('user_id')

    #check_perm(user_id, 'edit_topology')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    try:
        node_1 = DBSession.query(Node).filter(Node.node_id==link.node_1_id).one()
        node_2 = DBSession.query(Node).filter(Node.node_id==link.node_2_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Nodes for link not found")

    link_i = net_i.add_link(link.name, link.description, link.layout, node_1, node_2)

    add_attributes(link_i, link.attributes)

    DBSession.flush()

    if link.types is not None and len(link.types) > 0:
        res_types = []
        res_attrs = []
        for typesummary in link.types:
            ra, rt = template.set_resource_type(link_i,
                                        typesummary.id,
                                         **kwargs)
            res_types.append(rt)
            res_attrs.extend(ra)

        if len(res_types) > 0:
            DBSession.execute(ResourceType.__table__.insert(), res_types)
        if len(res_attrs) > 0:
            DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)

    DBSession.refresh(link_i)

    return link_i

def update_link(link,**kwargs):
    """
        Update a link.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    try:
        link_i = DBSession.query(Link).filter(Link.link_id == link.id).one()
        link_i.network.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link.id))

    link_i.link_name = link.name
    link_i.node_1_id = link.node_1_id
    link_i.node_2_id = link.node_2_id
    link_i.link_description = link.description
    link_i.layout           = link.get_layout()

    add_attributes(link_i, link.attributes)
    add_resource_types(link_i, link.types)
    DBSession.flush()
    return link_i

def set_link_status(link_id, status, **kwargs):
    """
        Set the status of a link
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    try:
        link_i = DBSession.query(Link).filter(Link.link_id == link_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id))

    link_i.network.check_write_permission(user_id)

    link_i.status = status
    DBSession.flush()

def delete_link(link_id, purge_data,**kwargs):
    """
        Remove link from DB completely
        If there are attributes on the link, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.
    """
    user_id = kwargs.get('user_id')
    try:
        link_i = DBSession.query(Link).filter(Link.link_id == link_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id))

    group_items = DBSession.query(ResourceGroupItem).filter(
                                                    ResourceGroupItem.link_id==link_id).all()
    for gi in group_items:
        DBSession.delete(gi)

    if purge_data == 'Y':
        #Find the number of times a a resource and dataset combination
        #occurs. If this equals the number of times the dataset appears, then
        #we can say this datset is unique to this link.
        count = DBSession.query(ResourceScenario.dataset_id).distinct(
            ResourceScenario.resource_attr_id,
            ResourceScenario.dataset_id).filter(
                    ResourceScenario.resource_attr_id==ResourceAttr.resource_attr_id,
                    ResourceAttr.link_id==link_id).count()

        link_data_qry = _unique_data_qry(count)
        link_data_qry = link_data_qry.filter(ResourceAttr.link_id==link_id)
        link_data = link_data_qry.all()

        for link_datum in link_data:
            log.warn("Deleting link dataset %s", link_datum.dataset_id)
            DBSession.delete(link_datum.dataset)

    log.info("Deleting link %s, id=%s", link_i.link_name, link_id)

    link_i.network.check_write_permission(user_id)
    DBSession.delete(link_i)
    DBSession.flush()

def add_group(network_id, group,**kwargs):
    """
        Add a resourcegroup to a network
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    res_grp_i = net_i.add_group(group.name, group.description, group.status)

    add_attributes(res_grp_i, group.attributes)

    DBSession.flush()


    if group.types is not None and len(group.types) > 0:
        res_types = []
        res_attrs = []
        for typesummary in group.types:
            ra, rt = template.set_resource_type(res_grp_i,
                                        typesummary.id,
                                         **kwargs)
            res_types.append(rt)
            res_attrs.extend(ra)
        if len(res_types) > 0:
            DBSession.execute(ResourceType.__table__.insert(), res_types)
        if len(res_attrs) > 0:
            DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)

    DBSession.refresh(res_grp_i)

    return res_grp_i

def set_group_status(group_id, status, **kwargs):
    """
        Set the status of a group to 'X'
    """
    user_id = kwargs.get('user_id')
    try:
        group_i = DBSession.query(ResourceGroup).filter(ResourceGroup.group_id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("ResourceGroup %s not found"%(group_id))

    group_i.network.check_write_permission(user_id)

    group_i.status = status

    DBSession.flush()

    return group_i


def delete_group(group_id, purge_data,**kwargs):
    """
        Remove group from DB completely
        If there are attributes on the group, use purge_data to try to
        delete the data. If no other resources group to this data, it
        will be deleted.
    """
    user_id = kwargs.get('user_id')
    try:
        group_i = DBSession.query(ResourceGroup).filter(ResourceGroup.group_id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Group %s not found"%(group_id))

    group_items = DBSession.query(ResourceGroupItem).filter(
                                                    ResourceGroupItem.group_id==group_id).all()
    for gi in group_items:
        DBSession.delete(gi)
    
    if purge_data == 'Y':
        #Find the number of times a a resource and dataset combination
        #occurs. If this equals the number of times the dataset appears, then
        #we can say this datset is unique to this group.
        count = DBSession.query(ResourceScenario.dataset_id).distinct(
            ResourceScenario.resource_attr_id,
            ResourceScenario.dataset_id).filter(
                    ResourceScenario.resource_attr_id==ResourceAttr.resource_attr_id,
                    ResourceAttr.group_id==group_id).count()
        group_data_qry = _unique_data_qry(count)
        group_data_qry = group_data_qry.filter(ResourceAttr.group_id==group_id)
        group_data = group_data_qry.all()

        for group_datum in group_data:
            log.warn("Deleting group dataset %s", group_datum.dataset_id)
            DBSession.delete(group_datum.dataset)

    log.info("Deleting group %s, id=%s", group_i.group_name, group_id)

    group_i.network.check_write_permission(user_id)
    DBSession.delete(group_i)
    DBSession.flush()

def get_scenarios(network_id,**kwargs):
    """
        Get all the scenarios in a given network.
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    return net_i.scenarios

def validate_network_topology(network_id,**kwargs):
    """
        Check for the presence of orphan nodes in a network.
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    nodes = []
    for node_i in net_i.nodes:
        if node_i.status == 'A':
            nodes.append(node_i.node_id)

    link_nodes = []
    for link_i in net_i.links:
        if link_i.status != 'A':
            continue
        if link_i.node_1_id not in link_nodes:
            link_nodes.append(link_i.node_1_id)

        if link_i.node_2_id not in link_nodes:
            link_nodes.append(link_i.node_2_id)

    nodes = set(nodes)
    link_nodes = set(link_nodes)

    isolated_nodes = nodes - link_nodes

    return isolated_nodes

def get_resources_of_type(network_id, type_id, **kwargs):
    """
        Return the Nodes, Links and ResourceGroups which
        have the type specified.
    """

    nodes_with_type = DBSession.query(Node).join(ResourceType).filter(Node.network_id==network_id, ResourceType.type_id==type_id).all()
    links_with_type = DBSession.query(Link).join(ResourceType).filter(Link.network_id==network_id, ResourceType.type_id==type_id).all()
    groups_with_type = DBSession.query(ResourceGroup).join(ResourceType).filter(ResourceGroup.network_id==network_id, ResourceType.type_id==type_id).all()

    return nodes_with_type, links_with_type, groups_with_type

def clean_up_network(network_id, **kwargs):
    """
        Purge any deleted nodes, links, resourcegroups and scenarios in a given network
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_network')
    try:
        log.debug("Querying Network %s", network_id)
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).\
        options(noload('scenarios')).options(noload('nodes')).options(noload('links')).options(noload('resourcegroups')).options(joinedload_all('types.templatetype.template')).one()
        net_i.attributes

        #Define the basic resource queries
        node_qry = DBSession.query(Node).filter(Node.network_id==network_id).filter(Node.status=='X').all()

        link_qry = DBSession.query(Link).filter(Link.network_id==network_id).filter(Link.status=='X').all()

        group_qry = DBSession.query(ResourceGroup).filter(ResourceGroup.network_id==network_id).filter(ResourceGroup.status=='X').all()

        scenario_qry = DBSession.query(Scenario).filter(Scenario.network_id==network_id).filter(Scenario.status=='X').all()


        for n in node_qry:
            DBSession.delete(n)
        for l in link_qry:
            DBSession.delete(l)
        for g in group_qry:
            DBSession.delete(g)
        for s in scenario_qry:
            DBSession.delete(s)

    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    DBSession.flush()
    return 'OK'

def get_attributes_for_resource(network_id, scenario_id, ref_key, ref_ids=None, include_metadata='N', **kwargs):

    try:
        DBSession.query(Network).filter(Network.network_id==network_id).one()
    except NoResultFound:
        raise HydraError("Network %s does not exist"%network_id)

    try:
        DBSession.query(Scenario).filter(Scenario.scenario_id==scenario_id, Scenario.network_id==network_id).one()
    except NoResultFound:
        raise HydraError("Scenario %s not found."%scenario_id)

    rs_qry = DBSession.query(ResourceScenario).filter(
                            ResourceAttr.resource_attr_id==ResourceScenario.resource_attr_id,
                            ResourceScenario.scenario_id==scenario_id,
                            ResourceAttr.ref_key==ref_key)\
            .join(ResourceScenario.dataset)\
            .options(noload('dataset.metadata'))

    log.info("Querying %s data",ref_key)
    if ref_ids is not None and len(ref_ids) < 999:
        if ref_key == 'NODE':
            rs_qry = rs_qry.filter(ResourceAttr.node_id.in_(ref_ids))
        elif ref_key == 'LINK':
            rs_qry = rs_qry.filter(ResourceAttr.link_id.in_(ref_ids))
        elif ref_key == 'GROUP':
            rs_qry = rs_qry.filter(ResourceAttr.group_id.in_(ref_ids))

    all_resource_scenarios = rs_qry.all()
    log.info("Data retrieved")
    resource_scenarios = []
    dataset_ids      = []
    if ref_ids is not None:
        log.info("Pulling out requested info")
        for r in all_resource_scenarios:
            ra = r.resourceattr
            if ref_key == 'NODE':
                if ra.node_id in ref_ids:
                    resource_scenarios.append(r)
                    if r.dataset_id not in dataset_ids:
                        dataset_ids.append(r.dataset_id)
            elif ref_key == 'LINK':
                if ra.link_id in ref_ids:
                    resource_scenarios.append(r)
                    if r.dataset_id not in dataset_ids:
                        dataset_ids.append(r.dataset_id)
            elif ref_key == 'GROUP':
                if ra.group_id in ref_ids:
                    resource_scenarios.append(r)
                    if r.dataset_id not in dataset_ids:
                        dataset_ids.append(r.dataset_id)
            else:
                resource_scenarios.append(ra)
        log.info("Requested info pulled out.")
    else:
        resource_scenarios = all_resource_scenarios

    log.info("Retrieved %s resource attrs", len(resource_scenarios))

    if include_metadata == 'Y':
        metadata_qry = DBSession.query(Metadata).filter(
                            ResourceAttr.ref_key==ref_key,
                            ResourceScenario.resource_attr_id==ResourceAttr.resource_attr_id,
                            ResourceScenario.scenario_id==scenario_id,
                            Dataset.dataset_id==ResourceScenario.dataset_id,
                            Metadata.dataset_id==Dataset.dataset_id)

        log.info("Querying node metadata")
        all_metadata = metadata_qry.all()
        log.info("Node metadata retrieved")

        metadata   = []
        if ref_ids is not None:
            for m in all_metadata:
                if m.dataset_id in dataset_ids:
                    metadata.append(m)
        else:
            metadata = all_metadata

        log.info("%s metadata items retrieved", len(metadata))
        metadata_dict = {}
        for m in metadata:
            if metadata_dict.get(m.dataset_id):
                metadata_dict[m.dataset_id].append(m)
            else:
                metadata_dict[m.dataset_id] = [m]

    for rs in resource_scenarios:
        d = rs.dataset
        if d.hidden == 'Y':
           try:
                d.check_read_permission(kwargs.get('user_id'))
           except:
               d.value      = None
               d.frequency  = None
               d.metadata = []
        else:
            if include_metadata == 'Y':
                rs.dataset.metadata = metadata_dict.get(d.dataset_id, [])

    return resource_scenarios

def get_all_resource_data(scenario_id, include_metadata='N', page_start=None, page_end=None, **kwargs):
    """
        A function which returns the data for all resources in a network.
    """

    rs_qry = DBSession.query(
               ResourceAttr.attr_id,
               ResourceAttr.resource_attr_id,
               ResourceAttr.ref_key,
               ResourceAttr.network_id,
               ResourceAttr.node_id,
               ResourceAttr.link_id,
               ResourceAttr.group_id,
               ResourceAttr.project_id,
               ResourceScenario.scenario_id,
               ResourceScenario.source,
               Dataset.dataset_id,
               Dataset.data_name,
               Dataset.value,
               Dataset.data_dimen,
               Dataset.data_units,
               Dataset.frequency,
               Dataset.hidden,
               Dataset.data_type,
               null().label('metadata'),
               case([
                    (ResourceAttr.node_id != None, Node.node_name),
                    (ResourceAttr.link_id != None, Link.link_name),
                    (ResourceAttr.group_id != None, ResourceGroup.group_name),
                    (ResourceAttr.network_id != None, Network.network_name),
               ]).label('ref_name'),
              ).join(ResourceScenario)\
                .join(Dataset).\
                outerjoin(Node, ResourceAttr.node_id==Node.node_id).\
                outerjoin(Link, ResourceAttr.link_id==Link.link_id).\
                outerjoin(ResourceGroup, ResourceAttr.group_id==ResourceGroup.group_id).\
                outerjoin(Network, ResourceAttr.network_id==Network.network_id).\
            filter(ResourceScenario.scenario_id==scenario_id)

    all_resource_data = rs_qry.all()

    if page_start is not None and page_end is None:
        all_resource_data = all_resource_data[page_start:]
    elif page_start is not None and page_end is not None:
        all_resource_data = all_resource_data[page_start:page_end]

    log.info("%s datasets retrieved", len(all_resource_data))

    if include_metadata == 'Y':
        metadata_qry = DBSession.query(distinct(Metadata.dataset_id).label('dataset_id'),
                                      Metadata.metadata_name,
                                      Metadata.metadata_val).filter(
                            ResourceScenario.resource_attr_id==ResourceAttr.resource_attr_id,
                            ResourceScenario.scenario_id==scenario_id,
                            Dataset.dataset_id==ResourceScenario.dataset_id,
                            Metadata.dataset_id==Dataset.dataset_id)

        log.info("Querying node metadata")
        metadata = metadata_qry.all()
        log.info("%s metadata items retrieved", len(metadata))

        metadata_dict = {}
        for m in metadata:
            if metadata_dict.get(m.dataset_id):
                metadata_dict[m.dataset_id].append(m)
            else:
                metadata_dict[m.dataset_id] = [m]

    for ra in all_resource_data:
        if ra.hidden == 'Y':
           try:
                d = DBSession.query(Dataset).filter(
                        Dataset.dataset_id==ra.dataset_id
                    ).options(noload('metadata')).one()
                d.check_read_permission(kwargs.get('user_id'))
           except:
               ra.value      = None
               ra.frequency  = None
               ra.metadata = []
        else:
            if include_metadata == 'Y':
                ra.metadata = metadata_dict.get(ra.dataset_id, [])

    DBSession.expunge_all()

    log.info("Returning %s datasets", len(all_resource_data))

    return all_resource_data 
