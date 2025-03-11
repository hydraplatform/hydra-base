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

import datetime
import time
import json
import six
import re

from ..exceptions import HydraError, ResourceNotFoundError
from . import scenario, rules
from . import data
from . import units
from .objects import JSONObject

from ..util.permissions import required_perms
from hydra_base.lib import template, attributes
from ..db.model import Project, Network, Scenario, Node, Link, ResourceGroup,\
        ResourceAttr, Attr, ResourceType, ResourceGroupItem, Dataset, Metadata, DatasetOwner,\
        ResourceScenario, TemplateType, TypeAttr, Template, NetworkOwner, User
from sqlalchemy.orm import noload, joinedload
from .. import db
from sqlalchemy import func, and_, or_, distinct
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import aliased
from ..util import hdb

from sqlalchemy import case
from sqlalchemy.sql import null

from collections import namedtuple

from hydra_base import config

import logging
log = logging.getLogger(__name__)

# Python 2 and 3 compatible string checking
# TODO remove this when Python2 support is dropped.
try:
    unicode
except NameError:
    unicode = str


def _update_attributes(resource_i, attributes):
    if attributes is None:
        return dict()
    attrs = {}

    resource_attribute_qry = db.DBSession.query(ResourceAttr)

    if resource_i.ref_key == 'NETWORK':
        resource_attribute_qry = resource_attribute_qry.filter(ResourceAttr.network_id==resource_i.id)
    elif resource_i.ref_key == 'NODE':
        resource_attribute_qry = resource_attribute_qry.filter(ResourceAttr.node_id==resource_i.id)
    elif resource_i.ref_key == 'LINK':
        resource_attribute_qry = resource_attribute_qry.filter(ResourceAttr.link_id==resource_i.link_id)
    elif resource_i.ref_key == 'GROUP':
        resource_attribute_qry = resource_attribute_qry.filter(ResourceAttr.group_id==resource_i.group_id)

    resource_attributes = resource_attribute_qry.all()

    attr_id_map = dict([(ra_i.id, ra_i) for ra_i in resource_attributes])

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
        scen = db.DBSession.query(Scenario).filter(and_(Scenario.network_id==network_id, func.lower(Scenario.id) == scenario_name.lower())).one()
        return scen.id
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

def _check_ra_duplicates(all_resource_attrs, resource_id_name_map):
    """
        Check for any duplicate resource attributes before inserting
        into the DB. This just helps to prevent an ugly DB contraint error
    """
    unique_ra_check = {}
    for ra in all_resource_attrs:
        k = (_get_resource_id(ra), ra['attr_id'])
        if unique_ra_check.get(k) is None:
            unique_ra_check[k] = ra
        else:
            ref_key = ra['ref_key']
            if ref_key == 'NODE':
                ref_id = ra['node_id']
            elif ref_key == 'LINK':
                ref_id = ra['link_id']
            elif ref_key == 'GROUP':
                ref_id = ra['group_id']
            elif ref_key == 'NETWORK':
                ref_id = ra['network_id']

            resource_name = resource_id_name_map[ref_id]
            attr_id = ra['attr_id']
            attr_name = db.DBSession.query(Attr.name).filter(Attr.id==attr_id).one()
            raise HydraError(f"Duplicate Resource Attr specified: {resource_name}  {attr_name}")

def _bulk_add_resource_attrs(network_id, ref_key, resources, resource_name_map, template_lookup=None):

    log.info("Bulk adding resource attributes")

    if template_lookup is None:
        template_lookup = {}

    start_time = datetime.datetime.now()

    #List of resource attributes
    resource_attrs = {}

    #Default ra / dataset pairings.
    defaults = {}

    attr_lookup = {}
    log.info("Getting attributes")
    attribute_ids = []
    for resource in resources:
        if resource.attributes is not None and isinstance(resource.attributes, list):
            for ra in resource.attributes:
                attribute_ids.append(ra.attr_id)
    all_attrs = db.DBSession.query(Attr).filter(Attr.id.in_(attribute_ids)).all()
    for a in all_attrs:
        attr_lookup[a.id] = a
    log.info("Attributes retrieved")
    #First get all the attributes assigned from the csv files.
    t0 = datetime.datetime.now()
    for resource in resources:
        #cast name as string here in case the name is a number
        resource_i = resource_name_map[str(resource.name)]
        resource_attrs[resource.id] = []
        if resource.attributes is not None:
            for ra in resource.attributes:
                if attr_lookup.get(ra.attr_id) is None:
                    raise HydraError(f"Unable to process attribute {ra.attr_id} on resource {resource.name} as it does not exist")
                resource_attrs[resource.id].append({
                    'ref_key'     : ref_key,
                    'node_id'     : resource_i.id if ref_key == 'NODE' else None,
                    'link_id'     : resource_i.id if ref_key == 'LINK' else None,
                    'group_id'    : resource_i.id if ref_key == 'GROUP' else None,
                    'network_id'  : resource_i.id if ref_key == 'NETWORK' else None,
                    'attr_id'     : ra.attr_id,
                    'attr_is_var' : ra.attr_is_var,
                })

    logging.info("Resource attributes from resources added in %s",
                 (datetime.datetime.now() - t0))
    #Now get all the attributes supposed to be on the resources based on the types.
    t0 = time.time()

    ##the current user is validated, but some checks require admin permissions,
    ##so call as a user with all permissions
    admin_id = config.get('DEFAULT', 'ALL_PERMISSION_USER', 1)

 #   template_lookup = {} #a lookup of all the templates used by the resource
    typeattr_lookup = {} # a lookup from type ID to a list of typeattrs

    #A lookup from type ID to the child template that it should be using.
    #We assume that a resource can't have 2 type IDS from the same network.
    type_child_template_id_lookup = {}

    #Holds all the attributes supposed to be on a resource based on its specified
    #type
    resource_resource_types = []
    resource_id_name_map = {}
    network_child_template_id = None
    checked_for_child_template = False
    for resource in resources:
        #cast name as string here in case the name is a number
        resource_i = resource_name_map[str(resource.name)]
        resource_id_name_map[resource_i.id] = str(resource.name)
        existing_attrs = [ra['attr_id'] for ra in resource_attrs[resource.id]]
        if resource.types is not None:
            for resource_type in resource.types:
                #Go through all the resource types and add the appropriate resource
                #type entries
                resource_type_id = resource_type.id
                if resource_type.child_template_id is None:
                    if type_child_template_id_lookup.get(resource_type_id) is None:
                        if network_child_template_id is None and checked_for_child_template == False:
                            network_child_template_id = template.get_network_template(network_id, resource_type.id)#TODO this should be type_id
                            checked_for_child_template = True
                        #ok, so no child ID found. We need to just use the template
                        #ID of the type which was given
                        if network_child_template_id is None:
                            tt = template.get_templatetype(resource_type.id, user_id=admin_id)

                            network_child_template_id = tt.template_id

                        type_child_template_id_lookup[resource_type_id] = network_child_template_id

                    resource_type.child_template_id = type_child_template_id_lookup[resource_type_id]

                ref_id = resource_i.id

                if resource_type.id is None:
                    raise HydraError(f"Resource type on resource {resource_i.name} has no ID")

                resource_resource_types.append(
                    {
                        'ref_key' : ref_key,
                        'node_id' : resource_i.id if ref_key == 'NODE' else None,
                        'link_id' : resource_i.id if ref_key == 'LINK' else None,
                        'group_id' : resource_i.id if ref_key == 'GROUP' else None,
                        'network_id' : resource_i.id if ref_key == 'NETWORK' else None,
                        'type_id' : resource_type.id,#TODO this should be type_id
                        'child_template_id' : resource_type.child_template_id
                    }
                )
                #Go through all types in the resource and add attributes from these types

                template_j = template_lookup.get(resource_type.child_template_id)
                if template_j is None:
                    #it's OK to use user ID 1 here because the calling function has been
                    #validated for the calling user's permission to get the network
                    tt = template.get_templatetype(resource_type.id, user_id=admin_id)
                    template_j = template.get_template(resource_type.child_template_id, user_id=admin_id)
                    template_lookup[template_j.id] = template_j
                for tt in template_j.templatetypes:
                    typeattr_lookup[tt.id] = tt.typeattrs

                typeattrs = typeattr_lookup.get(resource_type.id, []) #TODO this should be type_id
                for ta in typeattrs:
                    if ta.attr_id not in existing_attrs:
                        resource_attrs[resource.id].append({
                            'ref_key'     : ref_key,
                            'node_id'     : resource_i.id  if ref_key == 'NODE' else None,
                            'link_id'     : resource_i.id  if ref_key == 'LINK' else None,
                            'group_id'    : resource_i.id  if ref_key == 'GROUP' else None,
                            'network_id'  : resource_i.id  if ref_key == 'NETWORK' else None,
                            'attr_id' : ta.attr_id,
                            'attr_is_var' : ta.attr_is_var,
                        })
                        existing_attrs.append(ta.attr_id)

                        if ta.default_dataset_id is not None:
                            defaults[(ref_id, ta.attr_id)] = {'dataset_id':ta.default_dataset_id}

    if len(resource_resource_types) > 0:
        db.DBSession.bulk_insert_mappings(ResourceType, resource_resource_types)
    logging.info("%s ResourceTypes inserted in %s secs", \
                 len(resource_resource_types), str(time.time() - t0))

    logging.info("Resource attributes from types added in %s",
                 (datetime.datetime.now() - start_time))

    if len(resource_attrs) > 0:
        all_resource_attrs = []
        for na in resource_attrs.values():
            all_resource_attrs.extend(na)

        _check_ra_duplicates(all_resource_attrs, resource_id_name_map)


        if len(all_resource_attrs) > 0:
            db.DBSession.bulk_insert_mappings(ResourceAttr, all_resource_attrs)
            logging.info("ResourceAttr insert took %s secs", str(time.time() - t0))
        else:
            logging.warning("No attributes on any %s....", ref_key.lower())

    logging.info("Resource attributes insertion from types done in %s",\
                 (datetime.datetime.now() - start_time))

    #Now that the attributes are in, we need to map the attributes in the DB
    #to the attributes in the incoming data so that the resource scenarios
    #know what to refer to.
    res_qry = db.DBSession.query(ResourceAttr)
    if ref_key == 'NODE':
        res_qry = res_qry.join(Node).filter(Node.network_id == network_id)
    elif ref_key == 'GROUP':
        res_qry = res_qry.join(ResourceGroup).filter(ResourceGroup.network_id == network_id)
    elif ref_key == 'LINK':
        res_qry = res_qry.join(Link).filter(Link.network_id == network_id)
    elif ref_key == 'NETWORK':
        res_qry = res_qry.filter(ResourceAttr.network_id == network_id)

    real_resource_attrs = res_qry.all()
    logging.info("retrieved %s entries in %s",
                 len(real_resource_attrs), (datetime.datetime.now() - start_time))

    resource_attr_dict = {}
    for resource_attr in real_resource_attrs:
        if ref_key == 'NODE':
            ref_id = resource_attr.node_id
        elif ref_key == 'GROUP':
            ref_id = resource_attr.group_id
        elif ref_key == 'LINK':
            ref_id = resource_attr.link_id
        elif ref_key == 'NETWORK':
            ref_id = resource_attr.network_id

        resource_attr_dict[(ref_id, resource_attr.attr_id)] = resource_attr

        if defaults.get((ref_id, resource_attr.attr_id)):
            defaults[(ref_id, resource_attr.attr_id)]['id'] = resource_attr.id

    logging.info("Processing Query results took %s",
                 (datetime.datetime.now() - start_time))


    resource_attrs = {}
    for resource in resources:
        iface_resource = resource_name_map[str(resource.name)]
        if ref_key == 'NODE':
            ref_id = iface_resource.node_id
        elif ref_key == 'GROUP':
            ref_id = iface_resource.group_id
        elif ref_key == 'LINK':
            ref_id = iface_resource.link_id
        elif ref_key == 'NETWORK':
            ref_id = iface_resource.id

        if resource.attributes is not None:
            for ra in resource.attributes:
                resource_attrs[ra.id] = resource_attr_dict[(ref_id, ra.attr_id)]
    logging.info("Resource attributes added in %s",\
                 (datetime.datetime.now() - start_time))
    logging.debug(" resource_attrs   size: %s",\
                  len(resource_attrs))
    return resource_attrs, defaults, template_lookup

def _add_nodes_to_database(net_i, nodes):
    #First add all the nodes
    log.info("Adding nodes to network %s", net_i.id)
    node_list = []
    for node in nodes:
        node_dict = {'network_id'   : net_i.id,
                    'name' : node.name,
                     'description': node.description,
                     'layout'     : node.get_layout(),
                     'x'     : node.x,
                     'y'     : node.y,
                    }
        node_list.append(node_dict)
    t0 = time.time()
    if len(node_list):
        db.DBSession.bulk_insert_mappings(Node, node_list)
    db.DBSession.flush()
    logging.info("Node insert took %s secs"% str(time.time() - t0))

def _add_nodes(net_i, nodes, template_lookup):

    #check_perm(user_id, 'edit_topology')

    start_time = datetime.datetime.now()

    #List of resource attributes
    node_attrs = {}

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    if nodes is None or len(nodes) == 0:
        return node_id_map, node_attrs, {}

    _add_nodes_to_database(net_i, nodes)

    iface_nodes = dict()
    for n_i in net_i.nodes:
        if iface_nodes.get(n_i.name) is not None:
            raise HydraError("Duplicate Node Name: %s"%(n_i.name))

        iface_nodes[n_i.name] = n_i

    for node in nodes:
        #cast node.name as str here as a node name can sometimes be a number
        node_id_map[node.id] = iface_nodes[str(node.name)]

    node_attrs, defaults, template_lookup = _bulk_add_resource_attrs(net_i.id, 'NODE', nodes, iface_nodes, template_lookup)

    log.info("Nodes added in %s", get_timing(start_time))

    return node_id_map, node_attrs, defaults

def _add_links_to_database(net_i, links, node_id_map):
    log.info("Adding links to network")
    link_dicts = []
    for link in links:
        node_1 = node_id_map.get(link.node_1_id)
        node_2 = node_id_map.get(link.node_2_id)

        if node_1 is None or node_2 is None:
            raise HydraError("Node IDS (%s, %s)are incorrect!"%(node_1, node_2))

        link_dicts.append({'network_id' : net_i.id,
                           'name' : link.name,
                           'description' : link.description,
                           'layout' : link.get_layout(),
                           'node_1_id' : node_1.id,
                           'node_2_id' : node_2.id
                          })
    if len(link_dicts) > 0:
        db.DBSession.bulk_insert_mappings(Link, link_dicts)

def _add_links(net_i, links, node_id_map, template_lookup):

    #check_perm(user_id, 'edit_topology')

    start_time = datetime.datetime.now()

    #List of resource attributes
    link_attrs = {}
    #Map negative IDS to their new, positive, counterparts.
    link_id_map = dict()

    if links is None or len(links) == 0:
        return link_id_map, link_attrs, {}

    #check for duplicate names:
    link_names = []
    duplicate_link_names = []
    for link in links:
        if link.name in link_names:
            duplicate_link_names.append(link.name)
        else:
            link_names.append(link.name)

    if len(duplicate_link_names) > 0:
        raise HydraError(f"Duplicate link names: {duplicate_link_names}")


    #Then add all the links.
#################################################################
    _add_links_to_database(net_i, links, node_id_map)
###################################################################
    log.info("Links added in %s", get_timing(start_time))
    iface_links = {}

    for l_i in net_i.links:
        iface_links[str(l_i.name)] = l_i

    log.info("Link Map created %s", get_timing(start_time))

    for link in links:
        link_id_map[link.id] = iface_links[str(link.name)]

    log.info("Link ID Map created %s", get_timing(start_time))

    link_attrs, defaults, template_lookup = _bulk_add_resource_attrs(net_i.id, 'LINK', links, iface_links, template_lookup)

    log.info("Links added in %s", get_timing(start_time))

    return link_id_map, link_attrs, defaults

def _add_resource_groups(net_i, resourcegroups, template_lookup):
    start_time = datetime.datetime.now()
    #List of resource attributes
    group_attrs = {}
    #Map negative IDS to their new, positive, counterparts.
    group_id_map = dict()

    if resourcegroups is None or len(resourcegroups)==0:
        return group_id_map, group_attrs, {}
    #Then add all the groups.
    log.info("Adding groups to network")
    group_dicts = []
    if resourcegroups:
        for group in resourcegroups:

            group_dicts.append({'network_id' : net_i.id,
                           'name' : group.name,
                           'description' : group.description,
                          })

    iface_groups = {}

    if len(group_dicts) > 0:
        db.DBSession.bulk_insert_mappings(ResourceGroup, group_dicts)
        log.info("Resource Groups added in %s", get_timing(start_time))

        for g_i in net_i.resourcegroups:

            if iface_groups.get(g_i.name) is not None:
                raise HydraError("Duplicate Resource Group: %s"%(g_i.name))

            iface_groups[g_i.name] = g_i

        for group in resourcegroups:
            if group.id not in group_id_map:
                group_i = iface_groups[group.name]
                group_attrs[group.id] = []
                for ra in group.attributes:
                    group_attrs[group.id].append({
                        'ref_key' : 'GROUP',
                        'group_id' : group_i.id,
                        'attr_id' : ra.attr_id,
                        'attr_is_var' : ra.attr_is_var,
                    })
                group_id_map[group.id] = group_i

    group_attrs, defaults, template_lookup = _bulk_add_resource_attrs(net_i.id, 'GROUP', resourcegroups, iface_groups, template_lookup)
    log.info("Groups added in %s", get_timing(start_time))

    return group_id_map, group_attrs, defaults


@required_perms("add_network")
def add_network(network, **kwargs):
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
    db.DBSession.autoflush = False

    start_time = datetime.datetime.now()
    log.debug("Adding network")

    insert_start = datetime.datetime.now()

    proj_i = db.DBSession.query(Project)\
            .filter(Project.id == network.project_id).first()

    if proj_i is None:
        raise HydraError("Project ID is none. A project ID must be specified on the Network")

    existing_net = db.DBSession.query(Network)\
            .filter(Network.project_id == network.project_id,
                    Network.name == network.name).first()

    if existing_net is not None:
        raise HydraError(f"A network with the name {network.name} is already"
                         f" in project {network.project_id}")

    user_id = kwargs.get('user_id')
    proj_i.check_write_permission(user_id)

    net_i = Network()
    net_i.project_id = network.project_id
    net_i.name = network.name
    net_i.description = network.description
    net_i.created_by = user_id
    net_i.projection = network.projection
    net_i.layout = network.get_json('layout')
    net_i.appdata = network.get_json('appdata')

    network.id = net_i.id
    db.DBSession.add(net_i)
    db.DBSession.flush()
    #These two lists are used for comparison and lookup, so when
    #new attributes are added, these lists are extended.

    #List of all the resource attributes
    all_resource_attrs = {}

    name_map = {network.name:net_i}
    network_attrs, network_defaults, template_lookup = _bulk_add_resource_attrs(net_i.id, 'NETWORK', [network], name_map)
    hdb.add_resource_types(net_i, network.types)

    all_resource_attrs.update(network_attrs)

    log.info("Network attributes added in %s", get_timing(start_time))
    node_id_map, node_attrs, node_datasets = _add_nodes(net_i, network.nodes, template_lookup)
    all_resource_attrs.update(node_attrs)

    link_id_map, link_attrs, link_datasets = _add_links(net_i, network.links, node_id_map, template_lookup)
    all_resource_attrs.update(link_attrs)

    grp_id_map, grp_attrs, grp_datasets = _add_resource_groups(net_i, network.resourcegroups, template_lookup)
    all_resource_attrs.update(grp_attrs)

    defaults = list(grp_datasets.values()) + list(link_datasets.values()) \
        + list(node_datasets.values()) + list(network_defaults.values())

    start_time = datetime.datetime.now()

    scenario_names = []
    if network.scenarios is not None:
        log.info("Adding scenarios to network")
        for s in network.scenarios:

            log.info("Adding scenario %s", s.name)

            if s.name in scenario_names:
                raise HydraError("Duplicate scenario name: %s"%(s.name))

            scen = Scenario()
            scen.name                 = s.name
            scen.description          = s.description
            scen.layout               = s.get_layout()
            scen.start_time           = s.start_time
            scen.end_time             = s.end_time
            scen.time_step            = s.time_step
            scen.created_by           = user_id

            scenario_names.append(s.name)

            #extract the data from each resourcescenario
            incoming_datasets = []
            scenario_resource_attrs = []
            for r_scen in s.resourcescenarios:
                if all_resource_attrs.get(r_scen.resource_attr_id) is None:
                    raise HydraError(f"Couldn't find resource attribute {r_scen.resource_attr_id} "
                                     f"as defined on resource scenario {r_scen}. "
                                     f"Shot in the dark: "
                                     f"Does the exporting network have duplicate attributes?")
                ra = all_resource_attrs[r_scen.resource_attr_id]
                incoming_datasets.append(r_scen.dataset)
                scenario_resource_attrs.append(ra)

            data_start_time = datetime.datetime.now()

            for default in defaults:
                scen.add_resource_scenario(JSONObject(default),
                                           JSONObject({'id':default['dataset_id']}),
                                           source=kwargs.get('app_name'))

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
                                         "resource group item."%group_item.ref_key)

                    scen.resourcegroupitems.append(group_item_i)
            log.info("Group items insert took %s", get_timing(item_start_time))
            net_i.scenarios.append(scen)

            log.info("Scenario %s added", s.name)

    log.info("Scenarios added in %s", get_timing(start_time))
    net_i.set_owner(user_id)

    db.DBSession.flush()
    log.info("Insertion of network took: %s",(datetime.datetime.now()-insert_start))

    return net_i

def _get_all_resource_attributes(network_id, template_id=None, include_non_template_attributes=False):
    """
        Get all the attributes for the nodes, links and groups of a network.
        Return these attributes as a dictionary, keyed on type (NODE, LINK, GROUP)
        then by ID of the node or link.

        args:
            network_id (int) The ID of the network from which to retrieve the attributes
            template_id (int): Optional ID of a template, which when specified only returns
                               attributes relating to that template
           include_non_template_attributes (bool): If template_id is specified and any
                                resource has attribtues which are NOT associated to any
                                network template, this flag indicates whether to return them or not.
        returns:
            A list of sqlalchemy result proxy objects
    """
    base_qry = db.DBSession.query(
                               ResourceAttr.id.label('id'),
                               ResourceAttr.ref_key.label('ref_key'),
                               ResourceAttr.cr_date.label('cr_date'),
                               ResourceAttr.attr_is_var.label('attr_is_var'),
                               ResourceAttr.node_id.label('node_id'),
                               ResourceAttr.link_id.label('link_id'),
                               ResourceAttr.group_id.label('group_id'),
                               ResourceAttr.network_id.label('network_id'),
                               ResourceAttr.attr_id.label('attr_id'),
                               Attr.name.label('name'),
                               Attr.dimension_id.label('dimension_id'),
                              ).filter(Attr.id==ResourceAttr.attr_id)


    all_node_attribute_qry = base_qry.join(Node).filter(Node.network_id == network_id)

    all_link_attribute_qry = base_qry.join(Link).filter(Link.network_id == network_id)

    all_group_attribute_qry = base_qry.join(ResourceGroup)\
            .filter(ResourceGroup.network_id == network_id)

    network_attribute_qry = base_qry.filter(ResourceAttr.network_id == network_id)


    x = time.time()
    logging.info("Getting all attributes using execute")
    attribute_qry = all_node_attribute_qry.union(all_link_attribute_qry,
                                                 all_group_attribute_qry,
                                                 network_attribute_qry)
    all_resource_attributes = attribute_qry.all()
    log.info("%s attrs retrieved in %s", len(all_resource_attributes), time.time()-x)

    logging.info("Attributes retrieved. Processing results...")
    x = time.time()

    rt_attribute_dict = {
        'NODE' : {},
        'LINK' : {},
        'GROUP': {},
        'NETWORK': {},
    }

    template_attr_lookup, all_network_typeattrs = _get_network_template_attribute_lookup(network_id)

    for resource_attr in all_resource_attributes:
        if template_id is not None:
            #check if it's in the template. If not, it's either associated to another
            #template or to no template
            if resource_attr.attr_id not in template_attr_lookup.get(template_id, []):
                #check if it's in any other template
                if include_non_template_attributes is True:
                    #if it's associated to a template (but not this one because
                    #it wouldn't have reached this far) then ignore it
                    if resource_attr.attr_id in all_network_typeattrs:
                        continue
                else:
                    #The attr is associated to another template.
                    continue

        attr_dict = rt_attribute_dict[resource_attr.ref_key]
        resourceid = _get_resource_id(resource_attr)
        resourceattrlist = attr_dict.get(resourceid, [])
        resourceattrlist.append(resource_attr)
        attr_dict[resourceid] = resourceattrlist

    logging.info("Attributes processed in %s", time.time()-x)
    return rt_attribute_dict

def _get_resource_id(attr):
    """
        return either the node, link, group or network ID of an attribute.
        Whichever one is not None
    """

    for resourcekey in ('node_id', 'link_id', 'network_id', 'group_id'):
        if isinstance(attr, dict):
            ##this if statement is needed to continue the loop, rather than just
            #returning attr.get(resourcekey)
            if attr.get(resourcekey) is not None:
                return attr[resourcekey]
        else:
            if getattr(attr, resourcekey) is not None:
                return getattr(attr, resourcekey)

    return None

def _get_network_template_attribute_lookup(network_id):
    """
        Given a network ID, identify all the templates associated to the network
        and build a dictionary of template_id: [attr_id, attr_id...]
    """
    #First identify all templates associated to the network (assuming the network
    #types are 100% representative if all templates linked to this network)
    network_types = db.DBSession.query(TemplateType)\
                                .join(ResourceType, ResourceType.type_id == TemplateType.id)\
                                .filter(ResourceType.network_id == network_id).all()

    template_ids = [t.template_id for t in network_types]
    #Now with access to all templates, get all type attributes for all the templates.
    network_typeattrs = db.DBSession.query(TemplateType.template_id.label('template_id'),\
                                           TemplateType.id.label('type_id'),\
                                           TypeAttr.attr_id.label('attr_id'))\
                                           .join(TypeAttr, TypeAttr.type_id == TemplateType.id)\
                                           .filter(TemplateType.template_id.in_(template_ids)).all()
    typeattr_lookup = {}
    all_network_typeattrs = []

    for typeattr in network_typeattrs:
        if typeattr.template_id not in typeattr_lookup:
            typeattr_lookup[typeattr.template_id] = [typeattr.attr_id]
        else:
            typeattr_lookup[typeattr.template_id].append(typeattr.attr_id)

        all_network_typeattrs.append(typeattr.attr_id)

    return typeattr_lookup, all_network_typeattrs

def _get_all_templates(network_id, template_id):
    """
        Get all the templates for the nodes, links and groups of a network.
        Return these templates as a dictionary, keyed on type (NODE, LINK, GROUP)
        then by ID of the node or link.
    """
    base_qry = db.DBSession.query(
                               ResourceType.ref_key.label('ref_key'),
                               ResourceType.node_id.label('node_id'),
                               ResourceType.link_id.label('link_id'),
                               ResourceType.group_id.label('group_id'),
                               ResourceType.network_id.label('network_id'),
                               ResourceType.child_template_id.label('child_template_id'),
                               Template.name.label('template_name'),
                               Template.id.label('template_id'),
                               TemplateType.id.label('type_id'),
                               TemplateType.parent_id.label('parent_id'),
                               TemplateType.layout.label('layout'),
                               TemplateType.name.label('type_name'),
                              ).filter(TemplateType.id==ResourceType.type_id,
                                       Template.id==TemplateType.template_id)


    all_node_type_qry = base_qry.filter(Node.id==ResourceType.node_id,
                                        Node.network_id==network_id)

    all_link_type_qry = base_qry.filter(Link.id==ResourceType.link_id,
                                        Link.network_id==network_id)

    all_group_type_qry = base_qry.filter(ResourceGroup.id==ResourceType.group_id,
                                         ResourceGroup.network_id==network_id)

    network_type_qry = base_qry.filter(ResourceType.network_id==network_id)

    #Filter the group attributes by template
    if template_id is not None:
        all_node_type_qry = all_node_type_qry.filter(Template.id==template_id)
        all_link_type_qry = all_link_type_qry.filter(Template.id==template_id)
        all_group_type_qry = all_group_type_qry.filter(Template.id==template_id)

    x = time.time()
    log.info("Getting all types")
    type_qry = all_node_type_qry.union(all_link_type_qry, all_group_type_qry, network_type_qry)
    all_types = type_qry.all()
    log.info("%s types retrieved in %s", len(all_types), time.time()-x)


    log.info("Attributes retrieved. Processing results...")
    x = time.time()
    node_type_dict = dict()
    link_type_dict = dict()
    group_type_dict = dict()
    network_type_dict = dict()

    #a lookup to avoid having to query for the same child type every time
    child_type_lookup = {}

    ##the current user is validated, but some checks require admin permissions,
    ##so call as a user with all permissions
    admin_id = config.get('DEFAULT', 'ALL_PERMISSION_USER', 1)

    for t in all_types:
        child_layout = None
        child_name = None
        #Load all the inherited columns like layout and name and set them
        if t.parent_id is not None:
            if t.type_id in child_type_lookup:
                child_type = child_type_lookup[t.type_id]
            else:
                #no need to check for user credentials here as it's called from a
                #function which has done that for us
                child_type = template.get_templatetype(t.type_id, user_id=admin_id)
                child_type_lookup[t.type_id] = child_type
            #Now set the potentially missing columns
            child_layout = child_type.layout
            child_name = child_type.name

        templatetype = JSONObject({'template_id' : t.template_id,
                                   'id' : t.type_id,
                                   'template_name' :t.template_name,
                                   'layout' : child_layout if child_layout else t.layout,
                                   'name' : child_name if child_name else t.type_name,
                                   'child_template_id' : t.child_template_id})

        if t.ref_key == 'NODE':
            nodetype = node_type_dict.get(t.node_id, [])
            nodetype.append(templatetype)
            node_type_dict[t.node_id] = nodetype
        elif t.ref_key == 'LINK':
            linktype = link_type_dict.get(t.link_id, [])
            linktype.append(templatetype)
            link_type_dict[t.link_id] = linktype
        elif t.ref_key == 'GROUP':
            grouptype = group_type_dict.get(t.group_id, [])
            grouptype.append(templatetype)
            group_type_dict[t.group_id] = grouptype
        elif t.ref_key == 'NETWORK':
            nettype = network_type_dict.get(t.network_id, [])
            nettype.append(templatetype)
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
    base_qry = db.DBSession.query(ResourceGroupItem)

    item_qry = base_qry.join(Scenario).filter(Scenario.network_id==network_id)

    x = time.time()
    logging.info("Getting all items")
    all_items = item_qry.all()
    log.info("%s groups jointly retrieved in %s", len(all_items), time.time()-x)


    logging.info("items retrieved. Processing results...")
    x = time.time()
    item_dict = dict()
    for item in all_items:

        items = item_dict.get(item.scenario_id, [])
        items.append(JSONObject(item))
        item_dict[item.scenario_id] = items

    logging.info("items processed in %s", time.time()-x)

    return item_dict

def _get_nodes(network_id, template_id=None):
    """
        Get all the nodes in a network
    """
    extras = {'types':[], 'attributes':[]}

    node_qry = db.DBSession.query(Node).filter(
                        Node.network_id == network_id,
                        Node.status == 'A').options(
                            noload(Node.network)
                        )
    if template_id is not None:
        node_qry = node_qry.filter(ResourceType.node_id == Node.id,
                                   TemplateType.id == ResourceType.type_id,
                                   TemplateType.template_id == template_id)
    node_res = node_qry.all()

    nodes = []
    for n in node_res:
        nodes.append(JSONObject(n, extras=extras))

    return nodes

def _get_links(network_id, template_id=None):
    """
        Get all the links in a network
    """
    extras = {'types':[], 'attributes':[]}
    link_qry = db.DBSession.query(Link).filter(
                                        Link.network_id==network_id,
                                        Link.status=='A').options(
                                            noload(Link.network)
                                        )
    if template_id is not None:
        link_qry = link_qry.filter(ResourceType.link_id==Link.id,
                                   TemplateType.id==ResourceType.type_id,
                                   TemplateType.template_id==template_id)

    link_res = link_qry.all()

    links = []
    for l in link_res:
        links.append(JSONObject(l, extras=extras))

    return links

def _get_groups(network_id, template_id=None):
    """
        Get all the resource groups in a network
    """
    extras = {'types':[], 'attributes':[]}
    group_qry = db.DBSession.query(ResourceGroup).filter(
                                        ResourceGroup.network_id==network_id,
                                        ResourceGroup.status=='A').options(
                                            noload(ResourceGroup.network)
                                        )

    if template_id is not None:
        group_qry = group_qry.filter(ResourceType.group_id == ResourceGroup.id,
                                     TemplateType.id == ResourceType.type_id,
                                     TemplateType.template_id == template_id)

    group_res = group_qry.all()
    groups = []
    for g in group_res:
        groups.append(JSONObject(g, extras=extras))

    return groups

def _get_scenarios(network_id, include_data, include_results, user_id,
                   scenario_ids=None, include_metadata=False):
    """
        Get all the scenarios in a network
    """
    scen_qry = db.DBSession.query(Scenario).filter(
                    Scenario.network_id == network_id).options(
                        noload(Scenario.network)).filter(
                        Scenario.status == 'A')

    if scenario_ids:
        logging.info("Filtering by scenario_ids %s",scenario_ids)
        scen_qry = scen_qry.filter(Scenario.id.in_(scenario_ids))
    extras = {'resourcescenarios': [], 'resourcegroupitems': []}
    scens_i = scen_qry.all()
    scens = [JSONObject(s,extras=extras) for s in scens_i]

    all_resource_group_items = _get_all_group_items(network_id)

    #default to empty metadata
    metadata = {}

    for i, s in enumerate(scens):
        s_i = scens_i[i]
        s.resourcegroupitems = all_resource_group_items.get(s.id, [])

        if include_data == True:
            s.resourcescenarios  = s_i.get_all_resourcescenarios(
                user_id=user_id,
                include_results=include_results,
                include_metadata=include_metadata)

    return scens

def get_network(network_id,
                include_attributes=True,
                include_data=False,
                include_results=True,
                scenario_ids=None,
                template_id=None,
                include_non_template_attributes=False,
                include_metadata=False,
                include_topology=True,
                **kwargs):
    """
        Return a whole network as a dictionary.
        network_id: ID of the network to retrieve
        include_attributes (bool): include attributes to save on data
        include_data: (bool). Indicate whether scenario data is to be returned.
                      This has a significant speed impact as retrieving large amounts
                      of data can be expensive.
        include_results: (bool). If data is requested, this flag allows results
                         data to be ignored (attr is var), as this can often be very large.
        scenario_ids: list of IDS to be returned. Used if a network has multiple
                      scenarios but you only want one returned. Using this filter
                      will speed up this function call.
        template_id:  Return the network with only attributes associated with this
                      template on the network, groups, nodes and links.
        include_non_template_attribute: Return attributes which are not associated to any template.
        include_metadata (bool): If data is included, then this flag indicates whether to include metadata.
                          Setting this to True may have performance implications
        include_topology (bool): If true, return the network's nodes, links and groups.
    """
    log.debug("getting network %s"%network_id)

    user_id = kwargs.get('user_id')

    network_id = int(network_id)

    try:
        log.debug("Querying Network %s", network_id)
        net_i = db.DBSession.query(Network).filter(
            Network.id == network_id).options(
            noload(Network.scenarios)).options(
            noload(Network.nodes)).options(
            noload(Network.links)).options(
            noload(Network.types)).options(
            noload(Network.attributes)).options(
            noload(Network.resourcegroups)).one()

        net_i.check_read_permission(user_id)

        net = JSONObject(net_i)
        if include_topology is True:
            net.nodes = _get_nodes(network_id, template_id=template_id)
            net.links = _get_links(network_id, template_id=template_id)
            net.resourcegroups = _get_groups(network_id, template_id=template_id)
        net.owners = net_i.get_owners()

        if include_attributes in ('Y', True):
            all_attributes = _get_all_resource_attributes(network_id,
                                                          template_id,
                                                          include_non_template_attributes)
            log.info("Setting attributes")
            net.attributes = all_attributes['NETWORK'].get(network_id, [])
            for node_i in net.nodes:
                node_i.attributes = all_attributes['NODE'].get(node_i.id, [])
            log.info("Node attributes set")
            for link_i in net.links:
                link_i.attributes = all_attributes['LINK'].get(link_i.id, [])
            log.info("Link attributes set")
            for group_i in net.resourcegroups:
                group_i.attributes = all_attributes['GROUP'].get(group_i.id, [])
            log.info("Group attributes set")

        log.info("Setting types")
        all_types = _get_all_templates(network_id, template_id)
        net.types = all_types['NETWORK'].get(network_id, [])
        for node_i in net.nodes:
            node_i.types = all_types['NODE'].get(node_i.id, [])
        for link_i in net.links:
            link_i.types = all_types['LINK'].get(link_i.id, [])
        for group_i in net.resourcegroups:
            group_i.types = all_types['GROUP'].get(group_i.id, [])

        log.info("Getting scenarios")

        net.scenarios = _get_scenarios(network_id,
                                       include_data,
                                       include_results,
                                       user_id,
                                       scenario_ids,
                                       include_metadata=include_metadata)

    except NoResultFound:
        raise ResourceNotFoundError("Network (network_id=%s) not found." % network_id)

    return net

def get_networks(network_ids, **kwargs):
    """
        Get the list of networks specified in a list of network IDS
        args:
            network_ids (list(int)) : a list of network IDs
        returns:
            list(Network)
    """
    user_id = kwargs.get('user_id')

    networks = db.DBSession.query(Network).filter(
            Network.id.in_(network_ids))

    for n in networks:
        n.check_read_permission(user_id)

    return networks


def get_nodes(network_id, template_id=None, **kwargs):
    """
        Get all the nodes in a network.
        args:
            network_id (int): The network in which to search
            template_id (int): Only return nodes whose type is in this template.
    """
    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_read_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    node_qry = db.DBSession.query(Node).filter(
                        Node.network_id == network_id,
                        Node.status == 'A').options(
                            noload(Node.network)
                        ).options(
                            joinedload(Node.types).joinedload(ResourceType.templatetype)
                        ).options(
                            joinedload(Node.attributes).joinedload(ResourceAttr.attr)
                        )
    if template_id is not None:
        node_qry = node_qry.filter(ResourceType.node_id==Node.id,
                                   TemplateType.id==ResourceType.type_id,
                                   TemplateType.template_id==template_id)
    nodes = node_qry.all()

    return nodes

def get_links(network_id, template_id=None, **kwargs):
    """
        Get all the links in a network.
        args:
            network_id (int): The network in which to search
            template_id (int): Only return links whose type is in this template.
    """
    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_read_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    link_qry = db.DBSession.query(Link).filter(
                                        Link.network_id==network_id,
                                        Link.status=='A').options(
                                            noload(Link.network)
                                        ).options(
                                            joinedload(Link.types).joinedload(ResourceType.templatetype)
                                        ).options(
                                            joinedload(Link.attributes).joinedload(ResourceAttr.attr)
                                        )

    if template_id is not None:
        link_qry = link_qry.filter(ResourceType.link_id==Link.id,
                                   TemplateType.id==ResourceType.type_id,
                                   TemplateType.template_id==template_id)

    links = link_qry.all()
    return links


def get_groups(network_id, template_id=None, **kwargs):
    """
        Get all the resource groups in a network.
        args:
            network_id (int): The network in which to search
            template_id (int): Only return resource groups whose type is in this template.
    """
    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_read_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    group_qry = db.DBSession.query(ResourceGroup).filter(
                                        ResourceGroup.network_id==network_id,
                                        ResourceGroup.status=='A').options(
                                            noload(ResourceGroup.network)
                                        ).options(
                                            joinedload(ResourceGroup.types).joinedload(ResourceType.templatetype)
                                        ).options(
                                            joinedload(ResourceGroup.attributes).joinedload(ResourceAttr.attr)
                                        )
    if template_id is not None:
        group_qry = group_qry.filter(ResourceType.group_id==ResourceGroup.id,
                                     TemplateType.id==ResourceType.type_id,
                                     TemplateType.template_id==template_id)

    groups = group_qry.all()

    return groups

def get_network_simple(network_id,**kwargs):
    try:
        n = db.DBSession.query(Network).filter(Network.id==network_id).options(joinedload(Network.attributes).joinedload(ResourceAttr.attr)).one()
        n.types
        for t in n.types:
            t.templatetype.typeattrs
        return n
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id,))

def get_node(node_id, scenario_id=None, **kwargs):
    try:
        n = db.DBSession.query(Node).filter(Node.id==node_id).options(joinedload(Node.attributes).joinedload(ResourceAttr.attr)).one()
        n.types
        for t in n.types:
            t.templatetype.typeattrs
            t.templatetype.template
            #set this for easy access later by client
            #t.templatetype.template_name = t.templatetype.template.name

            for ta in t.templatetype.typeattrs:
                if ta.default_dataset_id:
                    ta.default_dataset
                    ta.default_dataset.metadata
                    ta.default_dataset.unit

    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id,))

    n = JSONObject(n)

    if scenario_id is not None:
        res_scens = scenario.get_resource_data('NODE', node_id, scenario_id, None, **kwargs)
        rs_dict = {}
        for rs in res_scens:
            rs_dict[rs.resource_attr_id] = JSONObject(rs)

        for ra in n.attributes:
            if rs_dict.get(ra.id):
                ra.resourcescenario = rs_dict[ra.id]

    return n

def get_link(link_id, scenario_id=None, **kwargs):
    try:
        l = db.DBSession.query(Link).filter(Link.id==link_id).options(joinedload(Link.attributes).joinedload(ResourceAttr.attr)).one()
        l.types
        for t in l.types:
            #lazy load the type's template
            t.templatetype.template
            #set the template name on the type
            t.templatetype.template_name = t.templatetype.template.name
            t.templatetype.typeattrs
            for ta in t.templatetype.typeattrs:
                if ta.default_dataset_id:
                    ta.default_dataset
                    ta.default_dataset.metadata
                    ta.default_dataset.unit

    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id,))

    l = JSONObject(l)

    if scenario_id is not None:
        res_scens = scenario.get_resource_data('LINK', link_id, scenario_id, None, **kwargs)
        rs_dict = {}
        for rs in res_scens:
            rs_dict[rs.resource_attr_id] = JSONObject(rs)

        for ra in l.attributes:
            if rs_dict.get(ra.id):
                ra.resourcescenario = rs_dict[ra.id]

    return l

def get_resourcegroup(group_id, scenario_id=None, **kwargs):
    try:
        rg = db.DBSession.query(ResourceGroup).filter(ResourceGroup.id==group_id).options(joinedload(ResourceGroup.attributes).joinedload(ResourceAttr.attr)).one()
        rg.types
        for t in rg.types:
            #lazy load the type's template
            t.templatetype.template
            #set the template name on the type
            t.templatetype.template_name = t.templatetype.template.name
            t.templatetype.typeattrs
            for ta in t.templatetype.typeattrs:
                if ta.default_dataset_id is not None:
                    ta.default_dataset
                    ta.default_dataset.metadata
                    ta.default_dataset.unit
    except NoResultFound:
        raise ResourceNotFoundError("ResourceGroup %s not found"%(group_id,))

    rg = JSONObject(rg)

    if scenario_id is not None:
        res_scens = scenario.get_resource_data('GROUP', group_id, scenario_id, None, **kwargs)
        rs_dict = {}
        for rs in res_scens:
            rs_dict[rs.resource_attr_id] = JSONObject(rs)

        for ra in rg.attributes:
            if rs_dict.get(ra.id):
                ra.resourcescenario = rs_dict[ra.id]

    return rg

def get_node_by_name(network_id, node_name,**kwargs):
    try:
        n = db.DBSession.query(Node).filter(Node.name==node_name,
                                         Node.network_id==network_id).\
                                         options(joinedload(Node.attributes).joinedload(ResourceAttr.attr)).one()
        return n
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found in network %s"%(node_name, network_id,))

def get_link_by_name(network_id, link_name,**kwargs):
    try:
        l = db.DBSession.query(Link).filter(Link.name==link_name,
                                         Link.network_id==network_id).\
                                         options(joinedload(Link.attributes).joinedload(ResourceAttr.attr)).one()
        return l
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found in network %s"%(link_name, network_id))

def get_resourcegroup_by_name(network_id, group_name,**kwargs):
    try:
        rg = db.DBSession.query(ResourceGroup).filter(ResourceGroup.name==group_name,
                                                   ResourceGroup.network_id==network_id).\
                                                    options(joinedload(ResourceGroup.attributes).joinedload(ResourceAttr.attr)).one()
        return rg
    except NoResultFound:
        raise ResourceNotFoundError("ResourceGroup %s not found in network %s"%(group_name,network_id))

def get_network_by_name(project_id, network_name,**kwargs):
    """
    Return a whole network as a complex model.
    """


    try:
        res = db.DBSession.query(Network.id).filter(func.lower(Network.name).like(network_name.lower()), Network.project_id == project_id).one()
        net = get_network(res.id, 'Y', None, **kwargs)
        return net
    except NoResultFound:
        raise ResourceNotFoundError("Network with name %s not found"%(network_name))


def network_exists(project_id, network_name,**kwargs):
    """
    Return a whole network as a complex model.
    """
    try:
        db.DBSession.query(Network.id).filter(func.lower(Network.name).like(network_name.lower()), Network.project_id == project_id).one()
        return 'Y'
    except NoResultFound:
        return 'N'

@required_perms("edit_network")
def update_network(network,
    update_nodes = True,
    update_links = True,
    update_groups = True,
    update_scenarios = True,
    **kwargs):
    """
        Update an entire network
    """
    log.info("Updating Network %s", network.name)
    user_id = kwargs.get('user_id')
    #check_perm('update_network')

    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network.id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Network with id %s not found"%(network.id))

    net_i.project_id = network.project_id
    net_i.name = network.name
    net_i.description = network.description
    net_i.projection = network.projection
    net_i.layout = network.get_json('layout')
    net_i.appdata = network.get_json('appdata')

    all_resource_attrs = {}
    new_network_attributes = _update_attributes(net_i, network.attributes)
    all_resource_attrs.update(new_network_attributes)
    hdb.add_resource_types(net_i, network.types)

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    if network.nodes is not None and update_nodes is True:
        log.info("Updating nodes")
        t0 = time.time()
        #First add all the nodes
        node_id_map = dict([(n.id, n) for n in net_i.nodes])
        for node in network.nodes:
            #If we get a negative or null node id, we know
            #it is a new node.
            if node.id is not None and node.id > 0:
                n = node_id_map[node.id]
                n.name        = node.name
                n.description = node.description
                n.x           = node.x
                n.y           = node.y
                n.status      = node.status
                n.layout      = node.get_layout()
            else:
                log.info("Adding new node %s", node.name)
                n = net_i.add_node(node.name,
                                   node.description,
                                   node.get_layout(),
                                   node.x,
                                   node.y)
                net_i.nodes.append(n)
                node_id_map[n.id] = n

            all_resource_attrs.update(_update_attributes(n, node.attributes))
            hdb.add_resource_types(n, node.types)
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
                                   link.get_layout(),
                                   node_1,
                                   node_2)
                net_i.links.append(l)
                link_id_map[link.id] = l
            else:
                l = link_id_map[link.id]
                l.name       = link.name
                l.link_descripion = link.description
                l.node_a          = node_1
                l.node_b          = node_2
                l.layout          = link.get_layout()


            all_resource_attrs.update(_update_attributes(l, link.attributes))
            hdb.add_resource_types(l, link.types)
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
                g_i.name        = group.name
                g_i.description = group.description
                g_i.status           = group.status
            else:
                log.info("Adding new group %s", group.name)
                g_i = net_i.add_group(group.name,
                                      group.description,
                                      group.status)
                net_i.resourcegroups.append(net_i)
                group_id_map[g_i.group_id] = g_i

            all_resource_attrs.update(_update_attributes(g_i, group.attributes))
            hdb.add_resource_types(g_i, group.types)
            group_id_map[group.id] = g_i
        log.info("Updating groups took %s", time.time() - t0)

    errors = []
    if network.scenarios is not None and update_scenarios is True:
        for s in network.scenarios:
            add_scenario = False
            if s.id is not None:
                if s.id > 0:
                    try:
                        scen_i = db.DBSession.query(Scenario).filter(Scenario.id==s.id).one()
                        if scen_i.locked == 'Y':
                            errors.append('Scenario %s was not updated as it is locked'%(s.id))
                            continue

                        scenario.update_scenario(s, flush=False, **kwargs)
                    except NoResultFound:
                        raise ResourceNotFoundError("Scenario %s not found"%(s.id))
                else:
                    add_scenario = True
            else:
                add_scenario = True

            if add_scenario is True:
                log.info("Adding new scenario %s to network", s.name)
                scenario.add_scenario(network.id, s, **kwargs)

    db.DBSession.flush()

    updated_net = get_network(network.id, summary=True, **kwargs)
    return updated_net

@required_perms("edit_network")
def move_network(network_id, target_project_id, **kwargs):
    """
        Move a network to the project with `target_project_id`
    """
    log.info(f"Moving {network_id} to {target_project_id}")
    user_id = kwargs.get('user_id')

    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Network with id %s not found"%(network_id))

    net_i.check_write_permission(user_id)

    net_i.project_id = target_project_id

    db.DBSession.flush()

    return JSONObject(net_i)

def update_resource_layout(resource_type, resource_id, key, value, **kwargs):
    log.info("Updating %s %s's layout with {%s:%s}", resource_type, resource_id, key, value)
    resource = get_resource(resource_type, resource_id, **kwargs)
    if resource.layout is None:
        layout = dict()
    else:
        layout = json.loads(resource.layout)

    layout[key] = value
    resource.layout = json.dumps(layout)

    db.DBSession.flush()

    return layout

def get_resource(resource_type, resource_id, **kwargs):
    user_id = kwargs.get('user_id')

    resource_type = resource_type.upper()
    if resource_type == 'NODE':
        return get_node(resource_id, **kwargs)
    elif resource_type == 'LINK':
        return get_link(resource_id, **kwargs)
    elif resource_type == 'GROUP':
        return get_resourcegroup(resource_id, **kwargs)
    elif resource_type == 'NETWORK':
        network = get_network_simple(resource_id, **kwargs)
        return network

def set_network_status(network_id,status,**kwargs):
    """
    Activates a network by setting its status attribute to 'A'.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_network')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_write_permission(user_id)
        net_i.status = status
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    db.DBSession.flush()
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
    rs = db.DBSession.query(Node.x, Node.y).filter(Node.network_id==network_id).all()
    if len(rs) == 0:
        return dict(
            network_id = network_id,
            min_x=None,
            max_x=None,
            min_y=None,
            max_y=None,
        )

    # Compute min/max extent of the network.
    x = [r.x for r in rs if r.x is not None]
    if len(x) > 0:
        x_min = min(x)
        x_max = max(x)
    else:
        # Default x extent if all None values
        x_min, x_max = 0, 1

    y = [r.y for r in rs if r.y is not None]
    if len(y) > 0:
        y_min = min(y)
        y_max = max(y)
    else:
        # Default y extent if all None values
        y_min, y_max = 0, 1

    ne = JSONObject(dict(
        network_id = network_id,
        min_x=x_min,
        max_x=x_max,
        min_y=y_min,
        max_y=y_max,
    ))
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
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    _add_nodes_to_database(net_i, nodes)

    net_i.project_id = net_i.project_id
    db.DBSession.flush()

    node_s =  db.DBSession.query(Node).filter(Node.network_id == network_id).all()

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    iface_nodes = dict()
    for n_i in node_s:
        iface_nodes[n_i.name] = n_i

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
    names = []        # used to check uniqueness of link name before saving links to database
    for l_i in links:
        if l_i.name in names:
            raise HydraError("Duplicate Link Name: %s"%(l_i.name))
        names.append(l_i.name)

    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    node_id_map=dict()
    for node in net_i.nodes:
       node_id_map[node.id] = node

    _add_links_to_database(net_i, links, node_id_map)

    net_i.project_id = net_i.project_id
    db.DBSession.flush()
    link_s = db.DBSession.query(Link).filter(Link.network_id == network_id).all()
    iface_links = {}
    for l_i in link_s:
        iface_links[l_i.name] = l_i
    _bulk_add_resource_attrs(net_i.id, 'LINK', links, iface_links)
    log.info("Nodes added in %s", get_timing(start_time))
    return link_s
#########################################


def add_node(network_id, node, **kwargs):

    """
    Add a node to a network:
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    new_node = net_i.add_node(node.name, node.description, node.layout, node.x, node.y)

    hdb.add_resource_attributes(new_node, node.attributes)

    db.DBSession.flush()

    if node.types is not None and len(node.types) > 0:
        res_types = []
        res_attrs = []
        res_scenarios = {}
        for typesummary in node.types:
            ra, rt, rs = template.set_resource_type(new_node,
                                                    typesummary.id,
                                                    network_id=network_id,
                                                    **kwargs)
            if rt is not None:
                res_types.append(rt)#rt is one object
            res_attrs.extend(ra)#ra is a list of objects
            res_scenarios.update(rs)

        if len(res_types) > 0:
            db.DBSession.bulk_insert_mappings(ResourceType, res_types)
        if len(res_attrs) > 0:
            db.DBSession.bulk_insert_mappings(ResourceAttr, res_attrs)

            new_res_attrs = db.DBSession.query(ResourceAttr)\
                    .order_by(ResourceAttr.id.desc())\
                    .limit(len(res_attrs)).all()

            all_rs = []
            for ra in new_res_attrs:
                ra_id = ra.id
                if ra.attr_id in res_scenarios:
                    rs_list = res_scenarios[ra.attr_id]
                    for rs in rs_list:
                        rs_list[rs]['resource_attr_id'] = ra_id
                        all_rs.append(rs_list[rs])

            if len(all_rs) > 0:
                db.DBSession.bulk_insert_mappings(ResourceScenario, all_rs)

    db.DBSession.refresh(new_node)
    #lazy load attributes
    new_node.attributes
    return new_node
#########################################################################

def update_node(node, flush=True, **kwargs):
    """
    Update a node.
    If new attributes are present, they will be added to the node.
    The non-presence of attributes does not remove them.

    The flush argument indicates whether dbsession.flush should be called. THis
    is set to False when update_node is called from another function which does
    the flush.

    """
    user_id = kwargs.get('user_id')
    try:
        node_i = db.DBSession.query(Node).filter(Node.id == node.id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node.id))

    node_i.network.check_write_permission(user_id)

    node_i.name = node.name if node.name is not None else node_i.name
    node_i.x    = node.x if node.x is not None else node_i.x
    node_i.y    = node.y if node.y is not None else node_i.y
    node_i.description = node.description if node.description is not None else node_i.description
    node_i.layout      = node.get_layout() if node.layout is not None else node_i.layout

    if node.attributes is not None:
        _update_attributes(node_i, node.attributes)

    if node.types is not None:
        hdb.add_resource_types(node_i, node.types)

    if flush is True:
        db.DBSession.flush()

    return node_i


def update_nodes(nodes,**kwargs):
    """
    Update multiple nodes.
    If new attributes are present, they will be added to the node.
    The non-presence of attributes does not remove them.

    %TODO:merge this with the 'update_nodes' functionality in the 'update_netework'
    function, so we're not duplicating functionality. D.R.Y!

    returns: a list of updated nodes
    """
    user_id = kwargs.get('user_id')
    updated_nodes = []
    for n in nodes:
        updated_node_i = update_node(n, flush=False, user_id=user_id)
        updated_nodes.append(updated_node_i)

    db.DBSession.flush()

    return updated_nodes

def set_node_status(node_id, status, **kwargs):
    """
        Set the status of a node to 'X'
    """
    user_id = kwargs.get('user_id')
    try:
        node_i = db.DBSession.query(Node).filter(Node.id == node_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id))

    node_i.network.check_write_permission(user_id)

    node_i.status = status

    for link in node_i.links_to:
        link.status = status
    for link in node_i.links_from:
        link.status = status

    db.DBSession.flush()

    return node_i

def _unique_data_qry(count=1):
    rs = aliased(ResourceScenario)

    subqry = db.DBSession.query(
                           rs.dataset_id,
                           func.count(rs.dataset_id).label('dataset_count')).\
                                group_by(rs.dataset_id).\
                                having(func.count(rs.dataset_id) == count).\
                                subquery()

    unique_data = db.DBSession.query(rs).\
                        join(subqry,
                                and_(rs.dataset_id==subqry.c.dataset_id)
                            ).\
                    filter(
                        rs.resource_attr_id == ResourceAttr.id
                    )
    return unique_data


def delete_network(network_id, purge_data,**kwargs):
    """
        Call the original purge network call for backward compatibility
    """
    return purge_network(network_id, purge_data, **kwargs)

def purge_network(network_id, purge_data,**kwargs):
    """
        Remove a network from DB completely
        Use purge_data to try to delete the data associated with only this network.
        If no other resources link to this data, it will be deleted.

    """
    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    log.info("Deleting network %s, id=%s", net_i.name, network_id)

    net_i.check_write_permission(user_id)
    db.DBSession.delete(net_i)
    db.DBSession.flush()
    return 'OK'


def _purge_datasets_unique_to_resource(ref_key, ref_id):
    """
        Find the number of times a a resource and dataset combination
        occurs. If this equals the number of times the dataset appears, then
        we can say this dataset is unique to this resource, therefore it can be deleted
    """
    count_qry = db.DBSession.query(ResourceScenario.dataset_id,
                                   func.count(ResourceScenario.dataset_id)).group_by(
                                       ResourceScenario.dataset_id).filter(
                                       ResourceScenario.resource_attr_id==ResourceAttr.id)

    if ref_key == 'NODE':
        count_qry.filter(ResourceAttr.node_id == ref_id)
    elif ref_key == 'LINK':
        count_qry.filter(ResourceAttr.link_id == ref_id)
    elif ref_key == 'GROUP':
        count_qry.filter(ResourceAttr.group_id == ref_id)

    count_rs = count_qry.all()

    for dataset_id, count in count_rs:
        full_dataset_count = db.DBSession.query(ResourceScenario)\
                .filter(ResourceScenario.dataset_id==dataset_id).count()
        if full_dataset_count == count:
            """First delete all the resource scenarios"""
            datasets_rs_to_delete = db.DBSession.query(ResourceScenario)\
                    .filter(ResourceScenario.dataset_id==dataset_id).all()
            for dataset_rs in datasets_rs_to_delete:
                db.DBSession.delete(dataset_rs)

            """Then delete all the datasets"""
            dataset_to_delete = db.DBSession.query(Dataset)\
                    .filter(Dataset.id == dataset_id).one()

            log.info("Deleting %s dataset %s (%s)",\
                     ref_key, dataset_to_delete.name, dataset_to_delete.id)

            db.DBSession.delete(dataset_to_delete)

def delete_node(node_id, purge_data,**kwargs):
    """
        Remove node from DB completely
        If there are attributes on the node, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.

    """
    user_id = kwargs.get('user_id')
    try:
        node_i = db.DBSession.query(Node).filter(Node.id == node_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id))

    group_items = db.DBSession.query(ResourceGroupItem).filter(
                                        ResourceGroupItem.node_id==node_id).all()
    for gi in group_items:
        db.DBSession.delete(gi)

    if purge_data == 'Y':
        _purge_datasets_unique_to_resource('NODE', node_id)

    log.info("Deleting node %s, id=%s", node_i.name, node_id)

    node_i.network.check_write_permission(user_id)
    db.DBSession.delete(node_i)
    db.DBSession.flush()
    return 'OK'

def add_link(network_id, link,**kwargs):
    """
        Add a link to a network
    """
    user_id = kwargs.get('user_id')

    #check_perm(user_id, 'edit_topology')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    try:
        node_1 = db.DBSession.query(Node).filter(Node.id==link.node_1_id).one()
        node_2 = db.DBSession.query(Node).filter(Node.id==link.node_2_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Nodes for link not found")

    link_i = net_i.add_link(link.name, link.description, link.layout, node_1, node_2)

    hdb.add_resource_attributes(link_i, link.attributes)

    db.DBSession.flush()

    if link.types is not None and len(link.types) > 0:
        res_types = []
        res_attrs = []
        res_scenarios = {}
        for typesummary in link.types:
            ra, rt, rs = template.set_resource_type(link_i,
                                                    typesummary.id,
                                                    network_id=network_id,
                                                    **kwargs)
            res_types.append(rt)
            res_attrs.extend(ra)
            res_scenarios.update(rs)#rs is a dict

        if len(res_types) > 0:
            db.DBSession.bulk_insert_mappings(ResourceType, res_types)
        if len(res_attrs) > 0:
            db.DBSession.bulk_insert_mappings(ResourceAttr, res_attrs)

            new_res_attrs = db.DBSession.query(ResourceAttr).order_by(ResourceAttr.id.desc()).limit(len(res_attrs)).all()
            all_rs = []
            for ra in new_res_attrs:
                ra_id = ra.id
                if ra.attr_id in res_scenarios:
                    rs_list = res_scenarios[ra.attr_id]
                    for rs in rs_list:
                        rs_list[rs]['resource_attr_id'] = ra_id
                        all_rs.append(rs_list[rs])

            if len(all_rs) > 0:
                db.DBSession.bulk_insert_mappings(ResourceScenario, all_rs)

    db.DBSession.refresh(link_i)

    #lazy load attributes
    link_i.attributes

    return link_i

@required_perms("edit_network")
def update_links(links, **kwargs):
    log.info("Updating %s links", len(links))
    for l in links:
        update_link(l, flush=False, **kwargs)
    db.DBSession.flush()

def update_link(link, flush=False, **kwargs):
    """
        Update a link.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    try:
        link_i = db.DBSession.query(Link).filter(Link.id == link.id).one()
        link_i.network.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link.id))

    #Each of thiese should be updateable independently
    if link.name is not None:
        link_i.name = link.name
    if link.node_1_id is not None:
        link_i.node_1_id = link.node_1_id
    if link.node_2_id is not None:
        link_i.node_2_id = link.node_2_id
    if link.description is not None:
        link_i.description = link.description
    if link.layout is not None:
        link_i.layout  = link.get_layout()
    if link.attributes is not None:
        hdb.add_resource_attributes(link_i, link.attributes)
    if link.types is not None:
        hdb.add_resource_types(link_i, link.types)
    if flush is True:
        db.DBSession.flush()
    return link_i

def set_link_status(link_id, status, **kwargs):
    """
        Set the status of a link
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    try:
        link_i = db.DBSession.query(Link).filter(Link.id == link_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id))

    link_i.network.check_write_permission(user_id)

    link_i.status = status
    db.DBSession.flush()

def delete_link(link_id, purge_data,**kwargs):
    """
        Remove link from DB completely
        If there are attributes on the link, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.
    """
    user_id = kwargs.get('user_id')
    try:
        link_i = db.DBSession.query(Link).filter(Link.id == link_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id))

    group_items = db.DBSession.query(ResourceGroupItem).filter(
                                                    ResourceGroupItem.link_id==link_id).all()
    for gi in group_items:
        db.DBSession.delete(gi)

    if purge_data == 'Y':
        _purge_datasets_unique_to_resource('LINK', link_id)

    log.info("Deleting link %s, id=%s", link_i.name, link_id)

    link_i.network.check_write_permission(user_id)
    db.DBSession.delete(link_i)
    db.DBSession.flush()

def add_group(network_id, group,**kwargs):
    """
        Add a resourcegroup to a network
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_write_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    res_grp_i = net_i.add_group(group.name, group.description, group.status)

    hdb.add_resource_attributes(res_grp_i, group.attributes)

    db.DBSession.flush()
    if group.types is not None and len(group.types) > 0:
        res_types = []
        res_attrs = []
        res_scenarios = {}
        for typesummary in group.types:
            ra, rt, rs = template.set_resource_type(res_grp_i,
                                                    typesummary.id,
                                                    network_id=network_id,
                                                    **kwargs)
            res_types.append(rt)
            res_attrs.extend(ra)
            res_scenarios.update(rs)#rs is a dict
        if len(res_types) > 0:
            db.DBSession.bulk_insert_mappings(ResourceType, res_types)
        if len(res_attrs) > 0:
            db.DBSession.bulk_insert_mappings(ResourceAttr, res_attrs)

            new_res_attrs = db.DBSession.query(ResourceAttr).order_by(ResourceAttr.id.desc()).limit(len(res_attrs)).all()
            all_rs = []
            for ra in new_res_attrs:
                ra_id = ra.id
                if ra.attr_id in res_scenarios:
                    rs_list = res_scenarios[ra.attr_id]
                    for rs in rs_list:
                        rs_list[rs]['resource_attr_id'] = ra_id
                        all_rs.append(rs_list[rs])

            if len(all_rs) > 0:
                db.DBSession.bulk_insert_mappings(ResourceScenario, all_rs)

    db.DBSession.refresh(res_grp_i)
    #lazy load attributes
    res_grp_i.attributes

    return res_grp_i

def update_group(group,**kwargs):
    """
        Update a group.
        If new attributes are present, they will be added to the group.
        The non-presence of attributes does not remove them.
    """
    user_id = kwargs.get('user_id')
    try:
        group_i = db.DBSession.query(ResourceGroup).filter(ResourceGroup.id == group.id).one()
    except NoResultFound:
        raise ResourceNotFoundError("group %s not found"%(group.id))

    group_i.network.check_write_permission(user_id)

    group_i.name = group.name if group.name != None else group_i.name
    group_i.description = group.description if group.description else group_i.description

    if group.attributes is not None:
        _update_attributes(group_i, group.attributes)

    if group.types is not None:
        hdb.add_resource_types(group_i, group.types)

    db.DBSession.flush()

    return group_i


def set_group_status(group_id, status, **kwargs):
    """
        Set the status of a group to 'X'
    """
    user_id = kwargs.get('user_id')
    try:
        group_i = db.DBSession.query(ResourceGroup).filter(ResourceGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("ResourceGroup %s not found"%(group_id))

    group_i.network.check_write_permission(user_id)

    group_i.status = status

    db.DBSession.flush()

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
        group_i = db.DBSession.query(ResourceGroup).filter(ResourceGroup.id == group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Group %s not found"%(group_id))

    group_items = db.DBSession.query(ResourceGroupItem).filter(
                                                    ResourceGroupItem.group_id==group_id).all()
    for gi in group_items:
        db.DBSession.delete(gi)

    if purge_data == 'Y':
        _purge_datasets_unique_to_resource('GROUP', group_id)

    log.info("Deleting group %s, id=%s", group_i.name, group_id)

    group_i.network.check_write_permission(user_id)
    db.DBSession.delete(group_i)
    db.DBSession.flush()

def get_scenarios(network_id,**kwargs):
    """
        Get all the scenarios in a given network.
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        net_i.check_read_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    return net_i.scenarios

def validate_network_topology(network_id,**kwargs):
    """
        Check for the presence of orphan nodes in a network.
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
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

def get_resource(resource_type, resource_id, **kwargs):
    user_id = kwargs.get('user_id')

    resource_type = resource_type.upper()
    if resource_type == 'NODE':
        return get_node(resource_id, **kwargs)
    elif resource_type == 'LINK':
        return get_link(resource_id, **kwargs)
    elif resource_type == 'GROUP':
        return get_resourcegroup(resource_id, **kwargs)
    elif resource_type == 'NETWORK':
        network = get_network_simple(resource_id, **kwargs)
        return network


def get_resources_of_type(network_id, type_id, **kwargs):
    """
        Return the Nodes, Links and ResourceGroups which
        have the type specified.
    """
    #'set a ref key on the resources to easily distinguish them'
    nodes_with_type = db.DBSession.query(Node).join(ResourceType).filter(Node.network_id==network_id, ResourceType.type_id==type_id).all()
    for n in nodes_with_type:
        n.ref_key = 'NODE'
    links_with_type = db.DBSession.query(Link).join(ResourceType).filter(Link.network_id==network_id, ResourceType.type_id==type_id).all()
    for l in links_with_type:
        l.ref_key = 'LINK'
    groups_with_type = db.DBSession.query(ResourceGroup).join(ResourceType).filter(ResourceGroup.network_id==network_id, ResourceType.type_id==type_id).all()
    for g in groups_with_type:
        g.ref_key = 'GROUP'

    return nodes_with_type+links_with_type+groups_with_type

def clean_up_network(network_id, **kwargs):
    """
        Purge any deleted nodes, links, resourcegroups and scenarios in a given network
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_network')
    try:
        log.debug("Querying Network %s", network_id)
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).\
        options(noload(Network.scenarios)).options(noload(Network.nodes)).options(noload(Network.links)).options(
            noload(Network.resourcegroups)).options(
              joinedload(Network.types)\
              .joinedload(ResourceType.templatetype)\
              .joinedload(TemplateType.template)
            ).one()
        net_i.attributes

        #Define the basic resource queries
        node_qry = db.DBSession.query(Node).filter(Node.network_id==network_id).filter(Node.status=='X').all()

        link_qry = db.DBSession.query(Link).filter(Link.network_id==network_id).filter(Link.status=='X').all()

        group_qry = db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id==network_id).filter(ResourceGroup.status=='X').all()

        scenario_qry = db.DBSession.query(Scenario).filter(Scenario.network_id==network_id).filter(Scenario.status=='X').all()


        for n in node_qry:
            db.DBSession.delete(n)
        for l in link_qry:
            db.DBSession.delete(l)
        for g in group_qry:
            db.DBSession.delete(g)
        for s in scenario_qry:
            db.DBSession.delete(s)

    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    db.DBSession.flush()
    return 'OK'

def get_all_node_data(network_id, scenario_id, node_ids=None, include_metadata=False, **kwargs):
    resource_scenarios = get_attributes_for_resource(network_id, scenario_id, 'NODE', ref_ids=node_ids, include_metadata='N', **kwargs)

    node_data = []

    for rs in resource_scenarios:
        resource_attr = JSONObject({
            'id': rs.resourceattr.id,
            'attr_id' : rs.resourceattr.attr_id,
            'attr_name' : rs.resourceattr.attr.name,
            'resourcescenario': rs
        })
        node_data.append(resource_attr)

    return node_data

def get_all_link_data(network_id, scenario_id, link_ids=None, include_metadata=False, **kwargs):
    resource_scenarios = get_attributes_for_resource(network_id, scenario_id, 'LINK', ref_ids=link_ids, include_metadata='N', **kwargs)

    link_data = []

    for rs in resource_scenarios:
        resource_attr = JSONObject({
            'id': rs.resourceattr.id,
            'attr_id' : rs.resourceattr.attr_id,
            'attr_name' : rs.resourceattr.attr.name,
            'resourcescenario': rs
        })
        link_data.append(resource_attr)

    return link_data


def get_all_group_data(network_id, scenario_id, group_ids=None, include_metadata=False, **kwargs):
    resource_scenarios = get_attributes_for_resource(network_id, scenario_id, 'GROUP', ref_ids=group_ids, include_metadata='N', **kwargs)

    group_data = []

    for rs in resource_scenarios:
        resource_attr = JSONObject({
            'id': rs.resourceattr.id,
            'attr_id' : rs.resourceattr.attr_id,
            'attr_name' : rs.resourceattr.attr.name,
            'resourcescenario': rs
        })
        group_data.append(resource_attr)

    return group_data

def get_attributes_for_resource(network_id, scenario_id, ref_key, ref_ids=None, include_metadata=False, **kwargs):

    try:
        db.DBSession.query(Network).filter(Network.id==network_id).one()
    except NoResultFound:
        raise HydraError("Network %s does not exist"%network_id)

    try:
        db.DBSession.query(Scenario).filter(Scenario.id==scenario_id, Scenario.network_id==network_id).one()
    except NoResultFound:
        raise HydraError("Scenario %s not found."%scenario_id)

    rs_qry = db.DBSession.query(ResourceScenario).filter(
                            ResourceAttr.id==ResourceScenario.resource_attr_id,
                            ResourceScenario.scenario_id==scenario_id,
                            ResourceAttr.ref_key==ref_key)\
            .join(ResourceScenario.dataset)

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
    dataset_ids = []
    if ref_ids is not None:
        log.info("Pulling out requested info")
        for rs in all_resource_scenarios:
            ra = rs.resourceattr
            if ref_key == 'NODE':
                if ra.node_id in ref_ids:
                    resource_scenarios.append(rs)
                    if rs.dataset_id not in dataset_ids:
                        dataset_ids.append(rs.dataset_id)
            elif ref_key == 'LINK':
                if ra.link_id in ref_ids:
                    resource_scenarios.append(rs)
                    if rs.dataset_id not in dataset_ids:
                        dataset_ids.append(rs.dataset_id)
            elif ref_key == 'GROUP':
                if ra.group_id in ref_ids:
                    resource_scenarios.append(rs)
                    if rs.dataset_id not in dataset_ids:
                        dataset_ids.append(rs.dataset_id)
            else:
                resource_scenarios.append(ra)
        log.info("Requested info pulled out.")
    else:
        resource_scenarios = all_resource_scenarios

    log.info("Retrieved %s resource attrs", len(resource_scenarios))

    if include_metadata is True:
        metadata_qry = db.DBSession.query(Metadata).filter(
            ResourceAttr.ref_key == ref_key,
            ResourceScenario.resource_attr_id == ResourceAttr.id,
            ResourceScenario.scenario_id == scenario_id,
            Dataset.id == ResourceScenario.dataset_id,
            Metadata.dataset_id == Dataset.id)

        log.info("Querying node metadata")
        all_metadata = metadata_qry.all()
        log.info("Node metadata retrieved")

        metadata = []
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
               d.metadata = []
        else:
            if include_metadata is True:
                rs.dataset.metadata = metadata_dict.get(d.id, [])

    return resource_scenarios

def get_all_resource_attributes_in_network(attr_id, network_id, include_resources=True, **kwargs):
    """
        Find every resource attribute in the network matching the supplied attr_id
        Args:
            attr_id (int): The attribute on which to match
            network_id (int): The ID of the network to search
            include_resources (bool): A flag to indicate whether to return the
                resource that the resource attribute belongs to.
                Including resources can have a performance implication
        Returns:
            List of JSONObjects
        Raises:
            HydraError if the attr_id or network_id do not exist
    """

    user_id = kwargs.get('user_id')

    try:
        a = db.DBSession.query(Attr).filter(Attr.id == attr_id).one()
    except NoResultFound:
        raise HydraError("Attribute %s not found"%(attr_id,))

    ra_qry = db.DBSession.query(ResourceAttr).filter(
        ResourceAttr.attr_id == attr_id,
        or_(Network.id == network_id,
            Node.network_id == network_id,
            Link.network_id == network_id,
            ResourceGroup.network_id == network_id)
        ).outerjoin(ResourceAttr.node)\
        .outerjoin(ResourceAttr.link)\
        .outerjoin(ResourceAttr.network)\
        .outerjoin(ResourceAttr.resourcegroup)\
        .options(joinedload(ResourceAttr.node))\
        .options(joinedload(ResourceAttr.link))\
        .options(joinedload(ResourceAttr.resourcegroup))\
        .options(joinedload(ResourceAttr.network))

    resourceattrs = ra_qry.all()

    json_ra = []
    #Load the metadata too
    for ra in resourceattrs:
        ra_j = JSONObject(ra, extras={'node':JSONObject(ra.node) if ra.node else None,
                                      'link':JSONObject(ra.link) if ra.link else None,
                                      'resourcegroup':JSONObject(ra.resourcegroup) if ra.resourcegroup else None,
                                      'network':JSONObject(ra.network) if ra.network else None})

        if ra_j.node is not None:
            ra_j.resource = ra_j.node
        elif ra_j.link is not None:
            ra_j.resource = ra_j.link
        elif ra_j.resourcegroup is not None:
            ra_j.resource = ra_j.resourcegroup
        elif ra.network is not None:
            ra_j.resource = ra_j.network

        json_ra.append(ra_j)

    return json_ra


def get_all_resource_data(
        scenario_id,
        include_metadata=False,
        page_start=None,
        page_end=None,
        include_values=True,
        **kwargs):
    """
        A function which returns the data for all resources in a network.
        -
    """

    rs_qry = db.DBSession.query(
               ResourceAttr.attr_id,
               Attr.name.label('attr_name'),
               ResourceAttr.id.label('resource_attr_id'),
               ResourceAttr.ref_key,
               ResourceAttr.network_id,
               ResourceAttr.node_id,
               ResourceAttr.link_id,
               ResourceAttr.group_id,
               ResourceAttr.project_id,
               ResourceAttr.attr_is_var,
               ResourceScenario.scenario_id,
               ResourceScenario.source,
               Dataset.id.label('dataset_id'),
               Dataset.name.label('dataset_name'),
               Dataset.unit_id,
               Dataset.hidden,
               Dataset.type,
               null().label('metadata'),
               case(
                   (ResourceAttr.node_id != None, Node.name),
                   (ResourceAttr.link_id != None, Link.name),
                   (ResourceAttr.group_id != None, ResourceGroup.name),
                   (ResourceAttr.network_id != None, Network.name),
               ).label('ref_name'),
              ).join(ResourceScenario, ResourceScenario.resource_attr_id==ResourceAttr.id)\
                .join(Dataset, ResourceScenario.dataset_id==Dataset.id).\
                join(Attr, ResourceAttr.attr_id==Attr.id).\
                outerjoin(Node, ResourceAttr.node_id==Node.id).\
                outerjoin(Link, ResourceAttr.link_id==Link.id).\
                outerjoin(ResourceGroup, ResourceAttr.group_id==ResourceGroup.id).\
                outerjoin(Network, ResourceAttr.network_id==Network.id).\
            filter(ResourceScenario.scenario_id==scenario_id)

    if include_values is True:
        rs_qry = rs_qry.add_columns(Dataset.value)

    all_resource_data = rs_qry.all()

    if page_start is not None and page_end is None:
        all_resource_data = all_resource_data[page_start:]
    elif page_start is not None and page_end is not None:
        all_resource_data = all_resource_data[page_start:page_end]

    log.info("%s datasets retrieved", len(all_resource_data))

    if include_metadata is True:
        metadata_qry = db.DBSession.query(
            distinct(Metadata.dataset_id).label('dataset_id'),
            Metadata.key,
            Metadata.value).filter(
                ResourceScenario.resource_attr_id == ResourceAttr.id,
                ResourceScenario.scenario_id == scenario_id,
                Dataset.id == ResourceScenario.dataset_id,
                Metadata.dataset_id == Dataset.id)

        log.info("Querying node metadata")
        metadata = metadata_qry.all()
        log.info("%s metadata items retrieved", len(metadata))

        metadata_dict = {}
        for m in metadata:
            if metadata_dict.get(m.dataset_id):
                metadata_dict[m.dataset_id].append(m)
            else:
                metadata_dict[m.dataset_id] = [m]

    return_data = []
    for ra in all_resource_data:
        ra_dict = ra._asdict()
        if ra.hidden == 'Y':
           try:
                d = db.DBSession.query(Dataset).filter(
                    Dataset.id == ra.dataset_id
                    ).options(noload(Dataset.metadata)).one()
                d.check_read_permission(kwargs.get('user_id'))
           except:
                ra_dict['value'] = None
                ra_dict['metadata'] = []
        else:
            if include_metadata is True:
                ra_dict['metadata'] = metadata_dict.get(ra.dataset_id, [])

        return_data.append(namedtuple('ResourceData', ra_dict.keys())(**ra_dict))

    log.info("Returning %s datasets", len(return_data))

    return return_data

def clone_network(network_id,
                  recipient_user_id=None,
                  new_network_name=None,
                  new_network_description=None,
                  project_id=None,
                  project_name=None,
                  new_project=True,
                  include_outputs=False,
                  scenario_ids=[],
                  creator_is_owner=False,
                  **kwargs):
    """
     Create an exact clone of the specified network for the specified user.

     If project_id is specified, put the new network in there.

     Otherwise create a new project with the specified name and put it in there.

     creator_is_owner (Bool) : The user who creates the network isn't added as an owner
        (won't have an entry in tNetworkOwner and therefore won't see the network in 'get_project')

    """

    user_id = kwargs['user_id']

    ex_net = db.DBSession.query(Network).filter(Network.id==network_id).one()

    ex_net.check_read_permission(user_id)

    if recipient_user_id is None:
        recipient_user_id = user_id

    if project_id is None and new_project == True:

        log.info("Creating a new project for cloned network")

        ex_proj = db.DBSession.query(Project).filter(Project.id==ex_net.project_id).one()

        user = db.DBSession.query(User).filter(User.id==user_id).one()

        project = Project()
        if project_name is None or project_name=="":
            project_name=ex_proj.name + " (Cloned by %s)" % user.display_name

        #check a project with this name doesn't already exist:
        ex_project = db.DBSession.query(Project).filter(Project.name == project_name,
                                                         Project.created_by == user_id).all()
        #If it exists, use it.
        if len(ex_project) > 0:
            project=ex_project[0]
        else:
            project.name = project_name
            project.created_by = user_id
            if creator_is_owner is True and user_id != recipient_user_id:
                project.set_owner(user_id)

        if recipient_user_id is not None:
            project.set_owner(recipient_user_id)

        db.DBSession.add(project)
        db.DBSession.flush()

        project_id = project.id

    elif project_id is None:
        log.info("Using current project for cloned network")
        project_id = ex_net.project_id

    if new_network_name is None or new_network_name == "":
        new_network_name = ex_net.name

    log.info('Cloning Network...')

    #Find if there's any projects with this name in the project already
    ex_network = db.DBSession.query(Network).filter(Network.project_id == project_id,
                                                    Network.name.like(
                                                        f"{new_network_name}%")).all()

    if len(ex_network) > 0:
        new_network_name = f"{new_network_name} ({str(len(ex_network))})"

    newnet = Network()

    newnet.project_id = project_id
    newnet.name = new_network_name
    newnet.description = ex_net.description if new_network_description is None else new_network_description
    newnet.layout = ex_net.layout
    newnet.status = ex_net.status
    newnet.projection = ex_net.projection
    newnet.created_by = user_id
    newnet.appdata = ex_net.appdata

    #if true, the the creator will see this network in their project.networks.
    if creator_is_owner is True and user_id != recipient_user_id:
        newnet.set_owner(user_id)

    #set the owner to the recipient. THis can be either the requesting user id (user_id)
    #or an explicitly defined user.
    newnet.set_owner(recipient_user_id)

    db.DBSession.add(newnet)

    db.DBSession.flush()

    newnetworkid = newnet.id

    log.info('CLoning Nodes')
    node_id_map = _clone_nodes(network_id, newnetworkid, user_id)

    log.info('Cloning Links')
    link_id_map = _clone_links(network_id, newnetworkid, node_id_map, user_id)

    log.info('CLoning Groups')
    group_id_map = _clone_groups(network_id,
                                 newnetworkid,
                                 node_id_map,
                                 link_id_map,
                                 user_id)

    log.info("Cloning Resource Attributes")
    ra_id_map = _clone_resourceattrs(network_id,
                                     newnetworkid,
                                     node_id_map,
                                     link_id_map,
                                     group_id_map,
                                     newnet.project_id,
                                     ex_net.project_id,
                                     user_id)

    log.info("Cloning Resource Types")
    _clone_resourcetypes(network_id, newnetworkid, node_id_map, link_id_map, group_id_map)

    log.info('Cloning Scenarios')
    scenario_id_map = _clone_scenarios(network_id,
                                       newnetworkid,
                                       ra_id_map,
                                       node_id_map,
                                       link_id_map,
                                       group_id_map,
                                       user_id,
                                       include_outputs=include_outputs,
                                       scenario_ids=scenario_ids)

    _clone_network_rules(
        network_id,
        newnetworkid,
        user_id)


    db.DBSession.flush()

    return newnetworkid

def clone_node(node_id,
               include_outputs=False,
               name=None,
               new_x = None,
               new_y = None,
                  **kwargs):
    """
     Create an exact clone of the specified node, including attributes and data
     Args:
        node_id: The ID of the node to clone
        include_outputs (bool): Flag to indicate whether output attributes and data should be cloned
        name (str): The name of the new node. Defaults to the name of the old node plus (x) after, like "The Node (1)"
        newx (float): The X-coordinate of the new node. Defaults to the coordinate of the node being cloned.
        newy (float): The Y-coordinate of the new node. Defaults to the coordinate of the node being cloned.

    """

    user_id = kwargs['user_id']

    node_net = db.DBSession.query(Network).filter(Network.id==Node.network_id, Node.id==node_id).one()

    node_net.check_write_permission(user_id)

    return _clone_node(node_id,
                       node_net,
                       user_id,
                       include_outputs=include_outputs,
                       name=name,
                       new_x = new_x,
                       new_y = new_y)



def clone_nodes(
        node_ids,
        include_outputs=False,
        names=None,
        new_x_list = None,
        new_y_list = None,
        **kwargs):
    """
     Create an exact clone of the specified nodes, including attributes and data
     Args:
        node_ids: An iterable of node ids to clone
        include_outputs (bool): Flag to indicate whether output attributes and data should be cloned
        names (str): The names of the new nodes. Defaults to the names of the old nodes plus (x) after, like "The Node (1)".
                    If this is not null, there MUST be a name specified for each new node.
        new_x_list (float): The X-coordinates of the new nodes.
                            If this is not null, there MUST be an X coordinate for every new node
        new_y_list (float): The Y-coordinates of the new nodes.
                            If this is not null, there MUST be an Y coordinate for every new node
    """

    user_id = kwargs['user_id']

    node_net = db.DBSession.query(Network).filter(Network.id==Node.network_id, Node.id==node_ids[0]).one()

    #verify that the lengths of the lists are equal
    if None not in (new_x_list, new_y_list) and len(new_x_list) != len(new_y_list):
        raise HydraError("Unable to clone nodes. The list of x coordinates must match the length of y coordinates")

    if names is not None and len(names) != len(new_x_list):
        raise HydraError("Unable to clone nodes. A name must be specified for each cloned node, or the names argument must be None. ")


    node_net.check_write_permission(user_id)
    cloned_ids = []
    for i, node_id in enumerate(node_ids):
        cloned_id = _clone_node(node_id,
                node_net,
                user_id,
                include_outputs=include_outputs,
                name  = names[i] if names else None,
                new_x = new_x_list[i] if new_x_list else None,
                new_y = new_y_list[i] if new_y_list else None)
        cloned_ids.append(cloned_id)

    return cloned_ids

def _make_cloned_node_name(network_id, node_name):
    """
    Get the closest node name in the spoecified network to the node specified.
    For example:
        "Bury_wtw" would be similar to "Bury_wtw_East", "Bury_wtw_South", etc,
        so this would return 'Bury_wtw', as it is the closest match.
        Additionally, "Bury_wtw (1) would be closer than "Bury_wtw (10)"


    If node name ends in ([0-9]+) then increment any match to be the next free number
    If node to clone doesn't end in ([0-9]+) then add the next free number in this format

    """
    #if the node to clone ends with '(number)' like 'Bury_wtw (1)'
    pattern = re.compile(r'\((\d+)\)$')
    node_base_name = node_name
    match = pattern.search(node_name)
    if match:
        node_base_name = node_name.replace(match.group(0))

    #get all the nodes which match either 'Bury_wrw' or 'Bury_wtw (X)'
    similar_names = db.DBSession.query(Node.name).filter(
        Node.network_id==network_id,
        or_(Node.name == node_base_name,
        Node.name.regexp_match(f'{node_base_name} {pattern.pattern}'))
    ).all()


    #the node name is already unique so just use it
    if len(similar_names) == 0:
        return node_name

    #go through all the matching names and find the one with the highest
    #number in parentheses. ex of 'Bury_wtw (1)' and 'Bury_wtw (10), return 11.
    highest_num = 0
    for n in similar_names:
        name = n.name
        match = pattern.search(name)
        if match:
            if int(match.group(1)) > highest_num:
                highest_num = int(match.group(1))

    next_num = highest_num + 1

    new_node_name = f"{node_base_name} ({next_num})"

    return new_node_name


def _clone_node(
        node_id,
        node_net,
        user_id,
        include_outputs=False,
        name=None,
        new_x = None,
        new_y = None):

    node_to_clone = db.DBSession.query(Node).filter(Node.id==node_id).one()

    log.info('Cloning Node...')

    newnode = Node()

    for nodecolumn in Node.__table__.columns:
        if nodecolumn.name in ('id', 'name', 'cr_date', 'created_by', 'updated_at', 'updated_by'):
            continue
        setattr(newnode, nodecolumn.name, getattr(node_to_clone, nodecolumn.name))

    if name is not None:
        node_with_same_name = db.DBSession.query(Node).filter(
            Node.network_id==node_net.id,
            Node.name == name
        ).all()

        if len(node_with_same_name) > 0:
            raise HydraError(f"A node with name {name} already exists in this network.")
        newnode.name = name
    else:
        newnode.name = _make_cloned_node_name(node_net.id, node_to_clone.name)


    if new_x is not None:
        try:
            newnode.x = float(new_x)
        except TypeError:
            raise HydraError(f"Unable to clone node {name}. Coordinate {new_x} must be numeric.")

    if new_y is not None:
        try:
            newnode.y = float(new_y)
        except TypeError:
            raise HydraError(f"Unable to clone node {name}. Coordinate {new_y} must be numeric.")

    db.DBSession.add(newnode)
    db.DBSession.flush()

    #Clone the resource attributes
    log.info("Cloning Resource Attributes")
    node_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.node_id==node_id)).all()
    new_ras = []
    ra_id_map = {}
    old_node_ra_map = {}
    for ra in node_ras:
        new_ras.append(dict(
            node_id=newnode.id,
            attr_id=ra.attr_id,
            attr_is_var=ra.attr_is_var,
            ref_key=ra.ref_key,
        ))
        old_node_ra_map[ra.attr_id] = ra.id
    log.info("Inserting new resource attributes")
    db.DBSession.bulk_insert_mappings(ResourceAttr, new_ras)
    db.DBSession.flush()

    log.info("Creating mapping from old resource attribute IDs to new")
    new_node_ras = db.DBSession.query(ResourceAttr).filter(
        ResourceAttr.node_id==newnode.id).all()

    for ra in new_node_ras:
        ra_id_map[old_node_ra_map[ra.attr_id]] = ra.id

    log.info("Cloning Resource Types")
    node_rts = db.DBSession.query(ResourceType).filter(and_(
        ResourceType.node_id==node_id)).all()
    new_resourcetypes = []
    for rt in node_rts:
        new_resourcetypes.append(dict(
            ref_key=rt.ref_key,
            node_id=newnode.id,
            type_id=rt.type_id,
            child_template_id=rt.child_template_id,
        ))

    db.DBSession.bulk_insert_mappings(ResourceType, new_resourcetypes)
    db.DBSession.flush()

    log.info('Cloning Data')
    rscen_to_clone_qry = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.scenario_id == Scenario.id,
        ResourceScenario.resource_attr_id == ResourceAttr.id,
        ResourceAttr.node_id==node_id,
        Scenario.network_id==node_net.id
    )
    #Filter out output data unless explicitly requested not to.
    if include_outputs is not True:
        rscen_to_clone_qry = rscen_to_clone_qry.filter(ResourceAttr.attr_is_var == 'N')

    new_rscens = []
    for rscen_to_clone in rscen_to_clone_qry.all():
        new_rscens.append(dict(
            dataset_id=rscen_to_clone.dataset_id,
            scenario_id=rscen_to_clone.scenario_id,
            resource_attr_id=ra_id_map[rscen_to_clone.resource_attr_id],
        ))

    log.info("Inserting new resource scenarios")
    db.DBSession.bulk_insert_mappings(ResourceScenario, new_rscens)
    db.DBSession.flush()

    log.info("Node clone complete. New node ID is %s", newnode.id)

    return newnode.id

def _clone_network_rules(old_network_id, new_network_id, user_id):
    """
    """
    rules.clone_resource_rules('NETWORK',
                               old_network_id,
                               target_ref_key='NETWORK',
                               target_ref_id=new_network_id,
                               user_id=user_id)

def _clone_nodes(old_network_id, new_network_id, user_id):

    nodes = db.DBSession.query(Node).filter(Node.network_id==old_network_id).all()
    newnodes = []
    old_node_name_map = {}
    id_map = {}
    for ex_n in nodes:
        new_n = dict(
            network_id=new_network_id,
            name = ex_n.name,
            description = ex_n.description,
            x = ex_n.x,
            y = ex_n.y,
            layout = ex_n.layout,
            status = ex_n.status,
        )

        old_node_name_map[ex_n.name] = ex_n.node_id

        newnodes.append(new_n)

    db.DBSession.bulk_insert_mappings(Node, newnodes)

    db.DBSession.flush()
    #map old IDS to new IDS

    nodes = db.DBSession.query(Node).filter(Node.network_id==new_network_id).all()

    for n in nodes:
        old_node_id = old_node_name_map[n.name]
        id_map[old_node_id] = n.node_id


    return id_map

def _clone_links(old_network_id, new_network_id, node_id_map, user_id):

    links = db.DBSession.query(Link).filter(Link.network_id==old_network_id).all()
    newlinks = []
    old_link_name_map = {}
    id_map = {}
    for ex_l in links:
        new_l = dict(
            network_id=new_network_id,
            name = ex_l.name,
            description = ex_l.description,
            node_1_id = node_id_map[ex_l.node_1_id],
            node_2_id = node_id_map[ex_l.node_2_id],
            layout = ex_l.layout,
            status = ex_l.status,
        )

        newlinks.append(new_l)

        old_link_name_map[ex_l.name] = ex_l.id

    db.DBSession.bulk_insert_mappings(Link, newlinks)

    db.DBSession.flush()
    #map old IDS to new IDS

    links = db.DBSession.query(Link).filter(Link.network_id==new_network_id).all()
    for l in links:
        old_link_id = old_link_name_map[l.name]
        id_map[old_link_id] = l.link_id

    return id_map

def _clone_groups(old_network_id, new_network_id, node_id_map, link_id_map, user_id):

    groups = db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id==old_network_id).all()
    newgroups = []
    old_group_name_map = {}
    id_map = {}
    for ex_g in groups:
        new_g = dict(
            network_id=new_network_id,
            name = ex_g.name,
            description = ex_g.group_description,
            status = ex_g.status,
        )

        newgroups.append(new_g)

        old_group_name_map[ex_g.name] = ex_g.id

    db.DBSession.bulk_insert_mappings(ResourceGroup, newgroups)

    db.DBSession.flush()
    #map old IDS to new IDS

    groups = db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id==new_network_id).all()
    for g in groups:
        old_group_id = old_group_name_map[g.name]
        id_map[old_group_id] = g.group_id

    return id_map

def _clone_attributes(network_id, newnetworkid, exnet_project_id, newnet_project_id, user_id):
    """
        Clone the attributes scoped to a network nad its project when cloning a network
        @returns:
            A lookup from the original scoped attr ID to any newly created scoped attribute.
            This is so that resource-attribute attr_id references can be updated to refer to the
            new scoped attribute ID
    """
    #first find any attributes which are scoped to the source network, and scope them to the parent project if the source
    #and target are in the same project, otherwise clone all the scoped attributes.

    #find any attributes scoped directly to the source
    network_scoped_attrs = attributes.get_attributes(network_id=network_id, user_id=user_id)
    project_scoped_attrs = []
    #get all the attributes scoped to the project of the source network (if it's not the same project as the target)
    new_scoped_attrs_lookup = {}

    if exnet_project_id != newnet_project_id:
        orig_scoped_attr_lookup = {}
        new_attributes = []
        exnet_project_scoped_attrs = attributes.get_attributes(project_id=exnet_project_id, user_id=user_id)
        for a in exnet_project_scoped_attrs:
            a.project_id = newnet_project_id
            new_attributes.append(a)
            orig_scoped_attr_lookup[a.name] = a.id

        for a in network_scoped_attrs:
            #the networks are in different projects, so clone the attributes
            a = JSONObject(a)
            a.network_id = newnetworkid
            new_attributes.append(a)
            orig_scoped_attr_lookup[a.name] = a.id

        new_attrs = attributes.add_attributes(new_attributes, user_id=user_id)
        #create a mapping from the old scpoed attr ID to the new scoped attr, so that we can
        #update references in the network from the old attribute to the new one.
        for na in new_attrs:
            old_scoped_attr_id = orig_scoped_attr_lookup[na.name]
            new_scoped_attrs_lookup[old_scoped_attr_id] = na
    else:
        for a in network_scoped_attrs:
            #the networks are in the same project, so re-scope the attribute
            #to the project, so it is shared by the networks
            a.network_id=None
            a.project_id=exnet_project_id
            attributes.update_attribute(a)

    return new_scoped_attrs_lookup

def _clone_resourceattrs(network_id, newnetworkid, node_id_map, link_id_map, group_id_map, exnet_project_id, newnet_project_id, user_id):

    #clone any attributes which are scoped to a network or to the network's project (if the networks)
    #are in different projects.
    new_scoped_attr_lookup = _clone_attributes(network_id, newnetworkid, exnet_project_id, newnet_project_id, user_id)

    log.info("Cloning Network Attributes")
    network_ras = db.DBSession.query(ResourceAttr).filter(ResourceAttr.network_id==network_id)
    id_map = {}
    new_ras = []
    old_ra_name_map = {}
    for ra in network_ras:
        new_attr = new_scoped_attr_lookup.get(ra.attr_id)
        attr_id = ra.attr_id
        if new_attr:
            attr_id = new_attr.id
        new_ras.append(dict(
            network_id=newnetworkid,
            node_id=None,
            group_id=None,
            link_id=None,
            ref_key='NETWORK',
            attr_id=attr_id,
            attr_is_var=ra.attr_is_var,
        ))
        #key is (network_id, node_id, link_id, group_id) -- only one of which can be not null for a given row
        old_ra_name_map[(newnetworkid, None, None, None, attr_id)] = ra.id
    log.info("Cloning Node Attributes")
    node_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.node_id==Node.id, Node.network_id==network_id)).all()
    for ra in node_ras:
        new_attr = new_scoped_attr_lookup.get(ra.attr_id)
        attr_id = ra.attr_id
        if new_attr:
            attr_id = new_attr.id
        new_ras.append(dict(
            node_id=node_id_map[ra.node_id],
            network_id=None,
            link_id=None,
            group_id=None,
            attr_id=attr_id,
            attr_is_var=ra.attr_is_var,
            ref_key=ra.ref_key,
        ))
        old_ra_name_map[(None, node_id_map[ra.node_id], None, None, attr_id)] = ra.id
    log.info("Cloning Link Attributes")
    link_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.link_id==Link.id, Link.network_id==network_id)).all()
    for ra in link_ras:
        new_attr = new_scoped_attr_lookup.get(ra.attr_id)
        attr_id = ra.attr_id
        if new_attr:
            attr_id = new_attr.id
        new_ras.append(dict(
            link_id=link_id_map[ra.link_id],
            network_id=ra.network_id,
            node_id=ra.node_id,
            group_id=ra.group_id,
            attr_id=attr_id,
            attr_is_var=ra.attr_is_var,
            ref_key=ra.ref_key,
        ))
        old_ra_name_map[(None, None, link_id_map[ra.link_id], None, attr_id)] = ra.id

    log.info("Cloning Group Attributes")
    group_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.group_id==ResourceGroup.id, ResourceGroup.network_id==network_id)).all()
    for ra in group_ras:
        new_attr = new_scoped_attr_lookup.get(ra.attr_id)
        attr_id = ra.attr_id
        if new_attr:
            attr_id = new_attr.id
        new_ras.append(dict(
            group_id=group_id_map[ra.group_id],
            network_id=ra.network_id,
            link_id=ra.link_id,
            node_id=ra.node_id,
            attr_id=attr_id,
            attr_is_var=ra.attr_is_var,
            ref_key=ra.ref_key,
        ))
        old_ra_name_map[(None, None, None, group_id_map[ra.group_id], attr_id)] = ra.id

    log.info("Inserting new resource attributes")
    db.DBSession.bulk_insert_mappings(ResourceAttr, new_ras)
    db.DBSession.flush()
    log.info("Insertion Complete")

    log.info("Getting new RAs and building ID map")

    new_network_ras = db.DBSession.query(ResourceAttr).filter(ResourceAttr.network_id==newnetworkid).all()
    for ra in new_network_ras:
        id_map[old_ra_name_map[(ra.network_id, ra.node_id, ra.link_id, ra.group_id, ra.attr_id)]] = ra.id

    new_node_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.node_id==Node.id, Node.network_id==newnetworkid)).all()
    for ra in new_node_ras:
        id_map[old_ra_name_map[(ra.network_id, ra.node_id, ra.link_id, ra.group_id, ra.attr_id)]] = ra.id


    new_link_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.link_id==Link.id, Link.network_id==newnetworkid)).all()
    for ra in new_link_ras:
        id_map[old_ra_name_map[(ra.network_id, ra.node_id, ra.link_id, ra.group_id, ra.attr_id)]] = ra.id

    new_group_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.group_id==ResourceGroup.id, ResourceGroup.network_id==newnetworkid)).all()
    for ra in new_group_ras:
        id_map[old_ra_name_map[(ra.network_id, ra.node_id, ra.link_id, ra.group_id, ra.attr_id)]] = ra.id
    log.info("ID map completed. Returning")

    return id_map

def _clone_resourcetypes(network_id, newnetworkid, node_id_map, link_id_map, group_id_map):

    log.info("Cloning Network Types")
    network_rts = db.DBSession.query(ResourceType).filter(
        ResourceType.network_id==network_id).all()
    new_rts = []
    for rt in network_rts:
        new_rts.append(dict(
            ref_key=rt.ref_key,
            network_id=newnetworkid,
            node_id=rt.node_id,
            link_id=rt.link_id,
            group_id=rt.group_id,
            type_id=rt.type_id,
            child_template_id=rt.child_template_id,
        ))
    log.info("Cloning Node Types")
    node_rts = db.DBSession.query(ResourceType).filter(and_(
        ResourceType.node_id==Node.id,
        Node.network_id==network_id)).all()
    for rt in node_rts:
        new_rts.append(dict(
            ref_key=rt.ref_key,
            network_id=rt.network_id,
            node_id=node_id_map[rt.node_id],
            link_id=rt.link_id,
            group_id=rt.group_id,
            type_id=rt.type_id,
            child_template_id=rt.child_template_id,
        ))
    log.info("Cloning Link Types")
    link_rts = db.DBSession.query(ResourceType).filter(and_(
        ResourceType.link_id==Link.id,
        Link.network_id==network_id)).all()
    for rt in link_rts:
        new_rts.append(dict(
            ref_key=rt.ref_key,
            network_id=rt.network_id,
            node_id=rt.node_id,
            link_id=link_id_map[rt.link_id],
            group_id=rt.group_id,
            type_id=rt.type_id,
            child_template_id=rt.child_template_id,
        ))

    log.info("Cloning Group Types")
    group_rts = db.DBSession.query(ResourceType).filter(and_(
        ResourceType.group_id==ResourceGroup.id,
        ResourceGroup.network_id==network_id)).all()
    for rt in group_rts:
        new_rts.append(dict(
            ref_key=rt.ref_key,
            network_id=rt.network_id,
            node_id=rt.node_id,
            link_id=rt.link_id,
            group_id=group_id_map[rt.group_id],
            type_id=rt.type_id,
            child_template_id=rt.child_template_id,
        ))

    log.info("Inserting new resource types")
    db.DBSession.bulk_insert_mappings(ResourceType, new_rts)
    db.DBSession.flush()
    log.info("Insertion Complete")

def _clone_scenarios(network_id,
                     newnetworkid,
                     ra_id_map,
                     node_id_map,
                     link_id_map,
                     group_id_map,
                     user_id,
                     include_outputs=False,
                     scenario_ids=[]):

    scenarios = db.DBSession.query(Scenario).filter(Scenario.network_id == network_id).all()

    id_map = {}

    for scenario in scenarios:
        #if scenario_ids are specified (the list is not empty) then filter out
        #the scenarios not specified.
        if len(scenario_ids) > 0 and scenario.id not in scenario_ids:
            log.info("Not cloning scenario %s", scenario.id)
            continue

        if scenario.status == 'A':
            new_scenario_id = _clone_scenario(scenario,
                                              newnetworkid,
                                              ra_id_map,
                                              node_id_map,
                                              link_id_map,
                                              group_id_map,
                                              user_id,
                                              include_outputs=include_outputs)
            id_map[scenario.id] = new_scenario_id

    return id_map

def _clone_scenario(old_scenario,
                    newnetworkid,
                    ra_id_map,
                    node_id_map,
                    link_id_map,
                    group_id_map,
                    user_id,
                    include_outputs=False):

    log.info("Adding scenario shell to get scenario ID")
    news = Scenario()
    news.network_id = newnetworkid
    news.name = old_scenario.name
    news.description = old_scenario.description
    news.layout = old_scenario.layout
    news.start_time = old_scenario.start_time
    news.end_time = old_scenario.end_time
    news.time_step = old_scenario.time_step
    news.parent_id = old_scenario.parent_id
    news.created_by = user_id

    db.DBSession.add(news)

    db.DBSession.flush()

    scenario_id = news.id
    log.info("New Scenario %s created", scenario_id)

    log.info("Getting old resource scenarios for scenario %s", old_scenario.id)
    old_rscen_qry = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.scenario_id == old_scenario.id,
        ResourceAttr.id == ResourceScenario.resource_attr_id,
    )

    #Filter out output data unless explicitly requested not to.
    if include_outputs is not True:
        old_rscen_qry = old_rscen_qry.filter(ResourceAttr.attr_is_var == 'N')

    old_rscen_rs = old_rscen_qry.all()

    new_rscens = []
    for old_rscen in old_rscen_rs:
        new_rscens.append(dict(
            dataset_id=old_rscen.dataset_id,
            scenario_id=scenario_id,
            resource_attr_id=ra_id_map[old_rscen.resource_attr_id],
        ))

    log.info("Inserting new resource scenarios")
    db.DBSession.bulk_insert_mappings(ResourceScenario, new_rscens)
    log.info("Insertion Complete")



    log.info("Getting old resource group items for scenario %s", old_scenario.id)
    old_rgis = db.DBSession.query(ResourceGroupItem).filter(
        ResourceGroupItem.scenario_id == old_scenario.id).all()

    new_rgis = []
    for old_rgi in old_rgis:
        new_rgis.append(dict(
            ref_key=old_rgi.ref_key,
            node_id=node_id_map.get(old_rgi.node_id),
            link_id=link_id_map.get(old_rgi.link_id),
            subgroup_id=group_id_map.get(old_rgi.subgroup_id),
            group_id=group_id_map.get(old_rgi.group_id),
            scenario_id=scenario_id,
        ))

    db.DBSession.bulk_insert_mappings(ResourceGroupItem, new_rgis)

    return scenario_id

@required_perms("edit_network")
def apply_unit_to_network_rs(network_id, unit_id, attr_id, scenario_id=None, **kwargs):
    """
        Set the unit on all the datasets in a network which have the same attribue
        as the supplied resource_attr_id.
        args:
            unit_id (int): The unit ID to set on the network's datasets
            attr_id (int): The attribute ID
            scenario_id (int) (optional): Supplied if only datasets in a
                                          specific scenario are to be affected
        returns:
            None
        raises:
            ValidationError if the supplied unit is incompatible with the attribute's dimension
    """

    #Now get all the RS associated to both the attr and network.
    network_rs_query = db.DBSession.query(ResourceScenario).filter(
        Scenario.network_id == network_id,
        ResourceScenario.scenario_id == Scenario.id,
        ResourceScenario.resource_attr_id == ResourceAttr.id,
        ResourceAttr.attr_id == attr_id)

    if scenario_id is not None:
        network_rs_query.filter(Scenario.id == scenario_id)

    network_rs_list = network_rs_query.all()

    #Get the attribute in question so we can check its dimension
    attr_i = db.DBSession.query(Attr).filter(Attr.id == attr_id).one()

    #now check whether the supplied unit can be applied by comparing it to the attribute's dimension
    units.check_unit_matches_dimension(unit_id, attr_i.dimension_id)

    #set the unit ID for each of the resource scenarios
    for network_rs in network_rs_list:
        network_rs.dataset.unit_id = unit_id

