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

import logging
import time
from ..exceptions import HydraError, PermissionError, ResourceNotFoundError
from .. import db
from ..util.permissions import required_perms
from ..db.model import Scenario,\
        ResourceGroupItem,\
        ResourceScenario,\
        TypeAttr,\
        ResourceAttr,\
        Dataset,\
        Metadata,\
        Network,\
        Attr,\
        Node,\
        Link,\
        User,\
        ResourceGroup,\
        ResourceAttrMap

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import joinedload, aliased
from . import data
from collections import namedtuple
from copy import deepcopy

from .network import get_resource

from .objects import JSONObject, Dataset as JSONDataset

log = logging.getLogger(__name__)

def _check_network_ownership(network_id, user_id):
    net = db.DBSession.query(Network).filter(Network.id == network_id).one()

    net.check_write_permission(user_id)

def _get_scenario(scenario_id, user_id, check_write=False, check_can_edit=False):
    log.info("Getting scenario %s", scenario_id)
    try:
        scenario_i = db.DBSession.query(Scenario).filter(Scenario.id == scenario_id).one()

        if check_write is True:
            scenario_i.network.check_write_permission(user_id)
        else:
            #always check read permission. only do this if check write is false to
            #avoid doing basically the same query twice.
            scenario_i.network.check_read_permission(user_id)

        if check_can_edit is True:
            if scenario_i.locked == 'Y':
                raise PermissionError(f'Cannot update scenario {scenario_id} as it is locked.')

        return scenario_i
    except NoResultFound:
        raise ResourceNotFoundError(f"Scenario {scenario_id} does not exist.")

@required_perms("edit_data")
def set_rs_dataset(resource_attr_id, scenario_id, dataset_id, **kwargs):
    rs = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.resource_attr_id == resource_attr_id,
        ResourceScenario.scenario_id == scenario_id).first()

    if rs is None:
        raise ResourceNotFoundError("Resource scenario for resource attr %s not found in scenario %s"%(resource_attr_id, scenario_id))

    dataset = db.DBSession.query(Dataset).filter(Dataset.id == dataset_id).first()

    if dataset is None:
        raise ResourceNotFoundError("Dataset %s not found"%(dataset_id,))

    rs.dataset_id = dataset_id

    db.DBSession.flush()

    rs = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.resource_attr_id == resource_attr_id,
        ResourceScenario.scenario_id == scenario_id).first()

    return rs

@required_perms("edit_network")
def merge_scenarios(source_scenario_id, target_scenario_id, match_all_names=True, ignore_missing_attributes=True, **kwargs):
    """
        Merge the data from one scenario to another. Matching is done by node / link name.

        Only attributes which exist on the target scenario are updated. No new attributes are
        added to the target as this would affect other scenarios in that target network which
        is not desireable

        If any node name does nto match, an error is thrown unless overwritten with
        the 'match_all_names = False'
    """
    user_id = kwargs.get('user_id')

    source_scenario = _get_scenario(source_scenario_id, user_id)
    target_scenario = _get_scenario(target_scenario_id, user_id, check_write=True)

    source_network = source_scenario.network
    target_network = target_scenario.network

    source_nodes = {n.name for n in source_network.nodes}
    target_nodes = {n.name for n in target_network.nodes}

    target_resource_lookup = dict((n.name, n) for n in target_network.nodes + target_network.links + target_network.resourcegroups)

    non_matching_node_names = source_nodes - target_nodes
    if len(non_matching_node_names) > 0:
        if match_all_names is True:
            raise HydraError(f"Unable to merge scenario {source_scenario.name} ({source_scenario.id}) into {target_scenario.name} ({target_scenario.id}) as the following node names do not match: {non_matching_node_names}")

        log.warning(f"The following nodes in scenario {source_scenario.name} ({source_scenario.id}) no not match nodes in scenario {target_scenario.name} ({target_scenario.id}) : {non_matching_node_names}")

    cloned_target = clone_scenario(target_scenario.id, user_id=user_id)
    #we need the ORM object here, not a JSONObject
    cloned_scenario = db.DBSession.query(Scenario).filter(Scenario.id == cloned_target.id).one()

    #Make it easy to access resource scenarios from the resource attr ID with a map
    source_ra_rs_map = dict((rs.resource_attr_id, rs) for rs in source_scenario.resourcescenarios)
    target_ra_rs_map = dict((rs.resource_attr_id, rs) for rs in cloned_scenario.resourcescenarios)

    #Create a mapping from the source resource attribute to the target resource
    #attribute so we can then match the resource scenarios
    for source_resource in [source_network] + source_network.nodes + source_network.links + source_network.resourcegroups:

        if hasattr(source_resource, 'projection'):#only a network has a projection
            target_resource = target_network
        else:
            target_resource = target_resource_lookup.get(source_resource.name)

        if target_resource is None:
            log.warning(f"Resource '{source_resource.name}' not found on target. Ignoring.")
            continue

        target_attribute_map = dict((a.attr_id, a.id) for a in target_resource.attributes)
        for source_ra in source_resource.attributes:
            #get the RS of this attribute in both scenarios
            target_ra_id = target_attribute_map.get(source_ra.attr_id)
            if target_ra_id is None:
                if ignore_missing_attributes is True:
                    log.warning(f"Unable to find attribute {source_ra.attr.name} on target {target_resource.name}. Ignoring.")
                    continue
                else:
                    new_ra = ResourceAttr()
                    new_ra.attr_id = source_ra.attr_id
                    new_ra.ref_key = source_ra.ref_key
                    ref = new_ra.ref_key
                    new_ra.node_id = target_resource.id if ref == 'NODE' else None
                    new_ra.network_id = target_resource.id if ref == 'NETWORK' else None
                    new_ra.link_id = target_resource.id if ref == 'LINK' else None
                    new_ra.group_id = target_resource.id if ref == 'GROUP' else None
                    new_ra.attr_is_var = source_ra.attr_is_var
                    db.DBSession.add(new_ra)
                    db.DBSession.flush()
                    target_ra_id = new_ra.id

                    log.info(f"Adding new attribute {source_ra.attr.name} to {target_resource.name}")

            source_rs = source_ra_rs_map.get(source_ra.id)
            target_rs = target_ra_rs_map.get(target_ra_id)
            if source_rs is None:
                continue ## no data associated to the source, so ignore.

            if target_rs is None:
                target_rs = ResourceScenario()
                target_rs.scenario_id = cloned_scenario.id
                target_rs.dataset_id = source_rs.dataset_id
                target_rs.resource_attr_id = target_ra_id
                db.DBSession.add(target_rs)

            #Check if the datasets are different and if so, apply the source dataset ID
            #to the target
            if source_rs.dataset_id != target_rs.dataset_id:
                target_rs.dataset_id = source_rs.dataset_id

    db.DBSession.flush()

@required_perms("edit_network")
def copy_data_from_scenario(resource_attrs, source_scenario_id, target_scenario_id, **kwargs):
    """
        For a given list of resource attribute IDS copy the dataset_ids from
        the resource scenarios in the source scenario to those in the 'target' scenario.
    """

    #Get all the resource scenarios we wish to update
    target_resourcescenarios = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.scenario_id == target_scenario_id,
        ResourceScenario.resource_attr_id.in_(resource_attrs)).all()

    target_rs_dict = {}
    for target_rs in target_resourcescenarios:
        target_rs_dict[target_rs.resource_attr_id] = target_rs

    #get all the resource scenarios we are using to get our datsets source.
    source_resourcescenarios = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.scenario_id == source_scenario_id,
        ResourceScenario.resource_attr_id.in_(resource_attrs)).all()

    #If there is an RS in scenario 'source' but not in 'target', then create
    #a new one in 'target'
    for source_rs in source_resourcescenarios:
        target_rs = target_rs_dict.get(source_rs.resource_attr_id)
        if target_rs is not None:
            target_rs.dataset_id = source_rs.dataset_id
        else:
            target_rs = ResourceScenario()
            target_rs.scenario_id = target_scenario_id
            target_rs.dataset_id = source_rs.dataset_id
            target_rs.resource_attr_id = source_rs.resource_attr_id
            db.DBSession.add(target_rs)

    db.DBSession.flush()

    return target_resourcescenarios

@required_perms("get_network")
def get_scenario_by_name(network_id, scenario_name, get_parent_data=False, include_data=True, include_group_items=True, **kwargs):
    """
        Get the specified scenario from a network by its name
        args:
            network_id: The ID of the network to retrieve the scenario from
            scenario_name: The ID of the scenario to retrieve
            get_parent_data: Flag to indicate whether to include the data from the parent scenario also, or just this one.
            include_data: Flag to indicate wheter to return the list of resource scenarios
            include_group_items: Flag to indicate whether to return the list of resource group items
        return:
            A scenario JSONObject
    """

    try:
        scenario_i = db.DBSession.query(Scenario).filter(
            func.lower(Scenario.name) == scenario_name.lower(),
            Scenario.network_id == network_id).first()

        return get_scenario(scenario_i.id,
                            get_parent_data=get_parent_data,
                            include_data=include_data,
                            include_group_items=include_group_items,
                            **kwargs)
    except NoResultFound:
        raise ResourceNotFoundError("Scenario %s not found"%(scenario_name))

@required_perms("get_network")
def get_scenario(scenario_id,
                 get_parent_data=False,
                 include_data=True,
                 include_group_items=True,
                 include_results=True,
                 include_only_results=False,
                 include_metadata=True,
                 include_attr=True,
                 **kwargs):
    """
        Get the specified scenario
        args:
            scenario_id: The ID of the scenario to retrieve
            get_parent_data: Flag to indicate whether to include the data from
                the parent scenario also, or just this one.
            include_data: Flag to indicate wheter to return the list of resource scenarios
            include_group_items: Flag to indicate whether to return the list of resource group items
            include_results: Flag (default True). Set to false if only inputs
                are necessary. This can have a significant performance impact
            include_only_results: Flag (default False). Set to True if you ONLY
                want results data. This will only work when include_results is True
        return:
            A scenario JSONObject
    """

    user_id = kwargs.get('user_id')

    scen_i = _get_scenario(scenario_id, user_id)

    scen_j = JSONObject(scen_i)
    rscen_rs = []
    t = time.time()
    if include_data is True:
        rscen_rs = scen_i.get_data(
            user_id,
            get_parent_data=get_parent_data,
            include_results=include_results,
            include_only_results=include_only_results,
            include_metadata=include_metadata)
    log.info(f"Time taken to get data: {time.time() - t:.2f}")
    rgi_rs = []
    if include_group_items is True:
        rgi_rs = scen_i.get_group_items(get_parent_items=get_parent_data)

    scen_j.resourcescenarios = []

    user = db.DBSession.query(User).filter(User.id == user_id).one()
    is_admin = user.is_admin()

    for rs in rscen_rs:
        rs_j = JSONObject(rs, extras={'resourceattr':JSONObject(rs.resourceattr)})
        scen_j.resourcescenarios.append(rs_j)

    scen_j.resourcegroupitems =[JSONObject(r) for r in rgi_rs]

    return scen_j

@required_perms("edit_network")
def add_scenario(network_id, scenario,**kwargs):
    """
        Add a scenario to a specified network.
    """
    user_id = int(kwargs.get('user_id'))
    log.info("Adding scenarios to network")

    _check_network_ownership(network_id, user_id)

    existing_scen = db.DBSession.query(Scenario).filter(Scenario.name==scenario.name, Scenario.network_id==network_id).first()
    if existing_scen is not None:
        raise HydraError("Scenario with name %s already exists in network %s"%(scenario.name, network_id))

    scen = Scenario()
    scen.name = scenario.name
    scen.description = scenario.description
    scen.layout = scenario.get_layout()
    scen.network_id = network_id
    scen.created_by = user_id
    scen.start_time = scenario.start_time
    scen.end_time = scenario.end_time
    scen.time_step = scenario.time_step
    scen.resourcescenarios = []
    scen.resourcegroupitems = []

    #Just in case someone puts in a negative ID for the scenario.
    if isinstance(scenario.id, int) and scenario.id < 0:
        scenario.id = None

    if scenario.resourcescenarios is not None:
        #extract the data from each resourcescenario so it can all be
        #inserted in one go, rather than one at a time
        all_data = [r.dataset for r in scenario.resourcescenarios]

        datasets = data._bulk_insert_data(all_data, user_id=user_id)

        #record all the resource attribute ids
        resource_attr_ids = [r.resource_attr_id for r in scenario.resourcescenarios]

        #get all the resource scenarios into a list and bulk insert them
        for i, ra_id in enumerate(resource_attr_ids):
            rs_i = ResourceScenario()
            rs_i.resource_attr_id = ra_id
            rs_i.dataset_id = datasets[i].id
            rs_i.scenario_id = scen.id
            rs_i.dataset = datasets[i]
            scen.resourcescenarios.append(rs_i)

    if scenario.resourcegroupitems is not None:
        #Again doing bulk insert.
        for group_item in scenario.resourcegroupitems:
            group_item_i = ResourceGroupItem()
            group_item_i.scenario_id = scen.id
            group_item_i.group_id = group_item.group_id
            group_item_i.ref_key = group_item.ref_key
            if group_item.ref_key == 'NODE':
                group_item_i.node_id = group_item.ref_id
            elif group_item.ref_key == 'LINK':
                group_item_i.link_id = group_item.ref_id
            elif group_item.ref_key == 'GROUP':
                group_item_i.subgroup_id = group_item.ref_id
            scen.resourcegroupitems.append(group_item_i)
    db.DBSession.add(scen)
    db.DBSession.flush()
    return scen

@required_perms("edit_network", "edit_data")
def update_scenario(scenario, update_data=True, update_groups=True, flush=True, **kwargs):
    """
        Update a single scenario
        as all resources already exist, there is no need to worry
        about negative IDS

        flush = True flushes to the DB at the end of the function.
        flush = False does not flush, assuming that it will happen as part
                of another process, like update_network.
    """
    user_id = kwargs.get('user_id')
    scen = _get_scenario(scenario.id, user_id)

    if scen.locked == 'Y':
        raise PermissionError('Scenario is locked. Unlock before editing.')

    scen.name = scenario.name
    scen.description = scenario.description
    scen.layout = scenario.get_layout()
    scen.start_time = scenario.start_time
    scen.end_time = scenario.end_time
    scen.time_step = scenario.time_step

    if scenario.resourcescenarios == None:
        scenario.resourcescenarios = []
    if scenario.resourcegroupitems == None:
        scenario.resourcegroupitems = []

    #lazy load resourcescenarios from the DB
    scen.resourcescenarios

    #creat a reverse mapping from the resource_attr_id to the RS to avoid querying
    #the table again later.
    #no need to use scenario id in key here cos it's always in the context of one scenario
    rsmap = {}
    for rs in scen.resourcescenarios:
        rsmap[rs.resource_attr_id] = rs

    if update_data is True:
        datasets = [rs.dataset for rs in scenario.resourcescenarios]
        updated_datasets = data._bulk_insert_data(datasets, user_id, kwargs.get('app_name'))
        for i, r_scen in enumerate(scenario.resourcescenarios):
            log.debug("updating resource scenario...")

            rscen_i = rsmap.get(r_scen.resource_attr_id)
            _update_resourcescenario(scen, r_scen, r_scen_i=rscen_i, dataset=updated_datasets[i], user_id=user_id, source=kwargs.get('app_name'))

    log.info('%s Resource Scenarios Updated', len(scenario.resourcescenarios))

    #lazy load resource grou items from the DB
    scen.resourcegroupitems

    if update_groups is True:
        #Get all the exiting resource group items for this scenario.
        #THen process all the items sent to this handler.
        #Any in the DB that are not passed in here are removed.
        for group_item in scenario.resourcegroupitems:
            _add_resourcegroupitem(group_item, scenario.id)

    log.info('Resource Group Items Updated')

    if flush is True:
        db.DBSession.flush()

    log.info('Scenario %s updated', scenario.name)

    return scen

@required_perms("edit_network")
def set_scenario_status(scenario_id, status, **kwargs):
    """
        Set the status of a scenario.
    """

    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    scenario_i.status = status
    db.DBSession.flush()
    return 'OK'

@required_perms("edit_network")
def purge_scenario(scenario_id, **kwargs):
    """
        Set the status of a scenario.
    """

    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    db.DBSession.delete(scenario_i)
    db.DBSession.flush()
    return 'OK'

@required_perms("edit_network")
def create_child_scenario(parent_scenario_id, child_name, **kwargs):
    """
        Create a new scenario which inherits from the given scenario. The new
        scenario contains no resource scenarios or groups. All data is inherited
        from the parent.
    """

    user_id = kwargs.get('user_id')

    scen_i = _get_scenario(parent_scenario_id, user_id)

    log.info("Creating child scenario of %s", scen_i.name)

    existing_scenarios = db.DBSession.query(Scenario).filter(Scenario.network_id==scen_i.network_id).all()
    if child_name is not None:
        for existing_scenario in existing_scenarios:
            if existing_scenario.name == child_name:
                raise HydraError("A scenario with the name {0} already exists in this network. ".format(child_name))
    else:
        child_name = "%s (child)"%(scen_i.name)
        num_child_scenarios = 0
        for existing_scenario in existing_scenarios:
            if existing_scenario.name.find(child_name) >= 0:
                num_child_scenarios = num_child_scenarios + 1

        if num_child_scenarios > 0:
            child_name = child_name + " %s"%(num_child_scenarios)


    child_scen = Scenario()
    child_scen.network_id           = scen_i.network_id
    child_scen.name                 = child_name
    child_scen.description          = scen_i.description
    child_scen.created_by           = kwargs['user_id']
    child_scen.parent_id            = scen_i.id

    child_scen.start_time           = scen_i.start_time
    child_scen.end_time             = scen_i.end_time
    child_scen.time_step            = scen_i.time_step

    db.DBSession.add(child_scen)

    db.DBSession.flush()

    log.info("New scenario created. Scenario ID: %s", child_scen.id)

    child_scenario_j = JSONObject(_get_scenario(child_scen.id, user_id))
    child_scenario_j.resourcescenarios = []
    child_scenario_j.resourcegroupitems =[]
    log.info("Returning child scenario")

    return child_scenario_j

@required_perms("edit_network")
def clone_scenario(scenario_id, retain_results=False, scenario_name=None, **kwargs):
    """
        Create an exact copy of a scenario and place it in the same network
        args:
            scenario_id (int): The scenario ID to clone
            retain_results (bool): Flag to indicated whether resource scenarios connected to resource attribtues which have an 'attr_is_var' are copied or not. Defaults to False, so results are not retained by default.
            scenario_name (string): The name of the new scenario. If None, the existing scenario's name is used, appended with '(clone'). Multiple clones of the same network result in  "... (clone) 1", "... (clone) 2" etc,
    """

    user_id = kwargs.get('user_id')

    scen_i = _get_scenario(scenario_id, user_id)

    log.info("cloning scenario %s", scen_i.name)


    if scenario_name is None:
        existing_scenarios = db.DBSession.query(Scenario).filter(Scenario.network_id==scen_i.network_id).all()
        num_cloned_scenarios = 0
        for existing_sceanrio in existing_scenarios:
            if existing_sceanrio.name.find('clone') >= 0:
                num_cloned_scenarios = num_cloned_scenarios + 1

        cloned_name = "%s (clone)"%(scen_i.name)
        if num_cloned_scenarios > 0:
            cloned_name = cloned_name + " %s"%(num_cloned_scenarios)
    else:
        cloned_name = scenario_name

        #check this scenario name is available
        existing_scenario_with_name = db.DBSession.query(Scenario).filter(
            Scenario.network_id==scen_i.network_id, Scenario.name==scenario_name).first()

        if existing_scenario_with_name is not None:
            netname = existing_scenario_with_name.network.name
            raise HydraError(f"A scenario with name {scenario_name} already exists in network {netname}")

    log.info("Cloned scenario name is %s", cloned_name)

    cloned_scen = Scenario()
    cloned_scen.network_id           = scen_i.network_id
    cloned_scen.name                 = cloned_name
    cloned_scen.description          = scen_i.description
    cloned_scen.created_by           = kwargs['user_id']
    cloned_scen.parent_id            = scen_i.parent_id

    cloned_scen.start_time           = scen_i.start_time
    cloned_scen.end_time             = scen_i.end_time
    cloned_scen.time_step            = scen_i.time_step

    db.DBSession.add(cloned_scen)

    db.DBSession.flush()

    cloned_scenario_id = cloned_scen.id
    log.info("New scenario created. Scenario ID: %s", cloned_scenario_id)


    log.info("Getting in resource scenarios to clone from scenario %s", scenario_id)
    if retain_results is False:
        old_rscen_rs = db.DBSession.query(ResourceScenario).filter(
            ResourceScenario.scenario_id == scenario_id,
            ResourceAttr.id == ResourceScenario.resource_attr_id,
            ResourceAttr.attr_is_var == 'N'
        ).all()
    else:
        old_rscen_rs = db.DBSession.query(ResourceScenario).filter(
            ResourceScenario.scenario_id == scenario_id
        ).all()

    new_rscens = []
    for old_rscen in old_rscen_rs:
        new_rscens.append(dict(
            dataset_id=old_rscen.dataset_id,
            scenario_id=cloned_scenario_id,
            resource_attr_id=old_rscen.resource_attr_id,
            source = kwargs.get('app_name', old_rscen.source)
        ))

    if len(new_rscens) > 0:
        db.DBSession.execute(ResourceScenario.__table__.insert(), new_rscens)

    log.info("ResourceScenarios cloned")

    log.info("Getting old resource group items for scenario %s", scenario_id)
    old_rgis = db.DBSession.query(ResourceGroupItem).filter(
        ResourceGroupItem.scenario_id == scenario_id).all()
    new_rgis = []
    for old_rgi in old_rgis:
        new_rgis.append(dict(
            ref_key=old_rgi.ref_key,
            node_id=old_rgi.node_id,
            link_id=old_rgi.link_id,
            subgroup_id=old_rgi.subgroup_id,
            group_id=old_rgi.group_id,
            scenario_id=cloned_scenario_id,
        ))
    if len(new_rgis) > 0:
        db.DBSession.execute(ResourceGroupItem.__table__.insert(), new_rgis)

    log.info("Cloning finished.")

    log.info("Retrieving cloned scenario")
    new_rscen_rs = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.scenario_id == cloned_scenario_id).all()
    new_rgis_rs  = db.DBSession.query(ResourceGroupItem).filter(
        ResourceGroupItem.scenario_id == cloned_scenario_id).all()

    cloned_scen = JSONObject(_get_scenario(cloned_scenario_id, user_id))
    cloned_scen.resourcescenarios = [JSONObject(r) for r in new_rscen_rs]
    cloned_scen.resourcegroupitems =[JSONObject(r) for r in new_rgis_rs]
    log.info("Returning cloned scenario")


    return cloned_scen

def _get_dataset_as_dict(rs, user_id):
    if rs.dataset is None:
        return None

    dataset = JSONDataset(rs.dataset)

    dataset['metadata'] = {}

    try:
        rs.dataset.check_read_permission(user_id)
    except PermissionError:
           dataset['value']      = None
           dataset['metadata'] = {}

    return dataset

def _get_scenario_network_resources(scenario_id, scenario_i=None):
    """
        Inspect the network of the given scenario and return a mapping of ID to Name
        for each element in that network, categorised by resource type.
    """
    if scenario_i is None:
        scenario_i = db.DBSession.query(Scenario).filter(Scenario.id==scenario_id).one()

    net = db.DBSession.query(Network.id, Network.name).filter(Network.id == scenario_i.network_id).all()
    nodes = db.DBSession.query(Node.id, Node.name).filter(Node.network_id == scenario_i.network_id).all()
    links = db.DBSession.query(Link.id, Link.name).filter(Link.network_id == scenario_i.network_id).all()
    groups = db.DBSession.query(ResourceGroup.id, ResourceGroup.name).filter(ResourceGroup.network_id == scenario_i.network_id).all()

    mapping_dict = {}

    mapping_dict['NETWORK'] = dict(net)
    mapping_dict['NODE'] = dict(nodes)
    mapping_dict['LINK'] = dict(links)
    mapping_dict['GROUP'] = dict(groups)

    return mapping_dict

def _get_resource_id(resourceattr):
    for col in ('node_id', 'link_id', 'group_id', 'network_id'):
        if resourceattr.get(col) is not None:
            return resourceattr.get(col)

@required_perms("get_network")
def compare_scenarios(scenario_id_1, scenario_id_2, allow_different_networks=False, include_results=False, **kwargs):
    """
        Compare two scenarios and return a 'diff' dictionary containing all the differences.
        Args:
            scenario_1_id (int): Scenario 1 ID
            scenario_2_id (int): Scenario 2 ID
            allow_different_networks (bool) (default False); Flag to indicate whether it should be allowed to compare the scenarios from two independent networks.
            include_results (bool) (default False): If set to true, includes all 'attr_is_var' values. Otherwise it only compares inputs.
        returns:
            dict: Containing the differences
        raises:
            HydraError if the scenarios are not in the same network (and the allow_different_networks is false)
    """
    user_id = kwargs.get('user_id')

    scenario_1 = get_scenario(scenario_id_1,
                              include_parent_data=True,
                              include_results=include_results,
                              include_attr=True,
                              include_metadata=False,
                              user_id=user_id)

    scenario_2 = get_scenario(scenario_id_2,
                              include_parent_data=True,
                              include_results=include_results,
                              include_attr=True,
                              include_metadata=False,
                              user_id=user_id)

    if allow_different_networks is False and scenario_1.network_id != scenario_2.network_id:
        raise HydraError("Cannot compare scenarios that are not"
                         " in the same network!")

    #find a mapping from ID to Name for all nodes / links / groups in scenario 1's network
    s1_resource_mapping = _get_scenario_network_resources(scenario_1.id, scenario_1)
    if scenario_1.network_id == scenario_2.network_id:
        s2_resource_mapping = s1_resource_mapping
    else:
        s2_resource_mapping = _get_scenario_network_resources(scenario_2.id, scenario_2)


    scenario_1_rs = scenario_1.resourcescenarios
    scenario_2_rs = scenario_2.resourcescenarios

    scenario_1_rgi = scenario_1.resourcegroupitems
    scenario_2_rgi = scenario_2.resourcegroupitems


    scenariodiff = dict(
       object_type = 'ScenarioDiff'
    )
    resource_diffs = []

    #Make a list of all the resource scenarios (aka data) that are unique
    #to scenario 1 and that are in both scenarios, but are not the same.

    #For efficiency, build a dictionary of the data in scenarios and refer
    #them rather than nesting for loops.
    r_scen_1_dict = dict()
    r_scen_2_dict = dict()
    for s1_rs in scenario_1_rs:
        r_scen_1_dict[s1_rs.resource_attr_id] = s1_rs
    for s2_rs in scenario_2_rs:
        r_scen_2_dict[s2_rs.resource_attr_id] = s2_rs

    rscen_1_dataset_ids = set([r_scen.dataset_id for r_scen in scenario_1_rs])
    rscen_2_dataset_ids = set([r_scen.dataset_id for r_scen in scenario_2_rs])

    log.info("Datasets In 1 not in 2: %s"%(len(rscen_1_dataset_ids - rscen_2_dataset_ids)))
    log.info("Datasets In 2 not in 1: %s"%(len(rscen_2_dataset_ids - rscen_1_dataset_ids)))

    for ra_id, s1_rs in r_scen_1_dict.items():
        s2_rs = r_scen_2_dict.get(ra_id)
        if s2_rs is not None:
            log.debug("Is %s == %s?"%(s1_rs.dataset_id, s2_rs.dataset_id))
            if s1_rs.dataset_id != s2_rs.dataset_id:
                resource_diff = dict(
                    resource_attr_id = s1_rs.resource_attr_id,
                    scenario_1_dataset = s1_rs.dataset,
                    scenario_2_dataset = s2_rs.dataset,
                    attr_name = s1_rs.resourceattr.attr.name,
                    resource_name = s1_resource_mapping[s1_rs.resourceattr.ref_key][_get_resource_id(s1_rs.resourceattr)],
                )
                resource_diffs.append(resource_diff)

            continue
        else:
            #this is unique in scenario 1
            resource_diff = dict(
                resource_attr_id = s1_rs.resource_attr_id,
                scenario_1_dataset = s1_rs.dataset,
                scenario_2_dataset = None,
                attr_name = s1_rs.resourceattr.attr.name,
                resource_name = s1_resource_mapping[s1_rs.resourceattr.ref_key][_get_resource_id(s1_rs.resourceattr)],
            )
            resource_diffs.append(resource_diff)

    #make a list of all the resource scenarios (aka data) that are unique
    #in scenario 2.
    for ra_id, s2_rs in r_scen_2_dict.items():
        s1_rs = r_scen_1_dict.get(ra_id)
        if s1_rs is None:
            resource_diff = dict(
                resource_attr_id = ra_id,
                scenario_1_dataset = None,
                scenario_2_dataset = s2_rs.dataset,
                attr_name = s2_rs.resourceattr.attr.name,
                resource_name = s2_resource_mapping[s2_rs.resourceattr.ref_key][_get_resource_id(s2_rs.resourceattr)],
            )
            resource_diffs.append(resource_diff)

    scenariodiff['resourcescenarios'] = resource_diffs

    #Now compare groups.
    #Return list of group items in scenario 1 not in scenario 2 and vice versa
    s1_items = []
    for s1_item in scenario_1_rgi:
        s1_items.append((s1_item.group_id, s1_item.ref_key, s1_item.node_id, s1_item.link_id, s1_item.subgroup_id))
    s2_items = []
    for s2_item in scenario_2_rgi:
        s2_items.append((s2_item.group_id, s2_item.ref_key, s2_item.node_id, s2_item.link_id, s2_item.subgroup_id))

    groupdiff = dict()
    scenario_1_items = []
    scenario_2_items = []
    for s1_only_item in set(s1_items) - set(s2_items):

        item = ResourceGroupItem(
            group_id = s1_only_item[0],
            ref_key  = s1_only_item[1],
            node_id   = s1_only_item[2],
            link_id   = s1_only_item[3],
            subgroup_id   = s1_only_item[4],
        )
        scenario_1_items.append(item)
    for s2_only_item in set(s2_items) - set(s1_items):
        item = ResourceGroupItem(
            group_id = s2_only_item[0],
            ref_key  = s2_only_item[1],
            node_id   = s2_only_item[2],
            link_id   = s2_only_item[3],
            subgroup_id   = s2_only_item[4],
        )
        scenario_2_items.append(item)

    groupdiff['scenario_1_items'] = scenario_1_items
    groupdiff['scenario_2_items'] = scenario_2_items
    scenariodiff['groups'] = groupdiff

    return scenariodiff

@required_perms("get_data")
def get_resource_scenario(resource_attr_id, scenario_id, get_parent_data=False, **kwargs):
    """
        Get the resource scenario object for a given resource atttribute and scenario.
        This is done when you know the attribute, resource and scenario and want to get the
        value associated with it.

        The get_parent_data flag indicates whether we should look only at this scenario, or if
        the resource scenario does not exist on this scenario to look in its parent.
    """
    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_rs = scenario_i.get_data(user_id, get_parent_data=get_parent_data, ra_ids=[resource_attr_id])

    for rs_i in scenario_rs:
        if rs_i.resource_attr_id == resource_attr_id:
            return rs_i
    else:
        raise ResourceNotFoundError("resource scenario for %s not found in scenario %s"%(resource_attr_id, scenario_id))

@required_perms("get_data")
def get_resourceattr_data(resource_attr_ids, scenario_id, get_parent_data=False, **kwargs):
    """
        Get the resource scenarios associated to a list of resource attribute IDs
    """
    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    if not isinstance(resource_attr_ids, list):
        resource_attr_ids = [resource_attr_ids]

    scenario_rs = scenario_i.get_data(user_id, get_parent_data=get_parent_data, ra_ids=resource_attr_ids)


    resource_scenario_dict = {}

    for rs_i in scenario_rs:
        if rs_i.resource_attr_id in resource_attr_ids:
            rs_i.dataset
            rs_i.dataset.metadata
            resource_scenario_dict[rs_i.resource_attr_id] = rs_i

    return resource_scenario_dict

@required_perms("edit_network")
def lock_scenario(scenario_id, **kwargs):
    """
        Unlock a scenario
    """
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')

    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_i.network.check_write_permission(user_id)

    scenario_i.locked = 'Y'

    db.DBSession.flush()
    return 'OK'

@required_perms("edit_network")
def unlock_scenario(scenario_id, **kwargs):
    """
        Unlock a scenario
    """
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')
    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_i.network.check_write_permission(user_id)

    scenario_i.locked = 'N'

    db.DBSession.flush()
    return 'OK'

@required_perms("get_data")
def get_dataset_scenarios(dataset_id, **kwargs):

    try:
        db.DBSession.query(Dataset).filter(Dataset.id == dataset_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Dataset %s not found"%dataset_id)

    log.info("dataset %s exists", dataset_id)

    scenarios = db.DBSession.query(Scenario).filter(
        Scenario.status == 'A',
        ResourceScenario.scenario_id == Scenario.id,
        ResourceScenario.dataset_id == dataset_id).distinct().all()

    log.info("%s scenarios retrieved", len(scenarios))

    return scenarios

@required_perms("edit_data", "edit_network")
def bulk_update_resourcedata(scenario_ids, resource_scenarios, **kwargs):
    """
        Update the data associated with a list of scenarios.
    """
    user_id = kwargs.get('user_id')
    res = None

    res = {}

    net_ids = db.DBSession.query(Scenario.network_id).filter(Scenario.id.in_(scenario_ids)).all()

    if len(set(net_ids)) != 1:
        raise HydraError("Scenario IDS are not in the same network")

    for scenario_id in scenario_ids:

        scen_i = _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

        #this is cast as a string so it can be read into a JSONObject
        res[str(scenario_id)] = []

        #make a lookup dict of all the resource scenarios that already exist from the
        #ones that have been passed in to avoid querying for every one individually.

        ra_ids = [rs.resource_attr_id for rs in resource_scenarios]
        r_scens_i = db.DBSession.query(ResourceScenario).filter(
                ResourceScenario.scenario_id == scenario_id,
                ResourceScenario.resource_attr_id.in_(ra_ids)).all()
        r_scen_dict = dict((rs.resource_attr_id, rs) for rs in r_scens_i)
        for rs in resource_scenarios:
            if rs.dataset is not None:
                updated_rs = _update_resourcescenario(scen_i,
                                                      rs,
                                                      r_scen_i=r_scen_dict.get(rs.resource_attr_id),
                                                      user_id=user_id,
                                                      source=kwargs.get('app_name'))
                #this is cast as a string so it can be read into a JSONObject
                res[str(scenario_id)].append(updated_rs)
            else:
                _delete_resourcescenario(scenario_id, rs.resource_attr_id)

        db.DBSession.flush()

    return res

@required_perms("edit_data", "edit_network")
def update_resourcedata(scenario_id, resource_scenarios, **kwargs):
    """
        Update the data associated with a scenario.
        Data missing from the resource scenario will not be removed
        from the scenario. Use the remove_resourcedata for this task.

        If the resource scenario does not exist, it will be created.
        If the value of the resource scenario is specified as being None, the
        resource scenario will be deleted.
        If the value of the resource scenario does not exist, it will be created.
        If the both the resource scenario and value already exist, the resource scenario
        will be updated with the ID of the dataset.

        If the dataset being set is being changed, already exists,
        and is only used by a single resource scenario,
        then the dataset itself is updated, rather than a new one being created.
    """
    user_id = kwargs.get('user_id')
    res = None

    scenario_i = _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    res = []
    for rs in resource_scenarios:
        if rs.dataset is not None:
            updated_rs = _update_resourcescenario(scenario_i, rs, user_id=user_id, source=kwargs.get('app_name'))
            res.append(updated_rs)
        else:
            _delete_resourcescenario(scenario_id, rs.resource_attr_id)

    db.DBSession.flush()

    return res

@required_perms("edit_data", "edit_network")
def delete_scenario_results(scenario_id, **kwargs):
    """
        Delete all the resource scenarios in a scenario which are linked to
        resource attributes that have 'attr_is_var' set to 'Y'
    """
    user_id = kwargs.get('user_id')

    _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    results_rs = db.DBSession.query(ResourceScenario)\
        .join(ResourceAttr)\
        .filter(ResourceScenario.scenario_id == scenario_id)\
        .filter(ResourceAttr.attr_is_var == 'Y').all()
    for rs in results_rs:
        db.DBSession.delete(rs)

    db.DBSession.flush()

    log.info("%s resource scenarios deleted", len(results_rs))

@required_perms("edit_data", "edit_network")
def delete_resource_scenario(scenario_id, resource_attr_id, quiet=False, **kwargs):
    """
        Remove the data associated with a resource in a scenario.
    """
    user_id = kwargs.get('user_id')

    _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    _delete_resourcescenario(scenario_id, resource_attr_id, suppress_error=quiet)

@required_perms("edit_data", "edit_network")
def delete_resource_scenarios(scenario_id, resource_attr_ids, quiet=False, **kwargs):
    """
        Remove the data associated with a list of resources in a scenario.
    """
    user_id = kwargs.get('user_id')

    log.info("Deleting %s resource scenarios from from scenario %s", len(resource_attr_ids), scenario_id)
    _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    for resource_attr_id in resource_attr_ids:
        _delete_resourcescenario(scenario_id, resource_attr_id, suppress_error=quiet)

@required_perms("edit_data", "edit_network")
def delete_resourcedata(scenario_id, resource_scenario, quiet = False, **kwargs):
    """
        Remove the data associated with a resource in a scenario.
        The 'quiet' parameter indicates whether an non-existent RS should throw
        an error.
    """
    user_id = kwargs.get('user_id')

    _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    _delete_resourcescenario(scenario_id, resource_scenario.resource_attr_id, suppress_error=quiet)


def _delete_resourcescenario(scenario_id, resource_attr_id, suppress_error=False):

    log.debug("Deleting resource scenario for RA %s from scenario %s", resource_attr_id, scenario_id)

    try:
        sd_i = db.DBSession.query(ResourceScenario).filter(
            ResourceScenario.scenario_id == scenario_id,
            ResourceScenario.resource_attr_id == resource_attr_id).one()
    except NoResultFound:
        if suppress_error is False:
            raise HydraError("ResourceAttr %s does not exist in scenario %s."%(resource_attr_id, scenario_id))
        return

    db.DBSession.delete(sd_i)
    db.DBSession.flush()

def _update_resourcescenario(scenario, resource_scenario, r_scen_i=None, dataset=None, new=False, user_id=None, source=None):
    """
        Insert or Update the value of a resource's attribute by first getting the
        resource, then parsing the input data, then assigning the value.

        returns a ResourceScenario object.
    """
    if scenario is None:
        scenario = db.DBSession.query(Scenario).filter(Scenario.id == 1).one()

    ra_id = resource_scenario.resource_attr_id

    log.debug("Assigning resource attribute: %s",ra_id)
    #count the number of new RS to report it in the logs
    new_rs = 0
    if r_scen_i is None:
        try:
            r_scen_i = db.DBSession.query(ResourceScenario).filter(
                ResourceScenario.scenario_id == scenario.id,
                ResourceScenario.resource_attr_id == resource_scenario.resource_attr_id).one()
        except NoResultFound as e:
            log.debug("Creating new RS for RS %s in scenario %s", resource_scenario.resource_attr_id, scenario.id)
            r_scen_i = ResourceScenario()
            r_scen_i.resource_attr_id = resource_scenario.resource_attr_id
            r_scen_i.scenario_id = scenario.id
            r_scen_i.scenario = scenario
            new_rs = new_rs + 1

            db.DBSession.add(r_scen_i)

    if scenario.locked == 'Y':
        log.info("Scenario %s is locked", scenario.id)
        return r_scen_i


    if dataset is not None:
        r_scen_i.dataset = dataset

        return r_scen_i

    dataset = resource_scenario.dataset

    dataset_j = JSONDataset(dataset)

    value = dataset_j.parse_value()

    log.debug("Assigning %s to resource attribute: %s", value, ra_id)

    if value is None:
        log.info("Cannot set data on resource attribute %s", ra_id)
        return None

    metadata = dataset_j.get_metadata_as_dict(source=source, user_id=user_id)
    data_unit_id = dataset_j.unit_id

    data_hash = dataset_j.get_hash(value, metadata)

    new_rscen_i = assign_value(r_scen_i,
                 dataset_j.type.lower(),
                 value,
                 data_unit_id,
                 dataset_j.name,
                 metadata=metadata,
                 data_hash=data_hash,
                 user_id=user_id,
                 source=source)

    return new_rscen_i

@required_perms("edit_data", "edit_network")
def assign_value(rs, data_type, val,
                 unit_id, name, metadata={}, data_hash=None, user_id=None, source=None):
    """
        Insert or update a piece of data in a scenario.
        If the dataset is being shared by other resource scenarios, a new dataset is inserted.
        If the dataset is ONLY being used by the resource scenario in question, the dataset
        is updated to avoid unnecessary duplication.
    """

    log.debug("Assigning value %s to rs %s in scenario %s",
              name, rs.resource_attr_id, rs.scenario_id)

    if rs.scenario.locked == 'Y':
        raise PermissionError("Cannot assign value. Scenario %s is locked"
                              %(rs.scenario_id))

    #Check if this RS is the only RS in the DB connected to this dataset.
    #If no results is found, the RS isn't in the DB yet, so the condition is false.
    update_dataset = False # Default behaviour is to create a new dataset.

    if rs.dataset is not None:

        #Has this dataset changed?
        if rs.dataset.hash == data_hash:
            log.info("Dataset has not changed. Returning.")
            return rs

        connected_rs = db.DBSession.query(ResourceScenario).filter(ResourceScenario.dataset_id == rs.dataset.id).all()
        #If there's no RS found, then the incoming rs is new, so the dataset can be altered
        #without fear of affecting something else.
        if len(connected_rs) == 0:
        #If it's 1, the RS exists in the DB, but it's the only one using this dataset or
        #The RS isn't in the DB yet and the datset is being used by 1 other RS.
            update_dataset = True

        if len(connected_rs) == 1:
            if connected_rs[0].scenario_id == rs.scenario_id and connected_rs[0].resource_attr_id == rs.resource_attr_id:
                update_dataset = True
        else:
            update_dataset = False

    if update_dataset is True:
        log.info("Updating dataset '%s'", name)
        dataset = data.update_dataset(rs.dataset.id, name, data_type, val, unit_id, metadata, flush=False, **dict(user_id=user_id))
        log.info("Updated dataset '%s'", name)
        rs.dataset = dataset
        rs.dataset_id = dataset.id
        log.info("Set RS dataset id to %s"%dataset.id)
    else:
        log.info("Creating new dataset %s in scenario %s", name, rs.scenario_id)
        dataset = data.add_dataset(
            data_type,
            val,
            unit_id,
            metadata=metadata,
            name=name,
            **dict(user_id=user_id)
        )
        rs.dataset = dataset
        rs.source = source


    db.DBSession.flush()

    return rs

@required_perms("edit_data", "edit_network")
def add_data_to_attribute(scenario_id, resource_attr_id, dataset,**kwargs):
    """
        Add data to a resource scenario outside of a network update
    """
    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id, check_write=True, check_can_edit=True)

    try:
        r_scen_i = db.DBSession.query(ResourceScenario).filter(
            ResourceScenario.scenario_id == scenario_id,
            ResourceScenario.resource_attr_id == resource_attr_id).one()
        log.info("Existing resource scenario found for %s in scenario %s", resource_attr_id, scenario_id)
    except NoResultFound:
        log.info("No existing resource scenarios found for %s in scenario %s. Adding a new one.", resource_attr_id, scenario_id)
        r_scen_i = ResourceScenario()
        r_scen_i.scenario_id      = scenario_id
        r_scen_i.resource_attr_id = resource_attr_id
        scenario_i.resourcescenarios.append(r_scen_i)

    data_type = dataset.type.lower()

    dataset_j = JSONDataset(dataset)
    value = dataset_j.parse_value()

    dataset_metadata = dataset_j.get_metadata_as_dict(user_id=kwargs.get('user_id'),
                                                      source=kwargs.get('source'))
    if value is None:
        raise HydraError(f"Cannot set value to attribute. No value was sent with dataset {dataset_j.id}")

    data_hash = dataset_j.get_hash(value, dataset_metadata)

    new_rscen_i = assign_value(r_scen_i, data_type, value, dataset_j.unit_id, dataset_j.name,
                          metadata=dataset_metadata, data_hash=data_hash, user_id=user_id)

    db.DBSession.flush()

    return new_rscen_i

@required_perms("get_data", "get_network")
def get_scenario_data(scenario_id, get_parent_data=False, **kwargs):
    """
        Get all the datasets from the scenario with the specified ID
        @returns a list of dictionaries
    """
    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_rs = scenario_i.get_data(user_id, get_parent_data=get_parent_data)

    dataset_ids = []
    datasets = []
    for rs in scenario_rs:
        if rs.dataset.id in dataset_ids:
            continue

        if rs.dataset.hidden == 'Y':
            try:
                rs.dataset.check_read_permission(user_id)
            except:
                rs.dataset.value = None
                rs.dataset.metadata = []

        datasets.append(rs.dataset)
        dataset_ids.append(rs.dataset.id)

    log.info("Retrieved %s datasets", len(datasets))
    return datasets

@required_perms("get_data", "get_network")
def get_attribute_data(attr_ids, node_ids, **kwargs):
    """
        For a given attribute or set of attributes, return  all the resources and
        resource scenarios in the network
    """
    node_attrs = db.DBSession.query(ResourceAttr).\
        options(joinedload(ResourceAttr.attr)).\
        filter(ResourceAttr.node_id.in_(node_ids),
               ResourceAttr.attr_id.in_(attr_ids)).all()

    ra_ids = []
    for ra in node_attrs:
        ra_ids.append(ra.id)


    resource_scenarios = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.resource_attr_id.in_(ra_ids)).options(
            joinedload(ResourceScenario.resourceattr)).options(
                joinedload(ResourceScenario.dataset).joinedload(Dataset.metadata)
            ).order_by(ResourceScenario.scenario_id).all()


    for rs in resource_scenarios:
        if rs.dataset.hidden == 'Y':
            try:
                rs.dataset.check_read_permission(kwargs.get('user_id'))
            except:
                rs.dataset.value = None
        db.DBSession.expunge(rs)

    return node_attrs, resource_scenarios

def _get_all_network_resource_attributes(network_id):
    """
        Get all the attributes for the nodes, links and groups of a network.
        Return these attributes as a dictionary, keyed on type (NODE, LINK, GROUP)
        then by ID of the node or link.

        args:
            network_id (int) The ID of the network from which to retrieve the attributes
        returns:
            A list of sqlalchemy result proxy objects
    """
    base_qry = db.DBSession.query(ResourceAttr).filter(Attr.id==ResourceAttr.attr_id)

    all_node_attribute_qry = base_qry.join(Node).filter(Node.network_id == network_id)

    all_link_attribute_qry = base_qry.join(Link).filter(Link.network_id == network_id)

    all_group_attribute_qry = base_qry.join(ResourceGroup)\
            .filter(ResourceGroup.network_id == network_id)

    network_attribute_qry = base_qry.filter(ResourceAttr.network_id == network_id)

    logging.info("Getting all attributes using execute")
    attribute_qry = all_node_attribute_qry.union(all_link_attribute_qry,
                                                 all_group_attribute_qry,
                                                 network_attribute_qry)
    all_resource_attributes = attribute_qry.all()

    return all_resource_attributes

@required_perms("get_data", "get_network")
def get_resource_data(ref_key,
                      ref_id,
                      scenario_id,
                      type_id=None,
                      expunge_session=True,
                      get_parent_data=False,
                      include_inputs=True,
                      include_outputs=True,
                      include_data_types=None,
                      exclude_data_types=None,
                      include_values=True,
                      include_data_type_values=None,
                      exclude_data_type_values=None,
                      **kwargs):
    """
        Get all the resource scenarios for a given resource
        in a given scenario. If type_id is specified, only
        return the resource scenarios for the attributes
        within the type.
        args:
            ref_key (string): 'NETWORK', 'NODE', 'LINK', 'GROUP'
            ref_id (int): The ID of the network / node / link / group
            scenario_id (int): The ID of the scenario from which to get the resource scenarios
            type_id (int): A filter which limits the resource scenarios to just the attributes defined by the resource type
            expunge_session (bool): Expunge the DB session -- means that modifying the results will not update the database. Default True
            get_parent_data (bool): Return the data of the parent scenario of the requestsed scenario in addition to the specified scenario. Default False.
            include_inputs (bool) : Return resource scenarios which relate to resource attributes where the attr_is_var=N. Default True
            include_outputs (bool): Return resource scenarios which relate to resource attributes where the attr_is_var=Y. Default True
            include_data_types (list(string)): Return only resource scenarios with a dataset that has the type of one of these specified data types. Default None, meaning no filter is applied.
            exclude_data_types (list(string)): Return resource scenarios with a dataset that do NOT have the type of one of these specified data types. Default None, meaning no filter is applied.
            include_values (bool): Return the 'value' column of tDataset. Default True. Setting this to False can increase performance substantially due to the size of some dataset values.
            include_data_type_values (list(string)): When include_values is True, specify which dataset types should return with the value column included.
            exclude_data_type_values (list(string)): When include_values is True, specify which dataset types should return with the value column NOT included.

        returns:
            A list of JSONObjects representing Resource Scenarios, with a 'dataset' attribute and 'attribute' attribute.
    """

    user_id = kwargs.get('user_id')

    if ref_key is not None and ref_id is not None:
        resource_i = get_resource(ref_key, ref_id)
        resource_attributes = resource_i.attributes
    elif ref_key is None and ref_id is None:
        scenario = _get_scenario(scenario_id, user_id)
        resource_attributes = _get_all_network_resource_attributes(scenario.network_id)
    elif None in (ref_key, ref_id): # One of them is None
        raise HydraError("Unable to get data. Must specify a resource type (ref_key) and resource id (ref_id)")


    if include_inputs is False or include_outputs is False:
        if include_inputs is False:
            #only include outputs
            resource_attributes = list(filter(lambda x:x.attr_is_var=='Y', resource_attributes))

        if include_outputs is False:
            #only include inputs
            resource_attributes = list(filter(lambda x:x.attr_is_var=='N', resource_attributes))

    ra_ids = [ra.id for ra in resource_attributes]

    ra_map = dict((ra.id, ra) for ra in resource_attributes)

    scenario_i = _get_scenario(scenario_id, user_id)

    requested_rs = scenario_i.get_data(user_id,
                                       get_parent_data=get_parent_data,
                                       ra_ids=ra_ids,
                                       include_data_types=include_data_types,
                                       exclude_data_types=exclude_data_types,
                                       include_values=include_values,
                                       include_data_type_values=include_data_type_values,
                                       exclude_data_type_values=exclude_data_type_values)

    #map an raID to an rs for uses later
    ra_rs_map = {}
    for rs in requested_rs:
        ra_rs_map[rs.resource_attr_id] = rs

    #make a lookup table between an attr ID and an RS for filtering by types later.
    attr_rs_lookup = {} # Used later to remove results if they're not required
    for ra in resource_attributes:
        #Is there data for this RA?
        if ra_rs_map.get(ra.id) is not None:
            attr_rs_lookup[ra.attr_id] = ra_rs_map[ra.id]

    #Remove RS that are not defined by the specified type.
    if type_id is not None:

        type_limited_rs = []

        rs = db.DBSession.query(TypeAttr).filter(TypeAttr.type_id == type_id).all()
        for r in rs:
            type_limited_rs.append(attr_rs_lookup[r.attr_id])

        requested_rs = type_limited_rs

    if expunge_session is True:
        db.DBSession.expunge_all()

    return requested_rs

@required_perms("get_data", "get_network")
def get_attribute_datasets(attr_id, scenario_id, get_parent_data=False, **kwargs):
    """
        Retrieve all the datasets in a scenario for a given attribute.
        Also return the resource attributes so there is a reference to the node/link
    """

    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    try:
        a = db.DBSession.query(Attr).filter(Attr.id == attr_id).one()
    except NoResultFound:
        raise HydraError("Attribute %s not found"%(attr_id,))

    scenario_rs_i = scenario_i.get_data(user_id, get_parent_data=get_parent_data)

    #just in case the calling funciton hasn't cast this as as int
    attr_id = int(attr_id)

    requested_rs = []
    for rs_i in scenario_rs_i:
        if rs_i.resourceattr.attr_id == attr_id:
            #Finally add it to the list of RS to return
            requested_rs.append(rs_i)

    json_rs = []
    #Load the metadata too
    for rs in requested_rs:
        tmp_rs = JSONObject(rs)
        tmp_rs.resourceattr = JSONObject(rs.resourceattr)
        ra = tmp_rs.resourceattr
        if rs.resourceattr.node_id is not None:
            tmp_rs.resourceattr.node = get_resource(ra.ref_key, ra.node_id)
        elif rs.resourceattr.link_id is not None:
            tmp_rs.resourceattr.link = get_resource(ra.ref_key, ra.link_id)
        elif rs.resourceattr.group_id is not None:
            tmp_rs.resourceattr.resourcegroup = get_resource(ra.ref_key, ra.group_id)
        elif rs.resourceattr.network_id is not None:
            tmp_rs.resourceattr.network = get_resource(ra.ref_key, ra.network_id)

        json_rs.append(tmp_rs)


    return json_rs

@required_perms("get_data", "get_network")
def get_resourcegroupitems(group_id, scenario_id, get_parent_items=False, **kwargs):

    """
        Get all the items in a group, in a scenario. If group_id is None, return
        all items across all groups in the scenario.
    """

    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    requested_items = scenario_i.get_group_items(get_parent_items=get_parent_items)

    if group_id is not None:
        tmp_items = []
        for item_i in requested_items:
            if item_i.group_id == group_id:
                tmp_items.append(item_i)
        requested_items = tmp_items

    return requested_items

@required_perms("edit_network")
def delete_resourcegroupitems(scenario_id, item_ids, **kwargs):
    """
        Delete specified items in a group, in a scenario.
    """
    user_id = int(kwargs.get('user_id'))
    #check the scenario exists
    _get_scenario(scenario_id, user_id)
    for item_id in item_ids:
        rgi = db.DBSession.query(ResourceGroupItem).\
                filter(ResourceGroupItem.id == item_id).one()
        db.DBSession.delete(rgi)

    db.DBSession.flush()

@required_perms("edit_network")
def empty_group(group_id, scenario_id, **kwargs):
    """
        Delete all itemas in a group, in a scenario.
    """
    user_id = int(kwargs.get('user_id'))
    #check the scenario exists
    _get_scenario(scenario_id, user_id)

    rgi = db.DBSession.query(ResourceGroupItem).\
            filter(ResourceGroupItem.group_id == group_id).\
            filter(ResourceGroupItem.scenario_id == scenario_id).all()
    rgi.delete()

@required_perms("edit_network")
def add_resourcegroupitems(scenario_id, items, scenario=None, **kwargs):

    """
        Get all the items in a group, in a scenario.
    """
    user_id = int(kwargs.get('user_id'))

    if scenario is None:
        scenario = _get_scenario(scenario_id, user_id)

    _check_network_ownership(scenario.network_id, user_id)

    newitems = []
    for group_item in items:
        group_item_i = _add_resourcegroupitem(group_item, scenario.id)
        newitems.append(group_item_i)

    db.DBSession.flush()

    return newitems

def _add_resourcegroupitem(group_item, scenario_id):
    """
        Add a single resource group item (no DB flush, as it's an internal function)
    """
    if group_item.id and group_item.id > 0:
        try:
            group_item_i = db.DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.id == group_item.id).one()
        except NoResultFound:
            raise ResourceNotFoundError("ResourceGroupItem %s not found" % (group_item.id))

    else:
        group_item_i = ResourceGroupItem()
        group_item_i.group_id = group_item.group_id
        if scenario_id is not None:
            group_item_i.scenario_id = scenario_id

        db.DBSession.add(group_item_i)

    ref_key = group_item.ref_key
    group_item_i.ref_key = ref_key
    if ref_key == 'NODE':
        group_item_i.node_id = group_item.ref_id if group_item.ref_id else group_item.node_id
    elif ref_key == 'LINK':
        group_item_i.link_id = group_item.ref_id if group_item.ref_id else group_item.link_id
    elif ref_key == 'GROUP':
        group_item_i.subgroup_id = group_item.ref_id if group_item.ref_id else group_item.subgroup_id

    return group_item_i

@required_perms("edit_data", "edit_network")
def update_value_from_mapping(source_resource_attr_id, target_resource_attr_id, source_scenario_id, target_scenario_id, **kwargs):
    """
        Using a resource attribute mapping, take the value from the source and apply
        it to the target. Both source and target scenarios must be specified (and therefor
        must exist).
    """
    user_id = int(kwargs.get('user_id'))

    rm = aliased(ResourceAttrMap, name='rm')
    #Check the mapping exists.
    mapping = db.DBSession.query(rm).filter(
        or_(
            and_(
                rm.resource_attr_id_a == source_resource_attr_id,
                rm.resource_attr_id_b == target_resource_attr_id
            ),
            and_(
                rm.resource_attr_id_a == target_resource_attr_id,
                rm.resource_attr_id_b == source_resource_attr_id
            )
        )
    ).first()

    if mapping is None:
        raise ResourceNotFoundError("Mapping between %s and %s not found"%
                                    (source_resource_attr_id,
                                     target_resource_attr_id))

    #check scenarios exist
    s1 = _get_scenario(source_scenario_id, user_id)
    s2 = _get_scenario(target_scenario_id, user_id)

    rs = aliased(ResourceScenario, name='rs')
    rs1 = db.DBSession.query(rs).filter(rs.resource_attr_id == source_resource_attr_id,
                                        rs.scenario_id == source_scenario_id).first()
    rs2 = db.DBSession.query(rs).filter(rs.resource_attr_id == target_resource_attr_id,
                                        rs.scenario_id == target_scenario_id).first()

    #3 possibilities worth considering:
    #1: Both RS exist, so update the target RS
    #2: Target RS does not exist, so create it with the dastaset from RS1
    #3: Source RS does not exist, so it must be removed from the target scenario if it exists
    return_value = None#Either return null or return a new or updated resource scenario
    if rs1 is not None:
        if rs2 is not None:
            log.info("Destination Resource Scenario exists. Updating dastaset ID")
            rs2.dataset_id = rs1.dataset_id
        else:
            log.info("Destination has no data, so making a new Resource Scenario")
            rs2 = ResourceScenario(
                resource_attr_id=target_resource_attr_id,
                scenario_id=target_scenario_id,
                dataset_id=rs1.dataset_id)
            db.DBSession.add(rs2)
        db.DBSession.flush()
        return_value = rs2
    else:
        log.info("Source Resource Scenario does not exist. Deleting destination Resource Scenario")
        if rs2 is not None:
            db.DBSession.delete(rs2)

    #lazy load the dataset
    return_value.dataset

    db.DBSession.flush()
    return return_value
