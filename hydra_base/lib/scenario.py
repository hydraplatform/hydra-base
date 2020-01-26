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
import six
from ..exceptions import HydraError, PermissionError, ResourceNotFoundError
from .. import db
from ..db.model import Scenario,\
        ResourceGroupItem,\
        ResourceScenario,\
        TypeAttr,\
        ResourceAttr,\
        NetworkOwner,\
        Dataset,\
        Network,\
        Attr,\
        ResourceAttrMap

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import joinedload, joinedload_all, aliased
from . import data
from ..util.hydra_dateutil import timestamp_to_ordinal
from collections import namedtuple
from copy import deepcopy
import zlib

from .network import get_resource

from .objects import JSONObject

log = logging.getLogger(__name__)

def _check_network_ownership(network_id, user_id):
    net = db.DBSession.query(Network).filter(Network.id==network_id).one()

    net.check_write_permission(user_id)

def _get_scenario(scenario_id, user_id):
    log.info("Getting scenario %s", scenario_id)
    try:
        scenario_qry = db.DBSession.query(Scenario).filter(Scenario.id==scenario_id)
        scenario = scenario_qry.one()
        return scenario
    except NoResultFound:
        raise ResourceNotFoundError("Scenario %s does not exist."%(scenario_id))

    scenario.network.check_read_permission(user_id)

def set_rs_dataset(resource_attr_id, scenario_id, dataset_id, **kwargs):
    rs = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.resource_attr_id==resource_attr_id,
        ResourceScenario.scenario_id==scenario_id).first()

    if rs is None:
        raise ResourceNotFoundError("Resource scenario for resource attr %s not found in scenario %s"%(resource_attr_id, scenario_id))

    dataset = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).first()

    if dataset is None:
        raise ResourceNotFoundError("Dataset %s not found"%(dataset_id,))

    rs.dataset_id=dataset_id

    db.DBSession.flush()

    rs = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.resource_attr_id==resource_attr_id,
        ResourceScenario.scenario_id==scenario_id).first()

    return rs

def copy_data_from_scenario(resource_attrs, source_scenario_id, target_scenario_id, **kwargs):
    """
        For a given list of resource attribute IDS copy the dataset_ids from
        the resource scenarios in the source scenario to those in the 'target' scenario.
    """

    #Get all the resource scenarios we wish to update
    target_resourcescenarios = db.DBSession.query(ResourceScenario).filter(
            ResourceScenario.scenario_id==target_scenario_id,
            ResourceScenario.resource_attr_id.in_(resource_attrs)).all()

    target_rs_dict = {}
    for target_rs in target_resourcescenarios:
        target_rs_dict[target_rs.resource_attr_id] = target_rs

    #get all the resource scenarios we are using to get our datsets source.
    source_resourcescenarios = db.DBSession.query(ResourceScenario).filter(
            ResourceScenario.scenario_id==source_scenario_id,
            ResourceScenario.resource_attr_id.in_(resource_attrs)).all()

    #If there is an RS in scenario 'source' but not in 'target', then create
    #a new one in 'target'
    for source_rs in source_resourcescenarios:
        target_rs = target_rs_dict.get(source_rs.resource_attr_id)
        if target_rs is not None:
            target_rs.dataset_id = source_rs.dataset_id
        else:
            target_rs = ResourceScenario()
            target_rs.scenario_id      = target_scenario_id
            target_rs.dataset_id       = source_rs.dataset_id
            target_rs.resource_attr_id = source_rs.resource_attr_id
            db.DBSession.add(target_rs)

    db.DBSession.flush()

    return target_resourcescenarios

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
        scenario_i = db.DBSession.query(Scenario).filter(func.lower(Scenario.name)==scenario_name.lower(),
                                                         Scenario.network_id==network_id).first()

        return get_scenario(scenario_i.id,
                            get_parent_data=get_parent_data,
                            include_data=include_data,
                            include_group_items=include_group_items,
                            **kwargs)
    except NoResultFound:
        raise ResourceNotFoundError("Scenario %s not found"%(scenario_name))

def get_scenario(scenario_id, get_parent_data=False, include_data=True, include_group_items=True, **kwargs):
    """
        Get the specified scenario
        args:
            scenario_id: The ID of the scenario to retrieve
            get_parent_data: Flag to indicate whether to include the data from the parent scenario also, or just this one.
            include_data: Flag to indicate wheter to return the list of resource scenarios
            include_group_items: Flag to indicate whether to return the list of resource group items
        return:
            A scenario JSONObject
    """

    user_id = kwargs.get('user_id')

    scen_i = _get_scenario(scenario_id, user_id)

    scen_j = JSONObject(scen_i)

    rscen_rs = []
    if include_data is True:
        rscen_rs = scen_i.get_data(get_parent_data=get_parent_data)

    #lazy load resource attributes and attributes
    for rs in rscen_rs:
        rs.resourceattr
        rs.resourceattr.attr
        rs.dataset
        rs.dataset.metadata

    rgi_rs = []
    if include_group_items is True:
        rgi_rs = scen_i.get_group_items(get_parent_items=get_parent_data)

    scen_j.resourcescenarios = []
    for rs in rscen_rs:
        rs_j = JSONObject(rs, extras={'resourceattr':JSONObject(rs.resourceattr)})
        if rs.dataset.check_read_permission(user_id, do_raise=False) is False:
            rs_j.dataset['value'] = None
            rs_j.dataset.metadata = JSONObject({})
        scen_j.resourcescenarios.append(rs_j)

    scen_j.resourcegroupitems =[JSONObject(r) for r in rgi_rs]

    return scen_j

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
    scen.name                 = scenario.name
    scen.description          = scenario.description
    scen.layout               = scenario.get_layout()
    scen.network_id           = network_id
    scen.created_by           = user_id
    scen.start_time           = str(timestamp_to_ordinal(scenario.start_time)) if scenario.start_time else None
    scen.end_time             = str(timestamp_to_ordinal(scenario.end_time)) if scenario.end_time else None
    scen.time_step            = scenario.time_step
    scen.resourcescenarios    = []
    scen.resourcegroupitems   = []

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
            rs_i.dataset_id       = datasets[i].id
            rs_i.scenario_id      = scen.id
            rs_i.dataset = datasets[i]
            scen.resourcescenarios.append(rs_i)

    if scenario.resourcegroupitems is not None:
        #Again doing bulk insert.
        for group_item in scenario.resourcegroupitems:
            group_item_i = ResourceGroupItem()
            group_item_i.scenario_id = scen.id
            group_item_i.group_id    = group_item.group_id
            group_item_i.ref_key     = group_item.ref_key
            if group_item.ref_key == 'NODE':
                group_item_i.node_id      = group_item.ref_id
            elif group_item.ref_key == 'LINK':
                group_item_i.link_id      = group_item.ref_id
            elif group_item.ref_key == 'GROUP':
                group_item_i.subgroup_id  = group_item.ref_id
            scen.resourcegroupitems.append(group_item_i)
    db.DBSession.add(scen)
    db.DBSession.flush()
    return scen

def update_scenario(scenario,update_data=True,update_groups=True,flush=True,**kwargs):
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

    start_time = None
    if isinstance(scenario.start_time, float):
        start_time = six.text_type(scenario.start_time)
    elif isinstance(scenario.start_time, int):
        start_time = six.text_type(float(scenario.start_time))
    else:
        start_time = timestamp_to_ordinal(scenario.start_time)
        if start_time is not None:
            start_time = six.text_type(start_time)

    end_time = None
    if isinstance(scenario.end_time, float):
        end_time = six.text_type(scenario.end_time)
    elif isinstance(scenario.end_time, int):
        end_time = six.text_type(float(scenario.end_time))
    else:
        end_time = timestamp_to_ordinal(scenario.end_time)
        if end_time is not None:
            end_time = six.text_type(end_time)

    scen.name                 = scenario.name
    scen.description          = scenario.description
    scen.layout               = scenario.get_layout()
    scen.start_time           = start_time
    scen.end_time             = end_time
    scen.time_step            = scenario.time_step
    if hasattr(scenario, 'parent_id'):
        scen.parent_id = scenario.parent_id
    else:
        scen.parent_id = None

    if scenario.resourcescenarios == None:
        scenario.resourcescenarios = []
    if scenario.resourcegroupitems == None:
        scenario.resourcegroupitems = []

    #lazy load resourcescenarios from the DB
    scen.resourcescenarios

    if update_data is True:
        datasets = [rs.dataset for rs in scenario.resourcescenarios]
        updated_datasets = data._bulk_insert_data(datasets, user_id, kwargs.get('app_name'))
        for i, r_scen in enumerate(scenario.resourcescenarios):
            _update_resourcescenario(scen, r_scen, dataset=updated_datasets[i], user_id=user_id, source=kwargs.get('app_name'))

    #lazy load resource grou items from the DB
    scen.resourcegroupitems

    if update_groups is True:
        #Get all the exiting resource group items for this scenario.
        #THen process all the items sent to this handler.
        #Any in the DB that are not passed in here are removed.
        for group_item in scenario.resourcegroupitems:
            _add_resourcegroupitem(group_item, scenario.id)

    if flush is True:
        db.DBSession.flush()

    return scen

def set_scenario_status(scenario_id, status, **kwargs):
    """
        Set the status of a scenario.
    """

    user_id = kwargs.get('user_id')

    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_i.status = status
    db.DBSession.flush()
    return 'OK'

def purge_scenario(scenario_id, delete_children=False, **kwargs):
    """
        Set the status of a scenario.
    """

    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenarios_to_delete = [scenario_i]
    scenario_ids_to_delete = []

    if delete_children:
        def recursive_get_children(parent_id):
            child_scenarios = db.DBSession.query(Scenario).filter(Scenario.parent_id == parent_id).all()
            if child_scenarios:
                for child_scenario_i in child_scenarios:
                    if child_scenario_i.id not in scenario_ids_to_delete:  # this is a check
                        scenario_ids_to_delete.append(child_scenario_i.id)
                        scenarios_to_delete.append(child_scenario_i)
                        recursive_get_children(child_scenario_i.id)

        recursive_get_children(scenario_id)

    for scen_i in scenarios_to_delete:
        db.DBSession.delete(scen_i)
    db.DBSession.flush()
    return 'OK'

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
                                        ResourceScenario.scenario_id==scenario_id,
                                        ResourceAttr.id==ResourceScenario.resource_attr_id,
                                        ResourceAttr.attr_is_var == 'N'
                                    ).all()
    else:
        old_rscen_rs = db.DBSession.query(ResourceScenario).filter(
                                        ResourceScenario.scenario_id==scenario_id
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
    old_rgis = db.DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.scenario_id==scenario_id).all()
    new_rgis = []
    for old_rgi in old_rgis:
        new_rgis.append(dict(
            ref_key=old_rgi.ref_key,
            node_id = old_rgi.node_id,
            link_id = old_rgi.link_id,
            group_id = old_rgi.group_id,
            scenario_id=cloned_scenario_id,
        ))
    if len(new_rgis) > 0:
        db.DBSession.execute(ResourceGroupItem.__table__.insert(), new_rgis)

    log.info("Cloning finished.")

    log.info("Retrieving cloned scenario")
    new_rscen_rs = db.DBSession.query(ResourceScenario).filter(
                                        ResourceScenario.scenario_id==cloned_scenario_id).all()
    new_rgis_rs  = db.DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.scenario_id==cloned_scenario_id).all()

    cloned_scen = JSONObject(_get_scenario(cloned_scenario_id, user_id))
    cloned_scen.resourcescenarios = [JSONObject(r) for r in new_rscen_rs]
    cloned_scen.resourcegroupitems =[JSONObject(r) for r in new_rgis_rs]
    log.info("Returning cloned scenario")


    return cloned_scen

def _get_dataset_as_dict(rs, user_id):
    if rs.dataset is None:
        return None

    dataset = deepcopy(rs.dataset.__dict__)

    dataset['metadata'] = {}

    del dataset['_sa_instance_state']

    try:
        rs.dataset.check_read_permission(user_id)
    except PermissionError:
           dataset['value']      = None
           dataset['metadata'] = {}

    for m in rs.dataset.metadata:
        dataset['metadata'][m.key] = m.value

    return dataset

def _get_as_obj(obj_dict, name):
    """
        Turn a dictionary into a named tuple so it can be
        passed into the constructor of a complex model generator.
    """
    if obj_dict.get('_sa_instance_state'):
        del obj_dict['_sa_instance_state']
    obj = namedtuple(name, tuple(obj_dict.keys()))
    for k, v in obj_dict.items():
        setattr(obj, k, v)
        log.info("%s = %s",k,getattr(obj,k))
    return obj


def compare_scenarios(scenario_id_1, scenario_id_2,**kwargs):
    user_id = kwargs.get('user_id')

    scenario_1 = _get_scenario(scenario_id_1, user_id)
    scenario_2 = _get_scenario(scenario_id_2, user_id)

    scenario_1_rs = db.DBSession.query(ResourceScenario).filter(ResourceScenario.scenario_id==scenario_id_1).all()
    scenario_2_rs = db.DBSession.query(ResourceScenario).filter(ResourceScenario.scenario_id==scenario_id_2).all()
    scenario_1_rgi = db.DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.scenario_id==scenario_id_1).all()
    scenario_2_rgi = db.DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.scenario_id==scenario_id_2).all()

    if scenario_1.network_id != scenario_2.network_id:
        raise HydraError("Cannot compare scenarios that are not"
                         " in the same network!")

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

    log.info("Datasets In 1 not in 2: %s"%(rscen_1_dataset_ids - rscen_2_dataset_ids))
    log.info("Datasets In 2 not in 1: %s"%(rscen_2_dataset_ids - rscen_1_dataset_ids))

    for ra_id, s1_rs in r_scen_1_dict.items():
        s2_rs = r_scen_2_dict.get(ra_id)
        if s2_rs is not None:
            log.debug("Is %s == %s?"%(s1_rs.dataset_id, s2_rs.dataset_id))
            if s1_rs.dataset_id != s2_rs.dataset_id:
                resource_diff = dict(
                    resource_attr_id = s1_rs.resource_attr_id,
                    scenario_1_dataset = _get_as_obj(_get_dataset_as_dict(s1_rs, user_id), 'Dataset'),
                    scenario_2_dataset = _get_as_obj(_get_dataset_as_dict(s2_rs, user_id), 'Dataset'),
                )
                resource_diffs.append(resource_diff)

            continue
        else:
            resource_diff = dict(
                resource_attr_id = s1_rs.resource_attr_id,
                scenario_1_dataset = _get_as_obj(_get_dataset_as_dict(s1_rs, user_id), 'Dataset'),
                scenario_2_dataset = None,
            )
            resource_diffs.append(resource_diff)

    #make a list of all the resource scenarios (aka data) that are unique
    #in scenario 2.
    for ra_id, s2_rs in r_scen_2_dict.items():
        s1_rs = r_scen_1_dict.get(ra_id)
        if s1_rs is None:
            resource_diff = dict(
                resource_attr_id = s1_rs.resource_attr_id,
                scenario_1_dataset = None,
                scenario_2_dataset = _get_as_obj(_get_dataset_as_dict(s2_rs, user_id), 'Dataset'),
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

    scenario_rs = scenario_i.get_data(get_parent_data=get_parent_data)


    for rs_i in scenario_rs:
        if rs_i.resource_attr_id == resource_attr_id:
            rs_i.dataset
            rs_i.dataset.metadata
            return rs_i
    else:
        raise ResourceNotFoundError("resource scenario for %s not found in scenario %s"%(resource_attr_id, scenario_id))

def lock_scenario(scenario_id, **kwargs):
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')

    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_i.network.check_write_permission(user_id)

    scenario_i.locked = 'Y'

    db.DBSession.flush()
    return 'OK'

def unlock_scenario(scenario_id, **kwargs):
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')
    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_i.network.check_write_permission(user_id)

    scenario_i.locked = 'N'

    db.DBSession.flush()
    return 'OK'

def get_dataset_scenarios(dataset_id, **kwargs):

    try:
        db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Dataset %s not found"%dataset_id)

    log.info("dataset %s exists", dataset_id)

    scenarios = db.DBSession.query(Scenario).filter(
        Scenario.status == 'A',
        ResourceScenario.scenario_id==Scenario.id,
        ResourceScenario.dataset_id == dataset_id).distinct().all()

    log.info("%s scenarios retrieved", len(scenarios))

    return scenarios

def bulk_update_resourcedata(scenario_ids, resource_scenarios,**kwargs):
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
        _check_can_edit_scenario(scenario_id, kwargs['user_id'])

        scen_i = _get_scenario(scenario_id, user_id)
        res[scenario_id] = []

        for rs in resource_scenarios:
            if rs.dataset is not None:
                updated_rs = _update_resourcescenario(scen_i, rs, user_id=user_id, source=kwargs.get('app_name'))
                res[scenario_id].append(updated_rs)
            else:
                _delete_resourcescenario(scenario_id, rs.resource_attr_id)

        db.DBSession.flush()

    return res

def update_resourcedata(scenario_id, resource_scenarios,**kwargs):
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

    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    scen_i = _get_scenario(scenario_id, user_id)

    res = []
    for rs in resource_scenarios:
        if rs.dataset is not None:
            updated_rs = _update_resourcescenario(scen_i, rs, user_id=user_id, source=kwargs.get('app_name'))
            res.append(updated_rs)
        else:
            _delete_resourcescenario(scenario_id, rs.resource_attr_id)

    db.DBSession.flush()

    return res

def delete_resource_scenario(scenario_id, resource_attr_id, quiet=False, **kwargs):
    """
        Remove the data associated with a resource in a scenario.
    """
    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    _delete_resourcescenario(scenario_id, resource_attr_id, suppress_error=quiet)

def delete_resource_scenarios(scenario_id, resource_attr_ids, quiet=False, **kwargs):
    """
        Remove the data associated with a list of resources in a scenario.
    """

    log.info("Deleting %s resource scenarios from from scenario %s", len(resource_attr_ids), scenario_id)

    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    for resource_attr_id in resource_attr_ids:
        _delete_resourcescenario(scenario_id, resource_attr_id, suppress_error=quiet)

def delete_resourcedata(scenario_id, resource_scenario, quiet = False, **kwargs):
    """
        Remove the data associated with a resource in a scenario.
        The 'quiet' parameter indicates whether an non-existent RS should throw
        an error.
    """


    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    _delete_resourcescenario(scenario_id, resource_scenario.resource_attr_id, suppress_error=quiet)


def _delete_resourcescenario(scenario_id, resource_attr_id, suppress_error=False):

    log.debug("Deleting resource scenario for RA %s from scenario %s", resource_attr_id, scenario_id)

    try:
        sd_i = db.DBSession.query(ResourceScenario).filter(
                            ResourceScenario.scenario_id==scenario_id,
                            ResourceScenario.resource_attr_id==resource_attr_id).one()
    except NoResultFound:
        if suppress_error == False:
            raise HydraError("ResourceAttr %s does not exist in scenario %s."%(resource_attr_id, scenario_id))
        return

    db.DBSession.delete(sd_i)
    db.DBSession.flush()

def _update_resourcescenario(scenario, resource_scenario, dataset=None, new=False, user_id=None, source=None):
    """
        Insert or Update the value of a resource's attribute by first getting the
        resource, then parsing the input data, then assigning the value.

        returns a ResourceScenario object.
    """
    if scenario is None:
        scenario = db.DBSession.query(Scenario).filter(Scenario.id==1).one()

    ra_id = resource_scenario.resource_attr_id

    log.debug("Assigning resource attribute: %s",ra_id)
    #count the number of new RS to report it in the logs
    new_rs = 0
    try:
        r_scen_i = db.DBSession.query(ResourceScenario).filter(
                        ResourceScenario.scenario_id==scenario.id,
                        ResourceScenario.resource_attr_id==ra_id).one()
    except NoResultFound as e:
        log.debug("Creating new RS for RS %s in scenario %s", resource_scenario.resource_attr_id, scenario.id)
        r_scen_i = ResourceScenario()
        r_scen_i.resource_attr_id = resource_scenario.resource_attr_id
        r_scen_i.scenario_id      = scenario.id
        r_scen_i.scenario = scenario
        new_rs = new_rs + 1

        db.DBSession.add(r_scen_i)

    if scenario.locked == 'Y':
        log.info("Scenario %s is locked",scenario.id)
        return r_scen_i


    if dataset is not None:
        r_scen_i.dataset = dataset

        return r_scen_i

    dataset = resource_scenario.dataset

    value = dataset.parse_value()

    log.debug("Assigning %s to resource attribute: %s", value, ra_id)

    if value is None:
        log.info("Cannot set data on resource attribute %s",ra_id)
        return None

    metadata = dataset.get_metadata_as_dict(source=source, user_id=user_id)
    data_unit_id = dataset.unit_id

    data_hash = dataset.get_hash(value, metadata)

    assign_value(r_scen_i,
                 dataset.type.lower(),
                 value,
                 data_unit_id,
                 dataset.name,
                 metadata=metadata,
                 data_hash=data_hash,
                 user_id=user_id,
                 source=source)
    return r_scen_i

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
            log.debug("Dataset has not changed. Returning.")
            return

        connected_rs = db.DBSession.query(ResourceScenario).filter(ResourceScenario.dataset_id==rs.dataset.id).all()
        #If there's no RS found, then the incoming rs is new, so the dataset can be altered
        #without fear of affecting something else.
        if len(connected_rs) == 0:
        #If it's 1, the RS exists in the DB, but it's the only one using this dataset or
        #The RS isn't in the DB yet and the datset is being used by 1 other RS.
            update_dataset = True

        if len(connected_rs) == 1 :
            if connected_rs[0].scenario_id == rs.scenario_id and connected_rs[0].resource_attr_id==rs.resource_attr_id:
                update_dataset = True
        else:
            update_dataset=False

    if update_dataset is True:
        log.info("Updating dataset '%s'", name)
        dataset = data.update_dataset(rs.dataset.id, name, data_type, val, unit_id, metadata, flush=False, **dict(user_id=user_id))
        rs.dataset = dataset
        rs.dataset_id = dataset.id
        log.info("Set RS dataset id to %s"%dataset.id)
    else:
        log.info("Creating new dataset %s in scenario %s", name, rs.scenario_id)
        dataset = data.add_dataset(data_type,
                                val,
                                unit_id,
                                metadata=metadata,
                                name=name,
                                **dict(user_id=user_id))
        rs.dataset = dataset
        rs.source = source

    db.DBSession.flush()

def add_data_to_attribute(scenario_id, resource_attr_id, dataset,**kwargs):
    """
        Add data to a resource scenario outside of a network update
    """
    user_id = kwargs.get('user_id')

    _check_can_edit_scenario(scenario_id, user_id)

    scenario_i = _get_scenario(scenario_id, user_id)

    try:
        r_scen_i = db.DBSession.query(ResourceScenario).filter(
                                ResourceScenario.scenario_id==scenario_id,
                                ResourceScenario.resource_attr_id==resource_attr_id).one()
        log.info("Existing resource scenario found for %s in scenario %s", resource_attr_id, scenario_id)
    except NoResultFound:
        log.info("No existing resource scenarios found for %s in scenario %s. Adding a new one.", resource_attr_id, scenario_id)
        r_scen_i = ResourceScenario()
        r_scen_i.scenario_id      = scenario_id
        r_scen_i.resource_attr_id = resource_attr_id
        scenario_i.resourcescenarios.append(r_scen_i)

    data_type = dataset.type.lower()

    value = dataset.parse_value()

    dataset_metadata = dataset.get_metadata_as_dict(user_id=kwargs.get('user_id'),
                                                    source=kwargs.get('source'))
    if value is None:
        raise HydraError("Cannot set value to attribute. "
            "No value was sent with dataset %s", dataset.id)

    data_hash = dataset.get_hash(value, dataset_metadata)

    assign_value(r_scen_i, data_type, value, dataset.unit_id, dataset.name,
                          metadata=dataset_metadata, data_hash=data_hash, user_id=user_id)

    db.DBSession.flush()
    return r_scen_i

def get_scenario_data(scenario_id, get_parent_data=False, **kwargs):
    """
        Get all the datasets from the scenario with the specified ID
        @returns a list of dictionaries
    """
    user_id = kwargs.get('user_id')

    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_rs = scenario_i.get_data(get_parent_data=get_parent_data)

    dataset_ids = []
    datasets = []
    for rs in scenario_rs:
        if rs.dataset.id in dataset_ids:
           continue

        if rs.dataset.hidden == 'Y':
           try:
                rs.dataset.check_read_permission(user_id)
           except:
               rs.dataset.value     = None
               rs.dataset.metadata = []

        datasets.append(rs.dataset)
        dataset_ids.append(rs.dataset.id)

    log.info("Retrieved %s datasets", len(datasets))
    return datasets

def get_attribute_data(attr_ids, node_ids, **kwargs):
    """
        For a given attribute or set of attributes, return  all the resources and
        resource scenarios in the network
    """
    node_attrs = db.DBSession.query(ResourceAttr).\
                                            options(joinedload('attr')).\
                                            filter(ResourceAttr.node_id.in_(node_ids),
                                            ResourceAttr.attr_id.in_(attr_ids)).all()

    ra_ids = []
    for ra in node_attrs:
        ra_ids.append(ra.id)


    resource_scenarios = db.DBSession.query(ResourceScenario).filter(ResourceScenario.resource_attr_id.in_(ra_ids)).options(joinedload('resourceattr')).options(joinedload('dataset').joinedload('metadata')).order_by(ResourceScenario.scenario_id).all()


    for rs in resource_scenarios:
       if rs.dataset.hidden == 'Y':
           try:
                rs.dataset.check_read_permission(kwargs.get('user_id'))
           except:
               rs.dataset.value      = None
       db.DBSession.expunge(rs)

    return node_attrs, resource_scenarios

def get_resource_data(ref_key, ref_id, scenario_id, type_id=None, expunge_session=True, get_parent_data=False, **kwargs):
    """
        Get all the resource scenarios for a given resource
        in a given scenario. If type_id is specified, only
        return the resource scenarios for the attributes
        within the type.
    """

    user_id = kwargs.get('user_id')

    resource_i = get_resource(ref_key, ref_id)
    ra_ids = [ra.id for ra in resource_i.attributes]

    scenario_i = _get_scenario(scenario_id, user_id)
    requested_rs = scenario_i.get_data(get_parent_data=get_parent_data, ra_ids=ra_ids)

    #map an raID to an rs for uses later
    ra_rs_map = {}
    for rs in requested_rs:
        ra_rs_map[rs.resource_attr_id] = rs

    #make a lookup table between an attr ID and an RS for filtering by types later.
    attr_rs_lookup = {} # Used later to remove results if they're not required
    for ra in resource_i.attributes:
        #Is there data for this RA?
        if ra_rs_map.get(ra.id) is not None:
            attr_rs_lookup[ra.attr_id] = ra_rs_map[ra.id]

    #Remove RS that are not defined by the specified type.
    if type_id is not None:

        type_limited_rs = []

        attr_ids = []
        rs = db.DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_id).all()
        for r in rs:
            type_limited_rs.append(attr_rs_lookup[r.attr_id])

        requested_rs = type_limited_rs

    for rs in requested_rs:

        #TODO: Design a mechanism to read the value of the dataset if it's stored externally

        if rs.dataset.hidden == 'Y':
           try:
                rs.dataset.check_read_permission(user_id)
           except:
               rs.dataset.value      = None

        #lazy load the dataset's unit and metadata
        rs.dataset.unit
        rs.dataset.metadata

        #lazy load the dataset's resourceattr object
        rs.resourceattr

    if expunge_session == True:
        db.DBSession.expunge_all()

    return requested_rs

def get_resource_attribute_data(ref_key, ref_id, scenario_id, attr_id, **kwargs):
    """
        Get all the resource scenarios for a given resource attribute
        in a given scenario. If type_id is specified, only
        return the resource scenarios for the attributes
        within the type.
    """

    user_id = kwargs.get('user_id')

    # This can be either a single ID or list, so make them consistent
    if not isinstance(scenario_id, list):
        scenario_id = [scenario_id]

    resource_data_qry = db.DBSession.query(ResourceScenario).filter(
        ResourceScenario.dataset_id == Dataset.id,
        ResourceAttr.id == ResourceScenario.resource_attr_id,
        ResourceScenario.scenario_id.in_(scenario_id),
        ResourceAttr.ref_key == ref_key,
        ResourceAttr.attr_id == attr_id,
        or_(
            ResourceAttr.network_id == ref_id,
            ResourceAttr.node_id == ref_id,
            ResourceAttr.link_id == ref_id,
            ResourceAttr.group_id == ref_id
        )).distinct().options(joinedload('resourceattr')).options(joinedload_all('dataset.metadata'))

    if attr_id is not None:
        if not isinstance(attr_id, list):
            attr_id = [attr_id]
        resource_data_qry = resource_data_qry.filter(ResourceAttr.attr_id.in_(attr_id))

    resource_data = resource_data_qry.all()

    for rs in resource_data:
        try:
            rs.dataset.value = zlib.decompress(rs.dataset.value)
        except zlib.error:
            pass

        if rs.dataset.hidden == 'Y':
            try:
                rs.dataset.check_read_permission(user_id)
            except:
                rs.dataset.value = None
                rs.dataset.frequency = None
                rs.dataset.start_time = None

    db.DBSession.expunge_all()
    return resource_data

def get_resource_attribute_datasets(resource_attr_id, scenario_id, **kwargs):
    """
        Retrieve all the datasets in given scenarios for given resource attributes.
    """

    try:
        a = db.DBSession.query(ResourceAttr).filter(ResourceAttr.id == resource_attr_id[0]).one()
    except NoResultFound:
        raise HydraError("Resource attribute %s not found" % (resource_attr_id,))

    ras = db.DBSession.query(ResourceAttr).filter(
        ResourceAttr.id.in_(resource_attr_id),
        ResourceScenario.scenario_id.in_(scenario_id),
        ResourceScenario.resource_attr_id == ResourceAttr.id
    ).all()

    return ras

def get_scenarios_data(scenario_id, attr_id, type_id, node_ids=None, link_ids=None, network_ids=None, **kwargs):
    """
        Get all the resource scenarios for a given attribute and/or type
        in a given scenario.
    """

    user_id = kwargs.get('user_id')

    # This can be either a single ID or list, so make them consistent
    if not isinstance(scenario_id, list):
        scenario_id = [scenario_id]

    scenarios = db.DBSession.query(Scenario).filter(Scenario.id.in_(scenario_id)).all()
    for scenario in scenarios:
        resource_data_qry = db.DBSession.query(ResourceScenario).filter(
            ResourceScenario.dataset_id == Dataset.id,
            ResourceAttr.id == ResourceScenario.resource_attr_id,
            ResourceScenario.scenario_id == scenario.scenario_id) \
            .distinct() \
            .options(joinedload('resourceattr')) \
            .options(joinedload_all('dataset.metadata'))

        attr_ids = []
        if type_id is not None:
            if not isinstance(type_id, list):
                type_id = [type_id]
            rs = db.DBSession.query(TypeAttr).filter(TypeAttr.type_id.in_(type_id)).all()
            for r in rs:
                attr_ids.append(r.attr_id)
        if attr_id is not None:
            if not isinstance(attr_id, list):
                attr_id = [attr_id]
            attr_ids.extend(attr_id)
        attr_ids = set(attr_ids)

        if attr_ids:
            resource_data_qry = resource_data_qry.filter(ResourceAttr.attr_id.in_(attr_ids))

        if network_ids and node_ids and link_ids:
            resource_data_qry = resource_data_qry.filter(or_(
                ResourceAttr.network_id.in_(set(network_ids)),
                ResourceAttr.node_id.in_(set(node_ids)),
                ResourceAttr.link_id.in_(set(link_ids))
            ))
        if node_ids and link_ids:
            resource_data_qry = resource_data_qry.filter(or_(
                ResourceAttr.node_id.in_(set(node_ids)),
                ResourceAttr.link_id.in_(set(link_ids))
            ))
        if network_ids and node_ids:
            resource_data_qry = resource_data_qry.filter(or_(
                ResourceAttr.network_id.in_(set(network_ids)),
                ResourceAttr.node_id.in_(set(node_ids)),
            ))
        if network_ids and link_ids:
            resource_data_qry = resource_data_qry.filter(or_(
                ResourceAttr.network_id.in_(set(network_ids)),
                ResourceAttr.link_id.in_(set(link_ids))
            ))
        elif network_ids:
            resource_data_qry = resource_data_qry.filter(ResourceAttr.network_id.in_(set(network_ids)))
        elif node_ids:
            resource_data_qry = resource_data_qry.filter(ResourceAttr.node_id.in_(set(node_ids)))
        elif link_ids:
            resource_data_qry = resource_data_qry.filter(ResourceAttr.link_id.in_(set(link_ids)))

        resource_data = resource_data_qry.all()

        for rs in resource_data:
            try:
                rs.dataset.value = zlib.decompress(rs.dataset.value)
            except zlib.error:
                pass

            if rs.dataset.hidden == 'Y':
                try:
                    rs.dataset.check_read_permission(user_id)
                except:
                    rs.dataset.value = None
                    rs.dataset.frequency = None
                    rs.dataset.start_time = None
        scenario.resourcescenarios = resource_data
        scenario.resourcegroupitems = []
    db.DBSession.expunge_all()
    return scenarios

def _check_can_edit_scenario(scenario_id, user_id):
    scenario_i = _get_scenario(scenario_id, user_id)

    scenario_i.network.check_write_permission(user_id)

    if scenario_i.locked == 'Y':
        raise PermissionError('Cannot update scenario %s as it is locked.'%(scenario_id))


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

    scenario_rs_i = scenario_i.get_data(get_parent_data=get_parent_data)

    #just in case the calling funciton hasn't cast this as as int
    attr_id = int(attr_id)

    requested_rs = []
    for rs_i in scenario_rs_i:
        if rs_i.resourceattr.attr_id == attr_id:
            #Make the ORM load the resource linked to the resourceattr, and its data
            rs_i.dataset
            rs_i.dataset.metadata
            rs_i.resourceattr.get_resource()
            #Finally add it to the list of RS to return
            requested_rs.append(rs_i)

    json_rs = []
    #Load the metadata too
    for rs in requested_rs:
        tmp_rs = JSONObject(rs)
        tmp_rs.resourceattr=JSONObject(rs.resourceattr)
        if rs.resourceattr.node_id is not None:
            tmp_rs.resourceattr.node = JSONObject(rs.resourceattr.node)
        elif rs.resourceattr.link_id is not None:
            tmp_rs.resourceattr.link = JSONObject(rs.resourceattr.link)
        elif rs.resourceattr.group_id is not None:
            tmp_rs.resourceattr.resourcegroup = JSONObject(rs.resourceattr.resourcegroup)
        elif rs.resourceattr.network_id is not None:
            tmp_rs.resourceattr.network = JSONObject(rs.resourceattr.network)

        json_rs.append(tmp_rs)


    return json_rs

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

def delete_resourcegroupitems(scenario_id, item_ids, **kwargs):
    """
        Delete specified items in a group, in a scenario.
    """
    user_id = int(kwargs.get('user_id'))
    #check the scenario exists
    _get_scenario(scenario_id, user_id)
    for item_id in item_ids:
        rgi = db.DBSession.query(ResourceGroupItem).\
                filter(ResourceGroupItem.id==item_id).one()
        db.DBSession.delete(rgi)

    db.DBSession.flush()

def empty_group(group_id, scenario_id, **kwargs):
    """
        Delete all itemas in a group, in a scenario.
    """
    user_id = int(kwargs.get('user_id'))
    #check the scenario exists
    _get_scenario(scenario_id, user_id)

    rgi = db.DBSession.query(ResourceGroupItem).\
            filter(ResourceGroupItem.group_id==group_id).\
            filter(ResourceGroupItem.scenario_id==scenario_id).all()
    rgi.delete()

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
        group_item_i.node_id =group_item.ref_id if group_item.ref_id else group_item.node_id
    elif ref_key == 'LINK':
        group_item_i.link_id =group_item.ref_id if group_item.ref_id else group_item.link_id
    elif ref_key == 'GROUP':
        group_item_i.subgroup_id = group_item.ref_id if group_item.ref_id else group_item.subgroup_id

    return group_item_i

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
            rs2 = ResourceScenario(resource_attr_id=target_resource_attr_id, scenario_id=target_scenario_id, dataset_id=rs1.dataset_id)
            db.DBSession.add(rs2)
        db.DBSession.flush()
        return_value = rs2
    else:
        log.info("Source Resource Scenario does not exist. Deleting destination Resource Scenario")
        if rs2 is not None:
            db.DBSession.delete(rs2)

    db.DBSession.flush()
    return return_value
