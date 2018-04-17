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

from ..exceptions import HydraError, ResourceNotFoundError
from . import scenario

from .. import db
from ..db.model import ResourceGroup, ResourceGroupItem, Node, Link
from sqlalchemy.orm.exc import NoResultFound

import logging
log = logging.getLogger(__name__)

def _get_group(group_id):
    try:
        return db.DBSession.query(ResourceGroup).filter(ResourceGroup.id==group_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("ResourceGroup %s not found"%(group_id,))

def _get_item(item_id):
    try:
        item = db.DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.id==item_id).one()
        return item
    except NoResultFound:
        raise ResourceNotFoundError("ResourceGroupItem %s not found"%(item_id,))

def add_resourcegroup(group, network_id,**kwargs):
    """
        Add a new group to a network.
    """
    group_i                   = ResourceGroup()
    group_i.name              = group.name
    group_i.description       = group.description
    group_i.status            = group.status
    group_i.network_id        = network_id
    db.DBSession.add(group_i)
    db.DBSession.flush()
    return group_i

def delete_resourcegroup(group_id,**kwargs):
    """
        Add a new group to a scenario.
    """
    group_i = _get_group(group_id)
    #This should cascaded to delete all the group items.
    db.DBSession.delete(group_i)
    db.DBSession.flush()

    return 'OK'

def update_resourcegroup(group,**kwargs):
    """
        Add a new group to a network.
    """

    group_i                 = _get_group(group.id)
    group_i.name            = group.name
    group_i.description     = group.description
    group_i.status          = group.status

    db.DBSession.flush()

    return group_i


def add_resourcegroupitem(group_item, scenario_id,**kwargs):

    scenario._check_can_edit_scenario(scenario_id, kwargs['user_id'])
    #Check whether the ref_id is correct.

    if group_item.ref_key == 'NODE':
        try:
            db.DBSession.query(Node).filter(Node.id==group_item.ref_id).one()

        except NoResultFound:
            raise HydraError("Invalid ref ID %s for a Node group item!"%(group_item.ref_id))
    elif group_item.ref_key == 'LINK':
        try:
            db.DBSession.query(Link).filter(Link.id==group_item.ref_id).one()
        except NoResultFound:
            raise HydraError("Invalid ref ID %s for a Link group item!"%(group_item.ref_id))
    elif group_item.ref_key == 'GROUP':
        try:
            db.DBSession.query(ResourceGroup).filter(ResourceGroup.id==group_item.ref_id).one()
        except NoResultFound:
            raise HydraError("Invalid ref ID %s for a Group group item!"%(group_item.ref_id))
    else:
        raise HydraError("Invalid ref key: %s"%(group_item.ref_key))

    group_item_i             = ResourceGroupItem()
    group_item_i.scenario_id = scenario_id
    group_item_i.group_id    = group_item.group_id
    group_item_i.ref_key     = group_item.ref_key
    if group_item.ref_key == 'NODE':
        group_item_i.node_id      = group_item.ref_id
    elif group_item.ref_key == 'LINK':
        group_item_i.link_id = group_item.ref_id
    elif group_item.ref_key == 'GROUP':
        group_item_i.subgroup_id = group_item.ref_id


    db.DBSession.add(group_item_i)
    db.DBSession.flush()

    return group_item_i

def delete_resourcegroupitem(item_id,**kwargs):
    group_item_i = _get_item(item_id)
    scenario._check_can_edit_scenario(group_item_i.scenario_id, kwargs['user_id'])
    db.DBSession.delete(group_item_i)
    db.DBSession.flush()

    return 'OK'
