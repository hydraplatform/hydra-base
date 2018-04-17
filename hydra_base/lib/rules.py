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

from ..db.model import Rule
from .. import db
from ..exceptions import HydraError, ResourceNotFoundError
from sqlalchemy.orm.exc import NoResultFound

def _get_rule(rule_id):
    try:
        rule_i = db.DBSession.query(Rule).filter(Rule.id == rule_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule %s not found"%rule_id)
    return rule_i

def get_rules(scenario_id, **kwargs):
    """
        Get all the rules for a given scenario.
    """
    rules = db.DBSession.query(Rule).filter(Rule.scenario_id==scenario_id, Rule.status=='A').all()

    return rules

def get_rule(rule_id, **kwargs):
    rule = _get_rule(rule_id)
    return rule

def add_rule(scenario_id, rule, **kwargs):
    rule_i = Rule()
    rule_i.ref_key = rule.ref_key
    if rule.ref_key == 'NETWORK':
        rule_i.network_id = rule.ref_id
    elif rule.ref_key == 'NODE':
        rule_i.node_id = rule.ref_id
    elif rule.ref_key == 'LINK':
        rule_i.link_id = rule.ref_id
    elif rule.ref_key == 'GROUP':
        rule_i.group_id = rule.group_id
    else:
        raise HydraError("Ref Key %s not recognised.")

    rule_i.scenario_id = scenario_id
    rule_i.name   = rule.name
    rule_i.description = rule.description

    rule_i.value = rule.value

    db.DBSession.add(rule_i)
    db.DBSession.flush()

    return rule_i

def update_rule(rule, **kwargs):
    rule_i = _get_rule(rule.id)

    if rule.ref_key != rule_i.ref_key:
        raise HydraError("Cannot convert a %s rule to a %s rule. Please create a new rule instead."%(rule_i.ref_key, rule.ref_key))

    if rule.ref_key == 'NETWORK':
        rule_i.network_id = rule.ref_id
    elif rule.ref_key == 'NODE':
        rule_i.node_id = rule.ref_id
    elif rule.ref_key == 'LINK':
        rule_i.link_id = rule.ref_id
    elif rule.ref_key == 'GROUP':
        rule_i.group_id = rule.group_id
    else:
        raise HydraError("Ref Key %s not recognised.")

    rule_i.scenario_id = rule.scenario_id
    rule_i.name   = rule.name
    rule_i.description = rule.description

    rule_i.value = rule.value

    db.DBSession.flush()

    return rule_i

def delete_rule(rule_id, **kwargs):

    rule_i = _get_rule(rule_id)

    rule_i.status = 'X'

    db.DBSession.flush()

def activate_rule(rule_id, **kwargs):
    rule_i = _get_rule(rule_id)

    rule_i.status = 'A'

    db.DBSession.flush()

def purge_rule(rule_id, **kwargs):
    rule_i = _get_rule(rule_id)

    db.DBSession.delete(rule_i)
    db.DBSession.flush()
