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

from ..db.model import Rule, RuleTypeDefinition, RuleTypeLink, RuleOwner
from .. import db
from ..exceptions import HydraError, ResourceNotFoundError
from sqlalchemy.orm.exc import NoResultFound
from ..util.permissions import required_perms
from hydra_base.lib.objects import JSONObject

def _get_rule(rule_id, user_id, check_write=False):
    try:
        rule_i = db.DBSession.query(Rule).filter(Rule.id == rule_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule {0} not found".format(rule_id))

    #lazy load owners
    rule_i.owners

    rule_i.check_read_permission(user_id)

    if check_write is True:
        rule_i.check_write_permission(user_id)

    return rule_i

@required_perms("view_network")
def get_scenario_rules(scenario_id, **kwargs):
    """
        Get all the rules for a given scenario.
    """
    rules = db.DBSession.query(Rule).filter(Rule.scenario_id==scenario_id, Rule.status=='A').all()

    return rules

@required_perms("view_network")
def get_resource_rules(ref_key, ref_id, scenario_id=None, **kwargs):
    """
        Get all the rules for a given resource.
        Args:
            ref_key (string): NETWORK, NODE, LINK, GROUP
            ref_id (int): ID of the resource
            scenario_id (int): Optional which filters on scenario ID also
        Returns:
            List of Rule SQLAlchemy objects
    """
    user_id = kwargs.get('user_id')

    ref_key == ref_key.upper() # turn 'network' into 'NETWORK'

    rule_qry = db.DBSession.query(Rule).filter(Rule.ref_key==ref_key, Rule.status=='A')

    if ref_key.upper() == 'NETWORK':
        rule_qry = rule_qry.filter(Rule.network_id==ref_id)
    elif ref_key.upper() == 'NODE':
        rule_qry = rule_qry.filter(Rule.node_id==ref_id)
    elif ref_key.upper() == 'LINK':
        rule_qry = rule_qry.filter(Rule.link_id==ref_id)
    elif ref_key.upper() == 'GROUP':
        rule_qry = rule_qry.filter(Rule.group_id==ref_id)
    else:
        raise HydraError("Ref Key {0} not recognised.".format(ref_key))

    rule_qry = rule_qry.join(RuleOwner).filter(RuleOwner.user_id==int(user_id))

    if scenario_id is not None:
        rule_qry = rule_qry.filter(Rule.scenario_id==scenario_id)

    rules = rule_qry.all()
    
    #lazy load types
    for r in rules:
        r.types

    return rules

@required_perms("view_network")
def get_rules_of_type(typecode, scenario_id=None, **kwargs):
    """
        Get all the rules for a given resource.
        Args:
            ref_key (string): NETWORK, NODE, LINK, GROUP
            ref_id (int): ID of the resource
            scenario_id (int): Optional which filters on scenario ID also
        Returns:
            List of Rule SQLAlchemy objects
    """

    user_id = kwargs.get('user_id')

    rule_qry = db.DBSession.query(Rule).filter(Rule.status=='A')\
                                        .join(RuleTypeLink)\
                                        .filter(RuleTypeLink.code == typecode)

    rule_qry = rule_qry.join(RuleOwner).filter(RuleOwner.user_id==int(user_id))

    if scenario_id is not None:
        rule_qry = rule_qry.filter(Rule.scenario_id==scenario_id)
    
    rules = rule_qry.all()

    return rules

@required_perms("edit_network")
def add_rule_owner(rule_id, new_rule_user_id, read='Y', write='Y', share='Y', **kwargs):
    user_id = kwargs.get('user_id')

    rule_i = _get_rule(rule_id, user_id)

    rule_i.check_write_permissions(user_id)

    rule_i.set_owner(new_rule_user_id, read, write, share)

@required_perms("edid_network")
def remove_rule_owner(rule_id, rule_user_id, **kwargs):
    user_id = kwargs.get('user_id')

    rule_i = _get_rule(rule_id, user_id, check_write=True)

    rule_i.check_write_permissions(user_id)

    rule_i.unset_owner(rule_user_id)
   
@required_perms("view_network")
def get_rule(rule_id, **kwargs):
    """
        Get a rule by its ID
        Args:
            rule_id (int); The ID of the rule
        Returns:
            A SQLAlchemy ORM Rule object
    """
    user_id = kwargs.get('user_id')
    rule = _get_rule(rule_id, user_id)
    return rule

@required_perms("edit_network")
def add_rule(rule, **kwargs):
    """
        Add a new rule.
        Args:
            rule: A JSONObject or Spyne Rule object
        Returns
            A SQLAlchemy ORM Rule object
    """

    user_id = kwargs.get('user_id')

    rule_i = Rule()
    rule_i.ref_key = rule.ref_key
    if rule.ref_key.upper() == 'NETWORK':
        rule_i.network_id = rule.network_id if rule.network_id else rule.ref_id
    elif rule.ref_key.upper() == 'NODE':
        rule_i.node_id = rule.node_id if rule.node_id else rule.ref_id
    elif rule.ref_key.upper() == 'LINK':
        rule_i.link_id = rule.link_id if rule.link_id else rule.ref_id
    elif rule.ref_key.upper() == 'GROUP':
        rule_i.group_id = rule.group_id if rule.group_id else rule.ref_id
    else:
        raise HydraError("Ref Key {0} not recognised.".format(rule.ref_key))

    rule_i.scenario_id = rule.scenario_id
    rule_i.name   = rule.name
    rule_i.description = rule.description
    rule_i.value = rule.value
    rule_i.format = rule.format

    rule_i.set_types(rule.types)

    rule_i.set_owner(user_id)

    db.DBSession.add(rule_i)

    db.DBSession.flush()

    return rule_i

@required_perms("edit_network")
def update_rule(rule, **kwargs):
    """
        Add a new rule.
        Args:
            rule: A JSONObject or Spyne Rule object
        Returns
            A SQLAlchemy ORM Rule object
    """

    user_id = kwargs.get('user_id')
    
    rule_i = _get_rule(rule.id, user_id, check_write=True)

    rule_i.ref_key = rule.ref_key
    if rule.ref_key.upper() == 'NETWORK':
        rule_i.network_id = rule.network_id if rule.network_id else rule.ref_id
    elif rule.ref_key.upper() == 'NODE':
        rule_i.node_id = rule.node_id if rule.node_id else rule.ref_id
    elif rule.ref_key.upper() == 'LINK':
        rule_i.link_id = rule.link_id if rule.link_id else rule.ref_id
    elif rule.ref_key.upper() == 'GROUP':
        rule_i.group_id = rule.group_id if rule.group_id else rule.ref_id
    else:
        raise HydraError("Ref Key {0} not recognised.".format(rule.ref_key))

    rule_i.scenario_id = rule.scenario_id
    rule_i.name   = rule.name
    rule_i.description = rule.description
    rule_i.format = rule.format
    rule_i.value  = rule.value

    rule_i.set_types(rule.types)

    db.DBSession.flush()

    return rule_i

@required_perms("edit_network")
def set_rule_type(rule_id, typecode, **kwargs):
    user_id = kwargs.get('user_id')

    rule_i = _get_rule(rule_id, user_id, check_write=True)
    
    ruletypelink = RuleTypeLink()
    ruletypelink.code = typecode

    rule_i.types.append(ruletypelink)

    db.DBSession.flush()

    return rule_i 

@required_perms("edit_network")
def clone_rule(rule_id, **kwargs):
    """
        Clone a rule
    """


    user_id = kwargs.get('user_id')
    rule_i = _get_rule(rule_id, user_id, check_write=True)

    #lazy load types
    rule_i.types

    #lazy load owners
    rule_i.owners
    
    rule_j = JSONObject(rule_i)
    cloned_rule = add_rule(rule_j, **kwargs)

    return cloned_rule

@required_perms("edit_network")
def delete_rule(rule_id, **kwargs):
    """
        Set the status of a rule to 'X'
    """

    user_id = kwargs.get('user_id')
    rule_i = _get_rule(rule_id, user_id, check_write=True)

    rule_i.status = 'X'

    db.DBSession.flush()

@required_perms("edit_network")
def activate_rule(rule_id, **kwargs):
    """
        Set the status of a rule to 'A'
    """
    
    user_id = kwargs.get('user_id')
    rule_i = _get_rule(rule_id, user_id, check_write=True)

    rule_i.status = 'A'

    db.DBSession.flush()

@required_perms("edit_network")
def purge_rule(rule_id, **kwargs):
    """
        Remove a rule from the DB permenantaly
    """
    user_id = kwargs.get('user_id')
    
    rule_i = _get_rule(rule_id, user_id, check_write=True)

    db.DBSession.delete(rule_i)
    db.DBSession.flush()

def add_rule_type_definition(ruletypedefinition, **kwargs):
    """
        Add a rule type definition
        Args:
            ruletype (Spyne or JSONObject object)
        Returns:
            ruletype_i (SQLAlchemy ORM Object) new rule type from DB
    """

    rule_type_i = RuleTypeDefinition()

    rule_type_i.code = ruletypedefinition.code
    rule_type_i.name = ruletypedefinition.name

    db.DBSession.add(rule_type_i)

    db.DBSession.flush()

    return rule_type_i

def get_rule_type_definitions(**kwargs):
    """
        Get all rule types
    """

    all_rule_type_definitions_i = db.DBSession.query(RuleTypeDefinition).all()

    return all_rule_type_definitions_i

def get_rule_type_definition(typecode,**kwargs):
    """
        Get a Type with the given typecode
    """
    try: 
        rule_type_definition_i = db.DBSession.query(RuleTypeDefinition).filter(RuleTypeDefinition.code==typecode).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule Type Definition {0} not found".format(typecode))

    return rule_type_definition_i

@required_perms("edit_network")
def purge_rule_type_definition(typecode, **kwargs):
    """
        Delete a rule type from the DB. Doing this will revert all existing rules to 
        having no type (rather than deleting them)
    """
    try:
        rule_type_definition_i = db.DBSession.query(RuleTypeDefinition).filter(RuleTypeDefinition.code==typecode).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule Type {0} not found.".format(typecode))

    db.DBSession.delete(rule_type_definition_i)
    
    db.DBSession.flush()

