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
from sqlalchemy.orm.exc import NoResultFound
from hydra_base.lib.objects import JSONObject
from ..db.model import Rule, RuleTypeDefinition, RuleTypeLink, NetworkOwner, \
                       Network, Template, Project
from .. import db
from ..exceptions import HydraError, ResourceNotFoundError

from ..util.permissions import required_perms

LOG = logging.getLogger(__name__)

def _get_rule(rule_id, user_id, check_write=False):
    try:
        rule_i = db.DBSession.query(Rule).filter(Rule.id == rule_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule {0} not found".format(rule_id))

    #lazy load types
    rule_i.types

    rule_i.check_read_permission(user_id)

    if check_write is True:
        rule_i.check_write_permission(user_id)

    return rule_i

@required_perms("get_network", "get_rules")
def get_network_rules(network_id, summary=True, **kwargs):
    """
        Get all the rules within a network
        Args:
            network_id (int): ID of the resource
        Returns:
            List of Rule SQLAlchemy objects
    """
    user_id = kwargs.get('user_id')

    #just in case
    network_id = int(network_id)
    network = db.DBSession.query(Network).filter(Network.id==network_id).one()
    network.check_read_permission(user_id)

    rule_qry = db.DBSession.query(Rule).filter(Rule.status != 'X')

    #Go through the template hierarchy for all types defined on this network and extract
    #all rules associated to them.
    all_template_rules = []
    for rtype in network.types:
        if not hasattr(rtype, "template_id"):
            continue
        template = db.DBSession.query(Template).filter(Template.id==rtype.template_id).one()
        #need this to go top-bottom to apply rules from the top level down
        template_hierarchy = template.get_hierarchy().reverse()
        for current_template in template_hierarchy:
            this_template_rules = rule_qry.filter(Rule.template_id == current_template.id).all()
            all_template_rules = all_template_rules + this_template_rules


    network_rules = rule_qry.filter(Rule.network_id == network_id).all()
    all_network_rules = network_rules + all_template_rules

    #lazy load types
    if summary is False:
        for rule in all_network_rules:
            rule.types

    return all_network_rules


@required_perms("get_template", "get_rules")
def get_template_rules(template_id, summary=True, **kwargs):
    """
       Retrieve all Rules defined in a template
       args:
         <template_id>: integer (or castable) identifying a template
         <summary>: bool specifying whether type-loading should be
                    omitted
       return:
         list of Rule SQLAlchemy objects
    """
    template_id = int(template_id)
    template = db.DBSession.query(Template).filter(Template.id==template_id).one()
    rule_qry = db.DBSession.query(Rule).filter(Rule.status != 'X')

    all_template_rules = rule_qry.filter(Rule.template_id == template_id).all()
    template_hierarchy = template.get_hierarchy(**kwargs).reverse()
    if template_hierarchy is not None:
        for current_template in template_hierarchy:
            this_template_rules = rule_qry.filter(Rule.template_id == current_template.id).all()
            all_template_rules = all_template_rules + this_template_rules

    if summary is False:
        for rule in all_template_rules:
            rule.types

    return all_template_rules


@required_perms("get_project", "get_rules")
def get_project_rules(project_id, summary=True, **kwargs):
    """
       Retrieve all Rules defined in a project
       args:
         <project_id>: integer (or castable) identifying a project
         <summary>: bool specifying whether type-loading should be
                    omitted
       return:
         list of Rule SQLAlchemy objects
    """
    user_id = kwargs.get("user_id")

    project_id = int(project_id)
    project = db.DBSession.query(Project).filter(Project.id==project_id).one()
    project.check_read_permission(user_id)

    rule_qry = db.DBSession.query(Rule).filter(Rule.status != 'X')

    project_rules = rule_qry.filter(Rule.project_id == project.id).all()

    if summary is False:
        for rule in project_rules:
            rule.types

    return project_rules


@required_perms("get_rules")
def get_resource_rules(ref_key, ref_id, summary=True, **kwargs):
    """
        Get all the rules for a given resource.
        Args:
            ref_key (string): NETWORK, PROJECT, TEMPLATE
            ref_id (int): ID of the resource
        Returns:
            List of Rule SQLAlchemy objects
    """
    ref_key == ref_key.upper()

    if ref_key.upper() == 'NETWORK':
        ret_func = get_network_rules
    elif ref_key.upper() == 'PROJECT':
        ret_func = get_project_rules
    elif ref_key.upper() == 'TEMPLATE':
        ret_func = get_template_rules
    else:
        raise HydraError("Ref Key {0} not recognised.".format(ref_key))

    rules = ret_func(ref_id, summary=summary, **kwargs)

    if summary is False:
        #lazy load types
        for rule in rules:
            rule.types

    return rules


@required_perms("get_rules")
def get_rules_of_type(typecode, scenario_id=None, **kwargs):
    """
        Get all the rules for a given resource.
        Args:
            ref_key (string): NETWORK, PROJECT, TEMPLATE
            ref_id (int): ID of the resource
            scenario_id (int): Optional which filters on scenario ID also
        Returns:
            List of Rule SQLAlchemy objects
    """

    user_id = kwargs.get('user_id')

    rule_qry = db.DBSession.query(Rule).filter(Rule.status == 'A')\
                                        .join(RuleTypeLink)\
                                        .filter(RuleTypeLink.code == typecode)

    rule_qry = rule_qry.join(NetworkOwner, Rule.network_id==NetworkOwner.network_id).filter(NetworkOwner.user_id == int(user_id))

    if scenario_id is not None:
        rule_qry = rule_qry.filter(Rule.scenario_id == scenario_id)

    rules = rule_qry.all()

    return rules

@required_perms("get_rules")
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

@required_perms("add_rules")
def add_rules(rules, **kwargs):
    LOG.info("Adding %s rules."%len(rules))
    for rule in rules:
        add_rule(rule, **kwargs)

    LOG.info("Rules Added")

@required_perms("add_rules")
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
    elif rule.ref_key.upper() == 'PROJECT':
        rule_i.project_id = rule.project_id if rule.project_id else rule.ref_id
    elif rule.ref_key.upper() == 'TEMPLATE':
        rule_i.template_id = rule.template_id if rule.template_id else rule.ref_id
    else:
        raise HydraError("Ref Key {0} not recognised.".format(rule.ref_key))

    rule_i.name = rule.name
    rule_i.description = rule.description
    rule_i.value = rule.value
    rule_i.format = rule.format

    rule_i.set_types(rule.types)

    db.DBSession.add(rule_i)

    db.DBSession.flush()

    return rule_i

@required_perms("update_rules")
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
    elif rule.ref_key.upper() == 'PROJECT':
        rule_i.network_id = rule.project_id if rule.project_id else rule.ref_id
    elif rule.ref_key.upper() == 'TEMPLATE':
        rule_i.network_id = rule.template_id if rule.template_id else rule.ref_id
    else:
        raise HydraError("Ref Key {0} not recognised.".format(rule.ref_key))

    rule_i.name = rule.name
    rule_i.description = rule.description
    rule_i.format = rule.format
    rule_i.value = rule.value
    rule_i.status = rule.status

    rule_i.set_types(rule.types)

    db.DBSession.flush()

    return rule_i

@required_perms("update_rules")
def set_rule_type(rule_id, typecode, **kwargs):
    """
        Assign a rule type to a rule
        args:
            rule_id (int): THe ID of the rule to apply the type to
            typecode (string): Types do not use IDS as identifiers, rather codes.
                               Apply the type with this code to the rule
        returns:
            The updated rule (Sqlalchemy ORM Object)
    """
    user_id = kwargs.get('user_id')

    rule_i = _get_rule(rule_id, user_id, check_write=True)

    ruletypelink = RuleTypeLink()
    ruletypelink.code = typecode

    rule_i.types.append(ruletypelink)

    db.DBSession.flush()

    return rule_i

@required_perms("edit_network", "get_rules", "add_rules")
def clone_resource_rules(ref_key, ref_id, target_ref_key=None, target_ref_id=None, **kwargs):
    """
        Clone a rule
        args:
            ref_key (int): NETWORK, PROJECT, TEMPLATE
            ref_id (int): The ID of the relevant resource
            target_ref_key (string): If the rule is to be cloned into a
                                     different resource, specify the new resources type
            target_ref_id (int): If the rule is to be cloned into a different
                                 resources, specify the resource ID.

        Cloning will only occur into a different resource if both
        ref_key AND ref_id are provided. Otherwise it will maintain its
        original ref_key and ref_id.

        returns:
            list of rule ORM objects.
    """

    user_id = kwargs.get('user_id')

    resource_rules = get_resource_rules(ref_key, ref_id, user_id=user_id)

    cloned_rules = []

    for rule in resource_rules:
        cloned_rules.append(clone_rule(rule.id,
                                       target_ref_key=target_ref_key,
                                       target_ref_id=target_ref_id,
                                       user_id=user_id))

    return cloned_rules

@required_perms("edit_network", "get_rules", "add_rules")
def clone_rule(rule_id, target_ref_key=None, target_ref_id=None, **kwargs):
    """
        Clone a rule
        args:
            rule_id (int): The rule to clone
            target_ref_key (string): If the rule is to be cloned into a different
                                     resource, specify the new resources type
            target_ref_id (int): If the rule is to be cloned into a
                                 different resources, specify the resource ID.
        Cloning will only occur into a different resource if both ref_key AND
        ref_id are provided. Otherwise it will maintain its original ref_key and ref_id.

        return:
            SQLAlchemy ORM object
    """


    user_id = kwargs.get('user_id')

    rule_i = _get_rule(rule_id, user_id, check_write=True)

    #lazy load types
    rule_i.types

    rule_j = JSONObject(rule_i)
    rule_j.owners = rule_i.get_owners()

    #Unset the reference ID for the rule in case the target resource type
    #has changed, then apply the new ref_key and ref_id
    if target_ref_key is not None and target_ref_id is not None:
        rule_j.network_id = None
        rule_j.project_id = None
        rule_j.template_id = None
        rule_j.ref_key = target_ref_key
        if target_ref_key == 'NETWORK':
            rule_j.network_id = target_ref_id
        elif target_ref_key == 'PROJECT':
            rule_j.project_id = target_ref_id
        elif target_ref_key == 'TEMPLATE':
            rule_j.template_id = target_ref_id

    cloned_rule = add_rule(rule_j, **kwargs)

    return cloned_rule

@required_perms("update_rules")
def delete_rule(rule_id, **kwargs):
    """
        Set the status of a rule to 'X'
        args:
            rule_id: The id to update
        returns:
            None
    """

    user_id = kwargs.get('user_id')
    rule_i = _get_rule(rule_id, user_id, check_write=True)

    rule_i.status = 'X'

    db.DBSession.flush()

@required_perms("update_rules")
def activate_rule(rule_id, **kwargs):
    """
        Set the status of a rule to 'A'
        args:
            rule_id: The id to update
        returns:
            None
    """

    user_id = kwargs.get('user_id')
    rule_i = _get_rule(rule_id, user_id, check_write=True)

    rule_i.status = 'A'

    db.DBSession.flush()

@required_perms("delete_rules")
def purge_rule(rule_id, **kwargs):
    """
        Remove a rule from the DB permanently
        args:
            rule_id: The id to purge
        returns:
            None
    """
    user_id = kwargs.get('user_id')

    rule_i = _get_rule(rule_id, user_id, check_write=True)

    db.DBSession.delete(rule_i)
    db.DBSession.flush()

@required_perms("update_rules")
def add_rule_type_definition(ruletypedefinition, **kwargs):
    """
        Add a rule type definition
        Args:
            ruletypedefinition (Spyne or JSONObject object). This looks like:
                                {
                                  'name': 'My Rule Type',
                                  'code': 'my_rule_type'
                                }
        Returns:
            ruletype_i (SQLAlchemy ORM Object) new rule type from DB
    """
    rule_type_i = RuleTypeDefinition()

    rule_type_i.code = ruletypedefinition.code
    rule_type_i.name = ruletypedefinition.name

    existing_rtd = db.DBSession.query(RuleTypeDefinition).filter(
        RuleTypeDefinition.code == ruletypedefinition.code).first()

    if existing_rtd is None:
        db.DBSession.add(rule_type_i)

        db.DBSession.flush()

        return rule_type_i
    else:
        return existing_rtd

@required_perms("get_rules")
def get_rule_type_definitions(**kwargs):
    """
        Get all rule types
        args: None
        returns:
            List of SQLAlchemy objects.
    """

    all_rule_type_definitions_i = db.DBSession.query(RuleTypeDefinition).all()

    return all_rule_type_definitions_i

@required_perms("get_rules")
def get_rule_type_definition(typecode, **kwargs):
    """
        Get a Type with the given typecode
        args:
            typecode: Type definitions do not used IDs, rather codes. This is to code to retrieve
        returns:
            SQLAlchemy ORM Object
        raises:
            ResourceNotFoundError if the rule type definintion code does not exist
    """
    try:
        rule_type_definition_i = db.DBSession.query(RuleTypeDefinition)\
            .filter(RuleTypeDefinition.code == typecode).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule Type Definition {0} not found".format(typecode))

    return rule_type_definition_i

@required_perms("update_rules")
def purge_rule_type_definition(typecode, **kwargs):
    """
        Delete a rule type from the DB. Doing this will revert all existing rules to
        having no type (rather than deleting them)
        args:
            typecode: Type definitions do not used IDs, rather codes. This is to code to purge
        returns:
             None
        raises:
            ResourceNotFoundError if the rule type definintion code does not exist
    """
    try:
        rule_type_definition_i = db.DBSession.query(RuleTypeDefinition)\
            .filter(RuleTypeDefinition.code == typecode).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule Type {0} not found.".format(typecode))

    db.DBSession.delete(rule_type_definition_i)

    db.DBSession.flush()
