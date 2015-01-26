from HydraServer.db.model import Rule
from HydraServer.db import DBSession
from HydraLib.HydraException import HydraError, ResourceNotFoundError
from sqlalchemy.orm.exc import NoResultFound

def _get_rule(rule_id):
    try:
        rule_i = DBSession.query(Rule).filter(Rule.rule_id == rule_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Rule %s not found"%rule_id)
    return rule_i

def get_rules(scenario_id, **kwargs):
    """
        Get all the rules for a given scenario.
    """
    rules = DBSession.query(Rule).filter(Rule.scenario_id==scenario_id, Rule.status=='A').all()

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
    rule_i.rule_name   = rule.name
    rule_i.rule_description = rule.description

    rule_i.rule_text = rule.text

    DBSession.add(rule_i)
    DBSession.flush()

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
    rule_i.rule_name   = rule.name
    rule_i.rule_description = rule.description

    rule_i.rule_text = rule.text

    DBSession.flush()

    return rule_i

def delete_rule(rule_id, **kwargs):

    rule_i = _get_rule(rule_id)

    rule_i.status = 'X'

    DBSession.flush()

def activate_rule(rule_id, **kwargs):
    rule_i = _get_rule(rule_id)

    rule_i.status = 'A'

    DBSession.flush()

def purge_rule(rule_id, **kwargs):
    rule_i = _get_rule(rule_id)
    
    DBSession.delete(rule_i)
    DBSession.flush()
