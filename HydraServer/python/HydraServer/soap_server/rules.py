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
from spyne.model.primitive import Integer, Unicode
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Rule
from HydraServer.lib import rules

from hydra_base import HydraService

class RuleService(HydraService):

    """
        The data SOAP service
    """

    @rpc(Integer, _returns=SpyneArray(Rule))
    def get_rules(ctx, scenario_id):
        """
            Get all the rules in a scenario
        """
        scenario_rules = rules.get_rules(scenario_id, **ctx.in_header.__dict__)
        return [Rule(r) for r in scenario_rules]

    @rpc(Integer, _returns=Rule)
    def get_rule(ctx, rule_id):
        """
            Get an individual role by its ID.
        """
        rule_i = rules.get_rule(rule_id, **ctx.in_header.__dict__)
        return Rule(rule_i)

    @rpc(Integer, Rule, _returns=Rule)
    def add_rule(ctx, scenario_id, rule):
        """
            Add a rule to a given scenario
        """
        rule_i = rules.add_rule(scenario_id, rule, **ctx.in_header.__dict__)
        return Rule(rule_i)

    @rpc(Integer, SpyneArray(Rule), _returns=SpyneArray(Rule))
    def add_rules(ctx, scenario_id, rule_list):
        """
            Add a rule to a given scenario
        """
        returned_rules = []
        for rule in rule_list:
            rule_i = rules.add_rule(scenario_id, rule, **ctx.in_header.__dict__)
            returned_rules.append(Rule(rule_i))

        return returned_rules

    @rpc(Rule, _returns=Rule)
    def update_rule(ctx, rule):
        """
            Update a rule. Ensure that scenario_id is specified in the rule.
        """
        rule_i = rules.update_rule(rule, **ctx.in_header.__dict__)
        return Rule(rule_i)

    @rpc(Integer, _returns=Unicode)
    def delete_rule(ctx, rule_id):
        """
            Set the status of a rule to inactive, so it will be excluded when
            'get_rules' is called.
        """
        rules.delete_rule(rule_id, **ctx.in_header.__dict__)
        return 'OK'
   
    @rpc(Integer, _returns=Unicode)
    def activate_rule(ctx, rule_id):
        """
            Set the status of a rule to active, so it will be included when
            'get_rules' is called.
        """
        rules.activate_rule(rule_id, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=Unicode)
    def purge_rule(ctx, rule_id):
        """
            Remove a rule permanently
        """
        rules.purge_rule(rule_id, **ctx.in_header.__dict__)
        return 'OK'


   
