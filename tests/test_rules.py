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
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from . import server
import datetime
import copy
import json
import hydra_base
from .fixtures import *
from hydra_base.util import testing as util
from hydra_base.exceptions import ResourceNotFoundError
import pytest
from hydra_base.exceptions import HydraError, PermissionError
from hydra_base.lib.objects import JSONObject, Dataset
from hydra_base.util.hydra_dateutil import timestamp_to_ordinal

import logging
log = logging.getLogger(__name__)

class TestRules:

    def add_rule(self, client, network, name='A Test Rule', text='e=mc^2', scenario_id=None, ref_key=None, ref_id=None, types=[]):
        """
            A utility function which creates a rule and associates it
            with either a resource type, resource instance or resource
            instance / scenario pair. 
        """

        rule = JSONObject({
            'name'        : name,
            'value'       : text,
            'scenario_id' : scenario_id,
            'ref_key'     : 'NETWORK' if ref_key is None else ref_key,
            'ref_id'      : network.id if ref_id is None else ref_id,
            'types'       : [{'code':typecode} for typecode in types]
        })

        new_rule_j = JSONObject(client.add_rule(rule))

        return new_rule_j

    def test_add_rule_type_definition(self, session, client):
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))

        rule_type_j = client.get_rule_type_definition('a_new_rule')
        
        assert rule_type_j.code == 'a_new_rule'

    def test_get_rule_type_definitions(self, session, client):
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))
    
        rule_types_j = client.get_rule_type_definitions()

        assert len(rule_types_j) == 2

    def test_get_rule_type_definition(self, session, client):
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
    
        rule_type_j = client.get_rule_type_definition('a_new_rule')

        assert rule_type_j.code == 'a_new_rule'

    def test_get_rule_by_id(self, session, client, network_with_data):
        
        new_rule_j = self.add_rule(client, network_with_data)

        rule_j = JSONObject(client.get_rule(new_rule_j.id))

        assert rule_j.name == 'A Test Rule'
        assert rule_j.value == 'e=mc^2'
        
        with pytest.raises(ResourceNotFoundError):
            rule_j = JSONObject(client.get_rule(2))
        
        #temporarily set the client user_id to a different (non-admin) user
        client.user_id = 4 #4 is not an admin
        with pytest.raises(PermissionError):
            client.get_rule(new_rule_j.id)

    def test_clone_network_with_rules(self, session, client, network_with_data):
        
        net_rule_j = self.add_rule(client, network_with_data)

        #add a rule to a node to ensure cloning of node-level rules are also working
        ##sort the nodes here to ensure we can identify the matching node in the cloned network
        node_rule_no_scenario_j = self.add_rule(client,
                                        network_with_data,
                                        ref_key='NODE',
                                        ref_id=sorted(network_with_data.nodes, key=lambda x: x.name)[0].id,
                                        text = "Node rule no scenario")

        #add a rule to a node AND a scenario to ensure cloning of node-level rules are working with scenarios. 
        #The new rule should have the ID of the new scenario
        node_rule_with_scenario_j = self.add_rule(client,
                                        network_with_data,
                                        ref_key='NODE',
                                        ref_id=sorted(network_with_data.nodes, key=lambda x: x.name)[0].id,
                                        scenario_id=network_with_data.scenarios[0].id,
                                        text = 'Scenario Node Rule')
        #sanity check
        assert node_rule_with_scenario_j.scenario_id == network_with_data.scenarios[0].id

        cloned_network_id = client.clone_network(network_with_data.id)

        cloned_network = client.get_network(cloned_network_id)

        assert len(client.get_resource_rules('NETWORK', cloned_network.id)) == 1 
        assert client.get_resource_rules('NETWORK', cloned_network.id)[0].value == net_rule_j.value;

        #sorted the nodes here to ensure we identified the matching node from the original network
        assert len(client.get_resource_rules('NODE', sorted(cloned_network.nodes, key=lambda x: x.name)[0].id)) == 2
        assert client.get_resource_rules('NODE', sorted(cloned_network.nodes, key=lambda x: x.name)[0].id)[0].scenario_id == None
        assert client.get_resource_rules('NODE', sorted(cloned_network.nodes, key=lambda x: x.name)[0].id)[0].value == node_rule_no_scenario_j.value

        #sorted the nodes here to ensure we identified the matching node from the original network
        assert client.get_resource_rules('NODE', sorted(cloned_network.nodes, key=lambda x: x.name)[0].id)[1].scenario_id == cloned_network.scenarios[0].id 
        assert client.get_resource_rules('NODE', sorted(cloned_network.nodes, key=lambda x: x.name)[0].id)[1].value == node_rule_with_scenario_j.value

    def test_get_rules_by_type(self, session, client, network_with_data):
        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))
        
        scenario_id = network_with_data.scenarios[0].id

        #Create 3 rules, 2 of type A and 1 of type B
        self.add_rule(client, network_with_data, name="Test1", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test2", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test3", types=[ruletype_A_j.code, ruletype_B_j.code], scenario_id=scenario_id)
        self.add_rule(client, network_with_data, name="Rule Type B", types=[ruletype_B_j.code])
        
        #Get all the rules of type A, of which there should be 2
        rules_of_type = client.get_rules_of_type(ruletype_A_j.code)
        rules_of_type_in_scenario = client.get_rules_of_type(ruletype_A_j.code, scenario_id=scenario_id)

        assert len(rules_of_type) == 3
        assert len(rules_of_type_in_scenario) == 1

    def test_add_rule_type(self, session, client, network_with_data):
        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))
        
        scenario_id = network_with_data.scenarios[0].id

        #Create 3 rules, 2 of type A and 1 of type B
        self.add_rule(client, network_with_data, name="Test1", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test2", types=[ruletype_A_j.code])
        rule3 = self.add_rule(client, network_with_data, name="Test3", types=[ruletype_A_j.code], scenario_id=scenario_id)
        
        #Get all the rules of type A, of which there should be 2
        rules_of_type = client.get_rules_of_type(ruletype_A_j.code)
        rules_of_type_in_scenario = client.get_rules_of_type(ruletype_A_j.code, scenario_id=scenario_id)

        assert len(rules_of_type) == 3
        assert len(rules_of_type_in_scenario) == 1


        client.set_rule_type(rule3.id, ruletype_B_j.code)

        rules_of_type_b = client.get_rules_of_type(ruletype_B_j.code)
        assert len(rules_of_type_b) == 1

    def test_add_rule(self, session, client, network_with_data):
        rulename = 'Added Rule'
        ruletext = 'e=mc^3'#yes this is delibrate, so it's different to the default
        new_rule_j = self.add_rule(client, network_with_data, name=rulename, text=ruletext)
        
        assert new_rule_j.id is not None
        assert new_rule_j.name == rulename
        assert new_rule_j.value == ruletext

    def test_clone_rule(self, session, client, network_with_data):

        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))

        rulename = 'Added Rule'
        ruletext = 'e=mc^3'#yes this is delibrate, so it's different to the default
        new_rule_j = self.add_rule(client, network_with_data, name=rulename, text=ruletext, types=[ruletype_A_j.code, ruletype_B_j.code])
        
        assert new_rule_j.id is not None
        assert new_rule_j.name == rulename
        assert new_rule_j.value == ruletext

        cloned_rule = client.clone_rule(new_rule_j.id)

        assert cloned_rule.name == new_rule_j.name
        assert cloned_rule.value == new_rule_j.value
        assert cloned_rule.id is not None
        assert cloned_rule.id != new_rule_j.id
        assert len(cloned_rule.types) == 2
        assert [t.code for t in new_rule_j.types] == [t.code for t in cloned_rule.types]


    def test_update_rule(self, session, client, network_with_data):

        typecode = 'a_new_rule_type'
        typecode1 = 'a_new_rule_type_1'

        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':typecode}))
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':typecode1}))

        rulename = 'Added Rule'
        ruletext = 'e=mc^3'#yes this is delibrate, so it's different to the default

        new_rule_j = self.add_rule(client, network_with_data, name=rulename, text=ruletext, types=[typecode])

        assert len(new_rule_j.types) == 1

        new_rule_j.name = 'Updated Rule'
        new_rule_j.value    = 'e=mc2' #fix the error
        new_rule_j.types.append(JSONObject({'code':typecode1}))
        
        client.update_rule(new_rule_j)

        updated_rule_j = client.get_rule(new_rule_j.id)

        assert updated_rule_j.name == 'Updated Rule'
        assert updated_rule_j.value == 'e=mc2'
        assert updated_rule_j.scenario_id is None
        assert len(updated_rule_j.types) == 2
        
        #update the rule again, setting it on a scenario
        updated_rule_j.scenario_id = network_with_data.scenarios[0].id

        client.update_rule(updated_rule_j)

        updated_rule_j_2 = client.get_rule(updated_rule_j.id)

        assert updated_rule_j_2.scenario_id == network_with_data.scenarios[0].id

    def test_add_rule_with_type(self, session, client, network_with_data):
        
        typecode = 'a_new_rule_type'
        typecode1 = 'a_new_rule_type_1'

        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':typecode}))
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':typecode1}))

        rulename = 'Added Rule'
        ruletext = 'e=mc^3'#yes this is delibrate, so it's different to the default
        new_rule_j = self.add_rule(client, network_with_data, name=rulename, text=ruletext, types=[typecode, typecode1])
        
        assert new_rule_j.id is not None
        assert new_rule_j.name == rulename
        assert new_rule_j.value == ruletext
        assert len(new_rule_j.types) == 2


        assert new_rule_j.id in [r.id for r in client.get_rules_of_type(typecode)]
            
    
    def test_add_rule_to_scenario(self, session, client, network_with_data):

        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))

        scenario_id = network_with_data.scenarios[0].id

        new_rule_a_j = self.add_rule(client, network_with_data, name='Added Rule', text='e=mc^3')
        new_rule_b_j = self.add_rule(client, network_with_data, name='Added Rule 1', text='e=mc^4')
        new_rule_c_j = self.add_rule(client, network_with_data, scenario_id=scenario_id, name='Added Rule 2', text='e=mc^5')

        scenario_rules = client.get_scenario_rules(scenario_id)
        resource_rules = client.get_resource_rules('NETWORK', network_with_data.id)
        resource_rules_with_scenario = client.get_resource_rules('NETWORK', network_with_data.id, scenario_id=scenario_id)

        assert len(scenario_rules) == 1
        assert scenario_rules[0].name == new_rule_c_j.name
        assert len(resource_rules) == 3
        assert sorted([r.name for r in resource_rules]) == sorted([r.name for r in (new_rule_a_j, new_rule_b_j, new_rule_c_j)])
        assert len(resource_rules_with_scenario) == 1
        assert resource_rules_with_scenario[0].name == new_rule_c_j.name

    def test_add_rule_to_node(self, session, client, network_with_data):
        scenario_id = network_with_data.scenarios[0].id
        node_id = network_with_data.nodes[0].id

        new_rule_a_j = self.add_rule(client, network_with_data, ref_key='NODE', ref_id=node_id, name='Added Rule', text='e=mc^3')
        new_rule_b_j = self.add_rule(client, network_with_data, ref_key='NODE', ref_id=node_id, name='Added Rule 1', text='e=mc^4')
        new_rule_c_j = self.add_rule(client, network_with_data, ref_key='NODE', ref_id=node_id, scenario_id=scenario_id, name='Added Rule 2', text='e=mc^5')

        scenario_rules = client.get_scenario_rules(scenario_id)
        all_resource_rules = client.get_resource_rules('NODE', node_id)
        resource_rules_with_scenario = client.get_resource_rules('NODE', node_id, scenario_id=scenario_id)

        assert len(scenario_rules) == 1
        assert scenario_rules[0].name == new_rule_c_j.name
        assert len(all_resource_rules) == 3
        assert sorted([r.name for r in all_resource_rules]) == sorted([r.name for r in (new_rule_a_j, new_rule_b_j, new_rule_c_j)])
        assert len(resource_rules_with_scenario) == 1
        assert resource_rules_with_scenario[0].name == new_rule_c_j.name
        

    def test_delete_rule(self, session, client, network_with_data):

        new_rule_j = self.add_rule(client, network_with_data)

        #prove it exists. If not, it would throw an exception
        client.get_rule(new_rule_j.id)

        assert new_rule_j.id in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]
        
        client.delete_rule(new_rule_j.id)

        assert new_rule_j.id not in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]
    
    def test_activate_rule(self, session, client, network_with_data):

        new_rule_j = self.add_rule(client, network_with_data)

        #prove it exists. If not, it would throw an exception
        client.get_rule(new_rule_j.id)
        
        assert new_rule_j.id in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]
        
        client.delete_rule(new_rule_j.id)

        assert new_rule_j.id not in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]

        client.activate_rule(new_rule_j.id)

        #prove it exists again. If not, it would throw an exception
        client.get_rule(new_rule_j.id)
        
        assert new_rule_j.id in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]

        
    def test_purge_rule(self, session, client, network_with_data):

        new_rule_j = self.add_rule(client, network_with_data)

        #prove it exists. If not, it would throw an exception
        client.get_rule(new_rule_j.id)

        assert new_rule_j.id in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]
        
        client.purge_rule(new_rule_j.id)

        with pytest.raises(HydraError):
            client.get_rule(new_rule_j.id)

    def test_delete_rule_type(self, session, client, network_with_data):
        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))
        
        scenario_id = network_with_data.scenarios[0].id

        #Create 3 rules, 2 of type A and 1 of type B
        self.add_rule(client, network_with_data, name="Test1", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test2", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test3", types=[ruletype_A_j.code, ruletype_B_j.code], scenario_id=scenario_id)
        self.add_rule(client, network_with_data, name="Rule Type B", types=[ruletype_B_j.code])
        
        #Get all the rules of type A, of which there should be 2
        rules_of_type = client.get_rules_of_type(ruletype_A_j.code)
        rules_of_type_in_scenario = client.get_rules_of_type(ruletype_A_j.code, scenario_id=scenario_id)

        assert len(rules_of_type) == 3
        assert len(rules_of_type_in_scenario) == 1

        client.purge_rule_type('a_new_rule')

        assert len(client.get_rules_of_type(ruletype_A_j.code)) == 0

        #check all the rules are still there
        assert len(client.get_resource_rules('NETWORK', network_with_data.id)) == 4
