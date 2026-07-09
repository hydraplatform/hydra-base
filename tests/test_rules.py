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

import copy
import datetime
import json
import pytest

from sqlalchemy.exc import IntegrityError

import hydra_base
from hydra_base.exceptions import ResourceNotFoundError
from hydra_base.exceptions import HydraError, PermissionError
from hydra_base.lib.objects import JSONObject, Dataset
from hydra_base.util.hydra_dateutil import timestamp_to_ordinal

from .templates.test_templates import template, template_json_object

import logging
log = logging.getLogger(__name__)

class TestRules:

    def add_rule(self, client, target=None, name='A Test Rule', text='e=mc^2',
                 ref_key=None, ref_id=None, types=[]):
        """
            A utility function which creates a rule and associates it
            with either a resource type, resource instance or resource
            instance / scenario pair.
        """

        rule = JSONObject({
            'name'        : name,
            'value'       : text,
            'ref_key'     : "NETWORK" if ref_key is None else ref_key,
            'ref_id'      : target.id if ref_id is None else ref_id,
            'types'       : [{'code':typecode} for typecode in types]
        })

        c_rule = client.add_rule(rule=rule)
        new_rule_j = JSONObject(c_rule)
        return new_rule_j

    def test_add_rule_type_definition(self, client):
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))

        rule_type_j = client.get_rule_type_definition('a_new_rule')

        assert rule_type_j.code == 'a_new_rule'

    def test_get_rule_type_definitions(self, client):
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))

        rule_types_j = client.get_rule_type_definitions()

        assert len(rule_types_j) == 2

    def test_get_rule_type_definition(self, client):
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))

        rule_type_j = client.get_rule_type_definition('a_new_rule')

        assert rule_type_j.code == 'a_new_rule'

    def test_get_rule_by_id(self, client, network_with_data):

        new_rule_j = self.add_rule(client, network_with_data)

        rule_j = JSONObject(client.get_rule(new_rule_j.id))

        assert rule_j.name == 'A Test Rule'
        assert rule_j.value == 'e=mc^2'

        #2nd error handle is to cover errors coming from the server
        with pytest.raises((ResourceNotFoundError, HydraError)):
            rule_j = JSONObject(client.get_rule(2))

        #temporarily set the client user_id to a different (non-admin) user
        client.user_id = 5 #5 is not an admin and is not an owner of the rule
        with pytest.raises((PermissionError, HydraError)):
            client.get_rule(new_rule_j.id)

    def test_clone_network_with_rules(self, client, network_with_data):

        net_rule_j = self.add_rule(client, network_with_data)

        cloned_network_id = client.clone_network(network_with_data.id)

        cloned_network = client.get_network(cloned_network_id)

        network_rules = client.get_resource_rules('NETWORK', cloned_network.id)
        assert len(network_rules) == 1
        assert network_rules[0].value == net_rule_j.value;

    def test_share_network_with_rules(self, client, network_with_data):

        net_rule_j = self.add_rule(client, network_with_data)

        cloned_network_id = client.clone_network(network_with_data.id)

        cloned_network = client.get_network(cloned_network_id)
        network_rules = client.get_resource_rules('NETWORK', cloned_network.id)

        assert len(network_rules) == 1
        assert network_rules[0].value == net_rule_j.value

        client.share_network(cloned_network.id, ['UserC'], 'Y', 'Y')
        client.user_id = pytest.user_c.id
        client.get_network(cloned_network_id)

        #User c should see the same rules
        user_c_rules = client.get_resource_rules('NETWORK', cloned_network.id)
        assert len(user_c_rules) == 1
        assert user_c_rules[0].value == net_rule_j.value

    def test_get_rules_by_type(self, client, network_with_data):
        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule',
                                                                   'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1',
                                                                   'code':'a_new_rule_1'}))

        scenario_id = network_with_data.scenarios[0].id

        #Create 3 rules, 2 of type A and 1 of type B
        self.add_rule(client, network_with_data, name="Test1", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test2", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test3", types=[ruletype_A_j.code, ruletype_B_j.code])
        self.add_rule(client, network_with_data, name="Rule Type B", types=[ruletype_B_j.code])

        #Get all the rules of type A, of which there should be 2
        rules_of_type = client.get_rules_of_type(ruletype_A_j.code)

        assert len(rules_of_type) == 3

    def test_add_rule_type(self, client, network_with_data):
        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))

        scenario_id = network_with_data.scenarios[0].id

        #Create 3 rules, 2 of type A and 1 of type B
        self.add_rule(client, network_with_data, name="Test1", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test2", types=[ruletype_A_j.code])
        rule3 = self.add_rule(client, network_with_data, name="Test3", types=[ruletype_A_j.code])

        #Get all the rules of type A, of which there should be 2
        rules_of_type = client.get_rules_of_type(ruletype_A_j.code)

        assert len(rules_of_type) >= 3
        #CHeck that the added nodes are indeed present
        assert {'Test1', 'Test2', 'Test3'}.issubset({r.name for r in rules_of_type})

        client.set_rule_type(rule3.id, ruletype_B_j.code)

        rules_of_type_b = client.get_rules_of_type(ruletype_B_j.code)
        assert len(rules_of_type_b) >= 1
        assert 'Test3' in [r.name for r in rules_of_type_b]

    def test_add_rule1(self, client, network_with_data):
        rule_network = client.get_network(network_with_data.id)

        #Sharae the network with user A to thest the sharing feature.
        client.share_network(network_with_data.id, ['UserA'], 'Y', 'Y')

        rulename = 'Added Rule'
        ruletext = 'e=mc^3'#yes this is delibrate, so it's different to the default
        new_rule_j = self.add_rule(client, network_with_data, name=rulename, text=ruletext)

        assert new_rule_j.id is not None
        assert new_rule_j.name == rulename
        assert new_rule_j.value == ruletext

        # Rule names must be unique within a Network
        with pytest.raises(IntegrityError):
            duplicate_rule = self.add_rule(client, network_with_data, name=rulename, text=ruletext)

        #sanity check to ensure we're actually testing that the ownership functionality
        #is testing correctly
        rule_network = client.get_network(network_with_data.id)
        assert len(rule_network.owners) == len(new_rule_j.owners)


    def test_clone_rule(self, client, network_with_data, projectmaker):

        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))

        rulename = 'Added Rule'
        ruletext = 'e=mc^3'#yes this is delibrate, so it's different to the default
        new_rule_j = self.add_rule(client,
                                   target=network_with_data,
                                   name=rulename,
                                   text=ruletext,
                                   types=[ruletype_A_j.code, ruletype_B_j.code])

        assert new_rule_j.id is not None
        assert new_rule_j.name == rulename
        assert new_rule_j.value == ruletext

        project = projectmaker.create('Target Network Parent')
        network = JSONObject()
        network.name = f"New network {datetime.datetime.now()}"
        network.description = "Clone rule target network"
        network.project_id = project.id

        target_network = client.add_network(network)

        # Clone to rule to new network
        cloned_rule = client.clone_rule(new_rule_j.id, target_ref_key="NETWORK", target_ref_id=target_network.id)

        assert cloned_rule.name == new_rule_j.name
        assert cloned_rule.value == new_rule_j.value
        assert cloned_rule.id is not None
        assert cloned_rule.id != new_rule_j.id
        assert len(cloned_rule.types) == 2
        assert [t.code for t in new_rule_j.types] == [t.code for t in cloned_rule.types]

        # Clone rule to new project
        src_proj = projectmaker.create("Rule Source Project")
        dest_proj = projectmaker.create("Rule Destination Project")


        proj_rulename = "Project Rule"
        proj_ruletext = "e=mc^3"
        proj_rule = self.add_rule(client,
                                  name=proj_rulename,
                                  text=proj_ruletext,
                                  types=[ruletype_A_j.code],
                                  ref_key="PROJECT",
                                  ref_id=project.id)

        proj_cloned_rule = client.clone_rule(
                             proj_rule.id,
                             target_ref_key="PROJECT",
                             target_ref_id=dest_proj.id)

        assert proj_cloned_rule.name == proj_rule.name
        assert proj_cloned_rule.value == proj_rule.value
        assert proj_cloned_rule.id is not None
        assert proj_cloned_rule.id != proj_rule.id
        assert len(proj_cloned_rule.types) == 1


    def test_project_rule(self, client, projectmaker):
        project = projectmaker.create("Rule Parent Project")
        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        rulename = "Rule 001"
        ruletext = "int(1)"
        new_rule_j = self.add_rule(client,
                                   target=project,
                                   ref_key="PROJECT",
                                   ref_id=project.id,
                                   name=rulename,
                                   text=ruletext,
                                   types=[ruletype_A_j.code])

        assert new_rule_j.id is not None
        assert new_rule_j.name == rulename
        assert new_rule_j.value == ruletext

        # Rule names must be unique within a Project
        with pytest.raises(IntegrityError):
            duplicate_rule = self.add_rule(client,
                                           target=project,
                                           ref_key="PROJECT",
                                           ref_id=project.id,
                                           name=rulename,
                                           text=ruletext,
                                           types=[ruletype_A_j.code])

        ret_rules = client.get_project_rules(project.id)

        assert ret_rules[0].id == new_rule_j.id
        assert ret_rules[0].name == new_rule_j.name
        assert ret_rules[0].value == new_rule_j.value

        res_ret_rules = client.get_resource_rules("PROJECT", project.id)

        assert res_ret_rules[0].id == new_rule_j.id
        assert res_ret_rules[0].name == new_rule_j.name
        assert res_ret_rules[0].value == new_rule_j.value

    def test_update_rule(self, client, network_with_data):

        typecode = 'a_new_rule_type'
        typecode1 = 'a_new_rule_type_1'

        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':typecode}))
        client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':typecode1}))

        rulename = 'Added Rule'
        ruletext = 'e=mc^3'#yes this is delibrate, so it's different to the default

        new_rule_j = self.add_rule(client, network_with_data, name=rulename, text=ruletext, types=[typecode])

        new_rule_j = client.get_rule(new_rule_j.id) #do this to get all the DB server defaults

        assert len(new_rule_j.types) == 1

        new_rule_j.name = 'Updated Rule'
        new_rule_j.format = 'text'
        new_rule_j.value    = 'e=mc2' #fix the error
        new_rule_j.types.append(JSONObject({'code':typecode1}))

        client.update_rule(new_rule_j)

        updated_rule_j = client.get_rule(new_rule_j.id)

        assert updated_rule_j.name == 'Updated Rule'
        assert updated_rule_j.value == 'e=mc2'
        assert updated_rule_j.format == 'text'
        assert updated_rule_j.scenario_id is None
        assert len(updated_rule_j.types) == 2


    def test_add_rule_with_type(self, client, network_with_data):

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

    def test_delete_rule(self, client, network_with_data):

        new_rule_j = self.add_rule(client, network_with_data)

        #prove it exists. If not, it would throw an exception
        client.get_rule(new_rule_j.id)

        assert new_rule_j.id in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]

        client.delete_rule(new_rule_j.id)

        assert new_rule_j.id not in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]

    def test_activate_rule(self, client, network_with_data):

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


    def test_purge_rule(self, client, network_with_data):
        new_rule_j = self.add_rule(client, network_with_data)

        #prove it exists. If not, it would throw an exception
        client.get_rule(new_rule_j.id)

        assert new_rule_j.id in [r.id for r in client.get_resource_rules('NETWORK', network_with_data.id)]

        client.purge_rule(new_rule_j.id)

        with pytest.raises(HydraError):
            client.get_rule(new_rule_j.id)

    def test_delete_rule_type(self, client, network_with_data):
        ruletype_A_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule', 'code':'a_new_rule'}))
        ruletype_B_j = client.add_rule_type_definition(JSONObject({'name':'A new Rule 1', 'code':'a_new_rule_1'}))

        scenario_id = network_with_data.scenarios[0].id

        #Create 3 rules, 2 of type A and 1 of type B
        self.add_rule(client, network_with_data, name="Test1", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Test2", types=[ruletype_A_j.code])
        self.add_rule(client, network_with_data, name="Rule Type B", types=[ruletype_B_j.code])

        #Get all the rules of type A, of which there should be 2
        rules_of_type = client.get_rules_of_type(ruletype_A_j.code)

        assert len(rules_of_type) >= 2
        #Check that the added nodes are indeed present
        assert {'Test1', 'Test2'}.issubset({r.name for r in rules_of_type})

        client.purge_rule_type_definition('a_new_rule')

        assert len(client.get_rules_of_type(ruletype_A_j.code)) == 0

        #check all the rules are still there
        assert len(client.get_resource_rules('NETWORK', network_with_data.id)) == 3

    def test_template_rules(self, client, template_json_object):
        rulename = "Rule 001"
        ruletext = "int(1)"
        new_rule = self.add_rule(client,
                                 target=template_json_object,
                                 ref_key="TEMPLATE",
                                 ref_id=template_json_object.id,
                                 name=rulename,
                                 text=ruletext)

        # Has Rule been added and returned correctly?
        assert new_rule.id is not None
        assert new_rule.name == rulename
        assert new_rule.value == ruletext

        # Rule names must be unique within a Template
        with pytest.raises(IntegrityError):
            duplicate_rule = self.add_rule(client,
                                           target=template_json_object,
                                           ref_key="TEMPLATE",
                                           ref_id=template_json_object.id,
                                           name=rulename,
                                           text=ruletext)

        # Is Rule added to and retrievable from correct Template?
        ret_rules = client.get_template_rules(template_json_object.id)

        assert len(ret_rules) == 1
        assert ret_rules[0].id == new_rule.id
        assert ret_rules[0].name == new_rule.name
        assert ret_rules[0].value == new_rule.value

    def test_rule_permissions(self, client, network_with_data):
        non_admin_user_id = 5
        admin_user_id = 1
        rule = self.add_rule(client, network_with_data)

        # Rule can be retrieved by admin
        ret_rule = client.get_rule(rule.id)

        # But not by non-admin user
        client.user_id = non_admin_user_id
        roles = client.get_user_roles(non_admin_user_id)
        for role in roles:
            assert role.code != "admin"
        with pytest.raises((PermissionError, HydraError)):
            no_read_rule = client.get_rule(rule.id)

        non_admin_rule = self.add_rule(client, network_with_data, name="non_admin_rule")

        # However admin user can read non-admin rule
        client.user_id = admin_user_id
        roles = client.get_user_roles(admin_user_id)
        rolecode_set = set(r.code for r in roles)
        assert "admin" in rolecode_set
        admin_read_rule = client.get_rule(non_admin_rule.id)
