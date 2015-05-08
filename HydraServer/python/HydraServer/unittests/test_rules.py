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

import server
import logging
from suds import WebFault
log = logging.getLogger(__name__)

class RuleTest(server.SoapServerTest):
    """
        Test for rules
    """

    def test_add_rule(self):
        net = self.create_network_with_data()

        s = net['scenarios'].Scenario[0]
        node = net.nodes.Node[0]

        rule = dict(
            name = 'My rule',
            description = "Rule for node in sceanrio",
            ref_key = 'NODE',
            ref_id  = node.id,
            text    = "I am a rule"
        )

        new_rule = self.client.service.add_rule(s.id, rule)
        assert new_rule.id is not None
        assert new_rule.name        == rule['name']
        assert new_rule.description == rule['description']
        assert new_rule.ref_key     == rule['ref_key']
        assert new_rule.ref_id      == rule['ref_id']
        assert new_rule.text        == rule['text']

        scenario_rules = self.client.service.get_rules(s.id)

        assert len(scenario_rules) == 1

    def test_get_rule(self):
        net = self.create_network_with_data()

        s = net['scenarios'].Scenario[0]
        node = net.nodes.Node[0]

        rule = dict(
            name = 'My rule',
            description = "Rule for node in sceanrio",
            ref_key = 'NODE',
            ref_id  = node.id,
            text    = "I am a rule"
        )

        new_rule = self.client.service.add_rule(s.id, rule)
        retrieved_rule = self.client.service.get_rule(new_rule.id)
        assert str(new_rule) == str(retrieved_rule)

    def test_delete_rule(self):
        net = self.create_network_with_data()

        s = net['scenarios'].Scenario[0]
        node = net.nodes.Node[0]

        rule = dict(
            name = 'My rule',
            description = "Rule for node in sceanrio",
            ref_key = 'NODE',
            ref_id  = node.id,
            text    = "I am a rule"
        )

        new_rule = self.client.service.add_rule(s.id, rule)
        retrieved_rule = self.client.service.get_rule(new_rule.id)
        assert str(new_rule) == str(retrieved_rule)

        self.client.service.delete_rule(new_rule.id)

        scenario_rules = self.client.service.get_rules(s.id)
        assert len(scenario_rules) == 0
        
        self.client.service.activate_rule(new_rule.id)

        scenario_rules = self.client.service.get_rules(s.id)
        assert len(scenario_rules) == 1


        self.client.service.delete_rule(new_rule.id)
        self.client.service.purge_rule(new_rule.id)
        scenario_rules = self.client.service.get_rules(s.id)
        assert len(scenario_rules) == 0
        self.assertRaises(WebFault, self.client.service.get_rule, new_rule.id)

if __name__ == '__main__':
    server.run()
