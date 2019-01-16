#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2019 University of Manchester
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

import hydra_base as hb
from hydra_base.exceptions import HydraError
from hydra_base.lib.objects import JSONObject
from fixtures import *
import pytest
import util
import logging
log = logging.getLogger(__name__)

class TestNotes():
    """
        Test for working with notes.
    """


    @pytest.fixture()
    def test_add_note(self):
        """
        """
        net = self.create_network_with_data()

        s = net['scenarios'].Scenario[0]
        node = net.nodes.Node[0]
        link = net.links.Link[0]
        grp = net.resourcegroups.ResourceGroup[0]

        note = dict(
            text    = "I am a note"
        )

        n_note = hb.add_node_note(node.id, note)
        l_note = hb.add_link_note(link.id, note)
        s_note = hb.add_scenario_note(s.id, note)
        g_note = hb.add_resourcegroup_note(grp.id, note)
        net_note = hb.add_network_note(net.id, note)



        assert n_note.id          is not None
        assert n_note.ref_key     == 'NODE'
        assert n_note.ref_id      == node.id
        assert n_note.text        == note['text']

        node_notes = hb.get_node_notes(node.id)
        assert len(node_notes) == 1
        scenario_notes = hb.get_scenario_notes(s.id)
        assert len(scenario_notes) == 1
        assert scenario_notes.Note[0].ref_id == s.id
        assert scenario_notes.Note[0].id is not None















if __name__ == '__main__':
    server.run()
