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
import server
import datetime
import copy
import pytest

from fixtures import *
import util

import hydra_base as hb
from hydra_base.lib.objects import JSONObject

import logging
log = logging.getLogger(__name__)

class TestProject:
    """
        Test for working with projects in Hydra
    """
    # Todo make this a fixture?
    user_id = util.user_id

    def add_attributes(self, proj):
        #Create some attributes, which we can then use to put data on our nodes
        attr1 = util.create_attr("proj_attr_1")
        attr2 = util.create_attr("proj_attr_2")
        attr3 = util.create_attr("proj_attr_3")

        proj_attr_1  = JSONObject({})
        proj_attr_1.id = -1
        proj_attr_1.attr_id = attr1.id
        proj_attr_2  = JSONObject({})
        proj_attr_2.attr_id = attr2.id
        proj_attr_2.id = -2
        proj_attr_3  = JSONObject({})
        proj_attr_3.attr_id = attr3.id
        proj_attr_3.id = -3

        attributes = []

        attributes.append(proj_attr_1)
        attributes.append(proj_attr_2)
        attributes.append(proj_attr_3)

        proj.attributes = attributes

        return proj

    def add_data(self, project):

        attribute_data = []

        attrs = project.attributes

        attribute_data.append(util.create_descriptor(attrs[0], val="just project desscriptor"))
        attribute_data.append(util.create_array(attrs[1]))
        attribute_data.append(util.create_timeseries(attrs[2]))

        project.attribute_data = attribute_data

        return project

    def test_update(self, session, network_with_data):
        """
            The network here is necessary for a scenario with ID 1 to exist.
            Under normal circumstances, scenario 1 will always exist, as it's an initial requirement
            of the database setup.
        """
        project = JSONObject({})
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.add_attributes(project)
        project = self.add_data(project)

        new_project_i = hb.add_project(project, user_id=self.user_id)

        #TODO: Fix issue in JSONObject caused by unusual project structure causing a recursion issue when loading attribute_data
        hb.db.DBSession.expunge_all()

        project_i = hb.get_project(new_project_i.id, user_id=self.user_id)

        project_j = JSONObject(project_i)

        new_project = copy.deepcopy(project_j)

        new_project.description = \
            'An updated project created through the Hydra Base interface.'

        updated_project = hb.update_project(new_project, user_id=self.user_id)

        assert project_j.id == updated_project.id, \
            "project_id changed on update."
        assert project_j.created_by is not None, \
            "created by is null."
        assert project_j.name == updated_project.name, \
            "project_name changed on update."
        assert project_j.description != updated_project.description,\
            "project_description did not update"
        assert updated_project.description == \
            'An updated project created through the Hydra Base interface.', \
            "Update did not work correctly."

        rs_to_check = updated_project.get_attribute_data()[0]
        assert rs_to_check.dataset.type == 'descriptor' and \
               rs_to_check.dataset.value == 'just project desscriptor', \
               "There is an inconsistency with the attributes."

    def test_load(self, session):
        project = JSONObject({})
        project.name = 'Test Project %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = hb.add_project(project, user_id=self.user_id)

        new_project = hb.get_project(project.id, user_id=self.user_id)

        assert new_project.name == project.name, \
            "project_name is not loaded correctly."
        assert project.description == new_project.description,\
            "project_description did not load correctly."

    def test_set_project_status(self, session):
        project = JSONObject({})
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = hb.add_project(project, user_id=self.user_id)

        hb.set_project_status(project.id, 'X', user_id=self.user_id)

        proj = hb.get_project(project.id, user_id=self.user_id)

        assert proj.status == 'X', \
            'Deleting project did not work correctly.'

    def test_delete(self, session, network_with_data):
        net = network_with_data

        project_id = net.project_id
        log.info("Purging project %s", project_id)
        res = hb.delete_project(project_id, user_id=self.user_id)

        assert res == 'OK'
        log.info("Trying to get project %s. Should fail.",project_id)
        with pytest.raises(hb.HydraError):
            hb.get_project(project_id, user_id=self.user_id)

    def test_get_projects(self, session):

        project = JSONObject({})

        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = hb.add_project(project, user_id=self.user_id)

        projects = hb.get_projects(self.user_id, user_id=self.user_id)

        assert len(projects) > 0, "Projects for user were not retrieved."

        assert projects[0].status == 'A'

    def test_get_networks(self, session, projectmaker, networkmaker):

        proj = projectmaker.create()

        net1 = networkmaker.create(project_id=proj.id)

        net2 = networkmaker.create(project_id=proj.id)
        nets = hb.get_networks(proj.id, user_id=self.user_id)

        test_net = nets[0]
        assert test_net.scenarios is not None
        test_scenario = test_net.scenarios[0]
        assert len(test_net.nodes) > 0
        assert len(test_net.links) > 0
        assert len(test_scenario.resourcescenarios) == 0


        assert len(nets) == 2, "Networks were not retrieved correctly"

        nets = hb.get_networks(proj.id, include_data='Y', user_id=self.user_id)

        test_scenario = nets[0].scenarios[0]
        assert len(test_scenario.resourcescenarios) > 0

if __name__ == '__main__':
    server.run()
