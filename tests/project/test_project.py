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
import datetime
import copy
import pytest

import hydra_base as hb
from hydra_base.lib.objects import JSONObject

import logging
log = logging.getLogger(__name__)

class TestProject:
    """
        Test for working with projects in Hydra
    """

    def add_attributes(self, client, proj):
        #Create some attributes, which we can then use to put data on our nodes
        attr1 = client.testutils.create_attribute("proj_attr_1")
        attr2 = client.testutils.create_attribute("proj_attr_2")
        attr3 = client.testutils.create_attribute("proj_attr_3")

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

    def add_data(self, client, project):

        attribute_data = []

        attrs = project.attributes

        attribute_data.append(client.testutils.create_descriptor(attrs[0], val="just project desscriptor"))
        attribute_data.append(client.testutils.create_array(attrs[1]))
        attribute_data.append(client.testutils.create_timeseries(attrs[2]))

        project.attribute_data = attribute_data

        return project

    def test_update(self, client, network_with_data):
        """
            The network here is necessary for a scenario with ID 1 to exist.
            Under normal circumstances, scenario 1 will always exist, as it's an initial requirement
            of the database setup.
        """
        project = JSONObject({})
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.add_attributes(client, project)
        project = self.add_data(client, project)

        new_project_i = client.add_project(project)

        project_j = client.get_project(new_project_i.id)

        new_project = copy.deepcopy(project_j)

        new_project.description = \
            'An updated project created through the Hydra Base interface.'

        updated_project = client.update_project(new_project)

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

        rs_to_check = client.get_project_attribute_data(new_project.id)[0]
        assert rs_to_check.dataset.type == 'descriptor' and \
               rs_to_check.dataset.value == 'just project desscriptor', \
               "There is an inconsistency with the attributes."

    def test_load(self, client):
        project = JSONObject({})
        project.name = 'Test Project %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = client.add_project(project)

        new_project = client.get_project(project.id)

        assert new_project.name == project.name, \
            "project_name is not loaded correctly."
        assert project.description == new_project.description,\
            "project_description did not load correctly."

    def test_set_project_status(self, client):
        project = JSONObject({})
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = client.add_project(project)

        client.set_project_status(project.id, 'X')

        proj = client.get_project(project.id)

        assert proj.status == 'X', \
            'Deleting project did not work correctly.'

    def test_delete(self, client, network_with_data):
        net = network_with_data
        project_id = net.project_id
        log.info("Purging project %s", project_id)
        res = client.delete_project(project_id)

        assert res == 'OK'
        log.info("Trying to get project %s. Should fail.",project_id)
        with pytest.raises(hb.HydraError):
            client.get_project(project_id)

    def test_get_projects(self, client):

        project = JSONObject({})

        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'

        project = client.add_project(project)

        projects = client.get_projects(pytest.root_user_id)

        assert len(projects) > 0, "Projects for user were not retrieved."

        assert projects[0].status == 'A'

    def test_get_networks(self, client, projectmaker, networkmaker):

        proj = projectmaker.create()

        net1 = networkmaker.create(project_id=proj.id)

        net2 = networkmaker.create(project_id=proj.id)
        nets = client.get_networks(proj.id)

        test_net = nets[0]
        assert test_net.scenarios is not None
        test_scenario = test_net.scenarios[0]
        assert len(test_net.nodes) > 0
        assert len(test_net.links) > 0
        assert len(test_scenario.resourcescenarios) == 0


        assert len(nets) == 2, "Networks were not retrieved correctly"

        nets = client.get_networks(proj.id, include_data=True)

        test_scenario = nets[0].scenarios[0]
        assert len(test_scenario.resourcescenarios) > 0

    def test_get_all_project_owners(self, client, projectmaker):
        proj = projectmaker.create()

        projectowners = client.get_all_project_owners()

        #there should be at LEAST 4, owing to the project for this test
        assert len(projectowners) >= 4

        projectowners = client.get_all_project_owners([proj.id])

        assert len(projectowners) == 4

        with pytest.raises(hb.exceptions.HydraError):
            #check for non-admin users
            client.user_id = 5
            projectowners = client.get_all_project_owners([proj.id])
        #set back to admin
        client.user_id=1

    def test_bulk_set_project_owners(self, client, projectmaker):
        proj = projectmaker.create(share=False)

        projectowners = client.get_all_project_owners([proj.id])

        assert len(projectowners) == 1

        projectowners = client.get_all_project_owners([proj.id])

        new_owner = JSONObject(dict(
            project_id=proj.id,
            user_id=2,
            view='Y',
            edit='Y',
            share='Y',
        ))

        client.bulk_set_project_owners([new_owner])

        projectowners = client.get_all_project_owners([proj.id])

        assert len(projectowners) == 2

    def test_clone_project(self, client, projectmaker, networkmaker):

        proj = projectmaker.create()

        net1 = networkmaker.create(project_id=proj.id)

        net2 = networkmaker.create(project_id=proj.id)

        recipient_user = client.get_user_by_name('UserA')

        new_project_name = 'New Project'

        cloned_project_id = client.clone_project(
            proj.id,
            recipient_user_id=recipient_user.id,
            new_project_name=new_project_name)

        cloned_project = client.get_project(cloned_project_id)

        assert cloned_project.name == new_project_name
        cloned_networks = client.get_networks(cloned_project.id)
        assert len(cloned_networks) == 2

        #check with no name provided
        cloned_project_id = client.clone_project(
            proj.id,
            recipient_user_id=recipient_user.id,
            new_project_name=None)

        cloned_project = client.get_project(cloned_project_id)

        assert cloned_project.name.find('Cloned') > 0
        cloned_networks = client.get_networks(cloned_project.id)
        assert len(cloned_networks) == 2

    def test_get_project_by_network_id(self, client, projectmaker, networkmaker):

        proj = projectmaker.create()

        net1 = networkmaker.create(project_id=proj.id)

        net2 = networkmaker.create(project_id=proj.id)
        nets = client.get_networks(proj.id)


        project_i = client.get_project_by_network_id(net1.id)

        assert project_i.id == proj.id
