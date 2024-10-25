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
        project.appdata = {'test':'metadata'}
        project = self.add_attributes(client, project)
        project = self.add_data(client, project)

        new_project_i = client.add_project(project)

        project_j = client.get_project(new_project_i.id)

        assert project_j.appdata['test'] == 'metadata'

        new_project = copy.deepcopy(project_j)

        new_project.description = \
            'An updated project created through the Hydra Base interface.'

        new_project.appdata['test1'] = 'metadata1'

        updated_project = client.update_project(new_project)

        assert updated_project.appdata['test'] == 'metadata'
        assert updated_project.appdata['test1'] == 'metadata1'

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

    def test_rename_status_x_project(self, client, projectmaker, networkmaker):
        sender_user_id = client.user_id
        proj = projectmaker.create()
        net1 = networkmaker.create(project_id=proj.id)
        net2 = networkmaker.create(project_id=proj.id)

        proj_name = proj.name

        #Set the status of the project to 'X'.
        client.set_project_status(proj.id, 'X')
        #now create a project with the name of the deleted project
        project = JSONObject({})
        project.name = proj_name
        project.description = \
            'A project created with the same name as a deleted project.'
        project = client.add_project(project)

        old_proj = client.get_project(proj.id)

        assert old_proj.status == 'X'
        assert old_proj.name != project.name



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

    def test_clone_project_to_other_user(self, client, projectmaker, networkmaker):
        sender_user_id = client.user_id
        proj = projectmaker.create()
        net1 = networkmaker.create(project_id=proj.id)
        net2 = networkmaker.create(project_id=proj.id)

        #"sharing with a non-admin user"
        recipient_user = client.get_user_by_id(pytest.user_c.id)

        sender_projects_before = client.get_projects(client.user_id)

        client.user_id = recipient_user.id
        recipient_projects_before = client.get_projects(client.user_id)

        client.user_id = sender_user_id

        new_project_name = 'Cloned Project'

        test_delete_project_id = client.clone_project(
            proj.id,
            recipient_user_id=recipient_user.id,
            new_project_name=new_project_name)

        #Check the recipient has received the project
        client.user_id = recipient_user.id
        recipient_user_project = client.get_project(test_delete_project_id)
        assert len(recipient_user_project.networks) == 2

        recipient_projects_after = client.get_projects(client.user_id)

        assert len(recipient_projects_after) == len(recipient_projects_before)+1

        client.user_id = sender_user_id
        sender_projects_after = client.get_projects(client.user_id)
        assert len(sender_projects_after) == len(sender_projects_before)

        #check with no name provided
        cloned_project_id = client.clone_project(
            proj.id,
            recipient_user_id=recipient_user.id,
            new_project_name=None)

        cloned_project = client.get_project(cloned_project_id)

        assert cloned_project.name.find('Cloned') > 0
        cloned_networks = client.get_networks(cloned_project.id)
        assert len(cloned_networks) == 2

        #Check that the resource attribute references have been updated correctly
        #by comparing the attr_id on the original network and the cloned network for the same attribute name,
        #ensuring they are not the same.
        for n in cloned_project.networks:
            net = client.get_network(network_id=n.id)
            for ra in net.attributes:
                a = client.get_attribute_by_id(attr_id=ra.attr_id)
                if a.network_id is not None:
                    for ra1 in net1.attributes:
                        if ra1.name == a.name:
                            assert ra1.network_id is not None
                            assert ra.attr_id != ra1.attr_id

    def test_rename_of_status_x_projects_in_clone(self, client, projectmaker, networkmaker):
        sender_user_id = client.user_id
        proj = projectmaker.create()
        net1 = networkmaker.create(project_id=proj.id)
        net2 = networkmaker.create(project_id=proj.id)

        #"sharing with a non-admin user"
        recipient_user = client.get_user_by_id(pytest.user_c.id)

        sender_projects_before = client.get_projects(client.user_id)

        client.user_id = recipient_user.id
        recipient_projects_before = client.get_projects(client.user_id)

        client.user_id = sender_user_id

        new_project_name = 'Cloned Project Test'

        test_delete_project_id = client.clone_project(
            proj.id,
            recipient_user_id=recipient_user.id,
            new_project_name=new_project_name)

        #test renaming of deleted projects
        client.set_project_status(test_delete_project_id, 'X')
        changed_project = client.get_project(test_delete_project_id)
        assert changed_project.status == 'X'

        cloned_project_id = client.clone_project(
            proj.id,
            recipient_user_id=recipient_user.id,
            new_project_name=new_project_name)

        renamed_project = client.get_project(test_delete_project_id)
        assert renamed_project.status == 'X'

    def test_get_project_by_network_id(self, client, projectmaker, networkmaker):

        proj = projectmaker.create()

        net1 = networkmaker.create(project_id=proj.id)

        net2 = networkmaker.create(project_id=proj.id)
        nets = client.get_networks(proj.id)


        project_i = client.get_project_by_network_id(net1.id)

        assert project_i.id == proj.id

    def test_share_project(self, client, projectmaker):

        proj_user = client.user_id
        proj = projectmaker.create(share=False)
        client.user_id = pytest.user_c.id
        with pytest.raises(hb.HydraError):
            client.get_project(proj.id)

        client.user_id = proj_user
        client.share_project(proj.id, ['UserC'], False, False)

        #Get the project as user C -- this should succeeed
        client.user_id = pytest.user_c.id
        client.get_project(proj.id)

        #now revolk access
        client.user_id = proj_user
        client.unshare_project(proj.id, ['UserC'])

        #user c no longer has access
        client.user_id = pytest.user_c.id
        with pytest.raises(hb.HydraError):
            client.get_project(proj.id)
