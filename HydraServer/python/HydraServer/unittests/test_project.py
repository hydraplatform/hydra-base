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
import datetime
import copy
import logging
import suds
log = logging.getLogger(__name__)

class ProjectTest(server.SoapServerTest):
    """
        Test for working with projects in Hydra
    """

    def add_attributes(self, proj):
        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr("proj_attr_1")
        attr2 = self.create_attr("proj_attr_2")
        attr3 = self.create_attr("proj_attr_3")

        proj_attr_1  = self.client.factory.create('hyd:ResourceAttr')
        proj_attr_1.id = -1
        proj_attr_1.attr_id = attr1.id
        proj_attr_2  = self.client.factory.create('hyd:ResourceAttr')
        proj_attr_2.attr_id = attr2.id
        proj_attr_2.id = -2
        proj_attr_3  = self.client.factory.create('hyd:ResourceAttr')
        proj_attr_3.attr_id = attr3.id
        proj_attr_3.id = -3

        attributes = self.client.factory.create('hyd:ResourceAttrArray')
        
        attributes.ResourceAttr.append(proj_attr_1)
        attributes.ResourceAttr.append(proj_attr_2)
        attributes.ResourceAttr.append(proj_attr_3)

        proj.attributes = attributes

        return proj

    def add_data(self, proj):

        attribute_data = self.client.factory.create('hyd:ResourceScenarioArray')
        
        attrs = proj.attributes.ResourceAttr

        attribute_data.ResourceScenario.append(self.create_descriptor(attrs[0], val="just project desscriptor"))
        attribute_data.ResourceScenario.append(self.create_array(attrs[1]))
        attribute_data.ResourceScenario.append(self.create_timeseries(attrs[2]))

        proj.attribute_data =attribute_data 

        return proj

    def test_update(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.add_attributes(project) 
        project = self.add_data(project)
        
        project = self.client.service.add_project(project)
        new_project = copy.deepcopy(project)

        new_project.description = \
            'An updated project created through the SOAP interface.'
 
        updated_project = self.client.service.update_project(new_project)
 
        log.debug(updated_project)

        assert project.id == updated_project.id, \
            "project_id changed on update."
        assert project.created_by is not None, \
            "created by is null."
        assert project.name == updated_project.name, \
            "project_name changed on update."
        assert project.description != updated_project.description,\
            "project_description did not update"
        assert updated_project.description == \
            'An updated project created through the SOAP interface.', \
            "Update did not work correctly."

        rs_to_check = updated_project.attribute_data.ResourceScenario[0]
        assert rs_to_check.value.type == 'descriptor' and \
               rs_to_check.value.value.desc_val == 'just project desscriptor', \
               "There is an inconsistency with the attributes."

    def test_load(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        new_project = self.client.service.get_project(project.id)

        assert new_project.name == project.name, \
            "project_name is not loaded correctly."
        assert project.description == new_project.description,\
            "project_description did not load correctly."

    def test_delete(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        self.client.service.delete_project(project.id)

        proj = self.client.service.get_project(project.id)

        assert proj.status == 'X', \
            'Deleting project did not work correctly.'

    def test_purge(self):
        net = self.create_network_with_data(new_proj=True)

        project_id = net.project_id
        log.info("Deleting project %s", project_id)
        res = self.client.service.purge_project(project_id)

        assert res == 'OK'
        log.info("Trying to get project %s. Should fail.",project_id)
        self.assertRaises(suds.WebFault, self.client.service.get_project, project_id)

    def test_get_projects(self):
        
        project = self.client.factory.create('hyd:Project')

        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        projects = self.client.service.get_projects()

        assert len(projects.ProjectSummary) > 0, "Projects for user were not retrieved."

    def test_get_networks(self):
        
        proj = self.create_project('Project with multiple networks @ %s'%(datetime.datetime.now()))

        self.create_network_with_data(proj.id)
        self.create_network_with_data(proj.id)

        nets = self.client.service.get_networks(proj.id)
        test_net = nets[0][0]
        assert test_net.scenarios is not None
        test_scenario = test_net.scenarios.Scenario[0]
        assert len(test_net.nodes.Node) > 0
        assert len(test_net.links.Link) > 0

        assert len(nets.Network) == 2, "Networks were not retrieved correctly"
        
        nets = self.client.service.get_networks(proj.id, 'N')

        test_scenario = nets[0][0].scenarios.Scenario[0]
        assert test_scenario.resourcescenarios is None

if __name__ == '__main__':
    server.run()
