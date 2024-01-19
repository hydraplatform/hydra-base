# (c) Copyright 2013 to 2020 University of Manchester
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

import logging
import json
import pytest
from hydra_base.lib.objects import JSONObject
from hydra_base.exceptions import HydraError

LOG = logging.getLogger(__name__)


class TestProjectTemplates:
    """
        TEMPLATES Functions
    """

    def test_add_project_template(self, client):
        """
            Scope a project to a template
        """

        template_j = client.testutils.create_template()
        project_j = client.testutils.create_project()

        client.add_project_template(project_id=project_j.id, template_id=template_j.id)

        pt = client.get_project_template(project_id=project_j.id, template_id=template_j.id)

        assert pt.project_id == project_j.id

        p = client.get_project(project_id=project_j.id)

        assert p.template.id == template_j.id

    def test_remove_project_template(self, client):
        """
            Remove a template - project association
        """

        template_j = client.testutils.create_template()
        project_j = client.testutils.create_project("Project with template to add and remove")

        client.add_project_template(project_id=project_j.id, template_id=template_j.id)

        pt = client.get_project_template(project_id=project_j.id, template_id=template_j.id)

        assert pt.project_id == project_j.id

        client.delete_project_template(project_id=project_j.id, template_id=template_j.id)

        with pytest.raises(HydraError):
            client.get_project_template(project_id=project_j.id, template_id=template_j.id)

        p = client.get_project(project_id=project_j.id)

        assert p.template == None
        

    def test_remove_template_from_project_with_network(self, client):
        """
            Test that removing a template - project association does not work
            when there is a network in the project that uses the template in question.
            Removing the template could result in inconsistency.
        """

        template_j = client.testutils.create_template()
        project_j = client.testutils.create_project()

        client.add_project_template(project_id=project_j.id,
                                    template_id=template_j.id)

        pt = client.get_project_template(project_id=project_j.id,
                                         template_id=template_j.id)

        assert pt.project_id == project_j.id

        #add a network using this type
        client.testutils.create_network_with_data(project_id=project_j.id,
                                                  template=template_j)        

        #Can not delete the template - project association because there is a network
        #in the project using that type.
        with pytest.raises(HydraError):
            client.delete_project_template(project_id=project_j.id,
                                           template_id=template_j.id)
            

    def test_add_project_template_in_add_project(self, client):
        """
            Test that a project template can be added in the add project function
        """

        template_j = client.testutils.create_template()

        project_j = client.testutils.create_project(name="Unittest Project with template",
                                                    template_id=template_j.id)

        pt = client.get_project_template(project_id=project_j.id,
                                         template_id=template_j.id)

        assert pt.project_id == project_j.id

        p = client.get_project(project_id=project_j.id)

        assert p.template.id == template_j.id