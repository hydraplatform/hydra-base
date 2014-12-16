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
from spyne.decorator import rpc
from spyne.model.primitive import Integer, Unicode 
from spyne.model.complex import Array as SpyneArray
from hydra_complexmodels import Project,\
ProjectSummary,\
Network
from hydra_base import HydraService
from lib import project as project_lib

class ProjectService(HydraService):
    """
        The project SOAP service
    """

    @rpc(Project, _returns=Project)
    def add_project(ctx, project):
        """
            Add a new project
            returns a project complexmodel
        """

        new_proj = project_lib.add_project(project, **ctx.in_header.__dict__) 
        ret_proj = Project(new_proj)
        return ret_proj

    @rpc(Project, _returns=Project)
    def update_project(ctx, project):
        """
            Update a project
            returns a project complexmodel
        """
        proj_i = project_lib.update_project(project,  **ctx.in_header.__dict__) 

        return Project(proj_i)


    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        """
            get a project complexmodel
        """
        proj_dict = project_lib.get_project(project_id,  **ctx.in_header.__dict__) 

        return Project(proj_dict)
 
    @rpc(Unicode, _returns=Project)
    def get_project_by_name(ctx, project_name):
        """
            get a project complexmodel
        """
        proj_dict = project_lib.get_project_by_name(project_name,  **ctx.in_header.__dict__) 

        return Project(proj_dict)

    @rpc(Integer, _returns=SpyneArray(ProjectSummary))
    def get_projects(ctx, user_id):
        """
            get a project complexmodel
        """
        if user_id is None:
            user_id = ctx.in_header.user_id
        project_dicts = project_lib.get_projects(user_id,  **ctx.in_header.__dict__)
        projects = [Project(p) for p in project_dicts]
        return projects


    @rpc(Integer, _returns=Unicode)
    def delete_project(ctx, project_id):
        """
            Set the status of a project to 'X'
        """
        project_lib.set_project_status(project_id, 'X',  **ctx.in_header.__dict__)
        return 'OK' 

    @rpc(Integer, _returns=Unicode)
    def purge_project(ctx, project_id):
        """
            Set the status of a project to 'X'
        """
        project_lib.delete_project(project_id,  **ctx.in_header.__dict__)
        return 'OK' 

    @rpc(Integer, Unicode(pattern="[YN]", default='Y'), _returns=SpyneArray(Network))
    def get_networks(ctx, project_id, include_data):
        """
            Get all networks in a project
            Returns an array of network objects.
        """
        net_dicts = project_lib.get_networks(project_id, include_data=include_data, **ctx.in_header.__dict__)
        networks = [Network(n, summary=True) for n in net_dicts]
        return networks

    @rpc(Integer, _returns=Project)
    def get_network_project(ctx, network_id):
        """
            Get the project of a specified network
        """
        proj = project_lib.get_network_project(network_id, **ctx.in_header.__dict__)

        return Project(proj)
