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
from HydraServer.lib import project as project_lib

class ProjectService(HydraService):
    """
        The project SOAP service
    """

    @rpc(Project, _returns=Project)
    def add_project(ctx, project):
        """
            Add a new project

            Args:
                project (hydra_complexmodels.Project): The new project to be added. The project does not include networks.

            Returns:
                hydra_complexmodels.Project: The project received, but with an ID this time.


            Raises:

        """

        new_proj = project_lib.add_project(project, **ctx.in_header.__dict__) 
        ret_proj = Project(new_proj)
        return ret_proj

    @rpc(Project, _returns=Project)
    def update_project(ctx, project):
        """
            Update a project

            Args:
                project (hydra_complexmodels.Project): The project to be updated. All the attributes of this project will be used to update the existing project

            Returns:
                hydra_complexmodels.Project: The updated project 

            Raises:
                ResourceNotFoundError: If the project is not found.

        """
        proj_i = project_lib.update_project(project,  **ctx.in_header.__dict__) 

        return Project(proj_i)


    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        """
        Get an existing Project

        Args:
            project_id (int): The ID of the project to retrieve 

        Returns:
            hydra_complexmodels.Project: The requested project 

        Raises:
            ResourceNotFoundError: If the project is not found.
        """
        proj_dict = project_lib.get_project(project_id,  **ctx.in_header.__dict__) 

        return Project(proj_dict)
 
    @rpc(Unicode, _returns=Project)
    def get_project_by_name(ctx, project_name):
        """
        If you don't know the ID of the project in question, but do know
        the name, use this function to retrieve it.
    
        Args:
            project_name (string): The name of the project to retrieve 

        Returns:
            hydra_complexmodels.Project: The requested project 

        Raises:
            ResourceNotFoundError: If the project is not found.

        """
        proj_dict = project_lib.get_project_by_name(project_name,  **ctx.in_header.__dict__) 

        return Project(proj_dict)

    @rpc(Integer, _returns=SpyneArray(ProjectSummary))
    def get_projects(ctx, user_id):
        """
        Get all the projects belonging to a user. 

        Args:
            user_id (int): The user ID whose projects you want 

        Returns:
            List(hydra_complexmodels.Project): The requested projects

        Raises:
            ResourceNotFoundError: If the User is not found.

        """
        if user_id is None:
            user_id = ctx.in_header.user_id
        project_dicts = project_lib.get_projects(user_id,  **ctx.in_header.__dict__)
        projects = [Project(p) for p in project_dicts]
        return projects


    @rpc(Integer, _returns=Unicode)
    def delete_project(ctx, project_id):
        """
        Set the status of a project to 'X'. This does NOT delete the project from
        the database (which also entails deleting all sub-networks, data etc). For
        that, use purge_project.
        
        Args:
            project_id (int): The ID of the project to delete. 

        Returns:
            string: 'OK' 

        Raises:
            ResourceNotFoundError: If the Project is not found.

        """
        project_lib.set_project_status(project_id, 'X',  **ctx.in_header.__dict__)
        return 'OK' 

    @rpc(Integer, _returns=Unicode)
    def purge_project(ctx, project_id):
        """
        Delete a project from the DB completely. WARNING: THIS WILL DELETE ALL
        THE PROJECT'S NETWORKS AND CANNOT BE REVERSED!

        Args:
            project_id (int): The ID of the project to purge. 

        Returns:
            string: 'OK' 

        Raises:
            ResourceNotFoundError: If the Project is not found.
        """
        project_lib.delete_project(project_id,  **ctx.in_header.__dict__)
        return 'OK' 

    @rpc(Integer, Unicode(pattern="[YN]", default='Y'), _returns=SpyneArray(Network))
    def get_networks(ctx, project_id, include_data):
        """
        Get all networks in a project
        
        Args:
            project_id (int): The ID of the project whose networks you want.
            include_data (string) ('Y' or 'N'): Include data with the networks? Defaults as 'Y' but using 'N' gives a significant performance boost.

        Returns:
            List(hydra_complexmodels.Network): All the project's Networks.

        Raises:
            ResourceNotFoundError: If the Project is not found.

        """
        net_dicts = project_lib.get_networks(project_id, include_data=include_data, **ctx.in_header.__dict__)
        networks = [Network(n, summary=True) for n in net_dicts]
        return networks

    @rpc(Integer, _returns=Project)
    def get_network_project(ctx, network_id):
        """
        Get the project of a specified network

        Args:
            network_id (int): The ID of the network whose project details you want 

        Returns:
            hydra_complexmodels.Project: The parent project of the specified Network

        Raises:
            ResourceNotFoundError: If the Network is not found.


        """
        proj = project_lib.get_network_project(network_id, **ctx.in_header.__dict__)

        return Project(proj)
