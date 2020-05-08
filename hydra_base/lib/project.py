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

import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import noload
from sqlalchemy import and_, or_

from ..exceptions import ResourceNotFoundError
from . import scenario
from ..exceptions import PermissionError as HydraPermissionError, HydraError
from ..db.model import Project, ProjectOwner, Network, NetworkOwner, User, Scenario
from .. import db
from . import network
from .objects import JSONObject
from ..util import hdb
from ..util.permissions import required_perms

LOG = logging.getLogger(__name__)

def _check_can_write_to_parent(parent_id, user_id):
    """
        Perform a check on the parent ID of a project to ensure it can be written
        to by the requesting user
    """
    #We can't add a child project to a parent unless we have write permission
    #on the parent project
    if parent_id is not None:
        parent_project = _get_project(parent_id)
        if parent_project.check_write_permission(user_id) is False:
            raise HydraPermissionError(f"User {user_id} does not "
                                       "have permission to add to "
                                       f"parent project {parent_id}")

def _get_project(project_id):
    try:
        project = db.DBSession.query(Project).filter(Project.id == project_id).one()
        return project
    except NoResultFound:
        raise ResourceNotFoundError("Project %s not found"%(project_id))

def _add_project_attribute_data(project_i, attr_map, attribute_data, user_id):
    if attribute_data is None:
        return []

    if project_i.scenario_id is None:
        LOG.info("Creating scenario for project %s (%s)", project_i.name, project_i.id)

        project_name = f"Project {project_i.id} Scenario"
        proj_scenario = scenario.add_scenario(1,#network 1
                                              JSONObject({"name" : project_name}),
                                              user_id=project_i.created_by)
        project_i.scenario_id = proj_scenario.id
    else:
        proj_scenario = db.DBSession.query(Scenario)\
            .filter(Scenario.id == project_i.scenario_id).first()


    #As projects do not have scenarios (or to be more precise, they can only use
    #scenario 1, we can put
    #resource scenarios directly into the 'attributes' attribute
    #meaning we can add the data directly here.
    resource_scenarios = []
    for attr in attribute_data:
        attr.scenario_id = proj_scenario.id
        if attr.resource_attr_id < 0:
            ra_i = attr_map[attr.resource_attr_id]
            attr.resource_attr_id = ra_i.id

        rscen = scenario._update_resourcescenario(proj_scenario, attr, user_id=user_id)

        resource_scenarios.append(rscen)
    return resource_scenarios

@required_perms('add_project')
def add_project(project, **kwargs):
    """
        Add a new project
        returns a project complexmodel
    """
    user_id = kwargs.get('user_id')

    existing_proj = get_project_by_name(project.name, user_id=user_id)

    if len(existing_proj) > 0:
        raise HydraError("A Project with the name \"%s\" already exists"%(project.name,))

    proj_i = Project()
    proj_i.name = project.name
    proj_i.description = project.description
    proj_i.created_by = user_id

    #We can't add a child project to a parent unless we have write permission
    #on the parent project. Throws a HydraPermissionError if not
    _check_can_write_to_parent(project.parent_id, user_id)
    proj_i.parent_id = project.parent_id

    attr_map = hdb.add_resource_attributes(proj_i, project.attributes)
    db.DBSession.flush() #Needed to get the resource attr's ID
    proj_data = _add_project_attribute_data(proj_i,
                                            attr_map,
                                            project.attribute_data,
                                            user_id)
    proj_i.attribute_data = proj_data

    proj_i.set_owner(user_id)

    db.DBSession.add(proj_i)
    db.DBSession.flush()

    return proj_i

@required_perms('edit_project')
def move_project(project_id, target_parent_id, **kwargs):
    """
        Set the parent_id of a project, thereby moving it to a new project
        returns a project ORM object
    """

    user_id = kwargs.get('user_id')

    proj_i = _get_project(project_id)

    proj_i.check_write_permission(user_id)

    #We can't add a child project to a parent unless we have write permission
    #on the parent project. Throws a HydraPermissionError if not
    _check_can_write_to_parent(target_parent_id, user_id)
    proj_i.parent_id = target_parent_id

    db.DBSession.flush()

@required_perms('edit_project')
def update_project(project, **kwargs):
    """
        Update a project
        returns a project complexmodel
    """

    user_id = kwargs.get('user_id')

    proj_i = _get_project(project.id)

    proj_i.check_write_permission(user_id)

    proj_i.name = project.name
    proj_i.description = project.description

    #We can't add a child project to a parent unless we have write permission
    #on the parent project. Throws a HydraPermissionError if not
    _check_can_write_to_parent(project.parent_id, user_id)
    proj_i.parent_id = project.parent_id

    attr_map = hdb.add_resource_attributes(proj_i, project.attributes)
    proj_data = _add_project_attribute_data(proj_i,
                                            attr_map,
                                            project.attribute_data,
                                            user_id)
    db.DBSession.flush()
    proj_i.attribute_data = proj_data

    return proj_i

def get_project(project_id,
                include_networks=True,
                include_deleted_networks=False,
                include_scenarios=True,
                **kwargs):
    """
        Get a project by its id. Include data, networks, and each network scenarios
        args:
            project_id (int): THe ID of the project to retrieve
            include_networks (bool) default True: Include the project's networks
                                     (has a performance implication)
            include_deleted_networks (bool) default False: Included deleted networks
            include_scenarios (bool) default True: If nettworks are needed but scenarios
                                                   are not, then this
                                                   flag can be used to ignore scenarios

    """
    user_id = kwargs.get('user_id')

    proj_i = _get_project(project_id)

    #lazy load owners
    proj_i.owners

    proj_i.check_read_permission(user_id)

    proj_j = JSONObject(proj_i)

    proj_j.networks = []
    if include_networks is True:
        for net_i in proj_i.networks:
            #lazy load owners
            net_i.owners

            if include_scenarios is True:
                net_i.scenarios

            if include_deleted_networks is False and net_i.status.lower() == 'x':
                continue

            can_read_network = net_i.check_read_permission(user_id, do_raise=False)
            if can_read_network is False:
                continue

            net_j = JSONObject(net_i)
            proj_j.networks.append(net_j)

    return proj_j

def get_project_by_network_id(network_id, **kwargs):
    """
        get a project complexmodel by a network_id
        args:
            network_id: The network id whose parent project is requested
        returns:
            SQLAlchemy ORM object of a project
    """
    user_id = kwargs.get('user_id')

    project_i = db.DBSession.query(Project).join(
        Network, Project.id == Network.project_id).filter(
            Network.id == network_id).one()

    project_i.check_read_permission(user_id)

    return project_i


def get_project_by_name(project_name, parent_id=None, **kwargs):
    """
        get a project by its name. Projects are unique to a user, within
        the context of a parent project -- a user can have more than 1 project
        with the same name, but only in the context of different parent projects
        args:
            project_name (string): THe name of the project
            parent_id (int) (optional): The parent ID of the project. None by
                default, meaning it defaults to getting top-level projects
        returns:
            List of projects Sqlalchemy ORM objects
    """
    user_id = kwargs.get('user_id')

    #If the parent is specified, check the user has read access
    if parent_id is not None:
        _get_project(parent_id).check_read_permission(user_id)

    projects_i = db.DBSession.query(Project)\
        .join(ProjectOwner)\
        .filter(
            Project.name == project_name,
            ProjectOwner.user_id == user_id)\
        .filter(Project.parent_id == parent_id)\
        .order_by('name').all()

    ret_projects = []
    for project_i in projects_i:
        try:
            project_i.check_read_permission(user_id)
            ret_projects.append(project_i)
        except HydraPermissionError:
            LOG.info("Can't return project %s. "
                     "User %s does not have permission to read it.",
                     project_i.id, user_id)

    return ret_projects

@required_perms('view_project')
def get_projects(uid,
                 include_shared_projects=True,
                 projects_ids_list_filter=None,
                 parent_id=None,
                 include_networks=True,
                 **kwargs):
    """
        Get all the projects owned by the specified user.
        These include projects created by the user, but also ones shared with the user.
        For shared projects, only include networks in those projects which are
        accessible to the user.

        the include_shared_projects flag indicates whether to include projects
        which have been shared
        with the user, or to only return projects created directly by this user.

        args:
            include_shared_projects (bool): include projects which have been shared,
                                        but not owned by the user
            project_ids_list_filter (list(int)): restrict returned netweork to supplied IDS
            parent_id (int): Get only the projects which are contained in this project.
                              If None, then only get projects without a parent project (top level)
            include_networks (bool): Flag to indicate whether network information
                                     should be returned.
                                     *Including networks may make the request slower.
    """
    req_user_id = kwargs.get('user_id')

    #If the parent is specified, check the user has read access
    if parent_id is not None:
        _get_project(parent_id).check_read_permission(req_user_id)

    ##Don't load the project's networks. Load them separately, as the networks
    #must be checked individually for ownership
    projects_qry = db.DBSession.query(Project)

    LOG.info("Getting projects for %s inside parent %s", uid, parent_id)

    if include_shared_projects is True:
        projects_qry = projects_qry.join(ProjectOwner)\
            .filter(Project.status == 'A', or_(ProjectOwner.user_id == uid,
                                               Project.created_by == uid))
    else:
        projects_qry = projects_qry.join(ProjectOwner).filter(Project.created_by == uid)

    if projects_ids_list_filter is not None:
        # Filtering the search of project id
        if isinstance(projects_ids_list_filter, str):
            # Trying to read a csv string
            projects_ids_list_filter = eval(projects_ids_list_filter)
            if isinstance(projects_ids_list_filter, int):
                projects_qry = projects_qry.filter(Project.id == projects_ids_list_filter)
            else:
                projects_qry = projects_qry.filter(Project.id.in_(projects_ids_list_filter))

    projects_qry = projects_qry.options(noload('networks')).order_by('id')

    projects_i = projects_qry.all()

    LOG.info("Project query done for user %s. %s projects found", uid, len(projects_i))

    projects_j = []
    for project_i in projects_i:

        if project_i.parent_id != parent_id:
            continue
        #Ensure the requesting user is allowed to see the project
        project_i.check_read_permission(req_user_id)
        #lazy load owners
        project_i.owners
        #lazy load attributes
        project_i.attributes
        project_i.get_attribute_data()

        project_j = JSONObject(project_i)

        if include_networks is True:
            project_j.networks = _get_project_networks(project_i.id, uid, req_user_id)
        else:
            project_j.networks = []

        projects_j.append(project_j)
    LOG.info("Projecs processed. Returning %s projects", len(projects_j))
    return projects_j

def _get_project_networks(project_id, uid, req_user_id):
    """
        Get all the networks in a project that the user can see.
    """

    user = db.DBSession.query(User).filter(User.id == req_user_id).one()
    isadmin = user.is_admin()

    network_qry = db.DBSession.query(Network)\
                            .filter(Network.project_id == project_id,\
                                    Network.status == 'A')
    if not isadmin:
        network_qry.outerjoin(NetworkOwner)\
        .filter(or_(
            and_(NetworkOwner.user_id is not None, NetworkOwner.view == 'Y'),
            Network.created_by == uid
        ))

    networks_i = network_qry.all()

    networks_j = []
    for network_i in networks_i:
        #lazy load networks
        network_i.owners
        net_j = JSONObject(network_i)
        if net_j.layout is not None:
            net_j.layout = JSONObject(net_j.layout)
        else:
            net_j.layout = JSONObject({})
        networks_j.append(net_j)
    LOG.info("Networks loaded projects for user %s", uid)
    return networks_j

@required_perms('delete_project')
def set_project_status(project_id, status, **kwargs):
    """
        Set the status of a project to 'X'
    """
    user_id = kwargs.get('user_id')
    project = _get_project(project_id)
    project.check_write_permission(user_id)
    project.status = status
    db.DBSession.flush()

@required_perms('edit_project', 'delete_project')
def delete_project(project_id, **kwargs):
    """
        Set the status of a project to 'X'
    """
    user_id = kwargs.get('user_id')
    project = _get_project(project_id)
    project.check_write_permission(user_id)
    db.DBSession.delete(project)
    db.DBSession.flush()

    return 'OK'

@required_perms("view_project")
def get_networks(project_id, include_data='N', **kwargs):
    """
        Get all networks in a project
        Returns an array of network objects.
    """
    LOG.info("Getting networks for project %s", project_id)
    user_id = kwargs.get('user_id')
    project = _get_project(project_id)
    project.check_read_permission(user_id)

    netresults = db.DBSession.query(Network.id, Network.status).filter(Network.project_id == project_id).all()
    networks = []
    for netdata in netresults:
        if netdata.status != 'A':
            continue
        try:
            net = network.get_network(netdata.id, summary=True, include_data=include_data, **kwargs)
            LOG.info("Network %s retrieved", net.name)
            networks.append(net)
        except HydraPermissionError:
            LOG.info("Not returning network %s as user %s does not have "
                     "permission to read it.", netdata.id, user_id)

    return networks

def get_network_project(network_id, **kwargs):
    """
        get the project that a network is in
        Deprecated. kept for backward-compatibility
    """
    return get_project_by_network_id(network_id, **kwargs)



def clone_project(project_id, recipient_user_id=None, new_project_name=None, new_project_description=None, **kwargs):
    """
        Create an exact clone of the specified project for the specified user.
    """

    user_id = kwargs['user_id']

    LOG.info("Creating a new project for cloned network")

    project = db.DBSession.query(Project).filter(Project.id == project_id).one()
    project.check_write_permission(user_id)

    if new_project_name is None:
        user = db.DBSession.query(User).filter(User.id==user_id).one()
        new_project_name = project.name + ' Cloned By {}'.format(user.display_name)

    #check a project with this name doesn't already exist:
    project_with_name =  db.DBSession.query(Project).filter(
        Project.name == new_project_name,
        Project.created_by == user_id).all()

    if len(project_with_name) > 0:
        raise HydraError("A project with the name {0} already exists".format(new_project_name))

    new_project = Project()
    new_project.name = new_project_name
    if new_project_description:
        new_project.description = new_project_description
    else:
        new_project.description = project.description

    new_project.created_by = user_id
    new_project.parent_id = project.parent_id

    if recipient_user_id is not None:
        project.check_share_permission(user_id)
        new_project.set_owner(recipient_user_id)

    new_project.set_owner(user_id)


    db.DBSession.add(new_project)
    db.DBSession.flush()

    network_ids = db.DBSession.query(Network.id).filter(
                                        Network.project_id == project_id).all()
    for n in network_ids:
        network.clone_network(n.id,
                              recipient_user_id=recipient_user_id,
                              project_id=new_project.id,
                              user_id=user_id)

    db.DBSession.flush()

    return new_project.id
