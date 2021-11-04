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

from ast import literal_eval

from ..exceptions import ResourceNotFoundError
from . import scenario
import logging
from collections import defaultdict
from ..exceptions import PermissionError, HydraError
from ..db.model import Project, ProjectOwner, Network, NetworkOwner, Template, User
from .. import db
from . import network
from .objects import JSONObject
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import class_mapper, noload, joinedload
from sqlalchemy import and_, or_
from ..util import hdb
from ..util.permissions import required_perms

log = logging.getLogger(__name__)

def _get_project(project_id):
    try:
        project = db.DBSession.query(Project).filter(Project.id==project_id).one()
        project.layout = project.layout or '{}'
        return project
    except NoResultFound:
        raise ResourceNotFoundError("Project %s not found"%(project_id))

def _add_project_attribute_data(project_i, attr_map, attribute_data):
    if attribute_data is None:
        return []
    #As projects do not have scenarios (or to be more precise, they can only use
    #scenario 1, we can put
    #resource scenarios directly into the 'attributes' attribute
    #meaning we can add the data directly here.
    resource_scenarios = []
    for attr in attribute_data:
        if attr.resource_attr_id < 0:
            ra_i = attr_map[attr.resource_attr_id]
            attr.resource_attr_id = ra_i.id

        rscen = scenario._update_resourcescenario(None, attr)

        #if attr.resource_attr_id < 0:
        #    ra_i = attr_map[attr.resource_attr_id]
        #    rscen.resourceattr = ra_i

        resource_scenarios.append(rscen)
    return resource_scenarios

# @required_perms('add_project')
def add_project(project,**kwargs):
    """
        Add a new project
        returns a project complexmodel
    """
    user_id = kwargs.get('user_id')

    existing_proj = get_project_by_name(project.name,user_id=user_id)

    if len(existing_proj) > 0:
        raise HydraError("A Project with the name \"%s\" already exists"%(project.name,))

    proj_i = Project()
    proj_i.name = project.name
    proj_i.description = project.description
    proj_i.created_by = user_id

    attr_map = hdb.add_resource_attributes(proj_i, project.attributes)
    db.DBSession.flush() #Needed to get the resource attr's ID
    proj_data = _add_project_attribute_data(proj_i, attr_map, project.attribute_data)
    proj_i.attribute_data = proj_data

    proj_i.set_owner(user_id)

    db.DBSession.add(proj_i)
    db.DBSession.flush()

    return proj_i

@required_perms('get_project')
def update_project(project,**kwargs):
    """
        Update a project
        returns a project complexmodel
    """

    user_id = kwargs.get('user_id')

    proj_i = _get_project(project.id)

    proj_i.check_write_permission(user_id)

    proj_i.name        = project.name
    proj_i.description = project.description

    attr_map = hdb.add_resource_attributes(proj_i, project.attributes)
    proj_data = _add_project_attribute_data(proj_i, attr_map, project.attribute_data)
    proj_i.attribute_data = proj_data
    db.DBSession.flush()

    return proj_i

@required_perms('get_project')
def get_project(project_id, include_deleted_networks=False, **kwargs):
    """
        get a project complexmodel
    """
    user_id = kwargs.get('user_id')
    log.info("Getting project %s", project_id)
    proj_i = _get_project(project_id)

    #lazy load owners
    proj_i.owners
    proj_i.attributes

    proj_i.check_read_permission(user_id)

    proj_j = JSONObject(proj_i)
    attr_data = get_project_attribute_data(proj_i.id, user_id=user_id)
    proj_j.attribute_data = [JSONObject(rs) for rs in attr_data]

    log.info("Getting networks for project %s", project_id)
    proj_j.networks = []
    for net_i in proj_i.networks:
        #lazy load owners
        net_i.owners
        net_i.scenarios

        if include_deleted_networks==False and net_i.status.lower() == 'x':
            continue

        can_read_network = net_i.check_read_permission(user_id, do_raise=False)
        if can_read_network is False:
            continue

        net_j = JSONObject(net_i)
        proj_j.networks.append(net_j)
    log.info("Project %s retrieved", project_id)

    return proj_j

@required_perms('get_project')
def get_project_by_network_id(network_id,**kwargs):
    """
        get a project complexmodel by a network_id
    """
    user_id = kwargs.get('user_id')

    projects_i = db.DBSession.query(Project).join(ProjectOwner).join(Network, Project.id==Network.project_id).filter(
                                                    Network.id==network_id,
                                                    ProjectOwner.user_id==user_id).order_by('name').all()

    ret_project = None
    for project_i in projects_i:
        try:
            project_i.check_read_permission(user_id)
            ret_project = project_i
        except:
            log.info("Can't return project %s. User %s does not have permission to read it.", project_i.id, user_id)
    return ret_project


@required_perms('get_project')
def get_project_by_name(project_name,**kwargs):
    """
        get a project complexmodel
    """
    user_id = kwargs.get('user_id')

    projects_i = db.DBSession.query(Project).join(ProjectOwner).filter(
                                                    Project.name==project_name,
                                                    ProjectOwner.user_id==user_id).order_by('name').all()

    ret_projects = []
    for project_i in projects_i:
        try:
            project_i.check_read_permission(user_id)
            ret_projects.append(project_i)
        except:
            log.info("Can't return project %s. User %s does not have permission to read it.", project_i.id, user_id)

    return ret_projects

@required_perms('get_project')
def get_projects(uid, include_shared_projects=True, projects_ids_list_filter=None, **kwargs):
    """
        Get all the projects owned by the specified user.
        These include projects created by the user, but also ones shared with the user.
        For shared projects, only include networks in those projects which are accessible to the user.

        the include_shared_projects flag indicates whether to include projects which have been shared
        with the user, or to only return projects created directly by this user.
    """
    req_user_id = kwargs.get('user_id')

    ##Don't load the project's networks. Load them separately, as the networks
    #must be checked individually for ownership
    projects_qry = db.DBSession.query(Project).options(joinedload('owners'))

    log.info("Getting projects for user %s", uid)

    if include_shared_projects is True:
        projects_qry = projects_qry.join(ProjectOwner).filter(Project.status=='A',
                                                        or_(ProjectOwner.user_id==uid,
                                                           Project.created_by==uid))
    else:
        projects_qry = projects_qry.join(ProjectOwner).filter(Project.created_by==uid)

    if projects_ids_list_filter is not None:
        # Filtering the search of project id
        if isinstance(projects_ids_list_filter, str):
            # Trying to read a csv string
            projects_ids_list_filter = eval(projects_ids_list_filter)
            if type(projects_ids_list_filter) is int:
                projects_qry = projects_qry.filter(Project.id==projects_ids_list_filter)
            else:
                projects_qry = projects_qry.filter(Project.id.in_(projects_ids_list_filter))



    projects_qry = projects_qry.options(noload('networks')).order_by('id')

    page = kwargs.get('page')
    max_per_page = kwargs.get('max_per_page', 10)
    if page is not None:
        projects_qry = projects_qry.offset(page*max_per_page).limit(max_per_page)

    projects_i = projects_qry.all()

    log.info("Project query done for user %s. %s projects found", uid, len(projects_i))

    user = db.DBSession.query(User).filter(User.id==req_user_id).one()
    isadmin = user.is_admin()

    project_network_lookup = get_projects_networks([p.id for p in projects_i], uid, isadmin=isadmin, **kwargs)

    #Load each
    projects_j = []
    for project_i in projects_i:
        if not isadmin:
            #Ensure the requesting user is allowed to see the project
            project_i.check_read_permission(req_user_id)

        project_i.attributes
        project_i.get_attribute_data()
        project_j = JSONObject(project_i)
        project_j.networks = project_network_lookup[project_i.id]
        projects_j.append(project_j)

    log.info("Networks loaded projects for user %s", uid)

    return projects_j

def get_projects_networks(project_ids, uid, isadmin=None, **kwargs):
    """
        Get all the networks in all the projects specified, checking for ownership
        with the user ID
        args:
            project_ids (list): a list of integer project IDs
            uid: the user ID for whom the request is being made so we can check for ownershiop
            isadmin: A flag to indicate if the requesting user is an admin. If null, does a query to find out
        returns:
            dict: A lookup keyed on project ID, with values being a list of networks
    """
    #Do a single query for all the networks in all the user's projects,
    #then make a lookup dictionary so that each projec's networks can be grouped
    #together, and accessed later.

    user_id = kwargs.get('user_id')
    if isadmin is None:
        req_user_id = kwargs.get('user_id')
        user = db.DBSession.query(User).filter(User.id==req_user_id).one()
        isadmin = user.is_admin()

    log.info("Getting for all the networks for in the specified projects...")
    network_qry = db.DBSession.query(Network)\
                                .options(joinedload('owners'))\
                                .filter(Network.project_id.in_(project_ids),\
                                        Network.status=='A')
    if not isadmin:
        network_qry.outerjoin(NetworkOwner)\
        .filter(or_(
            and_(NetworkOwner.user_id != None,
                    NetworkOwner.view == 'Y'),
            Network.created_by == uid
        ))

    networks = network_qry.all()
    project_network_lookup = defaultdict(list)
    for network_i in networks:
        net_j = JSONObject(network_i)
        if net_j.layout is not None:
            net_j.layout = JSONObject(net_j.layout)
        else:
            net_j.layout = JSONObject({})
        project_network_lookup[net_j.project_id].append(net_j)

    log.debug("Network query done")

    return project_network_lookup


@required_perms('get_project')
def get_project_attribute_data(project_id, **kwargs):
    req_user_id = kwargs.get('user_id')

    project_i = _get_project(project_id)

    project_i.check_read_permission(req_user_id)

    return project_i.get_attribute_data()

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
def delete_project(project_id,**kwargs):
    """
        Set the status of a project to 'X'
    """
    user_id = kwargs.get('user_id')
    project = _get_project(project_id)
    project.check_write_permission(user_id)

    templates = db.DBSession.query(Template).filter(Template.project_id==project_id).all()
    for template in templates:
        db.DBSession.delete(template)
    db.DBSession.flush()

    db.DBSession.delete(project)
    db.DBSession.flush()

    return 'OK'

@required_perms("get_project")
def get_networks(project_id, include_data='N', **kwargs):
    """
        Get all networks in a project
        Returns an array of network objects.
    """
    log.info("Getting networks for project %s", project_id)
    user_id = kwargs.get('user_id')
    project = _get_project(project_id)
    project.check_read_permission(user_id)

    rs = db.DBSession.query(Network.id, Network.status).filter(Network.project_id==project_id).all()
    networks=[]
    for r in rs:
        if r.status != 'A':
            continue
        try:
            net = network.get_network(r.id, summary=True, include_data=include_data, **kwargs)
            log.info("Network %s retrieved", net.name)
            networks.append(net)
        except PermissionError:
            log.info("Not returning network %s as user %s does not have "
                         "permission to read it."%(r.id, user_id))

    return networks

@required_perms('get_project')
def get_network_project(network_id, **kwargs):
    """
        get the project that a network is in
    """

    net_proj = db.DBSession.query(Project)\
        .join(Network, Project.id == Network.project_id)\
        .filter(Network.id == network_id).first()

    if net_proj is None:
        raise HydraError("Network %s not found"% network_id)

    return net_proj


@required_perms('get_project', 'add_project')
def clone_project(project_id, recipient_user_id=None, new_project_name=None, new_project_description=None, **kwargs):
    """
        Create an exact clone of the specified project for the specified user.
    """

    user_id = kwargs['user_id']

    log.info("Creating a new project for cloned network")

    project = db.DBSession.query(Project).filter(Project.id==project_id).one()
    project.check_write_permission(user_id)

    if new_project_name is None:
        user = db.DBSession.query(User).filter(User.id==user_id).one()
        new_project_name = project.name + ' Cloned By {}'.format(user.display_name)

    #check a project with this name doesn't already exist:
    project_with_name =  db.DBSession.query(Project).filter(Project.name==new_project_name,
                                                     Project.created_by==user_id).all()
    if len(project_with_name) > 0:
        raise HydraError("A project with the name {0} already exists".format(new_project_name))

    new_project = Project()
    new_project.name = new_project_name
    if new_project_description:
        new_project.description = new_project_description
    else:
        new_project.description = project.description

    new_project.created_by = user_id

    if recipient_user_id is not None:
        project.check_share_permission(user_id)
        new_project.set_owner(recipient_user_id)

    new_project.set_owner(user_id)


    db.DBSession.add(new_project)
    db.DBSession.flush()

    network_ids = db.DBSession.query(Network.id).filter(
                                        Network.project_id==project_id).all()
    for n in network_ids:
        network.clone_network(n.id,
                              recipient_user_id=recipient_user_id,
                              project_id=new_project.id,
                             user_id=user_id)

    db.DBSession.flush()

    return new_project.id
