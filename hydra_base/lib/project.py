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

from ..exceptions import ResourceNotFoundError
from . import scenario
import logging
from ..exceptions import PermissionError, HydraError
from ..db.model import Project, ProjectOwner, Network, NetworkOwner
from .. import db
from . import network
from .objects import JSONObject
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import class_mapper, joinedload_all, noload
from sqlalchemy import and_, or_
from ..util import hdb
from sqlalchemy.util import KeyedTuple

log = logging.getLogger(__name__)

def _get_project(project_id):
    try:
        project = db.DBSession.query(Project).filter(Project.id==project_id).one()
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

def add_project(project,**kwargs):
    """
        Add a new project
        returns a project complexmodel
    """
    user_id = kwargs.get('user_id')


    existing_proj = get_project_by_name(project.name,user_id=user_id)

    if len(existing_proj) > 0:
        raise HydraError("A Project with the name \"%s\" already exists"%(project.name,))

    #check_perm(user_id, 'add_project')
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

def update_project(project,**kwargs):
    """
        Update a project
        returns a project complexmodel
    """

    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'update_project')
    proj_i = _get_project(project.id)

    proj_i.check_write_permission(user_id)

    proj_i.name        = project.name
    proj_i.description = project.description

    attr_map = hdb.add_resource_attributes(proj_i, project.attributes)
    proj_data = _add_project_attribute_data(proj_i, attr_map, project.attribute_data)
    proj_i.attribute_data = proj_data
    db.DBSession.flush()

    return proj_i

def get_project(project_id,**kwargs):
    """
        get a project complexmodel
    """
    user_id = kwargs.get('user_id')
    proj_i = _get_project(project_id)

    proj_i.check_read_permission(user_id)

    return proj_i

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

def to_named_tuple(obj, visited_children=None, back_relationships=None, levels=None, ignore=[], extras={}):
    """
        Altered from an example found on stackoverflow
        http://stackoverflow.com/questions/23554119/convert-sqlalchemy-orm-result-to-dict
    """

    if visited_children is None:
        visited_children = []

    if back_relationships is None:
        back_relationships = []

    serialized_data = {c.key: getattr(obj, c.key) for c in obj.__table__.columns}


    #Any other non-column data to include in the keyed tuple
    for k, v in extras.items():
        serialized_data[k] = v

    relationships = class_mapper(obj.__class__).relationships

    #Set the attributes to 'None' first, so the attributes are there, even if they don't
    #get filled in:
    for name, relation in relationships.items():
        if relation.uselist:
            serialized_data[name] = tuple([])
        else:
            serialized_data[name] = None


    visitable_relationships = [(name, rel) for name, rel in relationships.items() if name not in back_relationships]

    if levels is not None and levels > 0:
        for name, relation in visitable_relationships:

            levels = levels - 1

            if name in ignore:
                continue

            if relation.backref:
                back_relationships.append(relation.backref)

            relationship_children = getattr(obj, name)

            if relationship_children is not None:
                if relation.uselist:
                    children = []
                    for child in [c for c in relationship_children if c not in visited_children]:
                        visited_children.append(child)
                        children.append(to_named_tuple(child, visited_children, back_relationships, ignore=ignore, levels=levels))
                    serialized_data[name] = tuple(children)
                else:
                    serialized_data[name] = to_named_tuple(relationship_children, visited_children, back_relationships, ignore=ignore, levels=levels)

    vals = []
    cols = []
    for k, v in serialized_data.items():
        vals.append(k)
        cols.append(v)

    result = KeyedTuple(cols, vals)

    return result


def get_projects(uid,**kwargs):
    """
        Get all the projects owned by the specified user.
        These include projects created by the user, but also ones shared with the user.
        For shared projects, only include networks in those projects which are accessible to the user.
    """
    req_user_id = kwargs.get('user_id')

    ##Don't load the project's networks. Load them separately, as the networks
    #must be checked individually for ownership
    projects_i = db.DBSession.query(Project).join(ProjectOwner)\
                                                 .filter(Project.status=='A',
                                                        or_(ProjectOwner.user_id==uid,
                                                           Project.created_by==uid))\
                                                 .options(noload('networks'))\
                                                 .order_by('id').all()

    #Load each 
    projects_j = []
    for project_i in projects_i:
        #Ensure the requesting user is allowed to see the project
        project_i.check_read_permission(req_user_id)
        #lazy load owners
        project_i.owners

        networks_i = db.DBSession.query(Network).join(NetworkOwner)\
                                .filter(Network.project_id==project_i.id,
                                        Network.status=='A',
                                        or_(Network.created_by==uid,\
                                            NetworkOwner.user_id==uid))

        for network_i in networks_i:
            network_i.check_read_permission(req_user_id)
        
        project_j = JSONObject(project_i)
        project_j.networks = [JSONObject(network_i) for network_i in networks_i]
        projects_j.append(project_j)


    return projects_j


def set_project_status(project_id, status, **kwargs):
    """
        Set the status of a project to 'X'
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_project')
    project = _get_project(project_id)
    project.check_write_permission(user_id)
    project.status = status
    db.DBSession.flush()

def delete_project(project_id,**kwargs):
    """
        Set the status of a project to 'X'
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_project')
    project = _get_project(project_id)
    project.check_write_permission(user_id)
    db.DBSession.delete(project)
    db.DBSession.flush()

    return 'OK'

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

def get_network_project(network_id, **kwargs):
    """
        get the project that a network is in
    """

    net_proj = db.DBSession.query(Project).join(Network, and_(Project.id==Network.id, Network.id==network_id)).first()

    if net_proj is None:
        raise HydraError("Network %s not found"% network_id)

    return net_proj
