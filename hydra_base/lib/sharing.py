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

from ..exceptions import HydraError, ResourceNotFoundError
import logging
log = logging.getLogger(__name__)
from .. import db
from ..db.model import Network, Project, User, Dataset
from sqlalchemy.orm.exc import NoResultFound

def _get_project(project_id):
    try:
        proj_i = db.DBSession.query(Project).filter(Project.id==project_id).one()
        return proj_i
    except NoResultFound:
        raise ResourceNotFoundError("Project %s not found"%(project_id,))

def _get_network(network_id):
    try:
        net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
        return net_i
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

def _get_user(username):
    try:
        user_i = db.DBSession.query(User).filter(User.username==username).one()
        return user_i
    except NoResultFound:
        raise ResourceNotFoundError("User %s not found"%(username))

def _get_dataset(dataset_id):
    try:
        dataset_i = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()
        return dataset_i
    except NoResultFound:
        raise ResourceNotFoundError("Dataset %s not found"%(dataset_id))

def share_network(network_id, usernames, read_only, share,**kwargs):
    """
        Share a network with a list of users, identified by their usernames.

        The read_only flag ('Y' or 'N') must be set
        to 'Y' to allow write access or sharing.

        The share flat ('Y' or 'N') must be set to 'Y' to allow the
        project to be shared with other users
    """

    user_id = kwargs.get('user_id')
    net_i = _get_network(network_id)
    net_i.check_share_permission(user_id)

    if read_only == 'Y':
        write = 'N'
        share = 'N'
    else:
        write = 'Y'

    if net_i.created_by != int(user_id) and share == 'Y':
        raise HydraError("Cannot share the 'sharing' ability as user %s is not"
                     " the owner of network %s"%
                     (user_id, network_id))

    for username in usernames:
        user_i = _get_user(username)
        #Set the owner ship on the network itself
        net_i.set_owner(user_i.id, write=write, share=share)
        for o in net_i.project.owners:
            if o.user_id == user_i.id:
                break
        else:
            #Give the user read access to the containing project
            net_i.project.set_owner(user_i.id, write='N', share='N')
    db.DBSession.flush()

def unshare_network(network_id, usernames,**kwargs):
    """
        Un-Share a network with a list of users, identified by their usernames.
    """

    user_id = kwargs.get('user_id')
    net_i = _get_network(network_id)
    net_i.check_share_permission(user_id)

    for username in usernames:
        user_i = _get_user(username)
        #Set the owner ship on the network itself

    net_i.unset_owner(user_i.id, write=write, share=share)
    db.DBSession.flush()

def share_project(project_id, usernames, read_only, share,**kwargs):
    """
        Share an entire project with a list of users, identifed by
        their usernames.

        The read_only flag ('Y' or 'N') must be set
        to 'Y' to allow write access or sharing.

        The share flat ('Y' or 'N') must be set to 'Y' to allow the
        project to be shared with other users
    """
    user_id = kwargs.get('user_id')

    proj_i = _get_project(project_id)

    #Is the sharing user allowed to share this project?
    proj_i.check_share_permission(int(user_id))

    user_id = int(user_id)

    for owner in proj_i.owners:
        if user_id == owner.user_id:
            break
    else:
       raise HydraError("Permission Denied. Cannot share project.")

    if read_only == 'Y':
        write = 'N'
        share = 'N'
    else:
        write = 'Y'

    if proj_i.created_by != user_id and share == 'Y':
        raise HydraError("Cannot share the 'sharing' ability as user %s is not"
                     " the owner of project %s"%
                     (user_id, project_id))

    for username in usernames:
        user_i = _get_user(username)

        proj_i.set_owner(user_i.id, write=write, share=share)

        for net_i in proj_i.networks:
            net_i.set_owner(user_i.id, write=write, share=share)
    db.DBSession.flush()

def unshare_project(project_id, usernames,**kwargs):
    """
        Un-share a project with a list of users, identified by their usernames.
    """

    user_id = kwargs.get('user_id')
    proj_i = _get_project(project_id)
    proj_i.check_share_permission(user_id)

    for username in usernames:
        user_i = _get_user(username)
        #Set the owner ship on the network itself
        proj_i.unset_owner(user_i.id, write=write, share=share)
    db.DBSession.flush()

def set_project_permission(project_id, usernames, read, write, share,**kwargs):
    """
        Set permissions on a project to a list of users, identifed by
        their usernames.

        The read flag ('Y' or 'N') sets read access, the write
        flag sets write access. If the read flag is 'N', then there is
        automatically no write access or share access.
    """
    user_id = kwargs.get('user_id')

    proj_i = _get_project(project_id)

    #Is the sharing user allowed to share this project?
    proj_i.check_share_permission(user_id)

    #You cannot edit something you cannot see.
    if read == 'N':
        write = 'N'
        share = 'N'

    for username in usernames:
        user_i = _get_user(username)

        #The creator of a project must always have read and write access
        #to their project
        if proj_i.created_by == user_i.id:
            raise HydraError("Cannot set permissions on project %s"
                             " for user %s as this user is the creator." %
                             (project_id, username))

        proj_i.set_owner(user_i.id, read=read, write=write)

        for net_i in proj_i.networks:
            net_i.set_owner(user_i.id, read=read, write=write, share=share)
    db.DBSession.flush()

def set_network_permission(network_id, usernames, read, write, share,**kwargs):
    """
        Set permissions on a network to a list of users, identifed by
        their usernames. The read flag ('Y' or 'N') sets read access, the write
        flag sets write access. If the read flag is 'N', then there is
        automatically no write access or share access.
    """

    user_id = kwargs.get('user_id')

    net_i = _get_network(network_id)

    #Check if the user is allowed to share this network.
    net_i.check_share_permission(user_id)

    #You cannot edit something you cannot see.
    if read == 'N':
        write = 'N'
        share = 'N'

    for username in usernames:

        user_i = _get_user(username)

        #The creator of a network must always have read and write access
        #to their project
        if net_i.created_by == user_i.id:
            raise HydraError("Cannot set permissions on network %s"
                             " for user %s as tis user is the creator." %
                             (network_id, username))

        net_i.set_owner(user_i.id, read=read, write=write, share=share)
    db.DBSession.flush()

def hide_dataset(dataset_id, exceptions, read, write, share,**kwargs):
    """
        Hide a particular piece of data so it can only be seen by its owner.
        Only an owner can hide (and unhide) data.
        Data with no owner cannot be hidden.

        The exceptions paramater lists the usernames of those with permission to view the data
        read, write and share indicate whether these users can read, edit and share this data.
    """

    user_id = kwargs.get('user_id')
    dataset_i = _get_dataset(dataset_id)
    #check that I can hide the dataset
    if dataset_i.created_by != int(user_id):
        raise HydraError('Permission denied. '
                        'User %s is not the owner of dataset %s'
                        %(user_id, dataset_i.name))

    dataset_i.hidden = 'Y'
    if exceptions is not None:
        for username in exceptions:
            user_i = _get_user(username)
            dataset_i.set_owner(user_i.id, read=read, write=write, share=share)
    db.DBSession.flush()

def unhide_dataset(dataset_id,**kwargs):
    """
        Hide a particular piece of data so it can only be seen by its owner.
        Only an owner can hide (and unhide) data.
        Data with no owner cannot be hidden.

        The exceptions paramater lists the usernames of those with permission to view the data
        read, write and share indicate whether these users can read, edit and share this data.
    """

    user_id = kwargs.get('user_id')
    dataset_i = _get_dataset(dataset_id)
    #check that I can unhide the dataset
    if dataset_i.created_by != int(user_id):
        raise HydraError('Permission denied. '
                        'User %s is not the owner of dataset %s'
                        %(user_id, dataset_i.name))

    dataset_i.hidden = 'N'
    db.DBSession.flush()
