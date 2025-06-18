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
from ..db.model import Network, Project, ProjectOwner, NetworkOwner, User, Dataset
from hydra_base.lib.objects import JSONObject
from sqlalchemy.orm.exc import NoResultFound
from hydra_base.util.permissions import required_role

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

def share_network(network_id, usernames, read_only, share, **kwargs):
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

    #Support wither boolean or character based flags for read_only and share
    if read_only in ('Y', True):
        write = 'N'
        share = 'N'
    else:
        write = 'Y'

    if share is True:
        share = 'Y'
    elif share is False:
        share = 'N'

    if net_i.created_by != int(user_id) and share == 'Y':
        raise HydraError(f"Cannot share the 'sharing' ability as user {user_id} is not"
                         f" the owner of network {network_id}")

    for username in usernames:
        user_i = _get_user(username)
        #Set the owner ship on the network itself
        net_i.set_owner(user_i.id, write=write, share=share)

        Project.clear_cache(user_i.id)
    db.DBSession.flush()

def unshare_network(network_id, usernames, **kwargs):
    """
        Un-Share a network with a list of users, identified by their usernames.
    """

    user_id = kwargs.get('user_id')
    net_i = _get_network(network_id)
    net_i.check_share_permission(user_id)

    for username in usernames:
        user_i = _get_user(username)
        #Set the owner ship on the network itself
        net_i.unset_owner(user_i.id)
        Project.clear_cache(user_i.id)

    db.DBSession.flush()

def share_project(project_id, usernames, read_only=False, share=False, flush=True, **kwargs):
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

    # for owner in proj_i.owners:
    #     if user_id == owner.user_id:
    #         break
    # else:
    #     raise HydraError("Permission Denied. Cannot share project.")

    if read_only in ('Y', True):
        write = 'N'
        share = 'N'
    if read_only in ('N', False):
        write = 'Y'
    if read_only is None:
        write = None

    if share in ('Y', True):
        share = 'Y'
    if share in ('N', False):
        share = 'N'

    # if proj_i.created_by != user_id and share == 'Y':
    #     raise HydraError("Cannot share the 'sharing' ability as user %s is not"
    #                  " the owner of project %s"%
    #                  (user_id, project_id))

    for username in usernames:
        user_i = _get_user(username)

        proj_i.set_owner(user_i.id, write=write, share=share)

        Project.clear_cache(user_i.id)


    if flush is True:
        db.DBSession.flush()

def unshare_project(project_id, usernames, **kwargs):
    """
        Un-share a project with a list of users, identified by their usernames.
    """

    user_id = kwargs.get('user_id')
    proj_i = _get_project(project_id)
    proj_i.check_share_permission(user_id)

    for username in usernames:
        user_i = _get_user(username)
        #Set the owner ship on the network itself
        proj_i.unset_owner(user_i.id)
        Project.clear_cache(user_i.id)
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

@required_role("admin")
def get_all_project_owners(project_ids=None, **kwargs):
    """
        Get the project owner entries for all the requested projects.
        If the project_ids argument is None, return all the owner entries
        for ALL projects
    """


    projowner_qry = db.DBSession.query(ProjectOwner)

    if project_ids is not None:
       projowner_qry = projowner_qry.filter(ProjectOwner.project_id.in_(project_ids))

    project_owners_i = projowner_qry.all()

    return [JSONObject(project_owner_i) for project_owner_i in project_owners_i]

@required_role("admin")
def get_all_network_owners(network_ids=None, **kwargs):
    """
        Get the network owner entries for all the requested networks.
        If the network_ids argument is None, return all the owner entries
        for ALL networks
    """


    networkowner_qry = db.DBSession.query(NetworkOwner)

    if network_ids is not None:
       networkowner_qry = networkowner_qry.filter(NetworkOwner.network_id.in_(network_ids))

    network_owners_i = networkowner_qry.all()

    return [JSONObject(network_owner_i) for network_owner_i in network_owners_i]

@required_role("admin")
def bulk_set_project_owners(project_owners, **kwargs):
    """
        Set the project owner of multiple projects at once.
        Accepts a list of JSONObjects which look like:
            {
             'project_id': XX,
             'user_id'   : YY,
             'view'      : 'Y'/ 'N'
             'edit'      : 'Y'/ 'N'
             'share'      : 'Y'/ 'N'
            }
           """

    project_ids = [po.project_id for po in project_owners]

    existing_projowners = db.DBSession.query(ProjectOwner).filter(ProjectOwner.project_id.in_(project_ids)).all()

    #Create a lookup based on the unique key for this table (project_id, user_id)
    po_lookup = {}

    for po in existing_projowners:
        po_lookup[(po.project_id, po.user_id)] = po

    for project_owner in project_owners:
        #check if the entry is already there
        if po_lookup.get((project_owner.project_id, project_owner.user_id)):
            continue

        new_po = ProjectOwner()
        new_po.project_id = project_owner.project_id
        new_po.user_id    = project_owner.user_id
        new_po.view       = project_owner.view
        new_po.edit       = project_owner.edit
        new_po.share      = project_owner.share

        db.DBSession.add(new_po)

    db.DBSession.flush()

@required_role("admin")
def bulk_set_network_owners(network_owners, **kwargs):
    """
        Set the network owner of multiple networks at once.
        Accepts a list of JSONObjects which look like:
            {
             'network_id': XX,
             'user_id'   : YY,
             'view'      : 'Y'/ 'N'
             'edit'      : 'Y'/ 'N'
             'share'      : 'Y'/ 'N'
            }
           """

    network_ids = [no.network_id for no in network_owners]

    existing_projowners = db.DBSession.query(NetworkOwner).filter(NetworkOwner.network_id.in_(network_ids)).all()

    #Create a lookup based on the unique key for this table (network_id, user_id)
    no_lookup = {}

    for no in existing_projowners:
        no_lookup[(no.network_id, no.user_id)] = no

    for network_owner in network_owners:
        #check if the entry is already there
        if no_lookup.get((network_owner.network_id, network_owner.user_id)):
            continue

        new_no = NetworkOwner()
        new_no.network_id = network_owner.network_id
        new_no.user_id    = network_owner.user_id
        new_no.view       = network_owner.view
        new_no.edit       = network_owner.edit
        new_no.share      = network_owner.share

        db.DBSession.add(new_no)

    db.DBSession.flush()
