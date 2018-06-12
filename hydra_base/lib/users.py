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

from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm.exc import NoResultFound

from ..db.model import User, Role, Perm, RoleUser, RolePerm
from .. import db

import bcrypt

from ..exceptions import ResourceNotFoundError, HydraError
import logging

log = logging.getLogger(__name__)

def _get_user_id(username):
    try:
        rs = db.DBSession.query(User.id).filter(User.username==username).one()
        return rs.id
    except:
        return None

def _get_user(user_id, **kwargs):
    try:
        user_i = db.DBSession.query(User).filter(User.id==user_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("User %s does not exist"%user_id)

    return user_i

def _get_role(role_id,**kwargs):
    try:
        role_i = db.DBSession.query(Role).filter(Role.id==role_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Role %s does not exist"%role_id)

    return role_i

def _get_perm(perm_id,**kwargs):
    try:
        perm_i = db.DBSession.query(Perm).filter(Perm.id==perm_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Permission %s does not exist"%perm_id)

    return perm_i

def get_username(uid,**kwargs):
    """
        Return the username of a given user_id
    """
    rs = db.DBSession.query(User.username).filter(User.id==uid).one()

    if rs is None:
        raise ResourceNotFoundError("User with ID %s not found"%uid)

    return rs.username

def get_usernames_like(username,**kwargs):
    """
        Return a list of usernames like the given string.
    """
    checkname = "%%%s%%"%username
    rs = db.DBSession.query(User.username).filter(User.username.like(checkname)).all()
    return [r.username for r in rs]


def add_user(user, **kwargs):
    """
        Add a user
    """
    #check_perm(kwargs.get('user_id'), 'add_user')
    u = User()

    u.username     = user.username
    u.display_name = user.display_name

    user_id = _get_user_id(u.username)

    #If the user is already there, cannot add another with
    #the same username.
    if user_id is not None:
        raise HydraError("User %s already exists!"%user.username)

    u.password = bcrypt.hashpw(str(user.password).encode('utf-8'), bcrypt.gensalt())

    db.DBSession.add(u)
    db.DBSession.flush()

    return u

def update_user_display_name(user,**kwargs):
    """
        Update a user's display name
    """
    #check_perm(kwargs.get('user_id'), 'edit_user')
    try:
        user_i = db.DBSession.query(User).filter(User.id==user.id).one()
        user_i.display_name = user.display_name
        return user_i
    except NoResultFound:
        raise ResourceNotFoundError("User (id=%s) not found"%(user.id))

def update_user_password(new_pwd_user_id, new_password,**kwargs):
    """
        Update a user's password
    """
    #check_perm(kwargs.get('user_id'), 'edit_user')
    try:
        user_i = db.DBSession.query(User).filter(User.id==new_pwd_user_id).one()
        user_i.password = bcrypt.hashpw(str(new_password).encode('utf-8'), bcrypt.gensalt())
        return user_i
    except NoResultFound:
        raise ResourceNotFoundError("User (id=%s) not found"%(new_pwd_user_id))

def get_user(uid, **kwargs):
    """
        Get a user by ID
    """
    user_id=kwargs.get('user_id')
    if uid is None:
        uid = user_id
    user_i = _get_user(uid)
    return user_i

def get_user_by_name(uname,**kwargs):
    """
        Get a user by username
    """
    try:
        user_i = db.DBSession.query(User).filter(User.username==uname).one()
        return user_i
    except NoResultFound:
        return None

def get_user_by_id(uid,**kwargs):
    """
        Get a user by username
    """
    user_id = kwargs.get('user_id')
    try:
        user_i = _get_user(uid)
        return user_i
    except NoResultFound:
        return None

def delete_user(deleted_user_id,**kwargs):
    """
        Delete a user
    """
    #check_perm(kwargs.get('user_id'), 'edit_user')
    try:
        user_i = db.DBSession.query(User).filter(User.id==deleted_user_id).one()
        db.DBSession.delete(user_i)
    except NoResultFound:
        raise ResourceNotFoundError("User (user_id=%s) does not exist"%(deleted_user_id))


    return 'OK'


def add_role(role,**kwargs):
    """
        Add a new role
    """
    #check_perm(kwargs.get('user_id'), 'add_role')
    role_i = Role(name=role.name, code=role.code)
    db.DBSession.add(role_i)
    db.DBSession.flush()

    return role_i

def delete_role(role_id,**kwargs):
    """
        Delete a role
    """
    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        role_i = db.DBSession.query(Role).filter(Role.id==role_id).one()
        db.DBSession.delete(role_i)
    except InvalidRequestError:
        raise ResourceNotFoundError("Role (role_id=%s) does not exist"%(role_id))

    return 'OK'

def add_perm(perm,**kwargs):
    """
        Add a permission
    """
    #check_perm(kwargs.get('user_id'), 'add_perm')
    perm_i = Perm(name=perm.name, code=perm.code)
    db.DBSession.add(perm_i)
    db.DBSession.flush()

    return perm_i

def delete_perm(perm_id,**kwargs):
    """
        Delete a permission
    """

    #check_perm(kwargs.get('user_id'), 'edit_perm')
    try:
        perm_i = db.DBSession.query(Perm).filter(Perm.id==perm_id).one()
        db.DBSession.delete(perm_i)
    except InvalidRequestError:
        raise ResourceNotFoundError("Permission (id=%s) does not exist"%(perm_id))

    return 'OK'


def set_user_role(new_user_id, role_id, **kwargs):
    """
        Apply `role_id` to `new_user_id`

        Note this function returns the `Role` instance associated with `role_id`
    """
    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        _get_user(new_user_id)
        role_i = _get_role(role_id)
        roleuser_i = RoleUser(user_id=new_user_id, role_id=role_id)
        role_i.roleusers.append(roleuser_i)
        db.DBSession.flush()
    except Exception as e: # Will occur if the foreign keys do not exist
        log.exception(e)
        raise ResourceNotFoundError("User or Role does not exist")

    return role_i

def delete_user_role(deleted_user_id, role_id,**kwargs):
    """
        Remove a user from a role
    """
    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        _get_user(deleted_user_id)
        _get_role(role_id)
        roleuser_i = db.DBSession.query(RoleUser).filter(RoleUser.user_id==deleted_user_id, RoleUser.role_id==role_id).one()
        db.DBSession.delete(roleuser_i)
    except NoResultFound:
        raise ResourceNotFoundError("User Role does not exist")

    return 'OK'

def set_role_perm(role_id, perm_id,**kwargs):
    """
        Insert a permission into a role
    """
    #check_perm(kwargs.get('user_id'), 'edit_perm')

    _get_perm(perm_id)
    role_i = _get_role(role_id)
    roleperm_i = RolePerm(role_id=role_id, perm_id=perm_id)

    role_i.roleperms.append(roleperm_i)

    db.DBSession.flush()

    return role_i

def delete_role_perm(role_id, perm_id,**kwargs):
    """
        Remove a permission from a role
    """
    #check_perm(kwargs.get('user_id'), 'edit_perm')
    _get_perm(perm_id)
    _get_role(role_id)

    try:
        roleperm_i = db.DBSession.query(RolePerm).filter(RolePerm.role_id==role_id, RolePerm.perm_id==perm_id).one()
        db.DBSession.delete(roleperm_i)
    except NoResultFound:
        raise ResourceNotFoundError("Role Perm does not exist")

    return 'OK'

def update_role(role,**kwargs):
    """
        Update the role.
        Used to add permissions and users to a role.
    """
    #check_perm(kwargs.get('user_id'), 'edit_role')
    try:
        role_i = db.DBSession.query(Role).filter(Role.id==role.id).one()
        role_i.name = role.name
        role_i.code = role.code
    except NoResultFound:
        raise ResourceNotFoundError("Role (role_id=%s) does not exist"%(role.id))

    for perm in role.permissions:
        _get_perm(perm.id)
        roleperm_i = RolePerm(role_id=role.id,
                              perm_id=perm.id
                              )

        db.DBSession.add(roleperm_i)

    for user in role.users:
        _get_user(user.id)
        roleuser_i = RoleUser(user_id=user.id,
                                         perm_id=perm.id
                                        )

        db.DBSession.add(roleuser_i)

    db.DBSession.flush()
    return role_i


def get_all_users(**kwargs):
    """
        Get the username & ID of all users.
    """

    rs = db.DBSession.query(User).all()

    return rs

def get_all_perms(**kwargs):
    """
        Get all permissions
    """
    rs = db.DBSession.query(Perm).all()
    return rs

def get_all_roles(**kwargs):
    """
        Get all roles
    """
    rs = db.DBSession.query(Role).all()
    return rs

def get_role(role_id,**kwargs):
    """
        Get a role by its ID.
    """
    try:
        role = db.DBSession.query(Role).filter(Role.id==role_id).one()
        return role
    except NoResultFound:
        raise HydraError("Role not found (role_id={})".format(role_id))

def get_user_roles(uid,**kwargs):
    """
        Get the roles for a user.
        @param user_id
    """
    try:
        user_roles = db.DBSession.query(Role).filter(Role.id==RoleUser.role_id,
                                                  RoleUser.user_id==uid).all()
        return user_roles
    except NoResultFound:
        raise HydraError("Roles not found for user (user_id={})".format(uid))


def get_user_permissions(uid, **kwargs):
    """
        Get the roles for a user.
        @param user_id
    """
    try:
        _get_user(uid)

        user_perms = db.DBSession.query(Perm).filter(Perm.id==RolePerm.perm_id,
                                                  RolePerm.role_id==Role.id,
                                                  Role.id==RoleUser.role_id,
                                                  RoleUser.user_id==uid).all()
        return user_perms
    except:
        raise HydraError("Permissions not found for user (user_id={})".format(uid))


def get_role_by_code(role_code,**kwargs):
    """
        Get a role by its code
    """
    try:
        role = db.DBSession.query(Role).filter(Role.code==role_code).one()
        return role
    except NoResultFound:
        raise ResourceNotFoundError("Role not found (role_code={})".format(role_code))


def get_perm(perm_id,**kwargs):
    """
        Get all permissions
    """

    try:
        perm = db.DBSession.query(Perm).filter(Perm.id==perm_id).one()
        return perm
    except NoResultFound:
        raise ResourceNotFoundError("Permission not found (perm_id={})".format(perm_id))

def get_perm_by_code(perm_code,**kwargs):
    """
        Get a permission by its code
    """

    try:
        perm = db.DBSession.query(Perm).filter(Perm.code==perm_code).one()
        return perm
    except NoResultFound:
        raise ResourceNotFoundError("Permission not found (perm_code={})".format(perm_code))
