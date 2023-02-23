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

from functools import wraps
from .. import db
from ..db.model import Perm, User, Role, RolePerm, RoleUser
from sqlalchemy.orm.exc import NoResultFound
from ..exceptions import PermissionError



def check_perm(user_id, permission_code):
    """
        Checks whether a user has permission to perform an action.
        The permission_code parameter should be a permission contained in tPerm.

        If the user does not have permission to perfom an action, a permission
        error is thrown.
    """
    try:
        perm = db.DBSession.query(Perm).filter(Perm.code==permission_code).one()
    except NoResultFound:
        raise PermissionError("Nonexistent permission type: %s"%(permission_code))


    try:
        #get all the roles where the specified user has the specified permission
        qry = db.DBSession.query(RoleUser.role_id)\
            .join(RolePerm, RolePerm.role_id == RoleUser.role_id)\
            .filter(RolePerm.perm_id == perm.id)\
            .filter(RoleUser.user_id == user_id)

        res = qry.all()
    except NoResultFound:
        raise PermissionError("Permission denied. User %s does not have permission %s"%
                        (user_id, permission_code))

def check_role(user_id, role_code):
    """
        Checks whether a user has been assigned the specified role
        The role_code parameter should be a role code contained in tRole.

        If the user does not have code a permission error is thrown.
    """
    try:
        role = db.DBSession.query(Role).filter(Role.code==role_code).one()
    except NoResultFound:
        raise PermissionError("Nonexistent role type: %s"%(role_code))


    try:
        #get all the roles where the specified user has the specified permission
        qry = db.DBSession.query(RoleUser.role_id)\
            .filter(RoleUser.role_id == role.id)\
            .filter(RoleUser.user_id == user_id)

        res = qry.all()
    except NoResultFound:
        raise PermissionError("Permission denied. User %s does not have role %s"%
                        (user_id, role_code))



def required_perms(*req_perms):
    """
       Decorator applied to functions requiring caller to possess permission
       Takes args tuple of required perms and raises PermissionsError
       via check_perm if these are not a subset of user perms
    """
    def dec_wrapper(wfunc):
        @wraps(wfunc)
        def wrapped(*args, **kwargs):
            user_id = kwargs.get("user_id")

            #bind this here so that the 'updated by' columns can be updated
            #automatically in the DB, in order to ensure correct auditing
            db.DBSession.user_id = user_id

            for perm in req_perms:
                check_perm(user_id, perm)

            return wfunc(*args, **kwargs)

        return wrapped
    return dec_wrapper

def required_role(req_role):
    """
       Decorator applied to functions requiring caller to possess the specified role
    """
    def dec_wrapper(wfunc):
        @wraps(wfunc)
        def wrapped(*args, **kwargs):
            user_id = kwargs.get("user_id")
            try:
                res = db.DBSession.query(RoleUser).filter(RoleUser.user_id==user_id).join(Role).filter(Role.code==req_role).one()
            except NoResultFound:
                raise PermissionError("Permission denied. User %s does not have role %s"%
                                (user_id, req_role))

            return wfunc(*args, **kwargs)

        return wrapped
    return dec_wrapper
