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
import threading
import time
from functools import wraps
from .. import db, config
from ..db.model import Perm, User, Role, RolePerm, RoleUser
from sqlalchemy.orm.exc import NoResultFound
from ..exceptions import PermissionError

log = logging.getLogger(__name__)

#Writing rule code, which job runners execute, needs this permission as well as the ordinary rule
#permissions. It is held by the `rule_author` role and not by the other roles.
AUTHOR_PERM = "author_rules"

_perm_cache = threading.local()

#Seconds a positive permission check is cached for before being re-verified
#against the DB. Keeps check_perm fast for hot paths while bounding how long
#a revoked permission/role can remain (incorrectly) usable.
PERM_CACHE_TTL = config.getint('permissions', 'cache_ttl', 30)


def _permission_mode():
    """
        'enforce' (default): a failed check raises PermissionError.
        'warn': a failed check is logged and allowed. Only intended for a staging run
                to find which calls would be denied before enforcing on a live system.
        Set with [permissions] mode in hydra.ini.
    """
    mode = str(config.get('permissions', 'mode', 'enforce')).strip().lower()
    return 'warn' if mode == 'warn' else 'enforce'


def _deny(message):
    if _permission_mode() == 'warn':
        log.warning("[permissions] WOULD DENY (mode=warn, allowed): %s", message)
        return
    raise PermissionError(message)


def check_perm(user_id, permission_code):
    """
        Checks whether a user has permission to perform an action.
        The permission_code parameter should be a permission contained in tPerm.

        If the user does not have permission to perfom an action, a permission
        error is thrown. A missing user_id is never permitted.
    """
    if user_id is None:
        _deny("Permission denied. No user specified for permission %s" % (permission_code,))
        return

    cache = _perm_cache.__dict__.setdefault('cache', {})
    key = (user_id, permission_code)
    cached_at = cache.get(key)
    if cached_at is not None and time.time() - cached_at < PERM_CACHE_TTL:
        return

    try:
        perm = db.DBSession.query(Perm).filter(Perm.code==permission_code).one()
    except NoResultFound:
        raise PermissionError("Nonexistent permission type: %s"%(permission_code))

    #is there any role assigned to the user which carries the permission?
    has_perm = db.DBSession.query(RoleUser.role_id)\
        .join(RolePerm, RolePerm.role_id == RoleUser.role_id)\
        .filter(RolePerm.perm_id == perm.id)\
        .filter(RoleUser.user_id == user_id)\
        .first() is not None

    if not has_perm:
        _deny("Permission denied. User %s does not have permission %s" % (user_id, permission_code))
        return

    cache[key] = time.time()

def check_role(user_id, role_code):
    """
        Checks whether a user has been assigned the specified role
        The role_code parameter should be a role code contained in tRole.

        If the user does not have the role a permission error is thrown.
        A missing user_id is never permitted.
    """
    if user_id is None:
        _deny("Permission denied. No user specified for role %s" % (role_code,))
        return

    try:
        role = db.DBSession.query(Role).filter(Role.code==role_code).one()
    except NoResultFound:
        raise PermissionError("Nonexistent role type: %s"%(role_code))

    has_role = db.DBSession.query(RoleUser.role_id)\
        .filter(RoleUser.role_id == role.id)\
        .filter(RoleUser.user_id == user_id)\
        .first() is not None

    if not has_role:
        _deny("Permission denied. User %s does not have role %s" % (user_id, role_code))



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


def get_rule_authors(user_id):
    """ Ids of the users who hold AUTHOR_PERM through any role. `user_id` is the caller, who must be known. """
    if user_id is None:
        raise PermissionError("Permission denied. No user specified")

    rows = db.DBSession.query(RoleUser.user_id)\
        .join(RolePerm, RolePerm.role_id == RoleUser.role_id)\
        .join(Perm, Perm.id == RolePerm.perm_id)\
        .filter(Perm.code == AUTHOR_PERM)\
        .distinct().all()
    return sorted(row[0] for row in rows)
