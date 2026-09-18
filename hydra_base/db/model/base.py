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
import json
import logging
import bcrypt
from collections import defaultdict
import logging

from sqlalchemy import Column,\
ForeignKey,\
text,\
Integer,\
String,\
LargeBinary,\
TIMESTAMP,\
BIGINT,\
SMALLINT,\
Float,\
Text, \
JSON, \
DateTime,\
Unicode

from sqlalchemy.orm import relationship, backref, noload, joinedload
from sqlalchemy import inspect, func
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import case
from sqlalchemy import UniqueConstraint, and_, or_
from sqlalchemy.dialects import mysql

from hydra_base.lib.objects import JSONObject, Dataset as JSONDataset
from hydra_base.lib.cache import cache
from hydra_base.exceptions import HydraError, PermissionError, ResourceNotFoundError
from hydra_base.db import DeclarativeBase as Base, get_session
from hydra_base.util import generate_data_hash, get_val, get_json_as_string
from hydra_base import config

log = logging.getLogger(__name__)

# Python 2 and 3 compatible string checking
# TODO remove this when Python2 support is dropped.
try:
    basestring
except NameError:
    basestring = str

def get_user_id_from_engine(ctx):
    """
        The session will have a user ID bound to it when checking for the permission.
    """
    if hasattr(get_session(), 'user_id'):
        return get_session().user_id
    else:
        return None

def _is_admin(user_id):
    """
        Is the specified user an admin
    """
    from .permissions import User

    user = get_session().query(User).filter(User.id==user_id).one()

    if user.is_admin():
        return True
    else:
        return False

class Inspect(object):
    _parents = []
    _children = []

    def get_columns_and_relationships(self):
        return inspect(self).attrs.keys()

class AuditMixin(object):

    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    @declared_attr
    def created_by(cls):
        return Column(Integer, ForeignKey('tUser.id'), default=get_user_id_from_engine)

    @declared_attr
    def updated_by(cls):
        return Column(Integer, ForeignKey('tUser.id'), onupdate=get_user_id_from_engine)

    updated_at = Column(DateTime, nullable=False, server_default=text(u'CURRENT_TIMESTAMP'), onupdate=func.current_timestamp())

class PermissionControlled(object):
    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = self.__ownerclass__()
            setattr(owner, self.__ownerfk__, self.id)
            owner.user_id = int(user_id)
            self.owners.append(owner)

        if read is not None:
            owner.view = read
        if write is not None:
            owner.edit = write
        if share is not None:
            owner.share = share

        return owner

    def unset_owner(self, user_id):
        owner = None
        if str(user_id) == str(self.created_by):
            log.warning("Cannot unset %s as owner, as they created the dataset", user_id)
            return
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                get_session().delete(owner)
                break

    def _is_open(self):
        """
            Check to see whether this entity is visible globally, without any
            need for permission checking
        """
        pass

    def check_read_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can read this dataset
        """
        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True


        #Check if this entity is publicly open, therefore no need to check permissions.
        if self._is_open() == True:
            return True

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have read"
                             " access on %s '%s' (id=%s)" %
                             (user_id, self.__class__.__name__, self.name, self.id))
            else:
                return False
        #Check that the user is in a group which can read this network
        self.check_group_read_permission(user_id)

        return True

    def check_group_read_permission(self, user_id):
        """
            1: Find which user groups a user is if
            2: Check if any of these groups has permission to read the object
        """

    def check_write_permission(self, user_id, do_raise=True, is_admin=None):
        """
            Check whether this user can write this dataset
        """
        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have edit"
                             " access on %s '%s' (id=%s)" %
                             (user_id, self.__class__.__name__, self.name, self.id))
            else:
                return False

        return True

    def check_share_permission(self, user_id, is_admin=None):
        """
            Check whether this user can write this dataset
        """

        if str(user_id) == str(self.created_by):
            return True

        if is_admin is None:
            is_admin = _is_admin(user_id)

        if is_admin is True:
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    return True
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on %s '%s' (id=%s)" %
                             (user_id, self.__class__.__name__, self.name, self.id))

