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
from sqlalchemy import Column,\
ForeignKey,\
text,\
Integer,\
String,\
LargeBinary,\
TIMESTAMP,\
BIGINT,\
Float,\
Text, \
DateTime,\
Unicode


from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.ext.declarative import declared_attr

import datetime

from sqlalchemy import inspect, func

from hydra_base.exceptions import HydraError, PermissionError, ResourceNotFoundError

from sqlalchemy.orm import relationship, backref

from hydra_base.util.hydra_dateutil import ordinal_to_timestamp, get_datetime

from .. import DeclarativeBase as Base, get_session

from hydra_base.util import generate_data_hash, get_val

from sqlalchemy.sql.expression import case
from sqlalchemy import UniqueConstraint, and_
from sqlalchemy.dialects import mysql

import pandas as pd

from sqlalchemy.orm import validates

import json
from hydra_base import config
import logging
import bcrypt
log = logging.getLogger(__name__)


# Python 2 and 3 compatible string checking
# TODO remove this when Python2 support is dropped.
try:
    basestring
except NameError:
    basestring = str


def get_timestamp(ordinal):
    """
        Turn an ordinal timestamp into a datetime string.
    """
    if ordinal is None:
        return None
    timestamp = str(ordinal_to_timestamp(ordinal))
    return timestamp

def get_user_id_from_engine(ctx):
    """
        The session will have a user ID bound to it when checking for the permission.
    """
    if hasattr(get_session(), 'user_id'):
        return get_session().user_id
    else:
        return None

#***************************************************
#Data
#***************************************************

def _is_admin(user_id):
    """
        Is the specified user an admin
    """

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

    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    @declared_attr
    def created_by(cls):
        return Column(Integer, ForeignKey('tUser.id'), default=get_user_id_from_engine)

    @declared_attr
    def updated_by(cls):
        return Column(Integer, ForeignKey('tUser.id'), onupdate=get_user_id_from_engine)

    updated_at = Column(DateTime,  nullable=False, default=datetime.datetime.utcnow(), onupdate=datetime.datetime.utcnow())

class PermissionControlled(object):
    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = None
        for o in self.owners:
            if user_id == o.user_id:
                owner = o
                break
        else:
            owner = self.__ownerclass__()
            setattr(owner, self.__ownerfk__,  self.id)
            owner.user_id = int(user_id)
            self.owners.append(owner)

        owner.view  = read
        owner.edit  = write
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

    def check_read_permission(self, user_id, do_raise=True):
        """
            Check whether this user can read this dataset
        """
        if _is_admin(user_id):
            return True

        if str(user_id) == str(self.created_by):
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
                             " access on dataset %s" %
                             (user_id, self.id))
            else:
                return False

        return True

    def check_write_permission(self, user_id, do_raise=True):
        """
            Check whether this user can write this dataset
        """
        if _is_admin(user_id):
            return True

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            if do_raise is True:
                raise PermissionError("Permission denied. User %s does not have edit"
                             " access on dataset %s" %
                             (user_id, self.id))
            else:
                return False

        return True

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this dataset
        """

        if _is_admin(user_id):
            return

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on dataset %s" %
                             (user_id, self.id))

class User(Base, Inspect):
    """
    """

    __tablename__='tUser'

    id = Column(Integer(), primary_key=True, nullable=False)
    username = Column(String(60),  nullable=False, unique=True)
    password = Column(LargeBinary(),  nullable=False)
    display_name = Column(String(200),  nullable=False, server_default=text(u"''"))
    last_login = Column(TIMESTAMP())
    last_edit = Column(TIMESTAMP())
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    roleusers = relationship('RoleUser', lazy='joined')

    _parents  = []
    _children = ['tRoleUser']

    def validate_password(self, password):
        if bcrypt.hashpw(password.encode('utf-8'), self.password.encode('utf-8')) == self.password.encode('utf-8'):
            return True
        return False

    @property
    def permissions(self):
        """Return a set with all permissions granted to the user."""
        perms = set()
        for r in self.roles:
            perms = perms | set(r.permissions)
        return perms

    @property
    def roles(self):
        """Return a set with all roles granted to the user."""
        roles = []
        for ur in self.roleusers:
            roles.append(ur.role)
        return set(roles)

    def is_admin(self):
        """
            Check that the user has a role with the code 'admin'
        """
        for ur in self.roleusers:
            if ur.role.code == 'admin':
                return True

        return False

    def __repr__(self):
        return "{0}".format(self.username)
