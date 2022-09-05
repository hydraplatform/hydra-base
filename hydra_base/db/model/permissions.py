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
from .base import *

#***************************************************
#Ownership & Permissions
#***************************************************
__all__ = ['Perm', 'Role', 'RolePerm', 'RoleUser', 'User']

class Perm(Base, Inspect):
    """
    """

    __tablename__='tPerm'

    id = Column(Integer(), primary_key=True, nullable=False)
    code = Column(String(60),  nullable=False)
    name = Column(String(200),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    _parents  = ['tRole', 'tPerm']
    _children = []

    def __repr__(self):
        return "{0} ({1})".format(self.name, self.code)

class Role(Base, Inspect):
    """
    """

    __tablename__='tRole'

    id = Column(Integer(), primary_key=True, nullable=False)
    code = Column(String(60),  nullable=False)
    name = Column(String(200),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    _parents  = []
    _children = ['tRolePerm', 'tRoleUser']

    @property
    def permissions(self):
        return set([rp.perm for rp in self.roleperms])

    def __repr__(self):
        return "{0} ({1})".format(self.name, self.code)


class RolePerm(Base, Inspect):
    """
    """

    __tablename__ = 'tRolePerm'

    perm_id = Column(Integer(), ForeignKey('tPerm.id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    perm = relationship('Perm', backref=backref('roleperms', uselist=True, lazy='joined'), lazy='joined')
    role = relationship('Role', backref=backref('roleperms', uselist=True, lazy='joined'), lazy='joined')

    _parents = ['tRole', 'tPerm']
    _children = []

    def __repr__(self):
        return "{0}".format(self.perm)

class RoleUser(Base, Inspect):
    """
    """

    __tablename__='tRoleUser'

    user_id = Column(Integer(), ForeignKey('tUser.id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    role = relationship('Role', backref=backref('roleusers', uselist=True))
    user = relationship('User', backref=backref('roleusers', uselist=True))

    _parents  = ['tRole', 'tUser']
    _children = []

    def __repr__(self):
        return "{0}".format(self.role.name)


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
    failed_logins = Column(SMALLINT, nullable=True, default=0)

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