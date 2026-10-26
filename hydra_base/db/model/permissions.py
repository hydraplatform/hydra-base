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
import uuid

from sqlalchemy import Boolean
from sqlalchemy.orm import validates

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
    _password = Column('password', LargeBinary(),  nullable=False)
    display_name = Column(String(200),  nullable=False, server_default=text(u"''"))
    last_login = Column(TIMESTAMP())
    last_edit = Column(TIMESTAMP())
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    failed_logins = Column(SMALLINT, nullable=True, default=0)

    # Flask-Security-required columns. This is the single mapping of
    # tUser -- HWI's Flask-Security User model imports this class rather
    # than declaring its own, so the schema is defined in one place.
    email = Column(String(255), nullable=False, unique=True)
    active = Column(Boolean(), nullable=False, server_default=text('1'))
    confirmed_at = Column(TIMESTAMP())
    first_name = Column(String(255))
    last_name = Column(String(255))
    demographic = Column(String(255))
    country_code = Column(String(255))
    organization = Column(String(255))
    current_login_at = Column(TIMESTAMP())
    last_login_ip = Column(String(255))
    current_login_ip = Column(String(255))
    login_count = Column(Integer)
    fs_uniquifier = Column(String(255), nullable=False, unique=True, default=lambda: uuid.uuid4().hex)

    _parents  = []
    _children = ['tRoleUser']

    @hybrid_property
    def password(self):
        # Stored as LargeBinary (raw bcrypt hash bytes); exposed as str
        # since that's what Flask-Security's hasher (and callers
        # generally) work with. Bytes in, bytes out for anything that
        # still passes bytes directly (e.g. hydra-base's own add_user).
        if self._password is None:
            return None
        return self._password.decode('utf-8') if isinstance(self._password, bytes) else self._password

    @password.setter
    def password(self, value):
        self._password = value.encode('utf-8') if isinstance(value, str) else value

    @validates('email')
    def _default_username_from_email(self, key, value):
        # username is hydra-base's own NOT NULL identity column; every
        # creation path (Flask-Security register/admin, hydra-base
        # add_user) sets email, so default username from it unless a
        # caller already set a different username.
        if not self.username:
            self.username = value
        return value

    def validate_password(self, password):
        if bcrypt.hashpw(password.encode('utf-8'), self.password.encode('utf-8')) == self.password.encode('utf-8'):
            return True
        return False

    # -- flask-login's expected duck-typed interface (deliberately not
    # importing flask_login here -- hydra-base has non-Flask consumers,
    # e.g. the Spyne-based hydra-server, and must stay framework-agnostic).
    @property
    def is_authenticated(self):
        return True

    @property
    def is_active(self):
        return bool(self.active)

    @property
    def is_anonymous(self):
        return False

    def get_id(self):
        return str(self.id)

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

    def has_role(self, role):
        """
        Flask-Security/flask-login call this with a role code string
        (e.g. 'admin', 'developer') -- hydra-base identifies roles by
        `code`, not `name`, so this checks against that, not the
        default RoleMixin behaviour of comparing role.name.
        """
        role_code = role.code if hasattr(role, 'code') else role
        return any(r.code == role_code for r in self.roles)

    def __repr__(self):
        return "{0}".format(self.username)