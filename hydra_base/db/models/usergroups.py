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

from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship, backref

from hydra_base.exceptions import HydraError

from .common import AuditMixin, Base, Inspect, PermissionControlled, OwnerMixin

from .. import DeclarativeBase as Base, get_session


class UserGroupType(AuditMixin, Base, Inspect):
    """
        The definition of a type of user group.
        Examples could include: 'organisation', 'department', 'team'
    """
    __tablename__ = 'tUserGroupType'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200), unique=True, nullable=False)

    def __repr__(self):
        return "User Group {0} (id={1})".format(self.name, self.id)

class UserGroupOwner(OwnerMixin, Base, Inspect):
    """
    The owner of a user group -- defines permissions for whether a user can
    read or write that usergroup (The share permission not used)
    """

    __tablename__ = 'tUserGroupOwner'

    @declared_attr
    def usergroup_id(cls):
        return Column(Integer(),
                      ForeignKey('tUserGroup.id'),
                      primary_key=True,
                      nullable=False)

    usergroup = relationship('UserGroup', backref=backref('owners',
                                                          uselist=True,
                                                          cascade="all, delete-orphan"))

    _parents = ['tUserGroup', 'tUser']

class UserGroup(AuditMixin, Base, Inspect, PermissionControlled):
    """
    """
    __tablename__ = 'tUserGroup'
    __ownerclass__ = UserGroupOwner
    __ownerfk__ = 'usergroup_id'

    __table_args__ = (
        UniqueConstraint('name', 'parent_id', 'created_by', name="unique net name"),
    )

    id = Column(Integer(), primary_key=True, index=True, nullable=False)
    name = Column(String(200), nullable=False)
    type_id = Column(Integer(), ForeignKey('tUserGroupType.id'), nullable=False)
    parent_id = Column(Integer(), ForeignKey('tUserGroup.id'), nullable=True)

    grouptype = relationship('UserGroupType', lazy='joined',
                             backref=backref("groups",
                                             uselist=True,
                                             order_by=id,
                                             cascade="all, delete-orphan"))

    parent = relationship('UserGroup', lazy='joined', remote_side=[id],
                          backref=backref("groups",
                                          uselist=True,
                                          order_by=id,
                                          cascade="all, delete-orphan"))

    def __repr__(self):
        return "{0}".format(self.name)

    def add_member(self, user_id):
        """
            Add a member to the group
        """

        for m in self.members:
            if m.user_id == user_id:
                raise HydraError(f"User {user_id} already exists in group {self.id}")

        new_member = UserGroupMember()

        new_member.user_id = user_id

        self.members.append(new_member)

    def remove_member(self, user_id):
        """
            Remove a member from the group
        """

        existing_member = get_session().query(UserGroupMember).filter(
            UserGroupMember.usergroup_id == self.id,
            UserGroupMember.user_id == user_id).first()

        if existing_member is None:
            raise HydraError(f"User {user_id} is not in in usergroup {self.id}")

        get_session().delete(existing_member)

class UserGroupMember(AuditMixin, Base, Inspect):
    """
        Lists the members of a group by linking to tuser User table
    """

    __tablename__ = 'tUserGroupMember'
    id = Column(Integer(), primary_key=True, index=True, nullable=False)
    usergroup_id = Column(Integer(), ForeignKey('tUserGroup.id'), nullable=False)
    user_id = Column(Integer(), ForeignKey('tUser.id'), nullable=False)

    group = relationship('UserGroup', lazy='joined',
                         backref=backref("members",
                                         uselist=True,
                                         order_by=usergroup_id,
                                         cascade="all, delete-orphan"))

    user = relationship('User', lazy='joined', foreign_keys=[user_id],
                        backref=backref("usergroups",
                                        uselist=True,
                                        order_by=user_id,
                                        cascade="all, delete-orphan"))
    def __repr__(self):
        return "Group {0} : Member {1}".format(self.usergroup_id, self.user_id)

class GroupRoleUser(Base, Inspect):
    """
    This class defines the role of a user within the context of a user group
    """

    __tablename__ = 'tGroupRoleUser'

    member_id = Column(Integer(),
                       ForeignKey('tUserGroupMember.id'),
                       primary_key=True,
                       nullable=False)
    role_id = Column(Integer(),
                     ForeignKey('tRole.id'),
                     primary_key=True,
                     nullable=False)

    user = relationship('UserGroupMember', lazy='joined')
    role = relationship('Role', lazy='joined')

    _parents = ['tRole', 'tUser', 'tUserGroup']
    _children = []

    def __repr__(self):
        return "{0}".format(self.role.name)
