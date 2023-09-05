"""
Types and definitions used by UserGroups and Organisations
"""
import enum

from hydra_base.db.model.base import *
from hydra_base import db


group_name_max_length = 200
typename_max_length = 60

__all__ = ("UserGroup", "Organisation")


class UserGroup(Base, Inspect):
    """
      The mapped type of a UserGroup
    """

    __tablename__ = "tUserGroup"

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(group_name_max_length), nullable=False)
    organisation_id = Column(Integer(), ForeignKey('tOrganisation.id', ondelete="CASCADE"), nullable=True)

    @property
    def admins(self):
        return {a.user_id for a in self._admins}

    @property
    def members(self):
        return {m.user_id for m in self._members}

    @property
    def organisation(self):
        if not self.organisation_id:
            return None

        org = db.DBSession.query(Organisation).filter(Organisation.id == self.organisation_id).one()
        return org


class GroupMembers(Base, Inspect):
    """
      Relational type mapping Users to UserGroups
    """

    __tablename__ = "tGroupMembers"

    __mapper_args__ = {
        "confirm_deleted_rows": False
    }

    group_id = Column(Integer(), ForeignKey("tUserGroup.id"), primary_key=True, nullable=False)
    user_id = Column(Integer(), ForeignKey("tUser.id"), primary_key=True, nullable=False)

    group = relationship("UserGroup", backref=backref("_members", uselist=True, cascade="all, delete-orphan"))
    user = relationship("User", backref=backref("groups", uselist=True, cascade="all, delete-orphan"))


class GroupAdmins(Base, Inspect):
    """
      Relational type mapping UserGroups to their Administrators
    """

    __tablename__ = "tGroupAdmins"

    __mapper_args__ = {
        "confirm_deleted_rows": False
    }

    group_id = Column(Integer(), ForeignKey("tUserGroup.id"), primary_key=True, nullable=False)
    user_id = Column(Integer(), ForeignKey("tUser.id"), primary_key=True, nullable=False)

    group = relationship("UserGroup", backref=backref("_admins", uselist=True, cascade="all, delete-orphan"))
    user = relationship("User", backref=backref("administers", uselist=True, cascade="all, delete-orphan"))


class Organisation(Base, Inspect):
    """
      The mapped type of an Organisation
    """

    __tablename__ = "tOrganisation"

    # Default UserGroup to which all member Users are automatically added
    everyone = "Everyone"

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(group_name_max_length), nullable=False)

    @property
    def members(self):
        return {m.user_id for m in self._members}


class OrganisationMembers(Base, Inspect):
    """
      Relational type mapping Users to Organisations of which they are members
    """

    __tablename__ = "tOrganisationMembers"

    organisation_id = Column(Integer(), ForeignKey("tOrganisation.id"), primary_key=True, nullable=False)
    user_id = Column(Integer(), ForeignKey("tUser.id"), primary_key=True, nullable=False)

    members = relationship("Organisation", backref=backref("_members", uselist=True, cascade="all, delete-orphan"))
    organisations = relationship("User", backref=backref("organisations", uselist=True, cascade="all, delete-orphan"))


class Perm(enum.IntEnum):
    """
      An Integer Enum which represents components of the access
      Permission mask applied to a resource.
    """
    Read  = 0x1
    Write = 0x2
    Share = 0x4


class ResourceAccess(Base, Inspect):
    """
      Relational type which maps Resource-Type-and-Resource-id combinations
      to UserGroup-and-access-bitmask pairs.
    """

    __tablename__ = "tResourceAccess"

    """
      - str resource type    "Network", "Project"
      - int holder_id        "{UserGroup.id}"
      - int resource_id      "{Network.id}"
      - Perm access bitmask  "PERM.READ | PERM.WRITE"
    """

    usergroup_id = Column(Integer(), primary_key=True, nullable=False)
    resource = Column(String(typename_max_length), primary_key=True, nullable=False)
    resource_id = Column(Integer(), primary_key=True, nullable=False)
    access = Column(Integer(), nullable=False)
