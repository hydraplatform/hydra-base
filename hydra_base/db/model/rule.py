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
from .permissions import User

from hydra_base.exceptions import PermissionError

__all__ = ['Rule', 'RuleTypeDefinition', 'RuleTypeLink']

class Rule(AuditMixin, Base, Inspect):
    """
        A rule is an arbitrary piece of text applied to either
        a Network, Project or Template.
    """

    __tablename__ = 'tRule'
    __table_args__ = (
        UniqueConstraint('network_id', 'name', name="unique network rule name"),
        UniqueConstraint('project_id', 'name', name="unique project rule name"),
        UniqueConstraint('template_id', 'name', name="unique template rule name")
    )

    __ownerfk__ = 'rule_id'

    id = Column(Integer(), primary_key=True, nullable=False)

    name = Column(String(200), nullable=False)
    description = Column(String(1000), nullable=True)

    format = Column(String(80), nullable=False, server_default='text')

    ref_key = Column(String(60), nullable=False, index=True)

    value = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), nullable=True)

    status = Column(String(1), nullable=False, server_default=text(u"'A'"))

    network_id = Column(Integer(), ForeignKey('tNetwork.id'), index=True, nullable=True)
    project_id = Column(Integer(), ForeignKey('tProject.id'), index=True, nullable=True)
    template_id = Column(Integer(), ForeignKey('tTemplate.id'), index=True, nullable=True)

    network = relationship('Network', backref=backref("rules",
                           order_by=network_id,
                           cascade="all, delete-orphan"),
                           lazy='joined')

    project = relationship('Project', backref=backref("rules",
                           order_by=project_id,
                           cascade="all, delete-orphan"),
                           lazy='joined')

    template = relationship('Template', backref=backref("templates",
                            order_by=project_id,
                            cascade="all, delete-orphan"),
                            lazy='joined')

    _parents = ['tProject', 'tNetwork', 'tTemplate']
    _children = []


    def set_types(self, types):
        """
            Accepts a list of type JSONObjects or spyne objects and sets
            the type of the rule to be exactly this. This means deleting rules
            which are not in the list
        """

        #We take this to mean don't touch types.
        if types is None:
            return

        existingtypes = set([t.code for t in self.types])

        #Map a type code to a type object
        existing_type_map = dict((t.code, t) for t in self.types)

        newtypes = set([t.code for t in types])

        types_to_add = newtypes - existingtypes
        types_to_delete = existingtypes - newtypes

        for ruletypecode in types_to_add:

            self.check_type_definition_exists(ruletypecode)

            ruletypelink = RuleTypeLink()
            ruletypelink.code = ruletypecode
            self.types.append(ruletypelink)

        for type_to_delete in types_to_delete:
            get_session().delete(existing_type_map[type_to_delete])

    def check_type_definition_exists(self, code):
        """
        A convenience function to check if a rule type definition exists before trying to add a link to it
        """

        try:
            get_session().query(RuleTypeDefinition).filter(RuleTypeDefinition.code == code).one()
        except NoResultFound:
            raise ResourceNotFoundError("Rule type definition with code {} does not exist".format(code))

    def get_network(self):
        """
        Rules are associated with a network directly or nodes/links/groups in a network,
        so rules are always associated to one network.
        This function returns that network
        """
        rule_network = None
        if self.ref_key.upper() == 'NETWORK':
            rule_network = self.network

        return rule_network


    @property
    def owners(self):
        rule_owners = []
        if self.network:
            rule_owners += self.network.get_owners()

        if self.project:
            rule_owners += self.project.get_owners()

        user_ids = set(o.user_id for o in rule_owners)
        return [{"user_id": user_id} for user_id in user_ids]


    def check_read_permission(self, user_id, do_raise=True):
        user = get_session().query(User).filter(User.id==user_id).one()
        if user.is_admin():
            return True

        if user_id in set(o["user_id"] for o in self.owners):
            return True
        else:
            if do_raise:
                raise PermissionError(f"user {user_id} does not have access to rule {self.id}")
            return False


    def check_write_permission(self, user_id, do_raise=True):
        if self.network:
            return self.network.check_write_permission(user_id, do_raise=do_raise)

        if self.project:
            return self.project.check_write_permission(user_id, do_raise=do_raise)

        if self.template:
            return self.template.check_write_permission(user_id, do_raise=do_raise)

        return False


    def asdict(self):
        """
         Dataclass-style dict representation for conversion to JSONObject
        """
        return {
          "id": self.id,
          "name": self.name,
          "value": self.value,
          "description": self.description,
          "status": self.status,
          "owners": self.owners
        }


    def get_owners(self):
        """
            Get all the owners of a rule, both those which are applied directly
            to this rule, but also who have been granted access via a project / network
        """
        return self.owners



class RuleTypeDefinition(AuditMixin, Base, Inspect):
    """
        Describes the types of rules available in the system

        A rule type is a simple way of categorising rules. A rule may have no
        type or it may have 1. A rule type consists of a unique code and a name.

        In addition to separating rules, this enables rules to be searched more easily.
    """

    __tablename__='tRuleTypeDefinition'

    __table_args__ = (
        UniqueConstraint('code', name="Unique Rule Code"),
    )

    code = Column(String(200), nullable=False, primary_key=True)
    name = Column(String(200), nullable=False)


class RuleTypeLink(AuditMixin, Base, Inspect):
    """
        Links rules to type definitions.

        A rule type is a simple way of categorising rules. A rule may have no
        type or it may have 1. A rule type consists of a unique code and a name.

        In addition to separating rules, this enables rules to be searched more easily.
    """

    __tablename__='tRuleTypeLink'

    __table_args__ = (
        UniqueConstraint('code', 'rule_id', name="Unique Rule / Type"),
    )

    code    = Column(String(200), ForeignKey('tRuleTypeDefinition.code'), primary_key=True, nullable=False)
    rule_id = Column(Integer(), ForeignKey('tRule.id'), primary_key=True, nullable=False)

    #Set the backrefs so that when a type definition or a rule are deleted, so are the links.
    typedefinition = relationship('RuleTypeDefinition', uselist=False, lazy='joined',
                                  backref=backref('ruletypes', cascade="all, delete-orphan"))
    rule = relationship('Rule', backref=backref('types', order_by=code, uselist=True, cascade="all, delete-orphan"))


