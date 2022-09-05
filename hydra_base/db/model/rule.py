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

from .ownership import RuleOwner

__all__ = ['Rule', 'RuleTypeDefinition', 'RuleTypeLink']

class Rule(AuditMixin, Base, Inspect, PermissionControlled):
    """
        A rule is an arbitrary piece of text applied to resources
        within a scenario. A scenario itself cannot have a rule applied
        to it.
    """

    __tablename__ = 'tRule'
    __table_args__ = (
        UniqueConstraint('scenario_id', 'name', name="unique rule name"),
    )

    __ownerclass__ = RuleOwner
    __ownerfk__ = 'rule_id'

    id = Column(Integer(), primary_key=True, nullable=False)

    name = Column(String(200), nullable=False)
    description = Column(String(1000), nullable=True)

    format = Column(String(80), nullable=False, server_default='text')

    ref_key = Column(String(60), nullable=False, index=True)

    value = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'), nullable=True)

    status = Column(String(1), nullable=False, server_default=text(u"'A'"))
    scenario_id = Column(Integer(), ForeignKey('tScenario.id'), nullable=True)

    network_id = Column(Integer(), ForeignKey('tNetwork.id'), index=True, nullable=True)
    node_id = Column(Integer(), ForeignKey('tNode.id'), index=True, nullable=True)
    link_id = Column(Integer(), ForeignKey('tLink.id'), index=True, nullable=True)
    group_id = Column(Integer(), ForeignKey('tResourceGroup.id'), index=True, nullable=True)

    scenario = relationship('Scenario',
                            backref=backref('rules',
                                            uselist=True,
                                            cascade="all, delete-orphan"),
                            lazy='joined')
    network = relationship('Network',
                           backref=backref("rules",
                                           order_by=network_id,
                                           cascade="all, delete-orphan"),
                           lazy='joined')
    node = relationship('Node',
                        backref=backref("rules",
                                        order_by=node_id,
                                        uselist=True,
                                        cascade="all, delete-orphan"),

                        lazy='joined')
    link = relationship('Link',
                        backref=backref("rules",
                                        order_by=link_id,
                                        uselist=True,
                                        cascade="all, delete-orphan"),
                        lazy='joined')
    group = relationship('ResourceGroup',
                         backref=backref("rules",
                                         order_by=group_id,#
                                         uselist=True,
                                         cascade="all, delete-orphan"),
                         lazy='joined')

    _parents = ['tScenario', 'tNode', 'tLink', 'tProject', 'tNetwork', 'tResourceGroup']
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
        elif self.ref_key.upper() == 'NODE':
            rule_network = self.node.network
        elif self.ref_key.upper() == 'LINK':
            rule_network = self.link.network
        elif self.ref_key.upper() == 'GROUP':
            rule_network = self.group.network

        return rule_network

    def get_owners(self):
        """
            Get all the owners of a rule, both those which are applied directly
            to this rule, but also who have been granted access via a project / network
        """

        owners = [JSONObject(o) for o in self.owners]
        owner_ids = [o.user_id for o in owners]

        network = self.get_network()
        network_owners = list(filter(lambda x:x.user_id not in owner_ids, network.get_owners()))

        for no in network_owners:
            no.source = f'Inherited from: {no.network_name} (ID:{no.network_id})'

        return owners + network_owners;



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


