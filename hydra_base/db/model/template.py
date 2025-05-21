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

__all__ = ['Template', 'TemplateType', 'TypeAttr', 'ResourceType']

from .attributes import Attr
from .units import Unit


class Template(Base, Inspect):
    """
    Template
    """

    __tablename__ = 'tTemplate'

    id = Column(Integer(), primary_key=True, nullable=False)
    parent_id = Column(Integer(), ForeignKey('tTemplate.id'))
    name = Column(String(200), unique=True)
    status = Column(String(1),  nullable=False, server_default=text(u"'A'"))
    description = Column(String(1000))
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    layout = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'))

    parent = relationship('Template', remote_side=[id], backref=backref("children", order_by=id))

    _parents = []
    _children = ['tTemplateType']

    def set_inherited_columns(self, parent, child, table):
        """
            Set the value on the column of a target child.
            This checks if the value is null on the child, and sets it from
            the parent if so
        """
        inherited_columns = []
        for column in table.__table__.columns:

            colname = column.name

            if hasattr(table, '_protected_columns')\
               and colname in table._protected_columns:
                # as a child, you can't change stuff like IDs, cr dates etc.
                continue

            newval = getattr(parent, colname)

            if colname == 'layout':
                newval = get_json_as_string(newval)

            refval = getattr(child, colname)

            if refval is None:
                inherited_columns.append(colname)
                setattr(child, colname, newval)

        if hasattr(child, 'inherited_columns') and child.inherited_columns is not None:
            for c in inherited_columns:
                if c not in child.inherited_columns:
                    child.inherited_columns.append(c)
        else:
            child.inherited_columns = inherited_columns

        return child

    def get_typeattr(self, typeattr_id, child_typeattr=None, get_parent_types=True):
        """
            Return an individual type attribute.
            If this type attribute inherits from another, look up the tree to compile
            the inherited data, with the leaf node taking priority
        """

        #get all the type attrs for this type, and add any which are missing
        this_typeattr_i = get_session().query(TypeAttr)\
            .filter(TypeAttr.id == typeattr_id)\
            .options(joinedload(TypeAttr.attr))\
            .options(joinedload(TypeAttr.default_dataset)).one()

        this_typeattr = JSONObject(this_typeattr_i)

        if child_typeattr is None:
            child_typeattr = this_typeattr
        else:
            child_typeattr = self.set_inherited_columns(this_typeattr, child_typeattr, this_typeattr_i)

        if this_typeattr.parent_id is not None and get_parent_types is True:
            return self.parent.get_typeattr(this_typeattr.parent_id,
                                            child_typeattr=child_typeattr,
                                            get_parent_types=get_parent_types)

        return child_typeattr

    def get_type(self, type_id, child_type=None, get_parent_types=True):
        """
            Return all the templatetypes relevant to this template.
            If this template inherits from another, look up the tree to compile
            an exhaustive list of types, removing any duplicates, prioritising
            the ones closest to this template (my immediate parent's values are used instead
            of its parents)

        """

        #Add resource attributes which are not defined already
        this_type_i = get_session().query(TemplateType).filter(
            TemplateType.id == type_id).options(noload(TemplateType.typeattrs)).one()

        this_type = JSONObject(this_type_i)

        this_type.typeattrs = []

        if not hasattr(this_type, 'ta_tree') or this_type.ta_tree is None:
            this_type.ta_tree = {}

        #get all the type attrs for this type, and add any which are missing
        typeattrs_i = get_session().query(TypeAttr)\
            .filter(TypeAttr.type_id == type_id)\
            .options(joinedload(TypeAttr.attr))\
            .options(joinedload(TypeAttr.unit))\
            .options(joinedload(TypeAttr.default_dataset)).all()
        typeattrs = [JSONObject(ta) for ta in typeattrs_i]


        #Is this type the parent of a type. If so, we don't want to add a new type
        #we want to update an existing one with any data that it's missing
        if child_type is not None:
            child_type = self.set_inherited_columns(this_type, child_type, this_type_i)
            #check the child's typeattrs. If a typeattr exists on the parent, with the
            #same attr_id, then it should be ignore. THis can happen when adding a typeattr
            #to the child first, then the parent
            child_typeattrs = [ta.attr_id for ta in child_type.typeattrs]

            for i, typeattr in enumerate(typeattrs):
                if typeattr.attr_id in child_typeattrs:
                    log.debug("Found a typeattr for attribute %s on the "
                             "child type %s (%s). Ignoring",
                             typeattr.attr_id, child_type.name, child_type.id)
                    continue
                #Does this typeattr have a child?
                child_typeattr = child_type.ta_tree.get(typeattr.id)
                if child_typeattr is None:
                    #there is no child, so check if it is a child
                    if typeattr.parent_id is not None:
                        #it has a parent, so add it to the type's tree dict
                        #for processing farther up the tree
                        child_type.ta_tree[typeattr.parent_id] = typeattr
                    child_type.typeattrs.append(typeattr)
                else:
                    child_typeattr = self.set_inherited_columns(typeattr, child_typeattr, typeattrs_i[i])

        else:
            if not hasattr(this_type, 'typeattrs'):
                setattr(this_type, 'typeattrs', [])
            for typeattr in typeattrs:
                this_type.typeattrs.append(typeattr)
            child_type = this_type

        if this_type.parent_id is not None and get_parent_types is True:
            return self.parent.get_type(this_type.parent_id,
                                         child_type=child_type,
                                         get_parent_types=get_parent_types)
        child_type.ta_tree = None
        return child_type


    @staticmethod
    def get_type_parent_ids(ttype_id):
        """
            Returns a list of the ids of TemplateTypes which
            are parents of the TemplateType specified in the
            <ttype_id> argument.
        """
        q = get_session().query(TemplateType)
        q = q.filter(TemplateType.id == ttype_id)
        q = q.options(noload(TemplateType.typeattrs))
        try:
            ttype = q.one()
        except sqlalchemy.exc.NoResultFound:
            raise HydraError(f"No TemplateType found with id {ttype_id}")

        parent_ids = []

        while ttype.parent_id:
            parent_ids.insert(0, ttype.parent_id)
            ttype = get_session().query(TemplateType).filter(TemplateType.id == ttype.parent_id).one()

        return parent_ids


    def get_types(self, type_tree={}, child_types=None, get_parent_types=True, child_template_id=None):
        """
            Return all the templatetypes relevant to this template.
            If this template inherits from another, look up the tree to compile
            an exhaustive list of types, removing any duplicates, prioritising
            the ones closest to this template (my immediate parent's values are used instead
            of its parents)

        """
        log.info("Getting Template Types..")

        #This avoids python's mutable keyword arguments causing child_data to keep its values between
        #function calls
        if child_types is None:
            child_types = []
            type_tree = {}

        #Add resource attributes which are not defined already
        types_i = get_session().query(TemplateType).filter(
            TemplateType.template_id == self.id).options(noload(TemplateType.typeattrs)).all()
        types = [JSONObject(t) for t in types_i]

        if child_template_id is None:
            child_template_id = self.id

        for i, this_type in enumerate(types):
            if this_type.parent_id is not None:
                this_type.parent_ids = Template.get_type_parent_ids(this_type.id)

            this_type.child_template_id = child_template_id

            #This keeps track of which type attributes are currently associated
            #to this type. We'll use the data in this dict to set the 'typeattrs'
            #at the end
            if not hasattr(this_type, 'ta_tree') or this_type.ta_tree is None:
                this_type.ta_tree = {}

            #get all the type attrs for this type, and add any which are missing
            typeattrs_i = get_session().query(TypeAttr)\
                .filter(TypeAttr.type_id == this_type.id)\
                .options(joinedload(TypeAttr.attr))\
                .options(joinedload(TypeAttr.default_dataset)).all()

            typeattrs = [JSONObject(ta) for ta in typeattrs_i]

            #Is this type the parent of a type. If so, we don't want to add a new type
            #we want to update an existing one with any data that it's missing
            if this_type.id in type_tree:
                #This is a deleted type, so ignore it in the parent
                if type_tree[this_type.id] is  None:
                    continue

                #Find the child type and update it.
                child_type = type_tree[this_type.id]

                child_type = self.set_inherited_columns(this_type, child_type, types_i[i])

                #check the child's typeattrs. If a typeattr exists on the parent, with the
                #same attr_id, then it should be ignore. THis can happen when adding a typeattr
                #to the child first, then the parent
                child_typeattrs = [ta.attr_id for ta in child_type.typeattrs]

                for typeattr in typeattrs:
                    if typeattr.attr_id in child_typeattrs:
                        log.debug("Found a typeattr for attribute %s on the "
                             "child type %s (%s). Ignoring",
                             typeattr.attr_id, child_type.name, child_type.id)
                        continue

                    #Does this typeattr have a child?
                    child_typeattr = type_tree[this_type.id].ta_tree.get(typeattr.id)
                    if child_typeattr is None:

                        #there is no child, so check if it is a child
                        if typeattr.parent_id is not None:
                            #it has a parent, so add it to the type's tree dict
                            #for processing farther up the tree
                            type_tree[this_type.id].ta_tree[typeattr.parent_id] = typeattr
                        child_type.typeattrs.append(typeattr)
                    else:
                        child_typeattr = self.set_inherited_columns(typeattr, child_typeattr, types_i[i])


                if this_type.parent_id is not None:
                    type_tree[this_type.parent_id] = child_type

            else:
                if not hasattr(this_type, 'typeattrs'):
                    setattr(this_type, 'typeattrs', [])
                for typeattr in typeattrs:
                    #is this a child? if so, register it as one
                    if typeattr.parent_id is not None:
                        this_type.ta_tree[typeattr.parent_id] = typeattr
                    this_type.typeattrs.append(typeattr)

                child_types.append(this_type)
                #set
                if this_type.parent_id is not None:
                    type_tree[this_type.parent_id] = this_type


        if self.parent is not None and get_parent_types is True:
            return self.parent.get_types(type_tree=type_tree,
                                         child_types=child_types,
                                         get_parent_types=get_parent_types,
                                        child_template_id=child_template_id)

        #clean up
        for child_type in child_types:
            child_type.ta_tree = None

        return child_types

    def get_hierarchy(self, user_id):

        hierarchy = [JSONObject(self)]
        if self.parent_id:
            hierarchy = hierarchy + self.parent.get_hierarchy(user_id)
        return hierarchy

class TemplateType(Base, Inspect):
    """
    Template Type
    """

    __tablename__ = 'tTemplateType'
    __table_args__ = (
        UniqueConstraint('template_id', 'name', 'resource_type', name="unique type name"),
    )

    #these are columns which are not allowed to be changed by a child type
    _protected_columns = ['id', 'template_id', 'parent_id', 'cr_date', 'updated_at']
    _hierarchy_columns = ['name', 'resource_type']

    id = Column(Integer(), primary_key=True, nullable=False)
    parent_id = Column(Integer(), ForeignKey('tTemplateType.id'))
    template_id = Column(Integer(), ForeignKey('tTemplate.id'), nullable=False)
    name = Column(String(200), nullable=True)
    description = Column(String(1000))
    resource_type = Column(String(200), nullable=True)
    alias = Column(String(100))
    status = Column(String(1),  nullable=True)
    layout = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'))
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    parent = relationship('TemplateType', remote_side=[id],
                          backref=backref("children", order_by=id))

    template = relationship('Template',
                            backref=backref("templatetypes",
                                            order_by=id,
                                            cascade="all, delete-orphan"))

    _parents = ['tTemplate']
    _children = ['tTypeAttr']

    def get_typeattrs(self, ta_tree={}, child_typeattrs=None, get_parent_types=True):
        """
            This is unfinished
        """
        #This avoids python's mutable keyword arguments causing child_data to keep its values between
        #function calls
        if child_typeattrs is None:
            child_typeattrs = []
            ta_tree = {}

        #get all the type attrs for this type, and add any which are missing
        typeattrs_i = get_session().query(TypeAttr)\
            .filter(TypeAttr.type_id == self.id)\
            .options(joinedload(TypeAttr.default_dataset)).all()
        typeattrs = [JSONObject(ta) for ta in typeattrs_i]


        for i, typeattr in enumerate(typeattrs):

            #Does this typeattr have a child?
            child_typeattr = ta_tree.get(typeattr.id)
            if child_typeattr is None:
                #there is no child, so check if it is a child
                if typeattr.parent_id is not None:
                    #it has a parent, so add it to the tree dict
                    #for processing farther up the tree
                    ta_tree[typeattr.parent_id] = typeattr

            else:
                child_typeattr = self.template.set_inherited_columns(typeattr, child_typeattr, typeattrs_i[i])
                child_typeattrs.append(typeattr)



        if self.parent is not None and get_parent_types is True:
            return self.parent.get_typeattrs(ta_tree=ta_tree,
                                              child_typeattrs=child_typeattrs,
                                              get_parent_types=get_parent_types)
        return child_typeattrs

    def get_children(self):
        """
            Get the child types of a template type
        """

        child_types = get_session().query(TemplateType)\
                .filter(TemplateType.parent_id==self.id).all()
        return child_types

    def check_can_delete_resourcetypes(self, delete_resourcetypes=False):
        """
            Check if the delete operation will allow the deletion
            of resourcetypes. Default is NO
        """
        #Check if there are any resourcetypes associated to this type. If so,
        #don't delete it.
        resourcetype_count = get_session().query(ResourceType.id)\
                .filter(ResourceType.type_id == self.id).count()

        if resourcetype_count > 0 and delete_resourcetypes is False:
            raise HydraError(f"Unable to delete type. Template Type {self.id} has "
                             f"{resourcetype_count} resources associated to it. "
                             "Use the 'delete_resourcetypes' flag to delete these also.")

    def delete_children(self, delete_resourcetypes=False):
        """
            Delete the children associated to this type.
            THIS SHOULD BE DONE WITH EXTREME CAUTION.
            args:
                delete_resourcetypes (bool): If any resourcetypes are found to be
                associated to a child, throw an error to avoid leaving nodes with no types.
                If this flag is is set to true, then delete the resourcetypes

            This function works its way all the way down the tree to the leaf nodes
            and then deletes them from the leaf to the source
        """

        children = self.get_children()

        for child in children:
            child.delete_children(delete_resourcetypes=delete_resourcetypes)

            child.check_can_delete_resourcetypes(delete_resourcetypes=delete_resourcetypes)
            #delete all the resource types associated to this type
            if delete_resourcetypes is True:
                self.delete_resourcetypes()

            #delete the typeattrs
            for ta in child.typeattrs:
                get_session().delete(ta)

            get_session().delete(child)

    def delete_resourcetypes(self):
        """
        Delete the resourcetypes associated to a type
        """
        type_rs = get_session().query(ResourceType).filter(ResourceType.type_id==self.id).all()

        log.warning("Forcing the deletion of %s resource types from type %s",\
                 len(type_rs), self.id)

        for resource_type in type_rs:
            get_session().delete(resource_type)

class TypeAttr(Base, Inspect):
    """
        Type Attribute
    """

    __tablename__ = 'tTypeAttr'

    __table_args__ = (
        UniqueConstraint('type_id', 'attr_id', name='type_attr_1'),
    )

    id = Column(Integer(), primary_key=True, nullable=False)
    parent_id = Column(Integer(), ForeignKey('tTypeAttr.id'))
    attr_id = Column(Integer(), ForeignKey('tAttr.id'), nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.id', ondelete='CASCADE'),
                     nullable=False)
    default_dataset_id = Column(Integer(), ForeignKey('tDataset.id'))
    attr_is_var = Column(String(1), server_default=text(u"'N'"))
    data_type = Column(String(60))
    data_restriction = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'))
    unit_id = Column(Integer(), ForeignKey('tUnit.id'))
    description = Column(String(1000))
    properties = Column(Text().with_variant(mysql.LONGTEXT, 'mysql'))
    status = Column(String(1),  nullable=True)
    cr_date = Column(TIMESTAMP(), nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))


    parent = relationship('TypeAttr', remote_side=[id], backref=backref("children", order_by=id))

    attr = relationship('Attr')
    #Don't use a cascade delete all here. Instead force the code to delete the typeattrs
    #manually, to avoid accidentally deleting them
    templatetype = relationship('TemplateType',
                                backref=backref("typeattrs",
                                                order_by=attr_id))
    unit = relationship('Unit',
                        backref=backref("typeattr_unit",
                                        order_by=unit_id))
    default_dataset = relationship('Dataset')

    _parents = ['tTemplateType', 'tUnit']
    _children = []

    def get_attr(self):
        """
            Get the attribute object
        """
        attr = None
        try:
            self.attr

            if self.attr is not None:
                attr = self.attr
        except:
            log.info("Unable to lazy-load attribute on typeattr %s", self.id)

        if attr is None:
            attr = get_session().query(Attr).filter(Attr.id == self.attr_id).first()

        return attr

    def get_unit(self):
        """
            Get the unit object
        """
        unit = None
        try:
            self.unit

            if self.unit is not None:
                unit = self.unit
        except:
            log.info("Unable to lazy-load unitibute on typeunit %s", self.id)

        if unit is None:
            unit = get_session().query(Unit).filter(unit.id == self.unit_id).first()

        return unit



    @property
    def is_var(self):
        return self.attr_is_var

    def get_properties(self):
        """
            This is here to match the JSONObject TypeAttr class which also
            has a get_properties, and which is required by some functions where
            both types can be validly passed in (like _set_typeattr)
        """
        return self.properties

class ResourceType(Base, Inspect):
    """
    """

    __tablename__='tResourceType'
    __table_args__ = (
        UniqueConstraint('network_id', 'type_id', name='net_type_1'),
        UniqueConstraint('node_id', 'type_id', name='node_type_1'),
        UniqueConstraint('link_id', 'type_id',  name = 'link_type_1'),
        UniqueConstraint('group_id', 'type_id', name = 'group_type_1'),

    )
    id = Column(Integer, primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.id'), primary_key=False, nullable=False)
    #This template id is used when the template and the type are not from the same template
    #i.e. the resource type is being used in a child template
    #If null, then the resources has either been created using a non-child template, or with a resource
    #type in a child template which has been entered to the DB, because the parent type has been altered in the child
    child_template_id = Column(Integer(), ForeignKey('tTemplate.id'), primary_key=False, nullable=True)
    ref_key = Column(String(60),nullable=False)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.id'), nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.id'), nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.id'), nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.id'), nullable=True)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    #Don't used a delete cascade here because deleting the type accidentally can delete
    #this data. INstead the resource types should be deleted manually before the deletion
    #of the type
    templatetype = relationship('TemplateType', backref=backref('resourcetypes', uselist=True))

    network = relationship('Network', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    node = relationship('Node', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    link = relationship('Link', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)
    resourcegroup = relationship('ResourceGroup', backref=backref('types', uselist=True, cascade="all, delete-orphan"), uselist=False)

    _parents  = ['tNode', 'tLink', 'tResourceGroup', 'tNetwork', 'tProject']
    _children = []

    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'PROJECT':
            return self.project
        elif ref_key == 'NETWORK':
            return self.network
        elif ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.group

    def get_resource_id(self):
        ref_key = self.ref_key
        if ref_key == 'PROJECT':
            return self.project_id
        elif ref_key == 'NETWORK':
            return self.network_id
        elif ref_key == 'NODE':
            return self.node_id
        elif ref_key == 'LINK':
            return self.link_id
        elif ref_key == 'GROUP':
            return self.group_id

    def get_templatetype(self):
        """
            If this type is that of a child templatetype, then the full type
            needs to be constructed by the template. So instead of getting the
            template type directly, we get the template, then request the type.
        """


        #This resource was created using a child template
        if self.child_template_id is not None:
            template_i = get_session().query(Template)\
                .filter(Template.id == self.child_template_id).one()
        else:
            type_i = get_session().query(TemplateType).filter(TemplateType.id == self.type_id).one()

            if type_i.parent_id is None:
                return type_i

            template_i = get_session().query(Template)\
                    .filter(Template.id == type_i.template_id).one()

        type_i = template_i.get_type(self.type_id)

        return JSONObject(type_i)
