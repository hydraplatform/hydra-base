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
log = logging.getLogger(__name__)

from ..db.model import Attr,\
        Node,\
        Link,\
        ResourceGroup,\
        Network,\
        Project,\
        Scenario,\
        TemplateType,\
        ResourceAttr,\
        TypeAttr,\
        ResourceAttrMap,\
        ResourceScenario,\
        Dataset,\
        AttrGroup,\
        AttrGroupItem
from .. import db
from sqlalchemy.orm.exc import NoResultFound
from ..exceptions import HydraError, ResourceNotFoundError
from sqlalchemy import or_, and_
from sqlalchemy.orm import aliased
from . import units

def _get_network(network_id):
    try:

        network_i = db.DBSession.query(Network).filter(Network.id==network_id).one()
    except NoResultFound:
        raise HydraError("Network %s not found" % (network_id))
    return network_i

def _get_project(project_id):
    try:

        project_i = db.DBSession.query(Project).filter(Project.id==project_id).one()
    except NoResultFound:
        raise HydraError("Project %s not found" % (project_id))
    return project_i


def _get_resource(ref_key, ref_id):
    try:
        if ref_key == 'NODE':
            return db.DBSession.query(Node).filter(Node.id == ref_id).one()
        elif ref_key == 'LINK':
            return db.DBSession.query(Link).filter(Link.id == ref_id).one()
        if ref_key == 'GROUP':
            return db.DBSession.query(ResourceGroup).filter(ResourceGroup.id == ref_id).one()
        elif ref_key == 'NETWORK':
            return db.DBSession.query(Network).filter(Network.id == ref_id).one()
        elif ref_key == 'SCENARIO':
            return db.DBSession.query(Scenario).filter(Scenario.id == ref_id).one()
        elif ref_key == 'PROJECT':
            return db.DBSession.query(Project).filter(Project.id == ref_id).one()
        else:
            return None
    except NoResultFound:
        raise ResourceNotFoundError("Resource %s with ID %s not found"%(ref_key, ref_id))

def get_attribute_by_id(attr_id, **kwargs):
    """
        Get a specific attribute by its ID.
    """

    try:
        attr_i = db.DBSession.query(Attr).filter(Attr.id==attr_id).one()
        return attr_i
    except NoResultFound:
        return None

def get_template_attributes(template_id, **kwargs):
    """
        Get a specific attribute by its ID.
    """

    try:
        attrs_i = db.DBSession.query(Attr).filter(TemplateType.template_id==template_id).filter(TypeAttr.type_id==TemplateType.id).filter(Attr.id==TypeAttr.id).all()
        log.info(attrs_i)
        return attrs_i
    except NoResultFound:
        return None



def get_attribute_by_name_and_dimension(name, dimension='dimensionless',**kwargs):
    """
        Get a specific attribute by its name.
    """

    try:
        attr_i = db.DBSession.query(Attr).filter(and_(Attr.name==name, or_(Attr.dimension==dimension, Attr.dimension == ''))).one()
        log.info("Attribute retrieved")
        return attr_i
    except NoResultFound:
        return None

def add_attribute(attr,**kwargs):
    """
    Add a generic attribute, which can then be used in creating
    a resource attribute, and put into a type.

    .. code-block:: python

        (Attr){
            id = 1020
            name = "Test Attr"
            dimen = "very big"
        }

    """
    log.debug("Adding attribute: %s", attr.name)

    if attr.dimension is None or attr.dimension.lower() == 'dimensionless':
        log.info("Setting 'dimesionless' on attribute %s", attr.name)
        attr.dimension = 'dimensionless'

    try:
        attr_i = db.DBSession.query(Attr).filter(Attr.name == attr.name,
                                              Attr.dimension == attr.dimension).one()
        log.info("Attr already exists")
    except NoResultFound:
        attr_i = Attr(name = attr.name, dimension = attr.dimension)
        attr_i.description = attr.description
        db.DBSession.add(attr_i)
        db.DBSession.flush()
        log.info("New attr added")
    return attr_i

def update_attribute(attr,**kwargs):
    """
    Add a generic attribute, which can then be used in creating
    a resource attribute, and put into a type.

    .. code-block:: python

        (Attr){
            id = 1020
            name = "Test Attr"
            dimen = "very big"
        }

    """

    if attr.dimension is None or attr.dimension.lower() == 'dimensionless':
        log.info("Setting 'dimesionless' on attribute %s", attr.name)
        attr.dimension = 'dimensionless'

    log.debug("Adding attribute: %s", attr.name)
    attr_i = _get_attr(attr.id)
    attr_i.name = attr.name
    attr_i.dimension = attr.dimension
    attr_i.description = attr.description

    #Make sure an update hasn't caused an inconsistency.
    #check_sion(attr_i.id)

    db.DBSession.flush()
    return attr_i

def add_attributes(attrs,**kwargs):
    """
    Add a generic attribute, which can then be used in creating
    a resource attribute, and put into a type.

    .. code-block:: python

        (Attr){
            id = 1020
            name = "Test Attr"
            dimen = "very big"
        }

    """

    log.debug("Adding s: %s", [attr.name for attr in attrs])
    #Check to see if any of the attributs being added are already there.
    #If they are there already, don't add a new one. If an attribute
    #with the same name is there already but with a different dimension,
    #add a new attribute.

    all_attrs = db.DBSession.query(Attr).all()
    attr_dict = {}
    for attr in all_attrs:
        attr_dict[(attr.name.lower(), attr.dimension.lower())] = attr

    attrs_to_add = []
    existing_attrs = []
    for potential_new_attr in attrs:
        if potential_new_attr.dimension is None or potential_new_attr.dimension.lower() == 'dimensionless':
            potential_new_attr.dimension = 'dimensionless'

        if attr_dict.get((potential_new_attr.name.lower(), potential_new_attr.dimension.lower())) is None:
            attrs_to_add.append(potential_new_attr)
        else:
            existing_attrs.append(attr_dict.get((potential_new_attr.name.lower(), potential_new_attr.dimension.lower())))

    new_attrs = []
    for attr in attrs_to_add:
        attr_i = Attr()
        attr_i.name = attr.name
        attr_i.dimension = attr.dimension
        attr_i.description = attr.description
        db.DBSession.add(attr_i)
        new_attrs.append(attr_i)

    db.DBSession.flush()

    new_attrs = new_attrs + existing_attrs

    return new_attrs

def get_attributes(**kwargs):
    """
        Get all attributes
    """

    attrs = db.DBSession.query(Attr).order_by(Attr.name).all()

    return attrs

def _get_attr(attr_id):
    try:
        attr = db.DBSession.query(Attr).filter(Attr.id == attr_id).one()
        return attr
    except NoResultFound:
        raise ResourceNotFoundError("Attribute with ID %s not found"%(attr_id,))

def _get_templatetype(type_id):
    try:
        typ = db.DBSession.query(TemplateType).filter(TemplateType.id == type_id).one()
        return typ
    except NoResultFound:
        raise ResourceNotFoundError("Template Type with ID %s not found"%(type_id,))

def update_resource_attribute(resource_attr_id, is_var, **kwargs):
    """
        Deletes a resource attribute and all associated data.
    """
    user_id = kwargs.get('user_id')
    try:
        ra = db.DBSession.query(ResourceAttr).filter(ResourceAttr.id == resource_attr_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Resource Attribute %s not found"%(resource_attr_id))

    ra.check_write_permission(user_id)

    ra.is_var = is_var

    return 'OK'

def delete_resource_attribute(resource_attr_id, **kwargs):
    """
        Deletes a resource attribute and all associated data.
    """
    user_id = kwargs.get('user_id')
    try:
        ra = db.DBSession.query(ResourceAttr).filter(ResourceAttr.id == resource_attr_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Resource Attribute %s not found"%(resource_attr_id))

    ra.check_write_permission(user_id)
    db.DBSession.delete(ra)
    db.DBSession.flush()
    return 'OK'


def add_resource_attribute(resource_type, resource_id, attr_id, is_var, error_on_duplicate=True, **kwargs):
    """
        Add a resource attribute attribute to a resource.

        attr_is_var indicates whether the attribute is a variable or not --
        this is used in simulation to indicate that this value is expected
        to be filled in by the simulator.
    """

    attr = db.DBSession.query(Attr).filter(Attr.id==attr_id).first()

    if attr is None:
        raise ResourceNotFoundError("Attribute with ID %s does not exist."%attr_id)

    resource_i = _get_resource(resource_type, resource_id)

    resourceattr_qry = db.DBSession.query(ResourceAttr).filter(ResourceAttr.ref_key==resource_type)

    if resource_type == 'NETWORK':
        resourceattr_qry = resourceattr_qry.filter(ResourceAttr.network_id==resource_id)
    elif resource_type == 'NODE':
        resourceattr_qry = resourceattr_qry.filter(ResourceAttr.node_id==resource_id)
    elif resource_type == 'LINK':
        resourceattr_qry = resourceattr_qry.filter(ResourceAttr.link_id==resource_id)
    elif resource_type == 'GROUP':
        resourceattr_qry = resourceattr_qry.filter(ResourceAttr.group_id==resource_id)
    elif resource_type == 'PROJECT':
        resourceattr_qry = resourceattr_qry.filter(ResourceAttr.project_id==resource_id)
    else:
        raise HydraError('Resource type "{}" not recognised.'.format(resource_type))
    resource_attrs = resourceattr_qry.all()

    for ra in resource_attrs:
        if ra.attr_id == attr_id:
            if not error_on_duplicate:
                return ra

            raise HydraError("Duplicate attribute. %s %s already has attribute %s"
                             %(resource_type, resource_i.get_name(), attr.name))

    attr_is_var = 'Y' if is_var == 'Y' else 'N'

    new_ra = resource_i.add_attribute(attr_id, attr_is_var)
    db.DBSession.flush()

    return new_ra

def add_resource_attrs_from_type(type_id, resource_type, resource_id,**kwargs):
    """
        adds all the attributes defined by a type to a node.
    """
    type_i = _get_templatetype(type_id)

    resource_i = _get_resource(resource_type, resource_id)

    resourceattr_qry = db.DBSession.query(ResourceAttr).filter(ResourceAttr.ref_key==resource_type)

    if resource_type == 'NETWORK':
        resourceattr_qry.filter(ResourceAttr.network_id==resource_id)
    elif resource_type == 'NODE':
        resourceattr_qry.filter(ResourceAttr.node_id==resource_id)
    elif resource_type == 'LINK':
        resourceattr_qry.filter(ResourceAttr.link_id==resource_id)
    elif resource_type == 'GROUP':
        resourceattr_qry.filter(ResourceAttr.group_id==resource_id)
    elif resource_type == 'PROJECT':
        resourceattr_qry.filter(ResourceAttr.project_id==resource_id)

    resource_attrs = resourceattr_qry.all()

    attrs = {}
    for res_attr in resource_attrs:
        attrs[res_attr.attr_id] = res_attr

    new_resource_attrs = []
    for item in type_i.typeattrs:
        if attrs.get(item.attr_id) is None:
            ra = resource_i.add_attribute(item.attr_id)
            new_resource_attrs.append(ra)

    db.DBSession.flush()

    return new_resource_attrs

def get_all_resource_attributes(ref_key, network_id, template_id=None, **kwargs):
    """
        Get all the resource attributes for a given resource type in the network.
        That includes all the resource attributes for a given type within the network.
        For example, if the ref_key is 'NODE', then it will return all the attirbutes
        of all nodes in the network. This function allows a front end to pre-load an entire
        network's resource attribute information to reduce on function calls.
        If type_id is specified, only
        return the resource attributes within the type.
    """

    user_id = kwargs.get('user_id')

    resource_attr_qry = db.DBSession.query(ResourceAttr).\
            outerjoin(Node, Node.id==ResourceAttr.node_id).\
            outerjoin(Link, Link.id==ResourceAttr.link_id).\
            outerjoin(ResourceGroup, ResourceGroup.id==ResourceAttr.group_id).filter(
        ResourceAttr.ref_key == ref_key,
        or_(
            and_(ResourceAttr.node_id != None,
                    ResourceAttr.node_id == Node.id,
                                        Node.network_id==network_id),

            and_(ResourceAttr.link_id != None,
                    ResourceAttr.link_id == Link.id,
                                        Link.network_id==network_id),

            and_(ResourceAttr.group_id != None,
                    ResourceAttr.group_id == ResourceGroup.id,
                                        ResourceGroup.network_id==network_id)
        ))

    if template_id is not None:
        attr_ids = []
        rs = db.DBSession.query(TypeAttr).join(TemplateType,
                                            TemplateType.id==TypeAttr.type_id).filter(
                                                TemplateType.template_id==template_id).all()
        for r in rs:
            attr_ids.append(r.attr_id)

        resource_attr_qry = resource_attr_qry.filter(ResourceAttr.attr_id.in_(attr_ids))

    resource_attrs = resource_attr_qry.all()

    return resource_attrs

def get_resource_attributes(ref_key, ref_id, type_id=None, **kwargs):
    """
        Get all the resource attributes for a given resource.
        If type_id is specified, only
        return the resource attributes within the type.
    """

    user_id = kwargs.get('user_id')

    resource_attr_qry = db.DBSession.query(ResourceAttr).filter(
        ResourceAttr.ref_key == ref_key,
        or_(
            ResourceAttr.network_id==ref_id,
            ResourceAttr.node_id==ref_id,
            ResourceAttr.link_id==ref_id,
            ResourceAttr.group_id==ref_id
        ))

    if type_id is not None:
        attr_ids = []
        rs = db.DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_id).all()
        for r in rs:
            attr_ids.append(r.attr_id)

        resource_attr_qry = resource_attr_qry.filter(ResourceAttr.attr_id.in_(attr_ids))

    resource_attrs = resource_attr_qry.all()

    return resource_attrs

def check_attr_dimension(attr_id, **kwargs):
    """
        Check that the dimension of the resource attribute data is consistent
        with the definition of the attribute.
        If the attribute says 'volume', make sure every dataset connected
        with this attribute via a resource attribute also has a dimension
        of 'volume'.
    """
    attr_i = _get_attr(attr_id)

    datasets = db.DBSession.query(Dataset).filter(Dataset.id==ResourceScenario.dataset_id,
                                               ResourceScenario.resource_attr_id == ResourceAttr.id,
                                               ResourceAttr.attr_id == attr_id).all()
    bad_datasets = []
    for d in datasets:
        if units.get_unit_dimension(d.unit) != attr_i.dimension:
            bad_datasets.append(d.id)

    if len(bad_datasets) > 0:
        raise HydraError("Datasets %s have a different dimension to attribute %s"%(bad_datasets, attr_id))

    return 'OK'

def get_resource_attribute(resource_attr_id, **kwargs):
    """
        Get a specific resource attribte, by ID
        If type_id is Gspecified, only
        return the resource attributes within the type.
    """

    resource_attr_qry = db.DBSession.query(ResourceAttr).filter(
        ResourceAttr.id == resource_attr_id,
        )

    resource_attr = resource_attr_qry.first()

    if resource_attr is None:
        raise ResourceNotFoundError("Resource attribute %s does not exist", resource_attr_id)

    return resource_attr

def set_attribute_mapping(resource_attr_a, resource_attr_b, **kwargs):
    """
        Define one resource attribute from one network as being the same as
        that from another network.
    """
    user_id = kwargs.get('user_id')
    ra_1 = get_resource_attribute(resource_attr_a)
    ra_2 = get_resource_attribute(resource_attr_b)

    mapping = ResourceAttrMap(resource_attr_id_a = resource_attr_a,
                             resource_attr_id_b  = resource_attr_b,
                             network_a_id     = ra_1.get_network().id,
                             network_b_id     = ra_2.get_network().id )

    db.DBSession.add(mapping)

    db.DBSession.flush()

    return mapping

def delete_attribute_mapping(resource_attr_a, resource_attr_b, **kwargs):
    """
        Define one resource attribute from one network as being the same as
        that from another network.
    """
    user_id = kwargs.get('user_id')

    rm = aliased(ResourceAttrMap, name='rm')

    log.info("Trying to delete attribute map. %s -> %s", resource_attr_a, resource_attr_b)
    mapping = db.DBSession.query(rm).filter(
                             rm.resource_attr_id_a == resource_attr_a,
                             rm.resource_attr_id_b == resource_attr_b).first()

    if mapping is not None:
        log.info("Deleting attribute map. %s -> %s", resource_attr_a, resource_attr_b)
        db.DBSession.delete(mapping)
        db.DBSession.flush()

    return 'OK'

def delete_mappings_in_network(network_id, network_2_id=None, **kwargs):
    """
        Delete all the resource attribute mappings in a network. If another network
        is specified, only delete the mappings between the two networks.
    """
    qry = db.DBSession.query(ResourceAttrMap).filter(or_(ResourceAttrMap.network_a_id == network_id, ResourceAttrMap.network_b_id == network_id))

    if network_2_id is not None:
        qry = qry.filter(or_(ResourceAttrMap.network_a_id==network_2_id, ResourceAttrMap.network_b_id==network_2_id))

    mappings = qry.all()

    for m in mappings:
        db.DBSession.delete(m)
    db.DBSession.flush()

    return 'OK'

def get_mappings_in_network(network_id, network_2_id=None, **kwargs):
    """
        Get all the resource attribute mappings in a network. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = db.DBSession.query(ResourceAttrMap).filter(or_(ResourceAttrMap.network_a_id == network_id, ResourceAttrMap.network_b_id == network_id))

    if network_2_id is not None:
        qry = qry.filter(or_(ResourceAttrMap.network_a_id==network_2_id, ResourceAttrMap.network_b_id==network_2_id))

    return qry.all()

def get_node_mappings(node_id, node_2_id=None, **kwargs):
    """
        Get all the resource attribute mappings in a network. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = db.DBSession.query(ResourceAttrMap).filter(
        or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == ResourceAttr.id,
                ResourceAttr.node_id == node_id),
            and_(
                ResourceAttrMap.resource_attr_id_b == ResourceAttr.id,
                ResourceAttr.node_id == node_id)))

    if node_2_id is not None:
        aliased_ra = aliased(ResourceAttr, name="ra2")
        qry = qry.filter(or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == aliased_ra.id,
                aliased_ra.node_id == node_2_id),
            and_(
                ResourceAttrMap.resource_attr_id_b == aliased_ra.id,
                aliased_ra.node_id == node_2_id)))

    return qry.all()

def get_link_mappings(link_id, link_2_id=None, **kwargs):
    """
        Get all the resource attribute mappings in a network. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = db.DBSession.query(ResourceAttrMap).filter(
        or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == ResourceAttr.id,
                ResourceAttr.link_id == link_id),
            and_(
                ResourceAttrMap.resource_attr_id_b == ResourceAttr.id,
                ResourceAttr.link_id == link_id)))

    if link_2_id is not None:
        aliased_ra = aliased(ResourceAttr, name="ra2")
        qry = qry.filter(or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == aliased_ra.id,
                aliased_ra.link_id == link_2_id),
            and_(
                ResourceAttrMap.resource_attr_id_b == aliased_ra.id,
                aliased_ra.link_id == link_2_id)))

    return qry.all()


def get_network_mappings(network_id, network_2_id=None, **kwargs):
    """
        Get all the mappings of network resource attributes, NOT ALL THE MAPPINGS
        WITHIN A NETWORK. For that, ``use get_mappings_in_network``. If another network
        is specified, only return the mappings between the two networks.
    """
    qry = db.DBSession.query(ResourceAttrMap).filter(
        or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == ResourceAttr.id,
                ResourceAttr.network_id == network_id),
            and_(
                ResourceAttrMap.resource_attr_id_b == ResourceAttr.id,
                ResourceAttr.network_id == network_id)))

    if network_2_id is not None:
        aliased_ra = aliased(ResourceAttr, name="ra2")
        qry = qry.filter(or_(
            and_(
                ResourceAttrMap.resource_attr_id_a == aliased_ra.id,
                aliased_ra.network_id == network_2_id),
            and_(
                ResourceAttrMap.resource_attr_id_b == aliased_ra.id,
                aliased_ra.network_id == network_2_id)))

    return qry.all()

def check_attribute_mapping_exists(resource_attr_id_source, resource_attr_id_target, **kwargs):
    """
        Check whether an attribute mapping exists between a source and target resource attribute.
        returns 'Y' if a mapping exists. Returns 'N' in all other cases.
    """
    qry = db.DBSession.query(ResourceAttrMap).filter(
                ResourceAttrMap.resource_attr_id_a == resource_attr_id_source,
                ResourceAttrMap.resource_attr_id_b == resource_attr_id_target).all()

    if len(qry) > 0:
        return 'Y'
    else:
        return 'N'


def get_attribute_group(group_id, **kwargs):
    """
        Get a specific attribute group
    """

    user_id=kwargs.get('user_id')

    try:
        group_i = db.DBSession.query(AttrGroup).filter(
                                            AttrGroup.id==group_id).one()
        group_i.project.check_read_permission(user_id)
    except NoResultFound:
        raise HydraError("Group %s not found" % (group_id,))

    return group_i


def add_attribute_group(attributegroup, **kwargs):
    """
        Add a new attribute group.

        An attribute group is a container for attributes which need to be grouped
        in some logical way. For example, if the 'attr_is_var' flag isn't expressive
        enough to delineate different groupings.

        an attribute group looks like:
            {
                'project_id' : XXX,
                'name'       : 'my group name'
                'description : 'my group description' (optional)
                'layout'     : 'my group layout'      (optional)
                'exclusive'  : 'N' (or 'Y' )          (optional, default to 'N')
            }
    """
    log.info("attributegroup.project_id %s",attributegroup.project_id) # It is None while it should be valued
    user_id=kwargs.get('user_id')
    project_i = db.DBSession.query(Project).filter(Project.id==attributegroup.project_id).one()
    project_i.check_write_permission(user_id)
    try:

        group_i = db.DBSession.query(AttrGroup).filter(
                                            AttrGroup.name==attributegroup.name,
                                            AttrGroup.project_id==attributegroup.project_id).one()
        log.info("Group %s already exists in project %s", attributegroup.name, attributegroup.project_id)

    except NoResultFound:

        group_i = AttrGroup()
        group_i.project_id  = attributegroup.project_id
        group_i.name        = attributegroup.name
        group_i.description = attributegroup.description
        group_i.layout      = attributegroup.get_layout()
        group_i.exclusive   = attributegroup.exclusive

        db.DBSession.add(group_i)
        db.DBSession.flush()

        log.info("Attribute Group %s added to project %s", attributegroup.name, attributegroup.project_id)

    return group_i

def update_attribute_group(attributegroup, **kwargs):
    """
        Add a new attribute group.

        An attribute group is a container for attributes which need to be grouped
        in some logical way. For example, if the 'attr_is_var' flag isn't expressive
        enough to delineate different groupings.

        an attribute group looks like:
            {
                'project_id' : XXX,
                'name'       : 'my group name'
                'description : 'my group description' (optional)
                'layout'     : 'my group layout'      (optional)
                'exclusive'  : 'N' (or 'Y' )          (optional, default to 'N')
            }
    """
    user_id=kwargs.get('user_id')

    if attributegroup.id is None:
        raise HydraError("cannot update attribute group. no ID specified")

    try:

        group_i = db.DBSession.query(AttrGroup).filter(AttrGroup.id==attributegroup.id).one()
        group_i.project.check_write_permission(user_id)

        group_i.name        = attributegroup.name
        group_i.description = attributegroup.description
        group_i.layout      = attributegroup.layout
        group_i.exclusive   = attributegroup.exclusive

        db.DBSession.flush()

        log.info("Group %s in project %s updated", attributegroup.id, attributegroup.project_id)
    except NoResultFound:

        raise HydraError('No Attribute Group %s was found in project %s', attributegroup.id, attributegroup.project_id)


    return group_i

def delete_attribute_group(group_id, **kwargs):
    """
        Delete an attribute group.
    """
    user_id = kwargs['user_id']

    try:

        group_i = db.DBSession.query(AttrGroup).filter(AttrGroup.id==group_id).one()

        group_i.project.check_write_permission(user_id)

        db.DBSession.delete(group_i)
        db.DBSession.flush()

        log.info("Group %s in project %s deleted", group_i.id, group_i.project_id)
    except NoResultFound:

        raise HydraError('No Attribute Group %s was found', group_id)


    return 'OK'

def get_network_attributegroup_items(network_id, **kwargs):
    """
        Get all the group items in a network
    """

    user_id=kwargs.get('user_id')


    net_i = _get_network(network_id)

    net_i.check_read_permission(user_id)

    group_items_i = db.DBSession.query(AttrGroupItem).filter(
                                    AttrGroupItem.network_id==network_id).all()

    return group_items_i

def get_group_attributegroup_items(network_id, group_id, **kwargs):
    """
        Get all the items in a specified group, within a network
    """
    user_id=kwargs.get('user_id')

    network_i = _get_network(network_id)

    network_i.check_read_permission(user_id)

    group_items_i = db.DBSession.query(AttrGroupItem).filter(
                                    AttrGroupItem.network_id==network_id,
                                    AttrGroupItem.group_id==group_id).all()

    return group_items_i


def get_attribute_item_groups(network_id, attr_id, **kwargs):
    """
        Get all the group items in a network with a given attribute_id
    """
    user_id=kwargs.get('user_id')

    network_i = _get_network(network_id)

    network_i.check_read_permission(user_id)

    group_items_i = db.DBSession.query(AttrGroupItem).filter(
                                        AttrGroupItem.network_id==network_id,
                                        AttrGroupItem.attr_id==attr_id).all()

    return group_items_i

def _get_attr_group(group_id):
    try:
        group_i = db.DBSession.query(AttrGroup).filter(AttrGroup.id==group_id).one()
    except NoResultFound:
        raise HydraError("Error adding attribute group item: group %s not found" % (agi.group_id))

    return group_i

def _get_attributegroupitems(network_id):
    existing_agis = db.DBSession.query(AttrGroupItem).filter(AttrGroupItem.network_id==network_id).all()
    return existing_agis

def add_attribute_group_items(attributegroupitems, **kwargs):

    """
        Populate attribute groups with items.
        ** attributegroupitems : a list of items, of the form:
            ```{
                    'attr_id'    : X,
                    'group_id'   : Y,
                    'network_id' : Z,
               }```

        Note that this approach supports the possibility of populating groups
        within multiple networks at the same time.

        When adding a group item, the function checks whether it can be added,
        based on the 'exclusivity' setup of the groups -- if a group is specified
        as being 'exclusive', then any attributes within that group cannot appear
        in any other group (within a network).
    """

    user_id=kwargs.get('user_id')

    if not isinstance(attributegroupitems, list):
        raise HydraError("Cannpt add attribute group items. Attributegroupitems must be a list")

    new_agis_i = []

    group_lookup = {}

    #for each network, keep track of what attributes are contained in which groups it's in
    #structure: {NETWORK_ID : {ATTR_ID: [GROUP_ID]}
    agi_lookup = {}

    network_lookup = {}

    #'agi' = shorthand for 'attribute group item'
    for agi in attributegroupitems:


        network_i = network_lookup.get(agi.network_id)

        if network_i is None:
            network_i = _get_network(agi.network_id)
            network_lookup[agi.network_id] = network_i

        network_i.check_write_permission(user_id)


        #Get the group so we can check for exclusivity constraints
        group_i = group_lookup.get(agi.group_id)
        if group_i is None:
            group_lookup[agi.group_id] = _get_attr_group(agi.group_id)

        network_agis = agi_lookup

        #Create a map of all agis currently in the network
        if agi_lookup.get(agi.network_id) is None:
            agi_lookup[agi.network_id] = {}
            network_agis = _get_attributegroupitems(agi.network_id)
            log.info(network_agis)
            for net_agi in network_agis:

                if net_agi.group_id not in group_lookup:
                    group_lookup[net_agi.group_id] = _get_attr_group(net_agi.group_id)

                if agi_lookup.get(net_agi.network_id) is None:
                    agi_lookup[net_agi.network_id][net_agi.attr_id] = [net_agi.group_id]
                else:
                    if agi_lookup[net_agi.network_id].get(net_agi.attr_id) is None:
                        agi_lookup[net_agi.network_id][net_agi.attr_id] = [net_agi.group_id]
                    elif net_agi.group_id not in agi_lookup[net_agi.network_id][net_agi.attr_id]:
                        agi_lookup[net_agi.network_id][net_agi.attr_id].append(net_agi.group_id)
        #Does this agi exist anywhere else inside this network?
        #Go through all the groups that this attr is in and make sure it's not exclusive
        if agi_lookup[agi.network_id].get(agi.attr_id) is not None:
            for group_id in agi_lookup[agi.network_id][agi.attr_id]:
                group = group_lookup[group_id]
                #Another group has been found.
                if group.exclusive == 'Y':
                    #The other group is exclusive, so this attr can't be added
                    raise HydraError("Attribute %s is already in Group %s for network %s. This group is exclusive, so attr %s cannot exist in another group."%(agi.attr_id, group.id, agi.network_id, agi.attr_id))

            #Now check that if this group is exclusive, then the attr isn't in
            #any other groups
            if group_lookup[agi.group_id].exclusive == 'Y':
                if len(agi_lookup[agi.network_id][agi.attr_id]) > 0:
                    #The other group is exclusive, so this attr can't be added
                    raise HydraError("Cannot add attribute %s to group %s. This group is exclusive, but attr %s has been found in other groups (%s)" % (agi.attr_id, agi.group_id, agi.attr_id, agi_lookup[agi.network_id][agi.attr_id]))


        agi_i = AttrGroupItem()
        agi_i.network_id = agi.network_id
        agi_i.group_id   = agi.group_id
        agi_i.attr_id    = agi.attr_id

        #Update the lookup table in preparation for the next pass.
        if agi_lookup[agi.network_id].get(agi.attr_id) is None:
            agi_lookup[agi.network_id][agi.attr_id] = [agi.group_id]
        elif agi.group_id not in agi_lookup[agi.network_id][agi.attr_id]:
            agi_lookup[agi.network_id][agi.attr_id].append(agi.group_id)


        db.DBSession.add(agi_i)

        new_agis_i.append(agi_i)
    log.info(agi_lookup)

    db.DBSession.flush()

    return new_agis_i

def delete_attribute_group_items(attributegroupitems, **kwargs):

    """
        remove attribute groups items .
        ** attributegroupitems : a list of items, of the form:
            ```{
                    'attr_id'    : X,
                    'group_id'   : Y,
                    'network_id' : Z,
               }```
    """


    user_id=kwargs.get('user_id')

    log.info("Deleting %s attribute group items", len(attributegroupitems))

    #if there area attributegroupitems from different networks, keep track of those
    #networks to ensure the user actually has permission to remove the items.
    network_lookup = {}

    if not isinstance(attributegroupitems, list):
        raise HydraError("Cannpt add attribute group items. Attributegroupitems must be a list")

    #'agi' = shorthand for 'attribute group item'
    for agi in attributegroupitems:

        network_i = network_lookup.get(agi.network_id)

        if network_i is None:
            network_i = _get_network(agi.network_id)
            network_lookup[agi.network_id] = network_i

        network_i.check_write_permission(user_id)

        agi_i = db.DBSession.query(AttrGroupItem).filter(AttrGroupItem.network_id == agi.network_id,
                                                 AttrGroupItem.group_id == agi.group_id,
                                                 AttrGroupItem.attr_id  == agi.attr_id).first()

        if agi_i is not None:
            db.DBSession.delete(agi_i)

    db.DBSession.flush()

    log.info("Attribute group items deleted")

    return 'OK'
