# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2020 University of Manchester
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

"""
    This module contains all functions which relate to the application,
    validation or manipulation of resources (nodes, links, groups, networks)
    in relation to a template, rather than manipulating
    the templates themselves.
    These include applying types to resources, validating resources etc.
"""

import json
import logging

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import noload, joinedload
from sqlalchemy import or_, and_

from hydra_base import db
from hydra_base.db.model import Template, TemplateType, Attr, \
                                Network, Node, Link, ResourceGroup,\
                                ResourceType, ResourceAttr, ResourceScenario, Scenario
from hydra_base.lib.objects import JSONObject
from hydra_base.exceptions import HydraError
from hydra_base.util import dataset_util
from hydra_base.lib import units
from hydra_base.util.permissions import required_perms

log = logging.getLogger(__name__)

def get_network_template(network_id, reference_type_id):
    """
        If a network type is defined with a child template id (it has been created
        using a child template), then find this so that it can be used when
        inserting nodes & links into that template.

        This is to avoid having to set the child template ID on all add_node add_link
        functions etc.

        The reference_type_id is the type of the node or link being inserted, and
        is needed because a network may have multiple types, and so a type from the same
        template as one of those network types needs to be used for reference.
    """
    #Get all the network types
    network_types = db.DBSession.query(ResourceType)\
        .filter(ResourceType.network_id == network_id).all()

    #get the template type of the incoming resource
    ref_type = db.DBSession.query(TemplateType)\
        .filter(TemplateType.id == reference_type_id).one()

    #a bit roundabout, but this gets the fully-populated, inherited child type
    ref_type_inherited = ref_type.template.get_type(reference_type_id)

    #Now go through each network type and try to find the matching template.
    #Assume that a network cannot have 2 types which inherit from the same template.
    for nt in network_types:
        network_tt = nt.get_templatetype()

        if ref_type.template_id == network_tt.template_id:
            #if the network's type and the incoming type's template ID, we've found
            #a match.
            return nt.child_template_id
        elif ref_type.template_id == nt.child_template_id:
            #If the child template id of the network is the same as the template ID of the incoming node type
            #then we've got a match -- this implies that the node type is defined in the child
            #itself, meaning it has been modified compared to its parent in some way.
            return nt.child_template_id
        #Get the ID of the template which is the parent of the child template which
        #the network was created with
        #TODO: THis only works for single inheritance. We can only look up one
        #level in the inheritance tree.
        network_template_parent_id = db.DBSession.query(Template)\
            .filter(Template.id==network_tt.template_id).one().parent_id

        if ref_type.template_id == network_template_parent_id:
            #if the parent template of the network type is the same as
            #the the template ID of this type
            return nt.child_template_id

    return None

def _get_type(type_id):
    """
        Utility function to get a template type by querying the parent for it.
        This must be done because types are constructed using template inheritance,
        so cannot be queried directly
    """

    log.info("Getting type %s", type_id)

    type_i = db.DBSession.query(TemplateType).filter(TemplateType.id == type_id).one()

    if type_i.parent_id is None:
        return type_i

    template_i = db.DBSession.query(Template)\
            .filter(Template.id == type_i.template_id).one()

    type_i = template_i.get_type(type_id)
    type_i.template = JSONObject(template_i)

    return type_i

@required_perms('get_template')
def get_types_by_attr(resource, resource_type, template_id=None, **kwargs):
    """
        Using the attributes of the resource, get all the
        types that this resource matches.
        args:
            resource (a resource object (node, link etc), assumed to have a
                     '.attributes' attribute)
            template_id: The ID of a template, which will filter the result to
                         just types in that template
        returns:
            dict: keyed on the template name, with the
            value being the list of type names which match the resources
            attributes.
    """

    resource_type_templates = []

    #Create a list of all of this resources attributes.
    attr_ids = []
    for res_attr in resource.attributes:
        attr_ids.append(res_attr.attr_id)
    all_resource_attr_ids = set(attr_ids)

    all_templates_qry = db.DBSession.query(Template)

    if template_id is not None:
        all_templates_qry = all_templates_qry.filter(Template.id == template_id)

    all_templates = all_templates_qry.all()

    all_types = []
    for template in all_templates:
        template_types = template.get_types()
        all_types.extend(template_types)

    #tmpl type attrs must be a subset of the resource's attrs
    for ttype in all_types:

        if ttype.resource_type != resource_type:
            continue

        type_attr_ids = []
        for typeattr in ttype.typeattrs:
            type_attr_ids.append(typeattr.attr_id)
        if set(type_attr_ids).issubset(all_resource_attr_ids):
            resource_type_templates.append(ttype)

    return resource_type_templates



@required_perms("edit_network")
def apply_template_to_network(template_id, network_id, **kwargs):
    """
        For each node and link in a network, check whether it matches
        a type in a given template. If so, assign the type to the node / link.
    """

    net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()
    #There should only ever be one matching type, but if there are more,
    #all we can do is pick the first one.
    try:
        network_template_i = db.DBSession.query(Template).filter(
            Template.id == template_id).one()

        for template_type in network_template_i.get_types():
            if template_type.resource_type == 'NETWORK':
                network_type_id = template_type.id
                assign_type_to_resource(network_type_id, 'NETWORK', network_id, **kwargs)
                break
        else:
            raise NoResultFound("A template type with resource type NETWORK "
                                f"not found in template {network_template_i.name}"
                                f" ({network_template_i.id})")

    except NoResultFound:
        log.debug("No network type to set.")

    for node_i in net_i.nodes:
        templates = get_types_by_attr(node_i, 'NODE', template_id, **kwargs)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].id, 'NODE', node_i.id, **kwargs)
    for link_i in net_i.links:
        templates = get_types_by_attr(link_i, 'LINK', template_id, **kwargs)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].id, 'LINK', link_i.id, **kwargs)

    for group_i in net_i.resourcegroups:
        templates = get_types_by_attr(group_i, 'GROUP', template_id, **kwargs)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].id, 'GROUP', group_i.id, **kwargs)

    db.DBSession.flush()

@required_perms("edit_network")
def set_network_template(template_id, network_id, **kwargs):
    """
       Apply an existing template to a network.
       Used when a template has changed, and additional attributes
       must be added to the network's elements.
    """

    resource_types = []

    try:
        network_type = db.DBSession.query(ResourceType).filter(
            ResourceType.ref_key == 'NETWORK',
            ResourceType.network_id == network_id,
            ResourceType.type_id == TemplateType.id,
            TemplateType.template_id == template_id).one()
        resource_types.append(network_type)
    except NoResultFound:
        log.debug("No network type to set.")

    node_types = db.DBSession.query(ResourceType).filter(
        ResourceType.ref_key == 'NODE',
        ResourceType.node_id == Node.id,
        Node.network_id == network_id,
        ResourceType.type_id == TemplateType.id,
        TemplateType.template_id == template_id).all()

    link_types = db.DBSession.query(ResourceType).filter(
        ResourceType.ref_key == 'LINK',
        ResourceType.link_id == Link.id,
        Link.network_id == network_id,
        ResourceType.type_id == TemplateType.id,
        TemplateType.template_id == template_id).all()

    group_types = db.DBSession.query(ResourceType).filter(
        ResourceType.ref_key == 'GROUP',
        ResourceType.group_id == ResourceGroup.id,
        ResourceGroup.network_id == network_id,
        ResourceType.type_id == TemplateType.id,
        TemplateType.template_id == template_id).all()

    resource_types.extend(node_types)
    resource_types.extend(link_types)
    resource_types.extend(group_types)

    assign_types_to_resources(resource_types)

    log.debug("Finished setting network template")

@required_perms("edit_network")
def remove_template_from_network(network_id, template_id, remove_attrs, **kwargs):
    """
        Remove all resource types in a network relating to the specified
        template.
        remove_attrs ('Y' or 'N')
            Flag to indicate whether the attributes associated with the template
            types should be removed from the resources in the network. These will
            only be removed if they are not shared with another template on the network
    """

    try:
        network = db.DBSession.query(Network).filter(Network.id == network_id).one()
    except NoResultFound:
        raise HydraError("Network %s not found"%network_id)

    try:
        template = db.DBSession.query(Template).filter(Template.id == template_id).one()
    except NoResultFound:
        raise HydraError("Template %s not found"%template_id)

    type_ids = [tmpltype.id for tmpltype in template.get_types()]

    node_ids = [n.id for n in network.nodes]
    link_ids = [l.id for l in network.links]
    group_ids = [g.id for g in network.resourcegroups]

    if remove_attrs == 'Y':
        #find the attributes to remove
        resource_attrs_to_remove = _get_resources_to_remove(network, template)
        for n in network.nodes:
            resource_attrs_to_remove.extend(_get_resources_to_remove(n, template))
        for l in network.links:
            resource_attrs_to_remove.extend(_get_resources_to_remove(l, template))
        for g in network.resourcegroups:
            resource_attrs_to_remove.extend(_get_resources_to_remove(g, template))

        for ra in resource_attrs_to_remove:
            db.DBSession.delete(ra)

    resource_types = db.DBSession.query(ResourceType).filter(
        and_(or_(
            ResourceType.network_id==network_id,
            ResourceType.node_id.in_(node_ids),
            ResourceType.link_id.in_(link_ids),
            ResourceType.group_id.in_(group_ids),
        ), ResourceType.type_id.in_(type_ids))).all()

    for resource_type in resource_types:
        db.DBSession.delete(resource_type)

    db.DBSession.flush()

def _get_resources_to_remove(resource, template):
    """
        Given a resource and a template being removed, identify the resource attribtes
        which can be removed.
    """
    type_ids = [tmpltype.id for tmpltype in template.get_types()]

    node_attr_ids = dict([(ra.attr_id, ra) for ra in resource.attributes])
    attrs_to_remove = []
    attrs_to_keep = []
    for nt in resource.types:
        templatetype = nt.get_templatetype()
        if templatetype in type_ids:
            for ta in templatetype.typeattrs:
                if node_attr_ids.get(ta.attr_id):
                    attrs_to_remove.append(node_attr_ids[ta.attr_id])
        else:
            for ta in templatetype.typeattrs:
                if node_attr_ids.get(ta.attr_id):
                    attrs_to_keep.append(node_attr_ids[ta.attr_id])
    #remove any of the attributes marked for deletion as they are
    #marked for keeping based on being in another type.
    final_attrs_to_remove = set(attrs_to_remove) - set(attrs_to_keep)

    return list(final_attrs_to_remove)

def get_matching_resource_types(resource_type, resource_id, **kwargs):
    """
        Get the possible types of a resource by checking its attributes
        against all available types.

        @returns A list of TypeSummary objects.
    """
    resource_i = None
    if resource_type == 'NETWORK':
        resource_i = db.DBSession.query(Network).filter(Network.id == resource_id).one()
    elif resource_type == 'NODE':
        resource_i = db.DBSession.query(Node).filter(Node.id == resource_id).one()
    elif resource_type == 'LINK':
        resource_i = db.DBSession.query(Link).filter(Link.id == resource_id).one()
    elif resource_type == 'GROUP':
        resource_i = db.DBSession.query(ResourceGroup).filter(
            ResourceGroup.id == resource_id).one()

    matching_types = get_types_by_attr(resource_i, resource_type, **kwargs)
    return matching_types

@required_perms("edit_network")
def assign_types_to_resources(resource_types, template_id=None, **kwargs):
    """
        Assign new types to list of resources.
        This function checks if the necessary
        attributes are present and adds them if needed. Non existing attributes
        are also added when the type is already assigned. This means that this
        function can also be used to update resources, when a resource type has
        changed.
    """

    log.info("Setting types of %s resources", len(resource_types))
    #Remove duplicate values from types by turning it into a set
    type_ids = list(set([rt.type_id for rt in resource_types]))

    #Get the template ID from the incoming data. We need this so we can get the
    #correct types from the template hierarchy.
    #We assume here that this function can only be called in the context of
    #one template -- you can't send resource types from 2 templates.
    if template_id is None and resource_types[0].template_id is None:
        log.info("No template ID specified. Getting from type")
        db_type = _get_type(resource_types[0].type_id)
        template_id = db_type.template_id
        log.info("Template ID set to: %s", template_id)

    template_i = db.DBSession.query(Template).filter(Template.id == template_id).one()

    template_types = template_i.get_types()

    db_types = []
    for tt in template_types:
        if tt.id in type_ids:
            db_types.append(tt)

    type_lookup = {}
    for db_type in db_types:
        if type_lookup.get(db_type.id) is None:
            type_lookup[db_type.id] = db_type
    log.debug("Retrieved all the appropriate template types")
    res_types = []
    res_attrs = []
    res_scenarios = []

    net_id = None
    node_ids = []
    link_ids = []
    grp_ids = []
    for resource_type in resource_types:
        ref_id = resource_type.ref_id
        ref_key = resource_type.ref_key
        if resource_type.ref_key == 'NETWORK':
            net_id = ref_id
        elif resource_type.ref_key == 'NODE':
            node_ids.append(ref_id)
        elif resource_type.ref_key == 'LINK':
            link_ids.append(ref_id)
        elif resource_type.ref_key == 'GROUP':
            grp_ids.append(ref_id)
    if net_id:
        net = db.DBSession.query(Network).filter(Network.id == net_id).one()
    nodes = _get_nodes(node_ids)
    links = _get_links(link_ids)
    groups = _get_groups(grp_ids)
    for resource_type in resource_types:
        ref_id = resource_type.ref_id
        ref_key = resource_type.ref_key
        type_id = resource_type.type_id
        if ref_key == 'NETWORK':
            resource = net
        elif ref_key == 'NODE':
            resource = nodes[ref_id]
        elif ref_key == 'LINK':
            resource = links[ref_id]
        elif ref_key == 'GROUP':
            resource = groups[ref_id]

        ra, rt, rs = set_resource_type(resource, type_id, type_lookup, **kwargs)
        if rt is not None:
            res_types.append(rt)
        if len(ra) > 0:
            res_attrs.extend(ra)
        if len(rs) > 0:
            res_scenarios.extend(rs)

    log.debug("Retrieved all the appropriate resources")
    if len(res_types) > 0:
        new_types = db.DBSession.execute(ResourceType.__table__.insert(), res_types)

    new_ras = []
    if len(res_attrs) > 0:
        new_res_attrs = db.DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)
        last_row_id = new_res_attrs.lastrowid or 0
        new_ras = db.DBSession.query(ResourceAttr).filter(
            and_(ResourceAttr.id >= last_row_id,
                 ResourceAttr.id < (last_row_id+len(res_attrs)))).all()

    ra_map = {}
    for new_ra in new_ras:
        ra_map[(new_ra.ref_key,
                new_ra.attr_id,
                new_ra.node_id,
                new_ra.link_id,
                new_ra.group_id,
                new_ra.network_id)] = new_ra.id

    for rs in res_scenarios:
        rs['resource_attr_id'] = ra_map[(rs['ref_key'],
                                         rs['attr_id'],
                                         rs['node_id'],
                                         rs['link_id'],
                                         rs['group_id'],
                                         rs['network_id'])]

    if len(res_scenarios) > 0:
        db.DBSession.execute(ResourceScenario.__table__.insert(), res_scenarios)

    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete.
    db.DBSession.query(ResourceAttr).filter(ResourceAttr.attr_id is None).delete()

    ret_val = list(type_lookup.values())

    return ret_val

@required_perms('get_template')
def check_type_compatibility(type_1_id, type_2_id, **kwargs):
    """
        When applying a type to a resource, it may be the case that the resource already
        has an attribute specified in the new type, but the template which defines this
        pre-existing attribute has a different unit specification to the new template.

        This function checks for any situations where different types specify the same
        attributes, but with different units.
        args:
            type_1_id: The ID of the type to compare
            type_2_id: The ID of the type to compare
        returns:
            list of strings, with errors describing any incompatibilities
    """
    errors = []

    type_1 = _get_type(type_1_id)
    type_2 = _get_type(type_2_id)

    template_1_name = type_1.template.name
    template_2_name = type_2.template.name

    type_1_attrs = set([t.attr_id for t in type_1.typeattrs])
    type_2_attrs = set([t.attr_id for t in type_2.typeattrs])

    shared_attrs = type_1_attrs.intersection(type_2_attrs)

    if len(shared_attrs) == 0:
        return []

    type_1_dict = {}
    for t in type_1.typeattrs:
        if t.attr_id in shared_attrs:
            type_1_dict[t.attr_id] = t

    for ta in type_2.typeattrs:
        type_2_unit_id = ta.unit_id
        type_1_unit_id = type_1_dict[ta.attr_id].unit_id

        fmt_dict = {
            'template_1_name':template_1_name,
            'template_2_name':template_2_name,
            'attr_name':ta.attr.name,
            'type_1_unit_id':type_1_unit_id,
            'type_2_unit_id':type_2_unit_id,
            'type_name' :type_1.name
        }

        if type_1_unit_id is None and type_2_unit_id is not None:
            errors.append("Type %(type_name)s in template %(template_1_name)s"
                          " stores %(attr_name)s with no units, while template"
                          "%(template_2_name)s stores it with unit %(type_2_unit_id)s"%fmt_dict)
        elif type_1_unit_id is not None and type_2_unit_id is None:
            errors.append("Type %(type_name)s in template %(template_1_name)s"
                          " stores %(attr_name)s in %(type_1_unit_id)s."
                          " Template %(template_2_name)s stores it with no unit."%fmt_dict)
        elif type_1_unit_id != type_2_unit_id:
            errors.append("Type %(type_name)s in template %(template_1_name)s"
                          " stores %(attr_name)s in %(type_1_unit_id)s, while"
                          " template %(template_2_name)s stores it in %(type_2_unit_id)s"%fmt_dict)
        return errors

def _get_links(link_ids):
    links = []

    if len(link_ids) == 0:
        return {}

    block_size = 500
    limit = len(link_ids)
    lower = 0
    while lower < limit:
        upper = lower+block_size
        rs = db.DBSession.query(Link)\
                   .options(joinedload(Link.attributes))\
                   .options(joinedload(Link.types))\
                   .filter(Link.id.in_(link_ids[lower:upper])).all()
        log.debug("Retrieved %s links", len(rs))
        links.extend(rs)
        lower = upper


    link_dict = {}

    for l in links:
        l.ref_id = l.id
        l.ref_key = 'LINK'
        link_dict[l.id] = l

    return link_dict

def _get_nodes(node_ids):
    nodes = []

    if len(node_ids) == 0:
        return {}

    block_size = 500
    limit = len(node_ids)
    lower = 0
    while lower < limit:
        upper = lower+block_size
        rs = db.DBSession.query(Node)\
                   .options(joinedload(Node.attributes))\
                   .options(joinedload(Node.types))\
                   .filter(Node.id.in_(node_ids[lower:upper])).all()
        log.debug("Retrieved %s nodes", len(rs))
        nodes.extend(rs)
        lower = upper

    node_dict = {}

    for n in nodes:
        n.ref_id = n.id
        n.ref_key = 'NODE'
        node_dict[n.id] = n

    return node_dict

def _get_groups(group_ids):
    groups = []

    if len(group_ids) == 0:
        return {}

    block_size = 500
    limit = len(group_ids)
    lower = 0
    while lower < limit:
        upper = lower+block_size
        rs = db.DBSession.query(ResourceGroup)\
                   .options(joinedload(ResourceGroup.attributes))\
                   .options(joinedload(ResourceGroup.types))\
                   .filter(ResourceGroup.id.in_(group_ids[lower:upper])).all()
        log.debug("Retrieved %s groups", len(rs))
        groups.extend(rs)
        lower = upper

    group_dict = {}

    for g in groups:
        g.ref_id = g.id
        g.ref_key = 'GROUP'
        group_dict[g.id] = g

    return group_dict

@required_perms("edit_network")
def assign_type_to_resource(type_id, resource_type, resource_id, **kwargs):
    """Assign new type to a resource. This function checks if the necessary
    attributes are present and adds them if needed. Non existing attributes
    are also added when the type is already assigned. This means that this
    function can also be used to update resources, when a resource type has
    changed.
    """

    if resource_type == 'NETWORK':
        resource = db.DBSession.query(Network).filter(Network.id == resource_id).one()
    elif resource_type == 'NODE':
        resource = db.DBSession.query(Node).filter(Node.id == resource_id).one()
    elif resource_type == 'LINK':
        resource = db.DBSession.query(Link).filter(Link.id == resource_id).one()
    elif resource_type == 'GROUP':
        resource = db.DBSession.query(ResourceGroup).filter(ResourceGroup.id == resource_id).one()

    res_attrs, res_type, res_scenarios = set_resource_type(resource, type_id, **kwargs)

    type_i = _get_type(type_id)

    if resource_type != type_i.resource_type:
        raise HydraError("Cannot assign a %s type to a %s"%
                         (type_i.resource_type, resource_type))

    if res_type is not None:
        db.DBSession.bulk_insert_mappings(ResourceType, [res_type])

    if len(res_attrs) > 0:
        db.DBSession.bulk_insert_mappings(ResourceAttr, res_attrs)

    if len(res_scenarios) > 0:
        db.DBSession.bulk_insert_mappings(ResourceScenario, res_scenarios)

    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete.
    db.DBSession.query(Attr).filter(Attr.id is None).delete()

    db.DBSession.flush()

    return_type = _get_type(type_id)

    return return_type

@required_perms("edit_network")
def set_resource_type(resource, type_id, types={}, **kwargs):
    """
        Set this resource to be a certain type.
        Type objects (a dictionary keyed on type_id) may be
        passed in to save on loading.
        This function does not call save. It must be done afterwards.
        New resource attributes are added to the resource if the template
        requires them. Resource attributes on the resource but not used by
        the template are not removed.
        @returns list of new resource attributes
        ,new resource type object
    """

    #get the resource#s network ID:
    if kwargs.get('network_id') is not None:
        network_id = kwargs['network_id']
    elif isinstance(resource, Network):
        network_id = resource.id
    elif resource.network_id:
        network_id = resource.network_id
    elif resource.network:
        network_id = resource.network.id

    child_template_id = kwargs.get('child_template_id')
    if kwargs.get('child_template_id') is None:
        if network_id is not None:
            child_template_id = get_network_template(network_id, type_id)

    ref_key = resource.ref_key

    existing_attr_ids = []
    for res_attr in resource.attributes:
        existing_attr_ids.append(res_attr.attr_id)

    if type_id in types:
        type_i = types[type_id]
    else:
        type_i = _get_type(type_id)

    type_attrs = dict()
    for typeattr in type_i.typeattrs:
        type_attrs[typeattr.attr_id] = {
            'is_var':typeattr.attr_is_var,
            'default_dataset_id': typeattr.default_dataset.id if typeattr.default_dataset else None
        }

    # check if attributes exist
    missing_attr_ids = set(type_attrs.keys()) - set(existing_attr_ids)

    # add attributes if necessary
    new_res_attrs = []

    #This is a dict as the length of the list may not match the new_res_attrs
    #Keyed on attr_id, as resource_attr_id doesn't exist yet, and there should only
    #be one attr_id per template.
    new_res_scenarios = {}
    for attr_id in missing_attr_ids:
        ra_dict = dict(
            ref_key=ref_key,
            attr_id=attr_id,
            attr_is_var=type_attrs[attr_id]['is_var'],
            node_id=resource.id if ref_key == 'NODE' else None,
            link_id=resource.id if ref_key == 'LINK' else None,
            group_id=resource.id if ref_key == 'GROUP' else None,
            network_id=resource.id if ref_key == 'NETWORK' else None,
        )
        new_res_attrs.append(ra_dict)



        if type_attrs[attr_id]['default_dataset_id'] is not None:
            if hasattr(resource, 'network'):
                for s in resource.network.scenarios:

                    if new_res_scenarios.get(attr_id) is None:
                        new_res_scenarios[attr_id] = {}

                    new_res_scenarios[attr_id][s.id] = dict(
                        dataset_id=type_attrs[attr_id]['default_dataset_id'],
                        scenario_id=s.id,
                        #Not stored in the DB, but needed to connect the RA ID later.
                        attr_id=attr_id,
                        ref_key=ref_key,
                        node_id=ra_dict['node_id'],
                        link_id=ra_dict['link_id'],
                        group_id=ra_dict['group_id'],
                        network_id=ra_dict['network_id'],
                    )


    resource_type = None
    for rt in resource.types:

        if rt.type_id == type_i.id:
            break

        errors = check_type_compatibility(rt.type_id, type_i.id, **kwargs)
        if len(errors) > 0:
            raise HydraError("Cannot apply type %s to resource %s as it "
                             "conflicts with type %s. Errors are: %s"
                             %(type_i.name, resource.get_name(),
                               rt.get_templatetype().name, ','.join(errors)))
    else:
        # add type to tResourceType if it doesn't exist already
        resource_type = dict(
            node_id=resource.id if ref_key == 'NODE' else None,
            link_id=resource.id if ref_key == 'LINK' else None,
            group_id=resource.id if ref_key == 'GROUP' else None,
            network_id=resource.id if ref_key == 'NETWORK' else None,
            ref_key=ref_key,
            type_id=type_id,
            child_template_id=child_template_id
        )

    return new_res_attrs, resource_type, new_res_scenarios

@required_perms("edit_network")
def remove_type_from_resource( type_id, resource_type, resource_id, **kwargs):
    """
        Remove a resource type trom a resource
    """
    node_id = resource_id if resource_type == 'NODE' else None
    link_id = resource_id if resource_type == 'LINK' else None
    group_id = resource_id if resource_type == 'GROUP' else None

    resourcetype = db.DBSession.query(ResourceType).filter(
        ResourceType.type_id == type_id,
        ResourceType.ref_key == resource_type,
        ResourceType.node_id == node_id,
        ResourceType.link_id == link_id,
        ResourceType.group_id == group_id).one()

    db.DBSession.delete(resourcetype)
    db.DBSession.flush()

    return 'OK'


@required_perms('get_network')
def validate_attr(resource_attr_id, scenario_id, template_id=None, **kwargs):
    """
        Check that a resource attribute satisfies the requirements of all the types of the
        resource.
    """
    rs = db.DBSession.query(ResourceScenario).\
                        filter(ResourceScenario.resource_attr_id == resource_attr_id,
                               ResourceScenario.scenario_id == scenario_id).options(
                                   joinedload(ResourceScenario.resourceattr)).options(
                                       joinedload(ResourceScenario.dataset)
                                   ).one()

    error = None

    try:
        validate_resourcescenario(rs, template_id)
    except HydraError as e:

        error = JSONObject(dict(
            ref_key=rs.resourceattr.ref_key,
            ref_id=rs.resourceattr.get_resource_id(),
            ref_name=rs.resourceattr.get_resource().get_name(),
            resource_attr_id=rs.resource_attr_id,
            attr_id=rs.resourceattr.attr.id,
            attr_name=rs.resourceattr.attr.name,
            dataset_id=rs.dataset_id,
            scenario_id=scenario_id,
            template_id=template_id,
            error_text=e.args[0]))
    return error

@required_perms('get_network')
def validate_attrs(resource_attr_ids, scenario_id, template_id=None, **kwargs):
    """
        Check that multiple resource attribute satisfy the requirements of the types of resources to
        which the they are attached.
    """
    multi_rs = db.DBSession.query(ResourceScenario).\
                            filter(ResourceScenario.resource_attr_id.in_(resource_attr_ids),\
                                   ResourceScenario.scenario_id == scenario_id).\
                                   options(joinedload(ResourceScenario.resourceattr)).\
                                   options(joinedload(ResourceScenario.dataset)).all()

    errors = []
    for rs in multi_rs:
        try:
            validate_resourcescenario(rs, template_id)
        except HydraError as e:

            error = dict(
                ref_key=rs.resourceattr.ref_key,
                ref_id=rs.resourceattr.get_resource_id(),
                ref_name=rs.resourceattr.get_resource().get_name(),
                resource_attr_id=rs.resource_attr_id,
                attr_id=rs.resourceattr.attr.id,
                attr_name=rs.resourceattr.attr.name,
                dataset_id=rs.dataset_id,
                scenario_id=scenario_id,
                template_id=template_id,
                error_text=e.args[0])

            errors.append(error)

    return errors

@required_perms('get_network')
def validate_scenario(scenario_id, template_id=None, **kwargs):
    """
        Check that the requirements of the types of resources in a scenario are
        correct, based on the templates in a network. If a template is specified,
        only that template will be checked.
    """
    scenario_rs = db.DBSession.query(ResourceScenario).filter(
                ResourceScenario.scenario_id==scenario_id)\
                .options(joinedload(ResourceScenario.resourceattr))\
                .options(joinedload(ResourceScenario.dataset)).all()

    errors = []
    for rs in scenario_rs:
        try:
            validate_resourcescenario(rs, template_id)
        except HydraError as e:

            error = dict(
                ref_key=rs.resourceattr.ref_key,
                ref_id=rs.resourceattr.get_resource_id(),
                ref_name=rs.resourceattr.get_resource().get_name(),
                resource_attr_id=rs.resource_attr_id,
                attr_id=rs.resourceattr.attr.id,
                attr_name=rs.resourceattr.attr.name,
                dataset_id=rs.dataset_id,
                scenario_id=scenario_id,
                template_id=template_id,
                error_text=e.args[0])
            errors.append(error)

    return errors


def validate_resourcescenario(resourcescenario, template_id=None, **kwargs):
    """
        Perform a check to ensure a resource scenario's datasets are correct given what the
        definition of that resource (its type) specifies.
    """
    res = resourcescenario.resourceattr.get_resource()

    types = res.types

    dataset = resourcescenario.dataset

    if len(types) == 0:
        return

    if template_id is not None:
        if template_id not in [r.get_templatetype().template_id for r in res.types]:
            raise HydraError("Template %s is not used for resource attribute %s in scenario %s"%\
                             (template_id, resourcescenario.resourceattr.attr.name,
                              resourcescenario.scenario.name))

    #Validate against all the types for the resource
    for resourcetype in types:
        #If a specific type has been specified, then only validate
        #against that type and ignore all the others
        if template_id is not None:
            if resourcetype.get_templatetype().template_id != template_id:
                continue
        #Identify the template types for the template
        tmpltype = resourcetype.get_templatetype()
        for ta in tmpltype.typeattrs:
            #If we find a template type which mactches the current attribute.
            #we can do some validation.
            if ta.attr_id == resourcescenario.resourceattr.attr_id:
                if ta.data_restriction:
                    log.debug("Validating against %s", ta.data_restriction)
                    validation_dict = json.loads(ta.data_restriction)
                    dataset_util.validate_value(validation_dict, dataset.get_val())

@required_perms('get_network')
def validate_network(network_id, template_id, scenario_id=None, **kwargs):
    """
        Given a network, scenario and template, ensure that all the nodes, links & groups
        in the network have the correct resource attributes as defined by
        the types in the template.
        Also ensure valid entries in tresourcetype.
        This validation will not fail if a resource has more than the required type,
        but will fail if it has fewer or if any attribute has a
        conflicting dimension or unit.
    """

    network = db.DBSession.query(Network).filter(
        Network.id == network_id).options(noload(Network.scenarios)).first()

    if network is None:
        raise HydraError("Could not find network %s"%(network_id))

    resource_scenario_dict = {}
    if scenario_id is not None:
        scenario = db.DBSession.query(Scenario).filter(Scenario.id == scenario_id).first()

        if scenario is None:
            raise HydraError("Could not find scenario %s"%(scenario_id,))

        for rs in scenario.resourcescenarios:
            resource_scenario_dict[rs.resource_attr_id] = rs

    template = db.DBSession.query(Template).filter(
        Template.id == template_id).first()

    if template is None:
        raise HydraError("Could not find template %s"%(template_id,))

    resource_type_defs = {
        'NETWORK' : {},
        'NODE'    : {},
        'LINK'    : {},
        'GROUP'   : {},
    }
    for tt in template.get_types():
        resource_type_defs[tt.resource_type][tt.id] = tt

    errors = []
    #Only check if there are type definitions for a network in the template.
    if resource_type_defs.get('NETWORK'):
        net_types = resource_type_defs['NETWORK']
        errors.extend(validate_resource(network, net_types, resource_scenario_dict))

    #check all nodes
    if resource_type_defs.get('NODE'):
        node_types = resource_type_defs['NODE']
        for node in network.nodes:
            errors.extend(validate_resource(node, node_types, resource_scenario_dict))

    #check all links
    if resource_type_defs.get('LINK'):
        link_types = resource_type_defs['LINK']
        for link in network.links:
            errors.extend(validate_resource(link, link_types, resource_scenario_dict))

    #check all groups
    if resource_type_defs.get('GROUP'):
        group_types = resource_type_defs['GROUP']
        for group in network.resourcegroups:
            errors.extend(validate_resource(group, group_types, resource_scenario_dict))

    return errors

def validate_resource(resource, tmpl_types, resource_scenarios=[], **kwargs):
    errors = []
    resource_type = None

    #No validation required if the link has no type.
    if len(resource.types) == 0:
        return []

    for rt in resource.types:
        if tmpl_types.get(rt.type_id) is not None:
            resource_type = tmpl_types[rt.type_id]
            break
    else:
        errors.append("Type %s not found on %s %s"%
                      (tmpl_types, resource_type, resource.get_name()))

    ta_dict = {}
    for ta in resource_type.typeattrs:
        ta_dict[ta.attr_id] = ta

    #Make sure the resource has all the attributes specified in the tempalte
    #by checking whether the template attributes are a subset of the resource
    #attributes.
    type_attrs = set([ta.attr_id for ta in resource_type.typeattrs])

    resource_attrs = set([ra.attr_id for ra in resource.attributes])

    if not type_attrs.issubset(resource_attrs):
        for ta in type_attrs.difference(resource_attrs):
            attr = db.DBSession.query(Attr).filter(Attr.id==ta).one()
            errors.append("Resource %s does not have attribute %s"%
                          (resource.get_name(), attr.name))

    resource_attr_ids = set([ra.id for ra in resource.attributes])
    #if data is included, check to make sure each dataset conforms
    #to the boundaries specified in the template: i.e. that it has
    #the correct dimension and (if specified) unit.
    if len(resource_scenarios) > 0:
        for ra_id in resource_attr_ids:
            rs = resource_scenarios.get(ra_id)
            if rs is None:
                continue
            attr_name = rs.resourceattr.attr.name
            rs_unit_id = rs.dataset.unit_id

            rs_dimension_id = units.get_dimension_by_unit_id(rs_unit_id,
                                                             do_accept_unit_id_none=True).id


            attr = db.DBSession.query(Attr).filter(Attr.id==rs.resourceattr.attr_id).one()
            type_dimension_id = attr.dimension_id
            type_unit_id = ta_dict[rs.resourceattr.attr_id].unit_id

            if rs_dimension_id != type_dimension_id:
                errors.append("Dimension mismatch on %s %s, attribute %s: "
                              "%s on attribute, %s on type"%
                              (resource.ref_key, resource.get_name(), attr_name,
                               rs_dimension_id, type_dimension_id))

            if type_unit_id is not None:
                if rs_unit_id != type_unit_id:
                    errors.append("Unit mismatch on attribute %s. "
                                  "%s on attribute, %s on type"%
                                  (attr_name, rs_unit_id, type_unit_id))
    if len(errors) > 0:
        log.warning(errors)

    return errors
