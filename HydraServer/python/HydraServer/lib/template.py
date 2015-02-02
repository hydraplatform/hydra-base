#(c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
from HydraServer.db import DBSession
from HydraServer.db.model import Template, TemplateType, TypeAttr, Attr, Network, Node, Link, ResourceGroup, ResourceType, ResourceAttr, ResourceScenario, Scenario
from data import add_dataset

from HydraLib.HydraException import HydraError, ResourceNotFoundError
from HydraLib import config, util
from lxml import etree
from decimal import Decimal
import logging
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import joinedload_all, noload
from sqlalchemy import or_, and_
import re
import units
log = logging.getLogger(__name__)

def _check_dimension(typeattr, unit=None):
    """
        Check that the unit and dimension on a type attribute match.
        Alternatively, pass in a unit manually to check against the dimension
        of the type attribute
    """
    if unit is None:
        unit = typeattr.unit
 
    dimension = typeattr.get_attr().attr_dimen

    if unit is not None and dimension is not None:
        unit_dimen = units.get_dimension(unit)

        if unit_dimen.lower() != dimension.lower():
            raise HydraError("Unit %s has dimension %s, but attribute has dimension %s"%
                            (unit, unit_dimen, dimension))

def get_types_by_attr(resource, template_id=None):
    """
        Using the attributes of the resource, get all the
        types that this resource matches.
        @returns a dictionary, keyed on the template name, with the
        value being the list of type names which match the resources
        attributes.
    """

    resource_type_templates = []

    #Create a list of all of this resources attributes.
    attr_ids = []
    for attr in resource.attributes:
        attr_ids.append(attr.attr_id)
    all_resource_attr_ids = set(attr_ids)

    all_types = DBSession.query(TemplateType).options(joinedload_all('typeattrs')).filter(TemplateType.resource_type==resource.ref_key)
    if template_id is not None:
        all_types = all_types.filter(TemplateType.template_id==template_id)

    all_types = all_types.all()

    #tmpl type attrs must be a subset of the resource's attrs
    for ttype in all_types:
        type_attr_ids = []
        for typeattr in ttype.typeattrs:
            type_attr_ids.append(typeattr.attr_id)
        if set(type_attr_ids).issubset(all_resource_attr_ids):
            resource_type_templates.append(ttype)

    return resource_type_templates

def _get_attr_by_name_and_dimension(name, dimension):
    """
        Search for an attribute with the given name and dimension.
        If such an attribute does not exist, create one.
    """
    
    attr = DBSession.query(Attr).filter(Attr.attr_name==name, Attr.attr_dimen==dimension).first()

    if attr is None: 
        attr         = Attr()
        attr.attr_dimen = dimension
        attr.attr_name  = name

        log.info("Attribute not found, creating new attribute: name:%s, dimen:%s",
                    attr.attr_name, attr.attr_dimen)

        DBSession.add(attr)

    return attr

def parse_attribute(attribute):

    if attribute.find('dimension') is not None:
        dimension = attribute.find('dimension').text
    elif attribute.find('unit') is not None:
        dimension = units.get_dimension(attribute.find('unit').text)

    name      = attribute.find('name').text
    
    attr = _get_attr_by_name_and_dimension(name, dimension)

    DBSession.flush()

    return attr

def parse_typeattr(type_i, attribute):

    attr = parse_attribute(attribute)

    for ta in type_i.typeattrs:
        if ta.attr_id == attr.attr_id:
           typeattr_i = ta
           break
    else:
        typeattr_i = TypeAttr()
        log.debug("Creating type attr: type_id=%s, attr_id=%s", type_i.type_id, attr.attr_id)
        typeattr_i.type_id=type_i.type_id
        typeattr_i.attr_id=attr.attr_id
        type_i.typeattrs.append(typeattr_i)
        DBSession.add(typeattr_i)

    unit = None
    if attribute.find('unit') is not None:
        unit = attribute.find('unit').text

    if unit is not None:
        typeattr_i.unit     = unit

    _check_dimension(typeattr_i)

    if attribute.find('is_var') is not None:
        typeattr_i.attr_is_var = attribute.find('is_var').text

    if attribute.find('data_type') is not None:
        typeattr_i.data_type = attribute.find('data_type').text

    if attribute.find('default') is not None:
        default = attribute.find('default')
        unit = default.find('unit').text

        if unit is None and typeattr_i.unit is not None:
            unit = typeattr_i.unit

        dimension = None
        if unit is not None:
            _check_dimension(typeattr_i, unit)
            dimension = units.get_dimension(unit)

        if unit is not None and typeattr_i.unit is not None:
            if unit != typeattr_i.unit:
                raise HydraError("Default value has a unit of %s but the attribute"
                             " says the unit should be: %s"%(typeattr_i.unit, unit))

        val  = default.find('value').text
        try:
            Decimal(val)
            data_type = 'scalar'
        except:
            data_type = 'descriptor'

        dataset = add_dataset(data_type,
                               val,
                               unit,
                               dimension,
                               name="%s Default"%attr.attr_name,)
        typeattr_i.default_dataset_id = dataset.dataset_id
   
    if attribute.find('restrictions') is not None:
        typeattr_i.data_restriction = str(util.get_restriction_as_dict(attribute.find('restrictions')))
    else:
        typeattr_i.data_restriction = None

    return typeattr_i

def upload_template_xml(template_xml,**kwargs):
    """
        Add the template, type and typeattrs described
        in an XML file.

        Delete type, typeattr entries in the DB that are not in the XML file
        The assumption is that they have been deleted and are no longer required.
    """

    template_xsd_path = config.get('templates', 'template_xsd_path')
    xmlschema_doc = etree.parse(template_xsd_path)

    xmlschema = etree.XMLSchema(xmlschema_doc)

    xml_tree = etree.fromstring(template_xml)

    xmlschema.assertValid(xml_tree)

    template_name = xml_tree.find('template_name').text

    template_layout = None
    if xml_tree.find('layout') is not None and \
               xml_tree.find('layout').text is not None:
        layout = xml_tree.find('layout')
        layout_string = get_layout_as_dict(layout)
        template_layout = str(layout_string)

    try:
        tmpl_i = DBSession.query(Template).filter(Template.template_name==template_name).options(joinedload_all('templatetypes.typeattrs.attr')).one()
        tmpl_i.layout = template_layout
        log.info("Existing template found. name=%s", template_name)
    except NoResultFound:
        log.info("Template not found. Creating new one. name=%s", template_name)
        tmpl_i = Template(template_name=template_name, layout=template_layout)
        DBSession.add(tmpl_i)

    types = xml_tree.find('resources')
    #Delete any types which are in the DB but no longer in the XML file
    type_name_map = {r.type_name:r.type_id for r in tmpl_i.templatetypes}
    attr_name_map = {}
    for type_i in tmpl_i.templatetypes:
        for attr in type_i.typeattrs:
            attr_name_map[attr.attr.attr_name] = (attr.attr_id, attr.type_id)

    existing_types = set([r.type_name for r in tmpl_i.templatetypes])

    new_types = set([r.find('name').text for r in types.findall('resource')])

    types_to_delete = existing_types - new_types

    for type_to_delete in types_to_delete:
        type_id = type_name_map[type_to_delete]
        try:
            type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()
            log.info("Deleting type %s", type_i.type_name)
            DBSession.delete(type_i)
        except NoResultFound:
            pass

    #Add or update types.
    for resource in types.findall('resource'):
        type_name = resource.find('name').text
        #check if the type is already in the DB. If not, create a new one.
        type_is_new = False
        if type_name in existing_types:
            type_id = type_name_map[type_name]
            type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).options(joinedload_all('typeattrs.attr')).one()
            
        else:
            log.info("Type %s not found, creating new one.", type_name)
            type_i = TemplateType()
            type_i.type_name = type_name 
            tmpl_i.templatetypes.append(type_i)
            type_is_new = True

        if resource.find('alias') is not None:
            type_i.alias = resource.find('alias').text

        if resource.find('type') is not None:
            type_i.resource_type = resource.find('type').text

        if resource.find('layout') is not None and \
            resource.find('layout').text is not None:
            layout = resource.find('layout')
            layout_string = get_layout_as_dict(layout)
            type_i.layout = str(layout_string)
       
        if resource.find('type') is not None and \
           resource.find('type').text is not None:
            type_i.resource_type = resource.find('type').text
        #delete any TypeAttrs which are in the DB but not in the XML file
        existing_attrs = []
        if not type_is_new:
            for r in tmpl_i.templatetypes:
                if r.type_name == type_name:
                    for typeattr in r.typeattrs:
                        existing_attrs.append(typeattr.attr.attr_name)

        existing_attrs = set(existing_attrs)

        template_attrs = set([r.find('name').text for r in resource.findall('attribute')])

        attrs_to_delete = existing_attrs - template_attrs
        for attr_to_delete in attrs_to_delete:
            attr_id, type_id = attr_name_map[attr_to_delete]
            try:
                attr_i = DBSession.query(TypeAttr).filter(TypeAttr.attr_id==attr_id, TypeAttr.type_id==type_id).options(joinedload_all('attr')).one()
                DBSession.delete(attr_i)
                log.info("Attr %s in type %s deleted",attr_i.attr.attr_name, attr_i.templatetype.type_name)
            except NoResultFound:
                log.debug("Attr %s not found in type %s"%(attr_id, type_id))
                continue

        #Add or update type typeattrs
        for attribute in resource.findall('attribute'):
            parse_typeattr(type_i, attribute)

    DBSession.flush()

    return tmpl_i

def apply_template_to_network(template_id, network_id, **kwargs):
    """
        For each node and link in a network, check whether it matches
        a type in a given template. If so, assign the type to the node / link.
    """

    net_i = DBSession.query(Network).filter(Network.network_id==network_id).one()
    #There should only ever be one matching type, but if there are more,
    #all we can do is pick the first one.
    try: 
        network_type_id = DBSession.query(TemplateType.type_id).filter(TemplateType.template_id==template_id,
                                                                       TemplateType.resource_type=='NETWORK').one()
        assign_type_to_resource(network_type_id.type_id, 'NETWORK', network_id,**kwargs)
    except NoResultFound:
        log.info("No network type to set.")
        pass

    for node_i in net_i.nodes:
        templates = get_types_by_attr(node_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].type_id, 'NODE', node_i.node_id,**kwargs)
    for link_i in net_i.links:
        templates = get_types_by_attr(link_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].type_id, 'LINK', link_i.link_id,**kwargs)

    for group_i in net_i.resourcegroups:
        templates = get_types_by_attr(group_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].type_id, 'GROUP', group_i.group_id,**kwargs)


def remove_template_from_network(network_id, template_id, remove_attrs, **kwargs):
    """
        Remove all resource types in a network relating to the specified
        template.
        remove_attrs
            Flag to indicate whether the attributes associated with the template
            types should be removed from the resources in the network. These will
            only be removed if they are not shared with another template on the network
    """

    try:
        network = DBSession.query(Network).filter(Network.network_id==network_id).one()
    except NoResultFound:
        raise HydraError("Network %s not found"%network_id)

    try:
        template = DBSession.query(Template).filter(Template.template_id==template_id).one()
    except NoResultFound:
        raise HydraError("Template %s not found"%template_id)

    type_ids = [tmpltype.type_id for tmpltype in template.templatetypes] 
    
    node_ids = [n.node_id for n in network.nodes]
    link_ids = [l.link_id for l in network.links]
    group_ids = [g.group_id for g in network.resourcegroups]

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
            DBSession.delete(ra)

    resource_types = DBSession.query(ResourceType).filter(
        and_(or_(
            ResourceType.network_id==network_id,
            ResourceType.node_id.in_(node_ids),
            ResourceType.link_id.in_(link_ids),
            ResourceType.group_id.in_(group_ids),
        ), ResourceType.type_id.in_(type_ids))).all()
    
    for resource_type in resource_types:
        DBSession.delete(resource_type)


def _get_resources_to_remove(resource, template):
    """
        Given a resource and a template being removed, identify the resource attribtes
        which can be removed.
    """
    type_ids = [tmpltype.type_id for tmpltype in template.templatetypes] 

    node_attr_ids = dict([(a.attr_id, a) for a in resource.attributes])
    attrs_to_remove = []
    attrs_to_keep   = []
    for nt in resource.types:
        if nt.templatetype.type_id in type_ids:
            for ta in nt.templatetype.typeattrs:
                if node_attr_ids.get(ta.attr_id):
                    attrs_to_remove.append(node_attr_ids[ta.attr_id])
        else:
            for ta in nt.templatetype.typeattrs:
                if node_attr_ids.get(ta.attr_id):
                    attrs_to_keep.append(node_attr_ids[ta.attr_id])
    #remove any of the attributes marked for deletion as they are
    #marked for keeping based on being in another type.
    final_attrs_to_remove = set(attrs_to_remove) - set(attrs_to_keep)

    return list(final_attrs_to_remove)

def get_matching_resource_types(resource_type, resource_id,**kwargs):
    """
        Get the possible types of a resource by checking its attributes
        against all available types.

        @returns A list of TypeSummary objects.
    """
    resource_i = None
    if resource_type == 'NETWORK':
        resource_i = DBSession.query(Network).filter(Network.network_id==resource_id).one()
    elif resource_type == 'NODE':
        resource_i = DBSession.query(Node).filter(Node.node_id==resource_id).one()
    elif resource_type == 'LINK':
        resource_i = DBSession.query(Link).filter(Link.link_id==resource_id).one()
    elif resource_type == 'GROUP':
        resource_i = DBSession.query(ResourceGroup).filter(ResourceGroup.resourcegroup_id==resource_id).one()

    matching_types = get_types_by_attr(resource_i)
    return matching_types

def assign_types_to_resources(resource_types,**kwargs):
    """
        Assign new types to list of resources.
        This function checks if the necessary
        attributes are present and adds them if needed. Non existing attributes
        are also added when the type is already assigned. This means that this
        function can also be used to update resources, when a resource type has
        changed.
    """
    #Remove duplicate values from types by turning it into a set
    type_ids = list(set([rt.type_id for rt in resource_types]))
    
    db_types = DBSession.query(TemplateType).filter(TemplateType.type_id.in_(type_ids)).options(joinedload_all('typeattrs')).all()

    types = {}
    for db_type in db_types:
        if types.get(db_type.type_id) is None:
            types[db_type.type_id] = db_type
    log.info("Retrieved all the appropriate template types")
    res_types = []
    res_attrs = []

    net_id = None
    node_ids = []
    link_ids = []
    grp_ids  = []
    for resource_type in resource_types:
        ref_id  = resource_type.ref_id
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
        net = DBSession.query(Network).filter(Network.network_id==net_id).one()
    nodes = _get_nodes(node_ids)
    links = _get_links(link_ids)
    groups = _get_groups(grp_ids)
    for resource_type in resource_types:
        ref_id  = resource_type.ref_id
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

        ra, rt = set_resource_type(resource, type_id, types)
        if rt is not None:
            res_types.append(rt)
        if len(ra) > 0:
            res_attrs.extend(ra)
    log.info("Retrieved all the appropriate resources")
    if len(res_types) > 0:
        DBSession.execute(ResourceType.__table__.insert(), res_types)
    if len(res_attrs) > 0:
        DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)

    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete. 
    DBSession.query(Attr).filter(Attr.attr_id==None).delete()

    ret_val = [t for t in types.values()]
    return ret_val

def _get_links(link_ids):
    links = []

    if len(link_ids) == 0:
        return links

    if len(link_ids) > 500:
        idx = 0
        extent = 500
        while idx < len(link_ids):
            log.info("Querying %s links", len(link_ids[idx:extent]))
            rs = DBSession.query(Link).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Link.link_id.in_(link_ids[idx:extent])).all()
            log.info("Retrieved %s links", len(rs))
            links.extend(rs)
            idx = idx + 500
            
            if idx + 500 > len(link_ids):
                extent = len(link_ids)
            else:
                extent = extent + 500
    else:
        links = DBSession.query(Link).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Link.link_id.in_(link_ids)).all()
    
    link_dict = {}

    for l in links:
        l.ref_id = l.link_id
        l.ref_key = 'LINK'
        link_dict[l.link_id] = l
    
    return link_dict

def _get_nodes(node_ids):
    nodes = []
    
    if len(node_ids) == 0:
        return nodes
    
    if len(node_ids) > 500:
        idx = 0
        extent = 500
        while idx < len(node_ids):
            log.info("Querying %s nodes", len(node_ids[idx:extent]))

            rs = DBSession.query(Node).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Node.node_id.in_(node_ids[idx:extent])).all()

            log.info("Retrieved %s nodes", len(rs))
            
            nodes.extend(rs)
            idx = idx + 500
            
            if idx + 500 > len(node_ids):
                extent = len(node_ids)
            else:
                extent = extent + 500
    else:
        nodes = DBSession.query(Node).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Node.node_id.in_(node_ids)).all()

    node_dict = {}

    for n in nodes:
        n.ref_id = n.node_id
        n.ref_key = 'NODE'
        node_dict[n.node_id] = n

    return node_dict

def _get_groups(group_ids):
    groups = []

    if len(group_ids) == 0:
        return groups

    if len(group_ids) > 500:
        idx = 0
        extent = 500
        while idx < len(group_ids):
            log.info("Querying %s groups", len(group_ids[idx:extent]))
            rs = DBSession.query(ResourceGroup).options(joinedload_all('attributes')).filter(ResourceGroup.group_id.in_(group_ids[idx:extent])).all()
            log.info("Retrieved %s groups", len(rs))
            groups.extend(rs)
            idx = idx + 500
            
            if idx + 500 > len(group_ids):
                extent = len(group_ids)
            else:
                extent = extent + 500
    else:
        groups = DBSession.query(ResourceGroup).options(joinedload_all('types')).options(joinedload_all('attributes')).filter(ResourceGroup.group_id.in_(group_ids))
    group_dict = {}

    for g in groups:
        g.ref_id = g.group_id
        g.ref_key = 'GROUP'
        group_dict[g.group_id] = g

    return group_dict

def assign_type_to_resource(type_id, resource_type, resource_id,**kwargs):
    """Assign new type to a resource. This function checks if the necessary
    attributes are present and adds them if needed. Non existing attributes
    are also added when the type is already assigned. This means that this
    function can also be used to update resources, when a resource type has
    changed.
    """
    if resource_type == 'NETWORK':
        resource = DBSession.query(Network).filter(Network.network_id==resource_id).one()
    elif resource_type == 'NODE':
        resource = DBSession.query(Node).filter(Node.node_id==resource_id).one()
    elif resource_type == 'LINK':
        resource = DBSession.query(Link).filter(Link.link_id==resource_id).one()
    elif resource_type == 'GROUP':
        resource = DBSession.query(ResourceGroup).filter(ResourceGroup.group_id==resource_id).one()
    res_attrs, res_type = set_resource_type(resource, type_id, **kwargs)

    type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()
    if resource_type != type_i.resource_type:
        raise HydraError("Cannot assign a %s type to a %s"%
                         (type_i.resource_type,resource_type))
    
    if res_type is not None:
        DBSession.execute(ResourceType.__table__.insert(), [res_type])

    if len(res_attrs) > 0:
        DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)

    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete. 
    DBSession.query(Attr).filter(Attr.attr_id==None).delete()

    return DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()

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

    ref_key = resource.ref_key

    existing_attr_ids = []
    for attr in resource.attributes:
        existing_attr_ids.append(attr.attr_id)

    if type_id in types.keys():
        type_i = types[type_id]
    else:
        type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).options(joinedload_all('typeattrs')).one()

    type_attrs = dict()
    for typeattr in type_i.typeattrs:
        type_attrs.update({typeattr.attr_id:
                           typeattr.attr_is_var})

    # check if attributes exist
    missing_attr_ids = set(type_attrs.keys()) - set(existing_attr_ids)

    # add attributes if necessary
    new_res_attrs = []
    for attr_id in missing_attr_ids:
        ra_dict = dict(
            ref_key = ref_key,
            attr_id = attr_id,
            attr_is_var = type_attrs[attr_id],
            node_id    = resource.node_id    if ref_key == 'NODE' else None,
            link_id    = resource.link_id    if ref_key == 'LINK' else None,
            group_id   = resource.group_id   if ref_key == 'GROUP' else None,
            network_id = resource.network_id if ref_key == 'NETWORK' else None,

        )
        new_res_attrs.append(ra_dict)
    resource_type = None
    for rt in resource.types:
        if rt.type_id == type_i.type_id:
            break
    else:
        # add type to tResourceType if it doesn't exist already
        resource_type = dict(
            node_id    = resource.node_id    if ref_key == 'NODE' else None,
            link_id    = resource.link_id    if ref_key == 'LINK' else None,
            group_id   = resource.group_id   if ref_key == 'GROUP' else None,
            network_id = resource.network_id if ref_key == 'NETWORK' else None,
            ref_key    = ref_key,
            type_id    = type_id,
        )

    return new_res_attrs, resource_type

def remove_type_from_resource( type_id, resource_type, resource_id,**kwargs): 
    """ 
        Remove a resource type trom a resource 
    """ 
    node_id = resource_id if resource_type == 'NODE' else None
    link_id = resource_id if resource_type == 'LINK' else None
    group_id = resource_id if resource_type == 'GROUP' else None

    resourcetype = DBSession.query(ResourceType).filter(
                                        ResourceType.type_id==type_id,
                                        ResourceType.ref_key==resource_type,
                                        ResourceType.node_id == node_id,
    ResourceType.link_id == link_id,
    ResourceType.group_id == group_id).one() 
    DBSession.delete(resourcetype) 

def _parse_data_restriction(restriction_dict):
#    {{soap_server.hydra_complexmodels}LESSTHAN}

    if restriction_dict is None or restriction_dict == '':
        return None

    dict_str = re.sub('{[a-zA-Z\.\_]*}', '', str(restriction_dict))

    new_dict = eval(dict_str)

    ret_dict = {}
    for k, v in new_dict.items():
        if len(v) == 1:
            ret_dict[k] = v[0]
        else:
            ret_dict[k] = v

    return str(ret_dict)

def add_template(template,**kwargs):
    """
        Add template and a type and typeattrs.
    """
    tmpl = Template()
    tmpl.template_name = template.name
    tmpl.layout        = str(template.layout)

    DBSession.add(tmpl)

    if template.types is not None:
        for templatetype in template.types:
            ttype = _update_templatetype(templatetype)
            tmpl.templatetypes.append(ttype)

    DBSession.flush()
    return tmpl

def update_template(template,**kwargs):
    """
        Update template and a type and typeattrs.
    """
    tmpl = DBSession.query(Template).filter(Template.template_id==template.id).one()
    tmpl.template_name = template.name
    tmpl.layout        = str(template.layout)
    if template.types is not None:
        for templatetype in template.types:
            if templatetype.id is not None:
                for type_i in tmpl.templatetypes:
                    if type_i.type_id == templatetype.id:
                        _update_templatetype(templatetype, type_i)
                        break
            else:
                _update_templatetype(templatetype)

    DBSession.flush()
 
    return tmpl

def delete_template(template_id,**kwargs):
    """
        Add template and a type and typeattrs.
    """
    try:
        tmpl = DBSession.query(Template).filter(Template.template_id==template_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Template %s not found"%(template_id,))
    DBSession.delete(tmpl)
    return tmpl

def get_templates(**kwargs):
    """
        Get all resource template templates.
    """

    templates = DBSession.query(Template).options(joinedload_all('templatetypes.typeattrs')).all()

    return templates 

def remove_attr_from_type(type_id, attr_id,**kwargs):
    """

        Remove an attribute from a type
    """
    typeattr_i = DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_id,
                                                  TypeAttr.attr_id==attr_id).one()
    DBSession.delete(typeattr_i)

def get_template(template_id,**kwargs):
    """
        Get a specific resource template template, either by ID or name.
    """

    tmpl = DBSession.query(Template).filter(Template.template_id==template_id).one()

    return tmpl

def get_template_by_name(name,**kwargs):
    """
        Get a specific resource template, either by ID or name.
    """
    try:
        tmpl = DBSession.query(Template).filter(Template.template_name == name).one()
        return tmpl
    except NoResultFound:
        log.info("%s is not a valid identifier for a template",name)
        return None
 
def add_templatetype(templatetype,**kwargs):
    """
        Add a template type with typeattrs.
    """

    type_i = _update_templatetype(templatetype)

    DBSession.flush()

    return type_i

def update_templatetype(templatetype,**kwargs):
    """
        Update a resource type and its typeattrs.
        New typeattrs will be added. typeattrs not sent will be ignored.
        To delete typeattrs, call delete_typeattr
    """
    tmpltype = DBSession.query(TemplateType).filter(TemplateType.type_id == templatetype.id).one()

    _update_templatetype(templatetype, tmpltype)

    DBSession.flush()

    return tmpltype

def _add_typeattr(typeattr, existing_ta = None):
    """
        Add or updsate a type attribute.
        If an existing type attribute is provided, then update.

        Checks are performed to ensure that the dimension provided on the
        type attr (not updateable) is the same as that on the referring attribute.
        The unit provided (stored on tattr) must conform to the dimension stored
        on the referring attribute (stored on tattr).

        This is done so that multiple tempaltes can all use the same attribute,
        but specify different units.

        If no attr_id is provided, but an attr_name and dimension are provided,
        then a new attribute can be created (or retrived) and used. I.e., no
        attribute ID must be specified if attr_name and dimension are specified.

        ***WARNING***
        Setting attribute ID to null means a new type attribute (and even a new attr)
        may be added, None are removed or replaced. To remove other type attrs, do it
        manually using delete_typeattr
    """
    if existing_ta is None:
        ta = TypeAttr(attr_id=typeattr.attr_id)
    else:
        ta = existing_ta

    ta.unit = typeattr.unit
    ta.type_id = typeattr.type_id
    ta.data_type = typeattr.data_type
    ta.default_dataset_id = typeattr.default_dataset_id
    ta.attr_is_var        = typeattr.is_var
    ta.data_restriction = _parse_data_restriction(typeattr.data_restriction)

    if typeattr.dimension is not None and typeattr.attr_id is not None:
        attr = ta.get_attr()
        if attr.attr_dimen != typeattr.dimension:
            raise HydraError("Cannot set a dimension on type attribute which "
                            "does not match its attribute. Create a new attribute if "
                            "you want to use attribute %s with dimension %s"%
                            (attr.attr_name, typeattr.dimension))
    elif typeattr.dimension is not None and typeattr.attr_id is None and typeattr.attr_name is not None:
        attr = _get_attr_by_name_and_dimension(typeattr.attr_name, typeattr.dimension)
        ta.attr_id = attr.attr_id
        ta.attr = attr

    _check_dimension(ta)

    if existing_ta is None:
        DBSession.add(ta)

    return ta

def _update_templatetype(templatetype, existing_tt=None):
    """
        Add or update a templatetype. If an existing template type is passed in,
        update that one. Otherwise search for an existing one. If not found, add.
    """
    if existing_tt is None:
        if templatetype.id is not None:
            tmpltype = DBSession.query(TemplateType).filter(TemplateType.type_id == templatetype.id).one()
        else:
            tmpltype = TemplateType()
    else:
        tmpltype = existing_tt
    
    tmpltype.template_id = templatetype.template_id
    tmpltype.type_name  = templatetype.name
    tmpltype.alias      = templatetype.alias
    tmpltype.layout     = templatetype.layout
    tmpltype.resource_type = templatetype.resource_type

    if templatetype.typeattrs is not None:
        for typeattr in templatetype.typeattrs:
            for typeattr_i in tmpltype.typeattrs:
                if typeattr_i.attr_id == typeattr.attr_id:
                    ta = _add_typeattr(typeattr, typeattr_i)
                    break
            else:

                ta = _add_typeattr(typeattr)
                tmpltype.typeattrs.append(ta)

    if existing_tt is None:
        DBSession.add(tmpltype)

    return tmpltype

def delete_templatetype(type_id,**kwargs):
    """
        Update a resource type and its typeattrs.
        New typeattrs will be added. typeattrs not sent will be ignored.
        To delete typeattrs, call delete_typeattr
    """
    try:
        tmpltype = DBSession.query(TemplateType).filter(TemplateType.type_id == type_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Template Type %s not found"%(type_id,))
    DBSession.delete(tmpltype)
    DBSession.flush()

def get_templatetype(type_id,**kwargs):
    """
        Get a specific resource type by ID.
    """
    templatetype = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()

    return templatetype

def get_templatetype_by_name(template_id, type_name,**kwargs):
    """
        Get a specific resource type by name.
    """

    try:
        templatetype = DBSession.query(TemplateType).filter(TemplateType.template_id==template_id, TemplateType.type_name==type_name).one()
    except NoResultFound:
        raise HydraError("%s is not a valid identifier for a type"%(type_name))

    return templatetype

def add_typeattr(typeattr,**kwargs):
    """
        Add an typeattr to an existing type.
    """
    
    ta = _add_typeattr(typeattr)
    
    DBSession.flush()

    updated_template_type = DBSession.query(TemplateType).filter(TemplateType.type_id==ta.type_id).one()

    return updated_template_type


def delete_typeattr(typeattr,**kwargs):
    """
        Remove an typeattr from an existing type
    """
    ta = DBSession.query(TypeAttr).filter(TypeAttr.type_id == typeattr.type_id,
                                          TypeAttr.attr_id == typeattr.attr_id).one()
    DBSession.delete(ta)

    return 'OK'

def validate_attr(resource_attr_id, scenario_id, type_id=None):
    """
        Check that a resource attribute satisfies the requirements of all the types of the 
        resource.
    """
    try:
        rs = DBSession.query(ResourceScenario).filter(ResourceScenario.resource_attr_id==resource_attr_id, 
            ResourceScenario.scenario_id==scenario_id).options(
            joinedload_all("resourceattr")).options(
            joinedload_all("dataset")
            ).one()

        _do_validate_resourcescenario(rs, type_id)
                    
    except NoResultFound:
        raise ResourceNotFoundError("Resource Scenario %s not found"%resource_attr_id)

def validate_attrs(resource_attr_ids, scenario_id, type_id=None):
    """
        Check that multiple resource attribute satisfy the requirements of the types of resources to
        which the they are attached.
    """
    try:
        multi_rs = DBSession.query(ResourceScenario).filter(ResourceScenario.resource_attr_id.in_(resource_attr_ids), ResourceScenario.scenario_id==scenario_id).options(joinedload_all("resourceattr")).options(joinedload_all("dataset")).all()
        
        for rs in multi_rs:
            _do_validate_resourcescenario(rs, type_id)
                    
    except NoResultFound:
        raise ResourceNotFoundError("Resource Scenarios %s not found"%resource_attr_ids)


def _do_validate_resourcescenario(resourcescenario, type_id=None):
    """
        Perform a check to ensure a resource scenario's datasets are correct given what the
        definition of that resource (its type) specifies.
    """
    res = resourcescenario.resourceattr.get_resource()

    types = res.types

    dataset = resourcescenario.dataset

    if len(types) == 0:
        return

    #Validate against all the types for the resource
    for resourcetype in types:
        #If a specific type has been specified, then only validate
        #against that type and ignore all the others
        if type_id is not None:
            if resourcetype.type_id != type_id:
                continue
        #Identify the template types for the template
        tmpltype = resourcetype.templatetype
        for ta in tmpltype.typeattrs:
            #If we find a template type which mactches the current attribute.
            #we can do some validation.
            if ta.attr_id == resourcescenario.resourceattr.attr_id:
                if ta.data_restriction:
                    validation_dict = eval(ta.data_restriction)
                    util.validate_value(validation_dict, dataset.get_val())

def validate_network(network_id, template_id, scenario_id=None):
    """
        Given a network, scenario and template, ensure that all the nodes, links & groups
        in the network have the correct resource attributes as defined by the types in the template.
        Also ensure valid entries in tresourcetype.
        This validation will not fail if a resource has more than the required type, but will fail if 
        it has fewer or if any attribute has a conflicting dimension or unit.
    """

    network = DBSession.query(Network).filter(Network.network_id==network_id).options(noload('scenarios')).first()

    if network is None:
        raise HydraError("Could not find network %s"%(network_id))

    resource_scenario_dict = {}
    if scenario_id is not None:
        scenario = DBSession.query(Scenario).filter(Scenario.scenario_id==scenario_id).first()

        if scenario is None:
            raise HydraError("Could not find scenario %s"%(scenario_id,))

        for rs in scenario.resourcescenarios:
            resource_scenario_dict[rs.resource_attr_id] = rs

    template = DBSession.query(Template).filter(Template.template_id == template_id).options(joinedload_all('templatetypes')).first()

    if template is None:
        raise HydraError("Could not find template %s"%(template_id,))

    resource_type_defs = {
        'NETWORK' : {},
        'NODE'    : {},
        'LINK'    : {},
        'GROUP'   : {},
    }
    for ta in template.templatetypes:
        resource_type_defs[ta.resource_type][ta.type_id] = ta

    errors = []
    #Only check if there are type definitions for a network in the template.
    if resource_type_defs.get('NETWORK'):
        net_types = resource_type_defs['NETWORK']
        errors.extend(_validate_resource(network, net_types, resource_scenario_dict))

    #check all nodes
    if resource_type_defs.get('NODE'):
        node_types = resource_type_defs['NODE']
        for node in network.nodes:
            errors.extend(_validate_resource(node, node_types, resource_scenario_dict))

    #check all links
    if resource_type_defs.get('LINK'):
        link_types = resource_type_defs['LINK']
        for link in network.links:
            errors.extend(_validate_resource(link, link_types, resource_scenario_dict))

    #check all groups
    if resource_type_defs.get('GROUP'):
        group_types = resource_type_defs['GROUP']
        for group in network.resourcegroups:
            errors.extend(_validate_resource(group, group_types, resource_scenario_dict))

    return errors

def _validate_resource(resource, tmpl_types, resource_scenarios=[]):
    errors = []
    resource_type = None 
    
    type_ids = tmpl_types.keys()
    
    #No validation required if the link has no type.
    if len(resource.types) == 0:
        return []

    for rt in resource.types:
        if rt.type_id in type_ids:
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
            errors.append("Resource %s does not have attribute %s"%
                          (resource.get_name(), ta_dict[ta].attr.attr_name))

    resource_attr_ids = set([ra.resource_attr_id for ra in resource.attributes])
    #if data is included, check to make sure each dataset conforms
    #to the boundaries specified in the template: i.e. that it has
    #the correct dimension and (if specified) unit.
    if len(resource_scenarios) > 0:
        for ra_id in resource_attr_ids:
            rs = resource_scenarios.get(ra_id)
            if rs is None:
                continue
            attr_name = rs.resourceattr.attr.attr_name
            rs_unit = rs.dataset.data_units
            rs_dimension = rs.dataset.data_dimen
            type_dimension = ta_dict[rs.resourceattr.attr_id].attr.attr_dimen
            type_unit = ta_dict[rs.resourceattr.attr_id].unit
            
            if rs_dimension != type_dimension:
                errors.append("Dimension mismatch on %s %s, attribute %s: "
                              "%s on attribute, %s on type"%
                             ( resource.ref_key, resource.get_name(), attr_name,
                              rs_dimension, type_dimension))

            if type_unit is not None:
                if rs_unit != type_unit:
                    errors.append("Unit mismatch on attribute %s. "
                                  "%s on attribute, %s on type"%
                                 (attr_name, rs_unit, type_unit))
    if len(errors) > 0:
        log.warn(errors)

    return errors

def get_network_as_xml_template(network_id,**kwargs):
    """
        Turn an existing network into an xml template
        using its attributes.
        If an optional scenario ID is passed in, default
        values will be populated from that scenario.
    """
    template_xml = etree.Element("template_definition")

    net_i = DBSession.query(Network).filter(Network.network_id==network_id).one()

    template_name = etree.SubElement(template_xml, "template_name")
    template_name.text = "TemplateType from Network %s"%(net_i.network_name)
    layout = _get_layout_as_etree(net_i.network_layout)

    resources = etree.SubElement(template_xml, "resources")
    if net_i.attributes:
        net_resource    = etree.SubElement(resources, "resource")

        resource_type   = etree.SubElement(net_resource, "type")
        resource_type.text   = "NETWORK"

        resource_name   = etree.SubElement(net_resource, "name")
        resource_name.text   = net_i.network_name

        layout = _get_layout_as_etree(net_i.network_layout)
        if layout is not None:
            net_resource.append(layout)

        for net_attr in net_i.attributes:
            _make_attr_element(net_resource, net_attr)

        resources.append(net_resource)

    existing_types = {'NODE': [], 'LINK': [], 'GROUP': []}
    for node_i in net_i.nodes:
        node_attributes = node_i.attributes
        attr_ids = [attr.attr_id for attr in node_attributes]
        if attr_ids>0 and attr_ids not in existing_types['NODE']:

            node_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(node_resource, "type")
            resource_type.text   = "NODE"

            resource_name   = etree.SubElement(node_resource, "name")
            resource_name.text   = node_i.node_name

            layout = _get_layout_as_etree(node_i.node_layout)

            if layout is not None:
                node_resource.append(layout)

            for node_attr in node_attributes:
                _make_attr_element(node_resource, node_attr)

            existing_types['NODE'].append(attr_ids)
            resources.append(node_resource)

    for link_i in net_i.links:
        link_attributes = link_i.attributes
        attr_ids = [attr.attr_id for attr in link_attributes]
        if attr_ids>0 and attr_ids not in existing_types['LINK']:
            link_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(link_resource, "type")
            resource_type.text   = "LINK"

            resource_name   = etree.SubElement(link_resource, "name")
            resource_name.text   = link_i.link_name

            layout = _get_layout_as_etree(link_i.link_layout)

            if layout is not None:
                link_resource.append(layout)

            for link_attr in link_attributes:
                _make_attr_element(link_resource, link_attr)

            existing_types['LINK'].append(attr_ids)
            resources.append(link_resource)

    for group_i in net_i.resourcegroups:
        group_attributes = group_i.attributes
        attr_ids = [attr.attr_id for attr in group_attributes]
        if attr_ids>0 and attr_ids not in existing_types['GROUP']:
            group_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(group_resource, "type")
            resource_type.text   = "GROUP"

            resource_name   = etree.SubElement(group_resource, "name")
            resource_name.text   = group_i.group_name

           # layout = _get_layout_as_etree(group_i.group_layout)

           # if layout is not None:
           #     group_resource.append(layout)

            for group_attr in group_attributes:
                _make_attr_element(group_resource, group_attr)

            existing_types['GROUP'].append(attr_ids)
            resources.append(group_resource)

    xml_string = etree.tostring(template_xml)

    return xml_string

def _make_attr_element(parent, resource_attr_i):
    """
        General function to add an attribute element to a resource element.
    """
    attr = etree.SubElement(parent, "attribute")
    attr_i = resource_attr_i.attr

    attr_name      = etree.SubElement(attr, 'name')
    attr_name.text = attr_i.attr_name

    attr_dimension = etree.SubElement(attr, 'dimension')
    attr_dimension.text = attr_i.attr_dimen

    attr_is_var    = etree.SubElement(attr, 'is_var')
    attr_is_var.text = resource_attr_i.attr_is_var

    # if scenario_id is not None:
    #     for rs in resource_attr_i.get_resource_scenarios():
    #         if rs.scenario_id == scenario_id
    #             attr_default   = etree.SubElement(attr, 'default')
    #             default_val = etree.SubElement(attr_default, 'value')
    #             default_val.text = rs.get_dataset().get_val()
    #             default_unit = etree.SubElement(attr_default, 'unit')
    #             default_unit.text = rs.get_dataset().unit

    return attr

def get_layout_as_dict(layout_tree):
    """
    Convert something that looks like this:
    <layout>
        <item>
            <name>color</name>
            <value>red</value>
        </item>
        <item>
            <name>shapefile</name>
            <value>blah.shp</value>
        </item>
    </layout>
    Into something that looks like this:
    {
        'color' : ['red'],
        'shapefile' : ['blah.shp']
    }
    """
    layout_dict = dict()

    for item in layout_tree.findall('item'):
        name  = item.find('name').text
        val_element = item.find('value')
        value = val_element.text.strip()
        if value == '':
            children = val_element.getchildren()
            value = etree.tostring(children[0], pretty_print=True)
        layout_dict[name] = [value]
    return layout_dict

def _get_layout_as_etree(layout_dict):
    """
    Convert something that looks like this:
    {
        'color' : ['red'],
        'shapefile' : ['blah.shp']
    }

    Into something that looks like this:
    <layout>
        <item>
            <name>color</name>
            <value>red</value>
        </item>
        <item>
            <name>shapefile</name>
            <value>blah.shp</value>
        </item>
    </layout>
    """
    if layout_dict is None:
        return None

    layout = etree.Element("layout")
    layout_dict = eval(layout_dict)
    for k, v in layout_dict.items():
        item = etree.SubElement(layout, "item")
        name = etree.SubElement(item, "name")
        name.text = k
        value = etree.SubElement(item, "value")
        value.text = str(v)

    return layout


