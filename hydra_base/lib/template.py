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

from .. import db
from ..db.model import Template, TemplateType, TypeAttr, Attr, Network, Node, Link, ResourceGroup, ResourceType, ResourceAttr, ResourceScenario, Scenario
from .objects import JSONObject, Dataset
from .data import add_dataset

from ..exceptions import HydraError, ResourceNotFoundError
from ..import config
from ..util import dataset_util, get_layout_as_string, get_layout_as_dict
from lxml import etree
from decimal import Decimal
import logging
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import joinedload_all, noload
from sqlalchemy import or_, and_
import re
from . import units
import json
log = logging.getLogger(__name__)

def _get_attr(attr_id):
    attr = db.DBSession.query(Attr).filter(Attr.id==attr_id).one()
    return JSONObject(attr)


def _check_dimension(typeattr, unit=None):
    """
        Check that the unit and dimension on a type attribute match.
        Alternatively, pass in a unit manually to check against the dimension
        of the type attribute
    """

    if unit is None:
        unit = typeattr.unit

    dimension = _get_attr(typeattr.attr_id).dimension

    if unit is not None and dimension is not None:
        unit_dimen = units.get_unit_dimension(unit)
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
    for res_attr in resource.attributes:
        attr_ids.append(res_attr.attr_id)
    all_resource_attr_ids = set(attr_ids)

    all_types = db.DBSession.query(TemplateType).options(joinedload_all('typeattrs')).filter(TemplateType.resource_type==resource.ref_key)
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

    attr = db.DBSession.query(Attr).filter(Attr.name==name, Attr.dimension==dimension).first()

    if attr is None:
        attr         = Attr()
        attr.dimension = dimension
        attr.name  = name

        log.info("Attribute not found, creating new attribute: name:%s, dimen:%s",
                    attr.name, attr.dimension)

        db.DBSession.add(attr)

    return attr

def parse_attribute(attribute):

    if attribute.find('dimension') is not None:
        dimension = attribute.find('dimension').text
        if dimension is not None:
            dimension = units.get_dimension(dimension.strip())

            if dimension is None:
                raise HydraError("Dimension %s does not exist."%dimension)

    elif attribute.find('unit') is not None:
        if attribute.find('unit').text is not None:
            dimension = units.get_unit_dimension(attribute.find('unit').text)

    if dimension is None or dimension.lower() in ('dimensionless', ''):
        dimension = 'dimensionless'

    name      = attribute.find('name').text.strip()

    attr = _get_attr_by_name_and_dimension(name, dimension)

    db.DBSession.flush()

    return attr

def parse_xml_typeattr(type_i, attribute):

    attr = parse_attribute(attribute)

    for ta in type_i.typeattrs:
        if ta.attr_id == attr.id:
           typeattr_i = ta
           break
    else:
        typeattr_i = TypeAttr()
        log.debug("Creating type attr: type_id=%s, attr_id=%s", type_i.id, attr.id)
        typeattr_i.type_id=type_i.id
        typeattr_i.attr_id=attr.id
        type_i.typeattrs.append(typeattr_i)
        db.DBSession.add(typeattr_i)

    unit = None
    if attribute.find('unit') is not None:
        unit = attribute.find('unit').text

    if unit is not None:
        typeattr_i.unit     = unit

    _check_dimension(typeattr_i)

    if attribute.find('description') is not None:
        typeattr_i.description = attribute.find('description').text

    if attribute.find('properties') is not None:
        properties_string = get_etree_layout_as_dict(attribute.find('properties'))
        typeattr_i.properties = str(properties_string)

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
            dimension = units.get_unit_dimension(unit)

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
                               name="%s Default"%attr.name)
        typeattr_i.default_dataset_id = dataset.id

    if attribute.find('restrictions') is not None:
        typeattr_i.data_restriction = str(dataset_util.get_restriction_as_dict(attribute.find('restrictions')))
    else:
        typeattr_i.data_restriction = None

    return typeattr_i

def parse_json_typeattr(type_i, typeattr_j, attribute_j, default_dataset_j):

    if attribute_j.dimension is not None:
        dimension_j = attribute_j.dimension
        if dimension_j is not None:
            if dimension_j.strip() == '':
                dimension_j = 'dimensionless'

            dimension = units.get_dimension(dimension_j.strip())

            if dimension is None:
                raise HydraError("Dimension '%s' does not exist."%dimension_j)

    elif attribute_j.unit is not None:
            dimension = units.get_unit_dimension(attribute_j.unit)

    if dimension is None or dimension.lower() in ('dimensionless', ''):
        dimension = 'dimensionless'

    name      = attribute_j.name.strip()

    attr_i = _get_attr_by_name_and_dimension(name, dimension)

    #Get an ID for the attribute
    db.DBSession.flush()

    for ta in type_i.typeattrs:
        if ta.attr_id == attr_i.id:
           typeattr_i = ta
           break
    else:
        typeattr_i = TypeAttr()
        log.debug("Creating type attr: type_id=%s, attr_id=%s", type_i.id, attr_i.id)
        typeattr_i.type_id=type_i.id
        typeattr_i.attr_id=attr_i.id
        typeattr_i.attr = attr_i
        type_i.typeattrs.append(typeattr_i)
        db.DBSession.add(typeattr_i)


    unit = None
    if attribute_j.unit is not None:
        typeattr_i.unit = typeattr_j.unit

    _check_dimension(typeattr_i)

    if typeattr_j.description is not None:
        typeattr_i.description = typeattr_j.description

    if typeattr_j.properties is not None:
        if isinstance(typeattr_j.properties, dict):
            typeattr_i.properties = json.dumps(typeattr_j.properties)
        else:
            typeattr_i.properties = typeattr_j.properties

    if typeattr_j.is_var is not None:
        typeattr_i.attr_is_var = typeattr_j.is_var

    if typeattr_j.data_type is not None:
        typeattr_i.data_type = typeattr_j.data_type

    if default_dataset_j is not None:
        default = default_dataset_j

        unit = default.unit

        if unit is None and typeattr_i.unit is not None:
            unit = typeattr_i.unit

        dimension = None
        if unit is not None:
            _check_dimension(typeattr_i, unit)
            dimension = units.get_unit_dimension(unit)

        if unit is not None and typeattr_i.unit is not None:
            if unit != typeattr_i.unit:
                raise HydraError("Default value has a unit of %s but the attribute"
                             " says the unit should be: %s"%(typeattr_i.unit, unit))

        val  = default.value

        data_type = default.type
        name = default.name if default.name is not None else "%s Default"%attr_i.name

        dataset_i = add_dataset(data_type,
                               val,
                               unit,
                               name= name)
        typeattr_i.default_dataset_id = dataset_i.id

    if typeattr_j.restriction is not None or typeattr_j.data_restriction is not None:
        restriction = typeattr_j.restriction if typeattr_j.restriction is not None else typeattr_j.data_restriction
        if isinstance(restriction, dict):
            typeattr_i.data_restriction = json.dumps(restriction)
        else:
            typeattr_i.data_restriction = restriction
    else:
        typeattr_i.data_restriction = None

    return typeattr_i

def get_template_as_json(template_id, **kwargs):
    """
        Get a template (including attribute and dataset definitions) as a JSON
        string. This is just a wrapper around the get_template_as_dict function.
    """
    user_id = kwargs['user_id']
    return json.dumps(get_template_as_dict(template_id, user_id=user_id))

def get_template_as_dict(template_id, **kwargs):
    attr_dict = {}
    dataset_dict = {}

    template_i = db.DBSession.query(Template).filter(
            Template.id==template_id).options(
                joinedload_all('templatetypes.typeattrs.default_dataset.metadata'
                              )
            ).one()

    #Load all the attributes
    for type_i in template_i.templatetypes:
        for typeattr_i in type_i.typeattrs:
            typeattr_i.attr

    template_j = JSONObject(template_i)

    for tmpltype_j in template_j.templatetypes:
        ##Try to load the json into an object, as it will be re-encoded as json,
        ##and we don't want double encoding:
        if tmpltype_j.layout is not None:
            tmpltype_j.layout = get_layout_as_dict(tmpltype_j.layout)

        for typeattr_j in tmpltype_j.typeattrs:
            typeattr_j.attr_id = typeattr_j.attr_id*-1
            attr_dict[typeattr_j.attr_id] = JSONObject(
                                   {
                                        'name': typeattr_j.attr.name,
                                        'dimension':typeattr_j.attr.dimension
                                     })

            if typeattr_j.default_dataset_id is not None:
                typeattr_j.default_dataset_id = typeattr_j.default_dataset_id * -1
                dataset_dict[typeattr_j.default_dataset_id] = JSONObject(
                            {
                                'name'     : typeattr_j.default_dataset.name,
                                'type'     : typeattr_j.default_dataset.type,
                                'unit'     : typeattr_j.default_dataset.unit,
                                'value'    : typeattr_j.default_dataset.value,
                                'metadata' : typeattr_j.default_dataset.metadata
                            })

            if hasattr(typeattr_j, 'default_dataset') and typeattr_j.default_dataset is not None:
                del(typeattr_j['default_dataset'])

            if hasattr(typeattr_j, 'attr') and typeattr_j.attr is not None:
                del(typeattr_j['attr'])

    output_data = {'attributes': attr_dict, 'datasets':dataset_dict, 'template': template_j}

    return output_data


def get_template_as_xml(template_id,**kwargs):
    """
        Turn a template into an xml template
    """
    template_xml = etree.Element("template_definition")

    template_i = db.DBSession.query(Template).filter(
            Template.id==template_id).options(
                joinedload_all('templatetypes.typeattrs.default_dataset.metadata'
                              )
            ).one()

    template_name = etree.SubElement(template_xml, "template_name")
    template_name.text = template_i.name
    resources = etree.SubElement(template_xml, "resources")

    for type_i in template_i.templatetypes:
        xml_resource    = etree.SubElement(resources, "resource")

        resource_type   = etree.SubElement(xml_resource, "type")
        resource_type.text   = type_i.resource_type

        name   = etree.SubElement(xml_resource, "name")
        name.text   = type_i.name

        alias   = etree.SubElement(xml_resource, "alias")
        alias.text   = type_i.alias

        if type_i.layout is not None and type_i.layout != "":
            layout = _get_layout_as_etree(type_i.layout)
            xml_resource.append(layout)

        for type_attr in type_i.typeattrs:
            _make_attr_element(xml_resource, type_attr)

        resources.append(xml_resource)

    xml_string = etree.tostring(template_xml, encoding="unicode")

    return xml_string

def import_template_json(template_json_string,allow_update=True, **kwargs):
    """
        Add the template, type and typeattrs described
        in a JSON file.

        Delete type, typeattr entries in the DB that are not in the XML file
        The assumption is that they have been deleted and are no longer required.

        The allow_update indicates whether an existing template of the same name should
        be updated, or whether it should throw an 'existing name' error.
    """

    user_id = kwargs.get('user_id')

    try:
        template_dict = json.loads(template_json_string)
    except:
        raise HydraError("Unable to parse JSON string. Plese ensure it is JSON compatible.")

    return import_template_dict(template_dict, allow_update=allow_update, user_id=user_id)

def import_template_dict(template_dict, allow_update=True, **kwargs):

    template_file_j = template_dict

    file_attributes = template_file_j.get('attributes')
    file_datasets   = template_file_j.get('datasets', {})
    template_j = JSONObject(template_file_j.get('template', {}))

    default_datasets_j = {}
    for k, v in file_datasets.items(): 
        default_datasets_j[int(k)] = Dataset(v)

    if file_attributes is None or default_datasets_j is None or len(template_j) == 0:
        raise HydraError("Invalid template. The template must have the following structure: " +
                            "{'attributes':\{...\}, 'datasets':\{...\}, 'template':\{...\}}")

    #Normalise attribute IDs so they're always ints (in case they're specified as strings)
    attributes_j = {}
    for k, v in file_attributes.items():
        attributes_j[int(k)] = JSONObject(v)

    template_name = template_j.name

    template_layout = None
    if template_j.layout is not None:
        if isinstance(template_j.layout, dict):
            template_layout = json.dumps(template_j.layout)
        else:
            template_layout = template_j.layout

    try:
        template_i = db.DBSession.query(Template).filter(Template.name==template_name).options(joinedload_all('templatetypes.typeattrs.attr')).one()
        if allow_update == False:
            raise HydraError("Existing Template Found with name %s"%(template_name,))
        else:
            log.info("Existing template found. name=%s", template_name)
            template_i.layout = template_layout
    except NoResultFound:
        log.info("Template not found. Creating new one. name=%s", template_name)
        template_i = Template(name=template_name, layout=template_layout)
        db.DBSession.add(template_i)

    types_j = template_j.templatetypes
    type_id_map = {r.id:r for r in template_i.templatetypes}
    #Delete any types which are in the DB but no longer in the JSON file
    type_name_map = {r.name:r.id for r in template_i.templatetypes}
    attr_name_map = {}
    for type_i in template_i.templatetypes:
        for typeattr in type_i.typeattrs:
            attr_name_map[typeattr.attr.name] = (typeattr.attr_id, typeattr.type_id)

    existing_types = set([r.name for r in template_i.templatetypes])
    log.info(["%s : %s" %(tt.name, tt.id) for tt in template_i.templatetypes])
    log.info("Existing types: %s", existing_types)

    new_types = set([t.name for t in types_j])
    log.info("New Types: %s", new_types)

    types_to_delete = existing_types - new_types
    log.info("Types to delete: %s", types_to_delete)
    log.info(type_name_map)
    for type_to_delete in types_to_delete:
        type_id = type_name_map[type_to_delete]
        try:
            for i, tt in enumerate(template_i.templatetypes):
                if tt.id == type_id:
                    type_i = template_i.templatetypes[i]
                    del(template_i.templatetypes[i])
                    log.info("Deleting type %s (%s)", type_i.name, type_i.id)
                    del(type_name_map[type_to_delete])
                    db.DBSession.delete(type_i)
        except NoResultFound:
            pass


    #Add or update types.
    for type_j in types_j:
        type_name = type_j.name

        #check if the type is already in the DB. If not, create a new one.
        type_is_new = False
        if type_name in existing_types:
            type_id = type_name_map[type_name]
            type_i = type_id_map[type_id]
        else:
            log.info("Type %s not found, creating new one.", type_name)
            type_i = TemplateType()
            type_i.name = type_name
            template_i.templatetypes.append(type_i)
            type_is_new = True

        if type_j.alias is not None:
            type_i.alias = type_j.alias

        #Allow 'type' or 'resource_type' to be accepted
        if type_j.type is not None:
            type_i.resource_type = type_j.type
        elif type_j.resource_type is not None:
            type_i.resource_type = type_j.resource_type

        if type_j.resource_type is None:
            raise HydraError("No resource type specified."
                             " 'NODE', 'LINK', 'GROUP' or 'NETWORK'")

        if type_j.layout is not None:
            if isinstance(type_j, dict):
                type_i.layout = json.dumps(type_j.layout)
            else:
                type_i.layout = type_j.layout

        #delete any TypeAttrs which are in the DB but not in the XML file
        existing_attrs = []
        if not type_is_new:
            for r in template_i.templatetypes:
                if r.name == type_name:
                    for typeattr in r.typeattrs:
                        existing_attrs.append(typeattr.attr.name)

        existing_attrs = set(existing_attrs)

        type_attrs = []
        for typeattr_j in type_j.typeattrs:
            if typeattr_j.attr_id is not None:
                attr_j = attributes_j[typeattr_j.attr_id].name
            elif typeattr_j.attr is not None:
                attr_j = typeattr_j.attr.name
            type_attrs.append(attr_j)

        type_attrs = set(type_attrs)

        attrs_to_delete = existing_attrs - type_attrs
        for attr_to_delete in attrs_to_delete:
            attr_id, type_id = attr_name_map[attr_to_delete]
            try:
                attr_i = db.DBSession.query(TypeAttr).filter(TypeAttr.attr_id==attr_id, TypeAttr.type_id==type_id).options(joinedload_all('attr')).one()
                db.DBSession.delete(attr_i)
                log.info("Attr %s in type %s deleted",attr_i.attr.name, attr_i.templatetype.name)
            except NoResultFound:
                log.debug("Attr %s not found in type %s"%(attr_id, type_id))
                continue

        #Add or update type typeattrs
        #Support an external attribute dict or embedded attributes.
        for typeattr_j  in type_j.typeattrs:
            if typeattr_j.attr_id is not None:
                attr_j = attributes_j[typeattr_j.attr_id]
            elif typeattr_j.attr is not None:
                attr_j = typeattr_j.attr

            default_dataset_j = None
            if typeattr_j.default_dataset is not None:
                default_dataset_j = typeattr_j.default_dataset
            elif typeattr_j.default is not None:
                default_dataset_j = typeattr_j.default_dataset
            elif typeattr_j.default_dataset_id is not None:
                default_dataset_j = default_datasets_j[int(typeattr_j.default_dataset_id)]

            parse_json_typeattr(type_i, typeattr_j, attr_j, default_dataset_j)

    db.DBSession.flush()

    return template_i


def import_template_xml(template_xml, allow_update=True, **kwargs):
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
        layout_string = get_etree_layout_as_dict(layout)
        template_layout = json.dumps(layout_string)

    try:
        tmpl_i = db.DBSession.query(Template).filter(Template.name==template_name).options(joinedload_all('templatetypes.typeattrs.attr')).one()

        if allow_update == False:
            raise HydraError("Existing Template Found with name %s"%(template_name,))
        else:
            log.info("Existing template found. name=%s", template_name)
            tmpl_i.layout = template_layout
    except NoResultFound:
        log.info("Template not found. Creating new one. name=%s", template_name)
        tmpl_i = Template(name=template_name, layout=template_layout)
        db.DBSession.add(tmpl_i)

    types = xml_tree.find('resources')
    #Delete any types which are in the DB but no longer in the XML file
    type_name_map = {r.name:r.id for r in tmpl_i.templatetypes}
    attr_name_map = {}
    for type_i in tmpl_i.templatetypes:
        for attr in type_i.typeattrs:
            attr_name_map[attr.attr.name] = (attr.id, attr.type_id)

    existing_types = set([r.name for r in tmpl_i.templatetypes])

    new_types = set([r.find('name').text for r in types.findall('resource')])

    types_to_delete = existing_types - new_types

    for type_to_delete in types_to_delete:
        type_id = type_name_map[type_to_delete]
        try:
            type_i = db.DBSession.query(TemplateType).filter(TemplateType.id==type_id).one()
            log.info("Deleting type %s", type_i.name)
            db.DBSession.delete(type_i)
        except NoResultFound:
            pass

    #Add or update types.
    for resource in types.findall('resource'):
        type_name = resource.find('name').text
        #check if the type is already in the DB. If not, create a new one.
        type_is_new = False
        if type_name in existing_types:
            type_id = type_name_map[type_name]
            type_i = db.DBSession.query(TemplateType).filter(TemplateType.id==type_id).options(joinedload_all('typeattrs.attr')).one()

        else:
            log.info("Type %s not found, creating new one.", type_name)
            type_i = TemplateType()
            type_i.name = type_name
            tmpl_i.templatetypes.append(type_i)
            type_is_new = True

        if resource.find('alias') is not None:
            type_i.alias = resource.find('alias').text

        if resource.find('type') is not None:
            type_i.resource_type = resource.find('type').text

        if resource.find('layout') is not None and \
            resource.find('layout').text is not None:
            layout = resource.find('layout')
            layout_string = get_etree_layout_as_dict(layout)
            type_i.layout = json.dumps(layout_string)

        #delete any TypeAttrs which are in the DB but not in the XML file
        existing_attrs = []
        if not type_is_new:
            for r in tmpl_i.templatetypes:
                if r.name == type_name:
                    for typeattr in r.typeattrs:
                        existing_attrs.append(typeattr.attr.name)

        existing_attrs = set(existing_attrs)

        template_attrs = set([r.find('name').text for r in resource.findall('attribute')])

        attrs_to_delete = existing_attrs - template_attrs
        for attr_to_delete in attrs_to_delete:
            attr_id, type_id = attr_name_map[attr_to_delete]
            try:
                attr_i = db.DBSession.query(TypeAttr).filter(TypeAttr.attr_id==attr_id, TypeAttr.type_id==type_id).options(joinedload_all('attr')).one()
                db.DBSession.delete(attr_i)
                log.info("Attr %s in type %s deleted",attr_i.attr.name, attr_i.templatetype.name)
            except NoResultFound:
                log.debug("Attr %s not found in type %s"%(attr_id, type_id))
                continue

        #Add or update type typeattrs
        for attribute in resource.findall('attribute'):
            parse_xml_typeattr(type_i, attribute)

    db.DBSession.flush()

    return tmpl_i

def apply_template_to_network(template_id, network_id, **kwargs):
    """
        For each node and link in a network, check whether it matches
        a type in a given template. If so, assign the type to the node / link.
    """

    net_i = db.DBSession.query(Network).filter(Network.id==network_id).one()
    #There should only ever be one matching type, but if there are more,
    #all we can do is pick the first one.
    try:
        network_type_id = db.DBSession.query(TemplateType.id).filter(TemplateType.template_id==template_id,
                                                                       TemplateType.resource_type=='NETWORK').one()
        assign_type_to_resource(network_type_id.id, 'NETWORK', network_id,**kwargs)
    except NoResultFound:
        log.info("No network type to set.")
        pass

    for node_i in net_i.nodes:
        templates = get_types_by_attr(node_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].id, 'NODE', node_i.id,**kwargs)
    for link_i in net_i.links:
        templates = get_types_by_attr(link_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].id, 'LINK', link_i.id,**kwargs)

    for group_i in net_i.resourcegroups:
        templates = get_types_by_attr(group_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].id, 'GROUP', group_i.id,**kwargs)

    db.DBSession.flush()

def set_network_template(template_id, network_id, **kwargs):
    """
       Apply an existing template to a network. Used when a template has changed, and additional attributes
       must be added to the network's elements.
    """

    resource_types = []

    #There should only ever be one matching type, but if there are more,
    #all we can do is pick the first one.
    try:
        network_type = db.DBSession.query(ResourceType).filter(ResourceType.ref_key=='NETWORK',
                                                            ResourceType.network_id==network_id,
                                                            ResourceType.type_id==TemplateType.type_id,
                                                            TemplateType.template_id==template_id).one()
        resource_types.append(network_type)

    except NoResultFound:
        log.info("No network type to set.")
        pass

    node_types = db.DBSession.query(ResourceType).filter(ResourceType.ref_key=='NODE',
                                                        ResourceType.node_id==Node.node_id,
                                                        Node.network_id==network_id,
                                                        ResourceType.type_id==TemplateType.type_id,
                                                        TemplateType.template_id==template_id).all()
    link_types = db.DBSession.query(ResourceType).filter(ResourceType.ref_key=='LINK',
                                                        ResourceType.link_id==Link.link_id,
                                                        Link.network_id==network_id,
                                                        ResourceType.type_id==TemplateType.type_id,
                                                        TemplateType.template_id==template_id).all()
    group_types = db.DBSession.query(ResourceType).filter(ResourceType.ref_key=='GROUP',
                                                        ResourceType.group_id==ResourceGroup.group_id,
                                                        ResourceGroup.network_id==network_id,
                                                        ResourceType.type_id==TemplateType.type_id,
                                                        TemplateType.template_id==template_id).all()

    resource_types.extend(node_types)
    resource_types.extend(link_types)
    resource_types.extend(group_types)

    assign_types_to_resources(resource_types)

    log.info("Finished setting network template")

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
        network = db.DBSession.query(Network).filter(Network.id==network_id).one()
    except NoResultFound:
        raise HydraError("Network %s not found"%network_id)

    try:
        template = db.DBSession.query(Template).filter(Template.id==template_id).one()
    except NoResultFound:
        raise HydraError("Template %s not found"%template_id)

    type_ids = [tmpltype.id for tmpltype in template.templatetypes]

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
    type_ids = [tmpltype.id for tmpltype in template.templatetypes]

    node_attr_ids = dict([(ra.attr_id, ra) for ra in resource.attributes])
    attrs_to_remove = []
    attrs_to_keep   = []
    for nt in resource.types:
        if nt.templatetype.id in type_ids:
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
        resource_i = db.DBSession.query(Network).filter(Network.id==resource_id).one()
    elif resource_type == 'NODE':
        resource_i = db.DBSession.query(Node).filter(Node.id==resource_id).one()
    elif resource_type == 'LINK':
        resource_i = db.DBSession.query(Link).filter(Link.id==resource_id).one()
    elif resource_type == 'GROUP':
        resource_i = db.DBSession.query(ResourceGroup).filter(ResourceGroup.id==resource_id).one()

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

    db_types = db.DBSession.query(TemplateType).filter(TemplateType.id.in_(type_ids)).options(joinedload_all('typeattrs')).all()

    types = {}
    for db_type in db_types:
        if types.get(db_type.id) is None:
            types[db_type.id] = db_type
    log.info("Retrieved all the appropriate template types")
    res_types = []
    res_attrs = []
    res_scenarios = []

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
        net = db.DBSession.query(Network).filter(Network.id==net_id).one()
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

        ra, rt, rs= set_resource_type(resource, type_id, types)
        if rt is not None:
            res_types.append(rt)
        if len(ra) > 0:
            res_attrs.extend(ra)
        if len(rs) > 0:
            res_scenarios.extend(rs)

    log.info("Retrieved all the appropriate resources")
    if len(res_types) > 0:
        new_types = db.DBSession.execute(ResourceType.__table__.insert(), res_types)
    if len(res_attrs) > 0:
        new_res_attrs = db.DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)
        new_ras = db.DBSession.query(ResourceAttr).filter(and_(ResourceAttr.resource_attr_id>=new_res_attrs.lastrowid, ResourceAttr.resource_attr_id<(new_res_attrs.lastrowid+len(res_attrs)))).all()

    ra_map = {}
    for ra in new_ras:
        ra_map[(ra.ref_key, ra.attr_id, ra.node_id, ra.link_id, ra.group_id, ra.network_id)] = ra.resource_attr_id

    for rs in res_scenarios:
        rs['resource_attr_id'] = ra_map[(rs['ref_key'], rs['attr_id'], rs['node_id'], rs['link_id'], rs['group_id'], rs['network_id'])]

    if len(res_scenarios) > 0:
        new_scenarios = db.DBSession.execute(ResourceScenario.__table__.insert(), res_scenarios)
    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete.

    db.DBSession.query(ResourceAttr).filter(ResourceAttr.attr_id==None).delete()

    ret_val = [t for t in types.values()]
    return ret_val

def check_type_compatibility(type_1_id, type_2_id):
    """
        When applying a type to a resource, it may be the case that the resource already
        has an attribute specified in the new type, but the template which defines this
        pre-existing attribute has a different unit specification to the new template.

        This function checks for any situations where different types specify the same
        attributes, but with different units.
    """
    errors = []

    type_1 = db.DBSession.query(TemplateType).filter(TemplateType.id==type_1_id).options(joinedload_all('typeattrs')).one()
    type_2 = db.DBSession.query(TemplateType).filter(TemplateType.id==type_2_id).options(joinedload_all('typeattrs')).one()
    template_1_name = type_1.template.name
    template_2_name = type_2.template.name

    type_1_attrs=set([t.attr_id for t in type_1.typeattrs])
    type_2_attrs=set([t.attr_id for t in type_2.typeattrs])

    shared_attrs = type_1_attrs.intersection(type_2_attrs)

    if len(shared_attrs) == 0:
        return []

    type_1_dict = {}
    for t in type_1.typeattrs:
        if t.attr_id in shared_attrs:
            type_1_dict[t.attr_id]=t

    for ta in type_2.typeattrs:
        type_2_unit = ta.unit
        type_1_unit = type_1_dict[ta.attr_id].unit

        fmt_dict = {
                    'template_1_name':template_1_name,
                    'template_2_name':template_2_name,
                    'attr_name':ta.attr.name,
                    'type_1_unit':type_1_unit,
                    'type_2_unit':type_2_unit,
                    'type_name' : type_1.name
                }

        if type_1_unit is None and type_2_unit is not None:
            errors.append("Type %(type_name)s in template %(template_1_name)s"
                          " stores %(attr_name)s with no units, while template"
                          "%(template_2_name)s stores it with unit %(type_2_unit)s"%fmt_dict)
        elif type_1_unit is not None and type_2_unit is None:
            errors.append("Type %(type_name)s in template %(template_1_name)s"
                          " stores %(attr_name)s in %(type_1_unit)s."
                          " Template %(template_2_name)s stores it with no unit."%fmt_dict)
        elif type_1_unit != type_2_unit:
            errors.append("Type %(type_name)s in template %(template_1_name)s"
                          " stores %(attr_name)s in %(type_1_unit)s, while"
                          " template %(template_2_name)s stores it in %(type_2_unit)s"%fmt_dict)
        return errors

def _get_links(link_ids):
    links = []

    if len(link_ids) == 0:
        return links

    if len(link_ids) > 500:
        idx = 0
        extent = 500
        while idx < len(link_ids):
            log.info("Querying %s links", len(link_ids[idx:extent]))
            rs = db.DBSession.query(Link).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Link.id.in_(link_ids[idx:extent])).all()
            log.info("Retrieved %s links", len(rs))
            links.extend(rs)
            idx = idx + 500

            if idx + 500 > len(link_ids):
                extent = len(link_ids)
            else:
                extent = extent + 500
    else:
        links = db.DBSession.query(Link).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Link.id.in_(link_ids)).all()

    link_dict = {}

    for l in links:
        l.ref_id = l.id
        l.ref_key = 'LINK'
        link_dict[l.id] = l

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

            rs = db.DBSession.query(Node).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Node.id.in_(node_ids[idx:extent])).all()

            log.info("Retrieved %s nodes", len(rs))

            nodes.extend(rs)
            idx = idx + 500

            if idx + 500 > len(node_ids):
                extent = len(node_ids)
            else:
                extent = extent + 500
    else:
        nodes = db.DBSession.query(Node).options(joinedload_all('attributes')).options(joinedload_all('types')).filter(Node.id.in_(node_ids)).all()

    node_dict = {}

    for n in nodes:
        n.ref_id = n.id
        n.ref_key = 'NODE'
        node_dict[n.id] = n

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
            rs = db.DBSession.query(ResourceGroup).options(joinedload_all('attributes')).filter(ResourceGroup.id.in_(group_ids[idx:extent])).all()
            log.info("Retrieved %s groups", len(rs))
            groups.extend(rs)
            idx = idx + 500

            if idx + 500 > len(group_ids):
                extent = len(group_ids)
            else:
                extent = extent + 500
    else:
        groups = db.DBSession.query(ResourceGroup).options(joinedload_all('types')).options(joinedload_all('attributes')).filter(ResourceGroup.id.in_(group_ids))
    group_dict = {}

    for g in groups:
        g.ref_id = g.id
        g.ref_key = 'GROUP'
        group_dict[g.id] = g

    return group_dict

def assign_type_to_resource(type_id, resource_type, resource_id,**kwargs):
    """Assign new type to a resource. This function checks if the necessary
    attributes are present and adds them if needed. Non existing attributes
    are also added when the type is already assigned. This means that this
    function can also be used to update resources, when a resource type has
    changed.
    """

    if resource_type == 'NETWORK':
        resource = db.DBSession.query(Network).filter(Network.id==resource_id).one()
    elif resource_type == 'NODE':
        resource = db.DBSession.query(Node).filter(Node.id==resource_id).one()
    elif resource_type == 'LINK':
        resource = db.DBSession.query(Link).filter(Link.id==resource_id).one()
    elif resource_type == 'GROUP':
        resource = db.DBSession.query(ResourceGroup).filter(ResourceGroup.id==resource_id).one()

    res_attrs, res_type, res_scenarios = set_resource_type(resource, type_id, **kwargs)

    type_i = db.DBSession.query(TemplateType).filter(TemplateType.id==type_id).one()
    if resource_type != type_i.resource_type:
        raise HydraError("Cannot assign a %s type to a %s"%
                         (type_i.resource_type,resource_type))

    if res_type is not None:
        db.DBSession.bulk_insert_mappings(ResourceType, [res_type])

    if len(res_attrs) > 0:
        db.DBSession.bulk_insert_mappings(ResourceAttr, res_attrs)

    if len(res_scenarios) > 0:
        db.DBSession.bulk_insert_mappings(ResourceScenario, res_scenarios)

    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete.
    db.DBSession.query(Attr).filter(Attr.id==None).delete()

    db.DBSession.flush()

    return db.DBSession.query(TemplateType).filter(TemplateType.id==type_id).one()

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
    for res_attr in resource.attributes:
        existing_attr_ids.append(res_attr.attr_id)

    if type_id in types:
        type_i = types[type_id]
    else:
        type_i = db.DBSession.query(TemplateType).filter(TemplateType.id==type_id).options(joinedload_all('typeattrs')).one()

    type_attrs = dict()
    for typeattr in type_i.typeattrs:
        type_attrs[typeattr.attr_id]={
                            'is_var':typeattr.attr_is_var,
                            'default_dataset_id': typeattr.default_dataset.id if typeattr.default_dataset else None}

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
            ref_key = ref_key,
            attr_id = attr_id,
            attr_is_var = type_attrs[attr_id]['is_var'],
            node_id    = resource.id   if ref_key == 'NODE' else None,
            link_id    = resource.id   if ref_key == 'LINK' else None,
            group_id   = resource.id   if ref_key == 'GROUP' else None,
            network_id = resource.id   if ref_key == 'NETWORK' else None,

        )
        new_res_attrs.append(ra_dict)



        if type_attrs[attr_id]['default_dataset_id'] is not None:
            if hasattr(resource, 'network'):
                for s in resource.network.scenarios:

                    if new_res_scenarios.get(attr_id) is None:
                        new_res_scenarios[attr_id] = {}
                    
                    new_res_scenarios[attr_id][s.id] =  dict(
                        dataset_id = type_attrs[attr_id]['default_dataset_id'],
                        scenario_id = s.id,
                        #Not stored in the DB, but needed to connect the RA ID later.
                        attr_id = attr_id,
                        ref_key = ref_key,
                        node_id    = ra_dict['node_id'],
                        link_id    = ra_dict['link_id'],
                        group_id   = ra_dict['group_id'],
                        network_id = ra_dict['network_id'],
                    )


    resource_type = None
    for rt in resource.types:
        if rt.type_id == type_i.id:
            break
        else:
            errors = check_type_compatibility(rt.type_id, type_i.id)
            if len(errors) > 0:
                raise HydraError("Cannot apply type %s to resource as it "
                                 "conflicts with type %s. Errors are: %s"
                                 %(type_i.name, resource.get_name(),
                                   rt.templatetype.name, ','.join(errors)))
    else:
        # add type to tResourceType if it doesn't exist already
        resource_type = dict(
            node_id    = resource.id   if ref_key == 'NODE' else None,
            link_id    = resource.id   if ref_key == 'LINK' else None,
            group_id   = resource.id   if ref_key == 'GROUP' else None,
            network_id = resource.id   if ref_key == 'NETWORK' else None,
            ref_key    = ref_key,
            type_id    = type_id,
        )

    return new_res_attrs, resource_type, new_res_scenarios

def remove_type_from_resource( type_id, resource_type, resource_id,**kwargs):
    """
        Remove a resource type trom a resource
    """
    node_id = resource_id if resource_type == 'NODE' else None
    link_id = resource_id if resource_type == 'LINK' else None
    group_id = resource_id if resource_type == 'GROUP' else None

    resourcetype = db.DBSession.query(ResourceType).filter(
                                        ResourceType.type_id==type_id,
                                        ResourceType.ref_key==resource_type,
                                        ResourceType.node_id == node_id,
    ResourceType.link_id == link_id,
    ResourceType.group_id == group_id).one()

    db.DBSession.delete(resourcetype)
    db.DBSession.flush()

    return 'OK'

def _parse_data_restriction(restriction_dict):
    if restriction_dict is None or len(restriction_dict) == 0:
        return None

    #replace soap text with an empty string
    #'{soap_server.hydra_complexmodels}' -> ''
    dict_str = re.sub('{[a-zA-Z\.\_]*}', '', str(restriction_dict))

    if isinstance(restriction_dict, dict):
        new_dict = restriction_dict
    else:
        new_dict = json.loads(restriction_dict)

    #Evaluate whether the dict actually contains anything.
    if not isinstance(new_dict, dict) or len(new_dict) == 0:
        log.critical('A restriction was specified, but it is null')
        return None

    ret_dict = {}
    for k, v in new_dict.items():
        if (isinstance(v, str) or isinstance(v, list)) and len(v) == 1:
            ret_dict[k] = v[0]
        else:
            ret_dict[k] = v

    return json.dumps(ret_dict)

def add_template(template, **kwargs):
    """
        Add template and a type and typeattrs.
    """
    tmpl = Template()
    tmpl.name = template.name
    if template.layout:
        tmpl.layout = get_layout_as_string(template.layout)

    db.DBSession.add(tmpl)

    if template.templatetypes is not None:
        types = template.templatetypes
        for templatetype in types:
            ttype = _update_templatetype(templatetype)
            tmpl.templatetypes.append(ttype)

    db.DBSession.flush()
    return tmpl

def update_template(template,**kwargs):
    """
        Update template and a type and typeattrs.
    """
    tmpl = db.DBSession.query(Template).filter(Template.id==template.id).one()
    tmpl.name = template.name

    #Lazy load the rest of the template
    for tt in tmpl.templatetypes:
        for ta in tt.typeattrs:
            ta.attr

    if template.layout:
        tmpl.layout = get_layout_as_string(template.layout)

    type_dict = dict([(t.id, t) for t in tmpl.templatetypes])
    existing_templatetypes = []

    if template.types is not None or template.templatetypes is not None:
        types = template.types if template.types is not None else template.templatetypes
        for templatetype in types:
            if templatetype.id is not None:
                type_i = type_dict[templatetype.id]
                _update_templatetype(templatetype, type_i)
                existing_templatetypes.append(type_i.id)
            else:
                #Give it a template ID if it doesn't have one
                templatetype.template_id = template.id
                new_templatetype_i = _update_templatetype(templatetype)
                existing_templatetypes.append(new_templatetype_i.id)

    for tt in tmpl.templatetypes:
        if tt.id not in existing_templatetypes:
            delete_templatetype(tt.id)

    db.DBSession.flush()

    return tmpl

def delete_template(template_id,**kwargs):
    """
        Delete a template and its type and typeattrs.
    """
    try:
        tmpl = db.DBSession.query(Template).filter(Template.id==template_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Template %s not found"%(template_id,))
    db.DBSession.delete(tmpl)
    db.DBSession.flush()
    return 'OK'

def get_templates(load_all=True, **kwargs):
    """
        Get all templates.
        Args:
            load_all Boolean: Returns just the template entry or the full template structure (template types and type attrs)
        Returns:
            List of Template objects
    """
    if load_all is False:
        templates = db.DBSession.query(Template).all()
    else:
        templates = db.DBSession.query(Template).options(joinedload_all('templatetypes.typeattrs')).all()

    return templates

def remove_attr_from_type(type_id, attr_id,**kwargs):
    """

        Remove an attribute from a type
    """
    typeattr_i = db.DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_id,
                                                  TypeAttr.attr_id==attr_id).one()
    db.DBSession.delete(typeattr_i)

def get_template(template_id,**kwargs):
    """
        Get a specific resource template template, by ID.
    """
    try:
        tmpl_i = db.DBSession.query(Template).filter(Template.id==template_id).options(joinedload_all('templatetypes.typeattrs.default_dataset.metadata')).one()

        #Load the attributes.
        for tmpltype_i in tmpl_i.templatetypes:
            for typeattr_i in tmpltype_i.typeattrs:
                typeattr_i.attr

        return tmpl_i
    except NoResultFound:
        raise HydraError("Template %s not found"%template_id)

def get_template_by_name(name,**kwargs):
    """
        Get a specific resource template, by name.
    """
    try:
        tmpl_i = db.DBSession.query(Template).filter(Template.name == name).options(joinedload_all('templatetypes.typeattrs.default_dataset.metadata')).one()
        return tmpl_i
    except NoResultFound:
        log.info("%s is not a valid identifier for a template",name)
        raise HydraError('Template "%s" not found'%name)

def add_templatetype(templatetype,**kwargs):
    """
        Add a template type with typeattrs.
    """

    type_i = _update_templatetype(templatetype)

    db.DBSession.flush()

    return type_i

def update_templatetype(templatetype,**kwargs):
    """
        Update a resource type and its typeattrs.
        New typeattrs will be added. typeattrs not sent will be ignored.
        To delete typeattrs, call delete_typeattr
    """

    tmpltype_i = db.DBSession.query(TemplateType).filter(TemplateType.id == templatetype.id).one()

    _update_templatetype(templatetype, tmpltype_i)

    db.DBSession.flush()

    return tmpltype_i

def _set_typeattr(typeattr, existing_ta = None):
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

    if hasattr(typeattr, 'default_dataset_id') and typeattr.default_dataset_id is not None:
        ta.default_dataset_id = typeattr.default_dataset_id

    ta.description        = typeattr.description

    ta.properties         = typeattr.get_properties()

    ta.attr_is_var        = typeattr.is_var if typeattr.is_var is not None else 'N'

    ta.data_restriction = _parse_data_restriction(typeattr.data_restriction)

    if typeattr.dimension is not None and typeattr.attr_id is not None and typeattr.attr_id > 0:
        attr = ta.attr
        if attr.dimension != typeattr.dimension:
            raise HydraError("Cannot set a dimension on type attribute which "
                            "does not match its attribute. Create a new attribute if "
                            "you want to use attribute %s with dimension %s"%
                            (attr.name, typeattr.dimension))
    elif typeattr.dimension is not None and typeattr.attr_id is None and typeattr.name is not None:
        attr = _get_attr_by_name_and_dimension(typeattr.name, typeattr.dimension)
        ta.attr_id = attr.id
        ta.attr = attr

    _check_dimension(ta)

    if existing_ta is None:
        log.info("Adding ta to DB")
        db.DBSession.add(ta)

    return ta

def _update_templatetype(templatetype, existing_tt=None):
    """
        Add or update a templatetype. If an existing template type is passed in,
        update that one. Otherwise search for an existing one. If not found, add.
    """
    if existing_tt is None:
        if templatetype.id is not None:
            tmpltype_i = db.DBSession.query(TemplateType).filter(TemplateType.id == templatetype.id).one()
        else:
            tmpltype_i = TemplateType()
    else:
        tmpltype_i = existing_tt

    tmpltype_i.template_id = templatetype.template_id
    tmpltype_i.name        = templatetype.name
    tmpltype_i.alias       = templatetype.alias

    if templatetype.layout is not None:
        tmpltype_i.layout = get_layout_as_string(templatetype.layout)

    tmpltype_i.resource_type = templatetype.resource_type

    ta_dict = {}
    for t in tmpltype_i.typeattrs:
        ta_dict[t.attr_id] = t

    existing_attrs = []

    if templatetype.typeattrs is not None:
        for typeattr in templatetype.typeattrs:
            if typeattr.attr_id in ta_dict:
                ta = _set_typeattr(typeattr, ta_dict[typeattr.attr_id])
                existing_attrs.append(ta.attr_id)
            else:
                ta = _set_typeattr(typeattr)
                tmpltype_i.typeattrs.append(ta)
                existing_attrs.append(ta.attr_id)

    log.info("Deleting any type attrs not sent")
    for ta in ta_dict.values():
        if ta.attr_id not in existing_attrs:
            delete_typeattr(ta)

    if existing_tt is None:
        db.DBSession.add(tmpltype_i)

    return tmpltype_i

def delete_templatetype(type_id,template_i=None, **kwargs):
    """
        Delete a template type and its typeattrs.
    """
    try:
        tmpltype_i = db.DBSession.query(TemplateType).filter(TemplateType.id == type_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Template Type %s not found"%(type_id,))

    if template_i is None:
        template_i = db.DBSession.query(Template).filter(Template.id==tmpltype_i.template_id).one()

    template_i.templatetypes.remove(tmpltype_i)

    db.DBSession.delete(tmpltype_i)
    db.DBSession.flush()

def get_templatetype(type_id,**kwargs):
    """
        Get a specific resource type by ID.
    """

    templatetype = db.DBSession.query(TemplateType).filter(
                        TemplateType.id==type_id).options(
                        joinedload_all("typeattrs")).one()

    return templatetype

def get_templatetype_by_name(template_id, type_name,**kwargs):
    """
        Get a specific resource type by name.
    """

    try:
        templatetype = db.DBSession.query(TemplateType).filter(TemplateType.id==template_id, TemplateType.name==type_name).one()
    except NoResultFound:
        raise HydraError("%s is not a valid identifier for a type"%(type_name))

    return templatetype

def add_typeattr(typeattr,**kwargs):
    """
        Add an typeattr to an existing type.
    """

    tmpltype = get_templatetype(typeattr.type_id, user_id=kwargs.get('user_id'))

    ta = _set_typeattr(typeattr)

    tmpltype.typeattrs.append(ta)

    db.DBSession.flush()

    return ta


def delete_typeattr(typeattr,**kwargs):
    """
        Remove an typeattr from an existing type
    """

    tmpltype = get_templatetype(typeattr.type_id, user_id=kwargs.get('user_id'))

    ta = db.DBSession.query(TypeAttr).filter(TypeAttr.type_id == typeattr.type_id,
                                          TypeAttr.attr_id == typeattr.attr_id).one()

    tmpltype.typeattrs.remove(ta)

    db.DBSession.flush()

    return 'OK'

def validate_attr(resource_attr_id, scenario_id, template_id=None):
    """
        Check that a resource attribute satisfies the requirements of all the types of the
        resource.
    """
    rs = db.DBSession.query(ResourceScenario).\
                        filter(ResourceScenario.resource_attr_id==resource_attr_id,
        ResourceScenario.scenario_id==scenario_id).options(
        joinedload_all("resourceattr")).options(
        joinedload_all("dataset")
        ).one()

    error = None

    try:
        _do_validate_resourcescenario(rs, template_id)
    except HydraError as e:

        error = JSONObject(dict(
                 ref_key = rs.resourceattr.ref_key,
                 ref_id  = rs.resourceattr.get_resource_id(),
                 ref_name = rs.resourceattr.get_resource().get_name(),
                 resource_attr_id = rs.resource_attr_id,
                 attr_id          = rs.resourceattr.attr.id,
                 attr_name        = rs.resourceattr.attr.name,
                 dataset_id       = rs.dataset_id,
                 scenario_id=scenario_id,
                 template_id=template_id,
                 error_text=e.args[0]))
    return error

def validate_attrs(resource_attr_ids, scenario_id, template_id=None):
    """
        Check that multiple resource attribute satisfy the requirements of the types of resources to
        which the they are attached.
    """
    multi_rs = db.DBSession.query(ResourceScenario).\
                            filter(ResourceScenario.resource_attr_id.in_(resource_attr_ids),\
                                   ResourceScenario.scenario_id==scenario_id).\
                                   options(joinedload_all("resourceattr")).\
                                   options(joinedload_all("dataset")).all()

    errors = []
    for rs in multi_rs:
        try:
            _do_validate_resourcescenario(rs, template_id)
        except HydraError as e:

            error = dict(
                     ref_key = rs.resourceattr.ref_key,
                     ref_id  = rs.resourceattr.get_resource_id(),
                     ref_name = rs.resourceattr.get_resource().get_name(),
                     resource_attr_id = rs.resource_attr_id,
                     attr_id          = rs.resourceattr.attr.id,
                     attr_name        = rs.resourceattr.attr.name,
                     dataset_id       = rs.dataset_id,
                     scenario_id      = scenario_id,
                     template_id      = template_id,
                     error_text       = e.args[0])

            errors.append(error)

    return errors

def validate_scenario(scenario_id, template_id=None):
    """
        Check that the requirements of the types of resources in a scenario are
        correct, based on the templates in a network. If a template is specified,
        only that template will be checked.
    """
    scenario_rs = db.DBSession.query(ResourceScenario).filter(
                ResourceScenario.scenario_id==scenario_id)\
                .options(joinedload_all("resourceattr"))\
                .options(joinedload_all("dataset")).all()

    errors = []
    for rs in scenario_rs:
        try:
            _do_validate_resourcescenario(rs, template_id)
        except HydraError as e:

            error = dict(
                     ref_key = rs.resourceattr.ref_key,
                     ref_id  = rs.resourceattr.get_resource_id(),
                     ref_name = rs.resourceattr.get_resource().get_name(),
                     resource_attr_id = rs.resource_attr_id,
                     attr_id          = rs.resourceattr.attr.id,
                     attr_name        = rs.resourceattr.attr.name,
                     dataset_id       = rs.dataset_id,
                     scenario_id=scenario_id,
                     template_id=template_id,
                     error_text=e.args[0])
            errors.append(error)

    return errors


def _do_validate_resourcescenario(resourcescenario, template_id=None):
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
        if template_id not in [r.templatetype.template_id for r in res.types]:
            raise HydraError("Template %s is not used for resource attribute %s in scenario %s"%\
                             (template_id, resourcescenario.resourceattr.attr.name,
                             resourcescenario.scenario.name))

    #Validate against all the types for the resource
    for resourcetype in types:
        #If a specific type has been specified, then only validate
        #against that type and ignore all the others
        if template_id is not None:
            if resourcetype.templatetype.template_id != template_id:
                continue
        #Identify the template types for the template
        tmpltype = resourcetype.templatetype
        for ta in tmpltype.typeattrs:
            #If we find a template type which mactches the current attribute.
            #we can do some validation.
            if ta.attr_id == resourcescenario.resourceattr.attr_id:
                if ta.data_restriction:
                    log.info("Validating against %s", ta.data_restriction)
                    validation_dict = eval(ta.data_restriction)
                    dataset_util.validate_value(validation_dict, dataset.get_val())

def validate_network(network_id, template_id, scenario_id=None):
    """
        Given a network, scenario and template, ensure that all the nodes, links & groups
        in the network have the correct resource attributes as defined by the types in the template.
        Also ensure valid entries in tresourcetype.
        This validation will not fail if a resource has more than the required type, but will fail if
        it has fewer or if any attribute has a conflicting dimension or unit.
    """

    network = db.DBSession.query(Network).filter(Network.id==network_id).options(noload('scenarios')).first()

    if network is None:
        raise HydraError("Could not find network %s"%(network_id))

    resource_scenario_dict = {}
    if scenario_id is not None:
        scenario = db.DBSession.query(Scenario).filter(Scenario.id==scenario_id).first()

        if scenario is None:
            raise HydraError("Could not find scenario %s"%(scenario_id,))

        for rs in scenario.resourcescenarios:
            resource_scenario_dict[rs.resource_attr_id] = rs

    template = db.DBSession.query(Template).filter(Template.id == template_id).options(joinedload_all('templatetypes')).first()

    if template is None:
        raise HydraError("Could not find template %s"%(template_id,))

    resource_type_defs = {
        'NETWORK' : {},
        'NODE'    : {},
        'LINK'    : {},
        'GROUP'   : {},
    }
    for tt in template.templatetypes:
        resource_type_defs[tt.resource_type][tt.id] = tt

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
            errors.append("Resource %s does not have attribute %s"%
                          (resource.get_name(), ta_dict[ta].attr.name))

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
            rs_unit = rs.dataset.unit
            rs_dimension = units.get_unit_dimension(rs_unit)
            type_dimension = ta_dict[rs.resourceattr.attr_id].attr.dimension
            type_unit = ta_dict[rs.resourceattr.attr_id].unit

            if units.get_unit_dimension(rs_unit) != type_dimension:
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

    net_i = db.DBSession.query(Network).filter(Network.id==network_id).one()

    template_name = etree.SubElement(template_xml, "template_name")
    template_name.text = "TemplateType from Network %s"%(net_i.name)
    layout = _get_layout_as_etree(net_i.layout)

    resources = etree.SubElement(template_xml, "resources")
    if net_i.attributes:
        net_resource    = etree.SubElement(resources, "resource")

        resource_type   = etree.SubElement(net_resource, "type")
        resource_type.text   = "NETWORK"

        resource_name   = etree.SubElement(net_resource, "name")
        resource_name.text   = net_i.name

        layout = _get_layout_as_etree(net_i.layout)
        if layout is not None:
            net_resource.append(layout)

        for net_attr in net_i.attributes:
            _make_attr_element(net_resource, net_attr)

        resources.append(net_resource)

    existing_types = {'NODE': [], 'LINK': [], 'GROUP': []}
    for node_i in net_i.nodes:
        node_attributes = node_i.attributes
        attr_ids = [res_attr.attr_id for res_attr in node_attributes]
        if len(attr_ids) > 0 and attr_ids not in existing_types['NODE']:

            node_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(node_resource, "type")
            resource_type.text   = "NODE"

            resource_name   = etree.SubElement(node_resource, "name")
            resource_name.text   = node_i.node_name

            layout = _get_layout_as_etree(node_i.layout)

            if layout is not None:
                node_resource.append(layout)

            for node_attr in node_attributes:
                _make_attr_element(node_resource, node_attr)

            existing_types['NODE'].append(attr_ids)
            resources.append(node_resource)

    for link_i in net_i.links:
        link_attributes = link_i.attributes
        attr_ids = [link_attr.attr_id for link_attr in link_attributes]
        if len(attr_ids) > 0 and attr_ids not in existing_types['LINK']:
            link_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(link_resource, "type")
            resource_type.text   = "LINK"

            resource_name   = etree.SubElement(link_resource, "name")
            resource_name.text   = link_i.link_name

            layout = _get_layout_as_etree(link_i.layout)

            if layout is not None:
                link_resource.append(layout)

            for link_attr in link_attributes:
                _make_attr_element(link_resource, link_attr)

            existing_types['LINK'].append(attr_ids)
            resources.append(link_resource)

    for group_i in net_i.resourcegroups:
        group_attributes = group_i.attributes
        attr_ids = [group_attr.attr_id for group_attr in group_attributes]
        if len(attr_ids) > 0 and attr_ids not in existing_types['GROUP']:
            group_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(group_resource, "type")
            resource_type.text   = "GROUP"

            resource_name   = etree.SubElement(group_resource, "name")
            resource_name.text   = group_i.group_name

           # layout = _get_layout_as_etree(group_i.layout)

           # if layout is not None:
           #     group_resource.append(layout)

            for group_attr in group_attributes:
                _make_attr_element(group_resource, group_attr)

            existing_types['GROUP'].append(attr_ids)
            resources.append(group_resource)

    xml_string = etree.tostring(template_xml, encoding="unicode")

    return xml_string

def _make_attr_element(parent, resource_attr_i):
    """
        General function to add an attribute element to a resource element.
    """
    attr = etree.SubElement(parent, "attribute")
    attr_i = resource_attr_i.attr

    attr_name      = etree.SubElement(attr, 'name')
    attr_name.text = attr_i.name

    attr_dimension = etree.SubElement(attr, 'dimension')
    attr_dimension.text = attr_i.dimension

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

def get_etree_layout_as_dict(layout_tree):
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
            value = etree.tostring(children[0], pretty_print=True, encoding="unicode")
        layout_dict[name] = value
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
