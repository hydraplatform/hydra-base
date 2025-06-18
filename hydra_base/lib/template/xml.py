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

import json
import logging
from decimal import Decimal
from lxml import etree

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import joinedload

from hydra_base import db
from hydra_base.db.model import Template, TemplateType, TypeAttr, Network, Dataset, Metadata
from hydra_base.lib.data import add_dataset
from hydra_base.exceptions import HydraError
from hydra_base import config
from hydra_base.util import dataset_util
from hydra_base.lib import units
from hydra_base.util.permissions import required_perms

from hydra_base.lib.template.utils import check_dimension, get_attr_by_name_and_dimension

log = logging.getLogger(__name__)


@required_perms("get_template")
def get_template_as_xml(template_id, **kwargs):
    """
        Turn a template into an xml template
    """
    template_xml = etree.Element("template_definition")

    template_i = db.DBSession.query(Template).filter(
        Template.id == template_id).options(
            joinedload(Template.templatetypes)\
            .joinedload(TemplateType.typeattrs)\
            .joinedload(TypeAttr.default_dataset)\
            .joinedload(Dataset.metadata)
        ).one()

    template_name = etree.SubElement(template_xml, "template_name")
    template_name.text = template_i.name
    template_description = etree.SubElement(template_xml, "template_description")
    template_description.text = template_i.description
    resources = etree.SubElement(template_xml, "resources")

    for type_i in template_i.templatetypes:
        xml_resource = etree.SubElement(resources, "resource")

        resource_type = etree.SubElement(xml_resource, "type")
        resource_type.text = type_i.resource_type

        name = etree.SubElement(xml_resource, "name")
        name.text = type_i.name

        description = etree.SubElement(xml_resource, "description")
        description.text = type_i.description

        alias = etree.SubElement(xml_resource, "alias")
        alias.text = type_i.alias

        if type_i.layout is not None and type_i.layout != "":
            layout = _get_layout_as_etree(type_i.layout)
            xml_resource.append(layout)

        for type_attr in type_i.typeattrs:
            _make_attr_element_from_typeattr(xml_resource, type_attr)

        resources.append(xml_resource)

    xml_string = etree.tostring(template_xml, encoding="unicode")

    return xml_string

@required_perms("add_template")
def import_template_xml(template_xml, allow_update=True, **kwargs):
    """
        Add the template, type and typeattrs described
        in an XML file.

        Delete type, typeattr entries in the DB that are not in the XML file
        The assumption is that they have been deleted and are no longer required.
    """
    user_id = kwargs.get('user_id')

    template_xsd_path = config.get('templates', 'template_xsd_path')
    xmlschema_doc = etree.parse(template_xsd_path)

    xmlschema = etree.XMLSchema(xmlschema_doc)

    xml_tree = etree.fromstring(template_xml)

    xmlschema.assertValid(xml_tree)

    template_name = xml_tree.find('template_name').text
    template_description = xml_tree.find('template_description')
    if template_description is not None:
        template_description = template_description.text

    template_layout = None
    if xml_tree.find('layout') is not None and \
               xml_tree.find('layout').text is not None:
        layout = xml_tree.find('layout')
        layout_string = get_etree_layout_as_dict(layout)
        template_layout = json.dumps(layout_string)

    try:
        tmpl_i = db.DBSession.query(Template).filter(Template.name == template_name)\
            .options(joinedload(Template.templatetypes)\
            .joinedload(TemplateType.typeattrs)\
            .joinedload(TypeAttr.attr)
            ).one()

        if allow_update == False:
            raise HydraError("Existing Template Found with name %s"%(template_name,))
        else:
            log.debug("Existing template found. name=%s", template_name)
            tmpl_i.layout = template_layout
            tmpl_i.description = template_description
    except NoResultFound:
        log.debug("Template not found. Creating new one. name=%s", template_name)
        tmpl_i = Template(name=template_name,
                          description=template_description, layout=template_layout)
        db.DBSession.add(tmpl_i)

    types = xml_tree.find('resources')
    #Delete any types which are in the DB but no longer in the XML file
    type_name_map = {r.name:r.id for r in tmpl_i.templatetypes}
    attr_name_map = {}
    for type_i in tmpl_i.templatetypes:
        for typeattr in type_i.typeattrs:
            attr_name_map[typeattr.attr.name] = (typeattr.attr.id, typeattr.type_id)

    existing_types = set([r.name for r in tmpl_i.templatetypes])

    new_types = set([r.find('name').text for r in types.findall('resource')])

    types_to_delete = existing_types - new_types

    for type_to_delete in types_to_delete:
        type_id = type_name_map[type_to_delete]
        try:
            type_i = db.DBSession.query(TemplateType).filter(TemplateType.id==type_id).one()
            log.debug("Deleting type %s", type_i.name)
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
            type_i = db.DBSession.query(TemplateType).filter(
                TemplateType.id == type_id).options(
                    joinedload(TemplateType.typeattrs)\
                    .joinedload(TypeAttr.attr)
                ).one()

        else:
            log.debug("Type %s not found, creating new one.", type_name)
            type_i = TemplateType()
            type_i.name = type_name
            tmpl_i.templatetypes.append(type_i)
            type_is_new = True

        if resource.find('alias') is not None:
            type_i.alias = resource.find('alias').text

        if resource.find('description') is not None:
            type_i.description = resource.find('description').text

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
                attr_i = db.DBSession.query(TypeAttr).filter(
                    TypeAttr.attr_id == attr_id,
                    TypeAttr.type_id == type_id).options(joinedload(TypeAttr.attr)).one()
                db.DBSession.delete(attr_i)
                log.debug("Attr %s in type %s deleted", attr_i.attr.name, attr_i.templatetype.name)
            except NoResultFound:
                log.debug("Attr %s not found in type %s",attr_id, type_id)
                continue

        #Add or update type typeattrs
        for attribute in resource.findall('attribute'):
            new_typeattr = _parse_xml_typeattr(type_i, attribute, user_id=user_id)

    db.DBSession.flush()

    return tmpl_i

@required_perms('get_network')
def get_network_as_xml_template(network_id, **kwargs):
    """
        Turn an existing network into an xml template
        using its attributes.
        If an optional scenario ID is passed in, default
        values will be populated from that scenario.
    """
    template_xml = etree.Element("template_definition")

    net_i = db.DBSession.query(Network).filter(Network.id == network_id).one()

    template_name = etree.SubElement(template_xml, "template_name")
    template_name.text = "TemplateType from Network %s"%(net_i.name)
    layout = _get_layout_as_etree(net_i.layout)

    resources = etree.SubElement(template_xml, "resources")
    if net_i.attributes:
        net_resource = etree.SubElement(resources, "resource")

        resource_type = etree.SubElement(net_resource, "type")
        resource_type.text = "NETWORK"

        resource_name = etree.SubElement(net_resource, "name")
        resource_name.text = net_i.name

        layout = _get_layout_as_etree(net_i.layout)
        if layout is not None:
            net_resource.append(layout)

        for net_attr in net_i.attributes:
            _make_attr_element_from_resourceattr(net_resource, net_attr)

        resources.append(net_resource)

    existing_types = {'NODE': [], 'LINK': [], 'GROUP': []}
    for node_i in net_i.nodes:
        node_attributes = node_i.attributes
        attr_ids = [res_attr.attr_id for res_attr in node_attributes]
        if len(attr_ids) > 0 and attr_ids not in existing_types['NODE']:

            node_resource = etree.Element("resource")

            resource_type = etree.SubElement(node_resource, "type")
            resource_type.text = "NODE"

            resource_name = etree.SubElement(node_resource, "name")
            resource_name.text = node_i.node_name

            layout = _get_layout_as_etree(node_i.layout)

            if layout is not None:
                node_resource.append(layout)

            for node_attr in node_attributes:
                _make_attr_element_from_resourceattr(node_resource, node_attr)

            existing_types['NODE'].append(attr_ids)
            resources.append(node_resource)

    for link_i in net_i.links:
        link_attributes = link_i.attributes
        attr_ids = [link_attr.attr_id for link_attr in link_attributes]
        if len(attr_ids) > 0 and attr_ids not in existing_types['LINK']:
            link_resource = etree.Element("resource")

            resource_type = etree.SubElement(link_resource, "type")
            resource_type.text = "LINK"

            resource_name = etree.SubElement(link_resource, "name")
            resource_name.text = link_i.link_name

            layout = _get_layout_as_etree(link_i.layout)

            if layout is not None:
                link_resource.append(layout)

            for link_attr in link_attributes:
                _make_attr_element_from_resourceattr(link_resource, link_attr)

            existing_types['LINK'].append(attr_ids)
            resources.append(link_resource)

    for group_i in net_i.resourcegroups:
        group_attributes = group_i.attributes
        attr_ids = [group_attr.attr_id for group_attr in group_attributes]
        if len(attr_ids) > 0 and attr_ids not in existing_types['GROUP']:
            group_resource = etree.Element("resource")

            resource_type = etree.SubElement(group_resource, "type")
            resource_type.text = "GROUP"

            resource_name = etree.SubElement(group_resource, "name")
            resource_name.text = group_i.group_name


            for group_attr in group_attributes:
                _make_attr_element_from_resourceattr(group_resource, group_attr)

            existing_types['GROUP'].append(attr_ids)
            resources.append(group_resource)

    xml_string = etree.tostring(template_xml, encoding="unicode")

    return xml_string

def _parse_xml_attribute(attribute):
    """
        Parse an attribute as defined in the template XML file
    """
    dimension_i = None

    attribute_name = attribute.find('name').text.strip()

    if attribute.find('dimension') is not None:
        dimension_name = attribute.find('dimension').text

        if dimension_name is not None and dimension_name.strip() != '':
            dimension_i = units.get_dimension_by_name(dimension_name.strip())

    elif attribute.find('unit') is not None:
        # Found the unit
        unit_abbr = attribute.find('unit').text
        if unit_abbr is not None and unit_abbr.strip() != '':
            unit_id = units.get_unit_by_abbreviation(unit_abbr).id
            dimension_i = units.get_dimension_by_unit_id(unit_id,
                                                         do_accept_unit_id_none=True)

    if dimension_i is None:
        attr = get_attr_by_name_and_dimension(attribute_name, None)
    else:
        attr = get_attr_by_name_and_dimension(attribute_name, dimension_i.id)

    db.DBSession.flush()

    return attr

def _parse_xml_typeattr(type_i, attribute, user_id=None):
    """
        convert a typeattr etree element and turn it into a hydra type attr
    """

    attr = _parse_xml_attribute(attribute)

    for ta in type_i.typeattrs:
        if ta.attr_id == attr.id:
            # Find the TypeAttr
            typeattr_i = ta
            break
    else:
        # Creating a new TypeAttr
        typeattr_i = TypeAttr()
        log.debug("Creating type attr: type_id=%s, attr_id=%s", type_i.id, attr.id)
        typeattr_i.type_id = type_i.id
        typeattr_i.attr_id = attr.id
        type_i.typeattrs.append(typeattr_i)
        db.DBSession.add(typeattr_i)

    typeattr_unit_id = None
    if attribute.find('unit') is not None:
        # Found the unit as child at first level
        unit = attribute.find('unit').text
        if unit not in ('', None):
            typeattr_unit_id = units.get_unit_by_abbreviation(unit).id

    if typeattr_unit_id is not None:
        typeattr_i.unit_id = typeattr_unit_id

    check_dimension(typeattr_i)

    if attribute.find('description') is not None:
        typeattr_i.description = attribute.find('description').text

    if attribute.find('properties') is not None:
        properties_string = get_etree_layout_as_dict(attribute.find('properties'))
        typeattr_i.properties = str(properties_string)

    if attribute.find('is_var') is not None:
        typeattr_i.attr_is_var = attribute.find('is_var').text
    if attribute.find('data_type') is not None:
        typeattr_i.data_type = attribute.find('data_type').text

    # Analyzing the "default" node
    if attribute.find('default') is not None:
        default = attribute.find('default')

        dataset_unit_id = None
        if default.find('unit') is not None:
            dataset_unit = default.find('unit').text
            if dataset_unit not in ('', None):
                dataset_unit_id = units.get_unit_by_abbreviation(dataset_unit).id

        if dataset_unit_id is None and typeattr_i.unit_id is not None:
            dataset_unit = typeattr_i.unit_id

        if dataset_unit_id is not None and typeattr_i.unit_id is not None:
            if dataset_unit_id != typeattr_i.unit_id:
                raise HydraError(f"Default value has a unit of {typeattr_i.unit_id}"+
                                 "but the attribute"+
                                 f" says the unit should be: {dataset_unit_id}")

        val = default.find('value').text
        try:
            Decimal(val)
            data_type = 'scalar'
        except:
            data_type = 'descriptor'

        dataset = add_dataset(data_type,
                              val,
                              dataset_unit_id,
                              name="%s Default"%attr.name,
                              user_id=user_id)

        typeattr_i.default_dataset_id = dataset.id

    if attribute.find('restrictions') is not None:
        restriction = str(dataset_util.get_restriction_as_dict(attribute.find('restrictions')))
        typeattr_i.data_restriction = restriction
    else:
        typeattr_i.data_restriction = None


    return typeattr_i

def _make_attr_element_from_typeattr(parent, type_attr_i):
    """
        General function to add an attribute element to a resource element.
        resource_attr_i can also e a type_attr if being called from get_tempalte_as_xml
    """

    attr = _make_attr_element(parent, type_attr_i.attr)

    if type_attr_i.unit_id is not None:
        attr_unit = etree.SubElement(attr, 'unit')
        attr_unit.text = units.get_unit(type_attr_i.unit_id).abbreviation

    attr_is_var = etree.SubElement(attr, 'is_var')
    attr_is_var.text = type_attr_i.attr_is_var

    if type_attr_i.data_type is not None:
        attr_data_type = etree.SubElement(attr, 'data_type')
        attr_data_type.text = type_attr_i.data_type

    if type_attr_i.data_restriction is not None:
        attr_data_restriction = etree.SubElement(attr, 'restrictions')
        attr_data_restriction.text = type_attr_i.data_restriction

    return attr

def _make_attr_element_from_resourceattr(parent, resource_attr_i):
    """
        General function to add an attribute element to a resource element.
    """

    attr = _make_attr_element(parent, resource_attr_i.attr)

    attr_is_var = etree.SubElement(attr, 'is_var')
    attr_is_var.text = resource_attr_i.attr_is_var

    return attr

def _make_attr_element(parent, attr_i):
    """
        create an attribute element from an attribute DB object
    """
    attr = etree.SubElement(parent, "attribute")

    attr_name = etree.SubElement(attr, 'name')
    attr_name.text = attr_i.name

    attr_desc = etree.SubElement(attr, 'description')
    attr_desc.text = attr_i.description

    attr_dimension = etree.SubElement(attr, 'dimension')
    attr_dimension.text = units.get_dimension(attr_i.dimension_id,
                                              do_accept_dimension_id_none=True).name

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
        name = item.find('name').text
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
