# (c) Copyright 2013, 2014, University of Manchester
#
# HydraLib is free software: you can redistribute it and/or modify
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
# -*- coding: utf-8 -*-

__all__ = ['set_resource_types', 'validate_template']

import os
import logging
log = logging.getLogger(__name__)

from lxml import etree

from HydraLib import config
from HydraLib import util
from HydraLib.HydraException import HydraPluginError


def set_resource_types(client, xml_template, network,
                       nodetype_dict, linktype_dict,
                       grouptype_dict, networktype):
    log.info("Setting resource types")

    template = client.service.upload_template_xml(xml_template)

    type_ids = dict()
    warnings = []

    for type_name in nodetype_dict.keys():
        for tmpltype in template.types.TemplateType:
            if tmpltype.name == type_name:
                type_ids.update({tmpltype.name: tmpltype.id})
                break

    for type_name in linktype_dict.keys():
        for tmpltype in template.types.TemplateType:
            if tmpltype.name == type_name:
                type_ids.update({tmpltype.name: tmpltype.id})
                break

    for type_name in grouptype_dict.keys():
        for tmpltype in template.types.TemplateType:
            if tmpltype.name == type_name:
                type_ids.update({tmpltype.name: tmpltype.id})
                break

    for tmpltype in template.types.TemplateType:
        if tmpltype.name == networktype:
            type_ids.update({tmpltype.name: tmpltype.id})
            break

    args = client.factory.create('hyd:ResourceTypeDefArray')
    if type_ids[networktype]:
        args.ResourceTypeDef.append(dict(
            ref_key='NETWORK',
            ref_id=network.id,
            type_id=type_ids[networktype],
        ))

    if network.nodes:
        for node in network.nodes.Node:
            for typename, node_name_list in nodetype_dict.items():
                if type_ids[typename] and node.name in node_name_list:
                    args.ResourceTypeDef.append(dict(
                        ref_key='NODE',
                        ref_id=node.id,
                        type_id=type_ids[typename],
                    ))
    else:
        warnings.append("No nodes found when setting template types")

    if network.links:
        for link in network.links.Link:
            for typename, link_name_list in linktype_dict.items():
                if type_ids[typename] and link.name in link_name_list:
                    args.ResourceTypeDef.append(dict(
                        ref_key='LINK',
                        ref_id=link.id,
                        type_id=type_ids[typename],
                    ))
    else:
        warnings.append("No links found when setting template types")

    if network.resourcegroups:
        for group in network.resourcegroups.ResourceGroup:
            for typename, group_name_list in grouptype_dict.items():
                if type_ids[typename] and group.name in group_name_list:
                    args.ResourceTypeDef.append(dict(
                        ref_key='GROUP',
                        ref_id=group.id,
                        type_id=type_ids[typename],
                    ))
    else:
        warnings.append("No resourcegroups found when setting template types")

    client.service.assign_types_to_resources(args)
    return warnings


def validate_template(template_file, connection):

    log.info('Validating template file (%s).' % template_file)

    with open(template_file) as f:
        xml_template = f.read()

    template_xsd_path = os.path.expanduser(config.get('templates',
                                                      'template_xsd_path'))
    log.info("Template xsd: %s", template_xsd_path)
    xmlschema_doc = etree.parse(template_xsd_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)
    xml_tree = etree.fromstring(xml_template)

    try:
        xmlschema.assertValid(xml_tree)
    except etree.DocumentInvalid as e:
        raise HydraPluginError('Template validation failed: ' + e.message)

    template_dict = {'name': xml_tree.find('template_name').text,
                     'resources': {}
                     }

    attributes = []

    for r in xml_tree.find('resources'):
        resource_dict = {}
        resource_name = r.find('name').text
        resource_type = r.find('type').text
        resource_dict['type'] = resource_type
        resource_dict['name'] = resource_name
        resource_dict['attributes'] = {}
        for attr in r.findall("attribute"):
            attr_dict = {}
            attr_name = attr.find('name').text
            attr_dict['name'] = attr_name
            if attr.find('dimension') is not None:
                attr_dict['dimension'] = attr.find('dimension').text
            if attr.find('unit') is not None:
                attr_dict['unit'] = attr.find('unit').text
            if attr.find('is_var') is not None:
                attr_dict['is_var'] = attr.find('is_var').text
            if attr.find('data_type') is not None:
                attr_dict['data_type'] = attr.find('data_type').text

            attributes.append({'name': attr_name,
                               'dimen': attr_dict['dimension']})

            restction_xml = attr.find("restrictions")
            attr_dict['restrictions'] = \
                util.get_restriction_as_dict(restction_xml)
            resource_dict['attributes'][attr_name] = attr_dict

        if template_dict['resources'].get(resource_type):
            template_dict['resources'][resource_type][resource_name] = \
                resource_dict
        else:
            template_dict['resources'][resource_type] = {resource_name:
                                                         resource_dict}

    stored_attrs = connection.call('get_attributes', {'attrs': attributes})
    attr_dict = {}
    for a in stored_attrs:
        if a:
            attr_dict[(a['name'], a.get('dimen'))] = a['id']

    log.info("Template attributes retrieved!")

    for rt in template_dict['resources'].values():
        for t in rt.values():
            for a in t['attributes'].values():
                a['id'] = attr_dict.get((a['name'], a.get('dimension')))

    log.info("Template attributes updated with IDS")

    log.info("Template OK")

    return template_dict
