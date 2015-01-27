# (c) Copyright 2013, 2014, University of Manchester
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
# -*- coding: utf-8 -*-

import config
from suds.client import Client
from suds.plugin import MessagePlugin

import os
import logging

log = logging.getLogger(__name__)
from lxml import objectify
from lxml import etree
from lxml.etree import XMLParser
from HydraException import HydraPluginError
import requests
import json
import util

class FixNamespace(MessagePlugin):
    """Hopefully a temporary fix for an unresolved namespace issue.
    """
    def marshalled(self, context):
        self.fix_ns(context.envelope)

    def fix_ns(self, element):
        if element.prefix == 'xs':
            element.prefix = 'ns0'

        for e in element.getChildren():
            self.fix_ns(e)

class HydraResource(object):
    """A prototype for Hydra resources. It supports attributes and groups
    object types by template. This allows to export group nodes by object
    type based on the template used.
    """
    def __init__(self):
        self.name = None
        self.ID = None
        self.attributes = []
        self.groups = []
        self.template = dict()
        self.template[None] = []

    def add_attribute(self, attr, res_attr, res_scen):
        attribute = HydraAttribute(attr, res_attr, res_scen)

        self.attributes.append(attribute)

    def delete_attribute(self, attribute):
        idx = self.attributes.index(attribute)
        del self.attributes[idx]

    def get_attribute(self, attr_name=None, attr_id=None):

        if attr_name is not None:
            return self._get_attr_by_name(attr_name)
        elif attr_id is not None:
            return self._get_attr_by_id(attr_id)

    def set_type(self, types):
        if types is not None:
            for obj_type in types:
                # Add resource type to template dictionary
                if obj_type.template_id not in self.template.keys():
                    self.template[obj_type.template_id] = []
                self.template[obj_type.template_id].append(obj_type.name)
                # Add resource type to default entry holding all resource types
                if obj_type.name not in self.template[None]:
                    self.template[None].append(obj_type.name)

    def group(self, group_id):
        self.groups.append(group_id)
        #attr = self._get_attr_by_name(group_attr)
        #if attr is not None:
        #    group = attr.value.__getitem__(0)
        #    self.groups.append(group)
        #    # The attribute is used for grouping and will not be exported
        #    self.delete_attribute(attr)

    def _get_attr_by_name(self, attr_name):
        for attr in self.attributes:
            if attr.name == attr_name:
                return attr

    def _get_attr_by_id(self, attr_id):
        for attr in self.attributes:
            if attr.attr_id == attr_id:
                return attr


class HydraNetwork(HydraResource):
    """
    """

    description = None
    scenario_id = None
    nodes = []
    links = []
    groups = []
    node_groups = []
    link_groups = []

    def load(self, soap_net, soap_attrs):

        # load network
        resource_scenarios = dict()
        for res_scen in \
                soap_net.scenarios[0].resourcescenarios:
            resource_scenarios.update({res_scen.resource_attr_id: res_scen})
        attributes = dict()
        for attr in soap_attrs:
            attributes.update({attr.id: attr})

        self.name = soap_net.name
        self.ID = soap_net.id
        self.description = soap_net.description
        self.scenario_id = soap_net.scenarios[0].id
        self.set_type(soap_net.types)

        if soap_net.attributes is not None:
            for res_attr in soap_net.attributes:
                self.add_attribute(attributes[res_attr.attr_id],
                                    res_attr,
                                    resource_scenarios.get(res_attr.id))

        # build dictionary of group members:
        if soap_net.scenarios[0].resourcegroupitems is not None:
            groupitems = \
                soap_net.scenarios[0].resourcegroupitems
        else:
            groupitems = []

        nodegroups = dict()
        linkgroups = dict()
        groupgroups = dict()
        log.info("Loading group items")
        for groupitem in groupitems:
            if groupitem.ref_key == 'NODE':
                if groupitem.ref_id not in nodegroups.keys():
                    nodegroups.update({groupitem.ref_id: [groupitem.group_id]})
                else:
                    nodegroups[groupitem.ref_id].append(groupitem.group_id)
            elif groupitem.ref_key == 'LINK':
                if groupitem.ref_id not in linkgroups.keys():
                    linkgroups.update({groupitem.ref_id: [groupitem.group_id]})
                else:
                    linkgroups[groupitem.ref_id].append(groupitem.group_id)
            elif groupitem.ref_key == 'GROUP':
                if groupitem.ref_id not in groupgroups.keys():
                    groupgroups.update({groupitem.ref_id: [groupitem.group_id]})
                else:
                    groupgroups[groupitem.ref_id].append(groupitem.group_id)
        log.info("Loading groups")
        # load groups
        if soap_net.resourcegroups is not None:
            for resgroup in soap_net.resourcegroups:
                new_group = HydraResource()
                new_group.ID = resgroup.id
                new_group.name = resgroup.name
                if resgroup.attributes is not None:
                    for res_attr in resgroup.attributes:
                        new_group.add_attribute(attributes[res_attr.attr_id],
                                                res_attr,
                                                resource_scenarios.get(res_attr.id))
                new_group.set_type(resgroup.types)
                if new_group.ID in groupgroups.keys():
                    new_group.group(groupgroups[new_group.ID])
                self.add_group(new_group)
                del new_group
        log.info("Loading nodes")
        # load nodes
        for node in soap_net.nodes:
            new_node = HydraResource()
            new_node.ID = node.id
            new_node.name = node.name
            if node.attributes is not None:
                for res_attr in node.attributes:
                    new_node.add_attribute(attributes[res_attr.attr_id],
                                            res_attr,
                                            resource_scenarios.get(res_attr.id))

            new_node.set_type(node.types)
            if new_node.ID in nodegroups.keys():
                for gid in nodegroups[new_node.ID]:
                    new_node.group(gid)
            self.add_node(new_node)
            del new_node

        # load links
        log.info("Loading links")
        for link in soap_net.links:
            new_link = HydraResource()
            new_link.ID = link.id
            new_link.name = link.name
            new_link.from_node = self.get_node(node_id=link.node_1_id).name
            new_link.to_node = self.get_node(node_id=link.node_2_id).name
            if link.attributes is not None:
                for res_attr in link.attributes:
                    new_link.add_attribute(attributes[res_attr.attr_id],
                                            res_attr,
                                            resource_scenarios.get(res_attr.id))
            new_link.set_type(link.types)
            if new_link.ID in linkgroups.keys():
                new_link.group(linkgroups[new_link.ID])
            self.add_link(new_link)
            del new_link

    def add_node(self, node):
        self.nodes.append(node)

    def delete_node(self, node):
        pass

    def get_node(self, node_name=None, node_id=None, node_type=None, group=None):
        if node_name is not None:
            return self._get_node_by_name(node_name)
        elif node_id is not None:
            return self._get_node_by_id(node_id)
        elif node_type is not None:
            return self._get_nodes_by_type(node_type)
        elif group is not None:
            return self._get_nodes_by_group(group)

    def add_link(self, link):
        self.links.append(link)

    def delete_link(self, link):
        pass

    def get_link(self, link_name=None, link_id=None, link_type=None, group=None):
        if link_name is not None:
            return self._get_link_by_name(link_name)
        elif link_id is not None:
            return self._get_link_by_id(link_id)
        elif link_type is not None:
            return self._get_links_by_type(link_type)
        elif group is not None:
            return self._get_links_by_group(group)

    def add_group(self, group):
        self.groups.append(group)

    def delete_group(self, group):
        pass

    def get_group(self, **kwargs):
        if kwargs.get('group_name') is not None:
            return self._get_group_by_name(kwargs.get('group_name'))
        elif kwargs.get('group_id') is not None:
            return self._get_group_by_id(kwargs.get('group_id'))
        elif kwargs.get('group_type') is not None:
            return self._get_groups_by_type(kwargs.get('group_type'))
        elif kwargs.get('group') is not None:
            return self._get_groups_by_group(kwargs.get('group'))

    def get_node_types(self, template_id=None):
        node_types = []
        for node in self.nodes:
            for n_type in node.template[template_id]:
                if n_type not in node_types:
                    node_types.append(n_type)
        return node_types

    def get_link_types(self, template_id=None):
        link_types = []
        for link in self.links:
            for l_type in link.template[template_id]:
                if l_type not in link_types:
                    link_types.append(l_type)
        return link_types

    def _get_node_by_name(self, name):
        for node in self.nodes:
            if node.name == name:
                return node

    def _get_node_by_id(self, ID):
        for node in self.nodes:
            if node.ID == ID:
                return node

    def _get_nodes_by_type(self, node_type):
        nodes = []
        for node in self.nodes:
            if node_type in node.template[None]:
                nodes.append(node)
        return nodes

    def _get_nodes_by_group(self, node_group):
        nodes = []
        for node in self.nodes:
            if node_group in node.groups:
                nodes.append(node)
        return nodes

    def _get_link_by_name(self, name):
        for link in self.links:
            if link.name == name:
                return link

    def _get_link_by_id(self, ID):
        for link in self.links:
            if link.ID == ID:
                return link

    def _get_links_by_type(self, link_type):
        links = []
        for link in self.links:
            if link_type in link.template[None]:
                links.append(link)
        return links

    def _get_links_by_group(self, link_group):
        links = []
        for link in self.links:
            if link_group in link.groups:
                links.append(link)
        return links

    def _get_group_by_name(self, name):
        for group in self.groups:
            if group.name == name:
                return group

    def _get_group_by_id(self, ID):
        for group in self.groups:
            if group.ID == ID:
                return group

    def _get_groups_by_type(self, group_type):
        groups = []
        for group in self.groups:
            if group_type in group.template[None]:
                groups.append(group)
        return groups

    def _get_groups_by_group(self, group_group):
        groups = []
        for group in self.groups:
            if group_group in group.groups:
                groups.append(group)
        return groups


class HydraAttribute(object):

    name = None

    attr_id = None
    resource_attr_id = None
    is_var = False

    dataset_id = None
    dataset_type = ''

    value = None

    def __init__(self, attr, res_attr, res_scen):
        self.name = attr.name
        self.attr_id = attr.id
        self.resource_attr_id = res_attr.id
        if res_attr.attr_is_var == 'Y':
            self.is_var = True
        if res_scen is not None:
            self.dataset_id = res_scen.value.id
            self.dataset_type = res_scen.value.type
            self.value = res_scen.value.value

class JSONObject(dict):
    def __init__(self, obj_dict):
        for k, v in obj_dict.items():
            self[k] = v
            setattr(self, k, v)

def object_hook(x):
    return JSONObject(x)

def _get_path(url):
    """
        Find the path in a url. (The bit after the hostname
        and port).
        ex: http://www.google.com/test
        returns: test
    """

    if url.find('http://') == 0:
        url = url.replace('http://', '')
    if url.find('https://') == 0:
        url = url.replace('https://', '')

    hostname = url.split('/')
    if len(hostname) == 1:
        return ''
    else:
        return "/%s"%("/".join(hostname[1:]))

def _get_hostname(url):
    """
        Find the hostname in a url.
        Assume url can take these forms. The () means optional.:
        1: (http(s)://)hostname
        2: (http(s)://)hostname:port
        3: (http(s)://)hostname:port/path
    """

    if url.find('http://') == 0:
        url = url.replace('http://', '')
    if url.find('https://') == 0:
        url = url.replace('https://', '')

    hostname = url.split('/')[0]

    #is a user-defined port specified?
    port_parts = url.split(':')
    if len(port_parts) > 1:
        hostname = port_parts[0]

    return hostname

def _get_port(url):
    """
        Get the port of a url.
        Default port is 80. A specified port
        will come after the first ':' and before the next '/'
    """

    if url.find('http://') == 0:
        url = url.replace('http://', '')
    if url.find('https://') == 0:
        url = url.replace('https://', '')

    port = 80

    url_parts = url.split(':')

    if len(url_parts) == 1:
        return port
    else:
        port_part = url_parts[1]
        port_section = port_part.split('/')[0]
        try:
            int(port_section)
        except:
            return port
        return int(port_section)

    return port

def _get_protocol(url):
    """
        Get the port of a url.
        Default port is 80. A specified port
        will come after the first ':' and before the next '/'
    """

    if url.find('http://') == 0:
        return 'http'
    elif url.find('https://') == 0:
        return 'https'
    else:
        return 'http'

class JsonConnection(object):
    url = None
    session_id = None

    def __init__(self, url=None):
        if url is None:
            port = config.getint('hydra_server', 'port', 8080)
            domain = config.get('hydra_server', 'domain', '127.0.0.1')
            self.url = "http://%s:%s/json"%(domain, port)
        else:
            log.info("Using user-defined URL: %s", url)
            port = _get_port(url)
            hostname = _get_hostname(url)
            path = _get_path(url)
            protocol = _get_protocol(url)
            self.url = "%s://%s:%s%s/json"%(protocol,hostname,port,path)
        log.info("Setting URL %s", self.url)

    def call(self, func, args):
        log.info("Calling: %s"%(func))
        call = {func:args}
        headers = {
                    'Content-Type': 'application/json',
                    'session_id'  : self.session_id,
                    'app_name'    : 'Import CSV'
                }
        r = requests.post(self.url, data=json.dumps(call), headers=headers)
        if not r.ok:
            try:
                resp = json.loads(r.content)
                err = "%s:%s"%(resp['faultcode'], resp['faultstring'])
            except:
                if r.content != '':
                    err = r.content
                else:
                    err = "An unknown server has occurred."
            raise HydraPluginError(err)

        ret_obj = json.loads(r.content, object_hook=object_hook)

        log.info('done')

        return ret_obj


    def login(self, username=None, password=None):
        if username is None:
            username = config.get('hydra_client', 'user')
        if password is None:
            password = config.get('hydra_client', 'password')
        login_params = {'username':username, 'password':password}

        resp = self.call('login', login_params)
        #set variables for use in request headers
        self.session_id = resp.session_id
        log.info("Session ID=%s", self.session_id)
        return self.session_id

def connect(**kwargs):
    """Establish a connection to the specified server. If the URL of the server
    is not specified as an argument of this function, the URL defined in the
    configuration file is used."""

    # Parse keyword arguments
    url = kwargs.get('url')
    if url is None:
        url = config.get('hydra_server', 'url')

    session_id = kwargs.get('session_id')

    retxml = kwargs.get('retxml', False)

    # Connect
    logging.info("Connecting to : %s",url)
    cli = Client(url, timeout=3600, plugins=[FixNamespace()], retxml=retxml)
    cache = cli.options.cache
    cache.setduration(days=10)

    token = cli.factory.create('RequestHeader')
    if session_id is None:
        user = config.get('hydra_client', 'user')
        passwd = config.get('hydra_client', 'password')
        login_response = cli.service.login(user, passwd)
        token.user_id  = login_response.user_id
        session_id     = login_response.session_id
        token.username = user

    token.session_id = session_id
    cli.set_options(soapheaders=token)
    cli.add_prefix('hyd', 'soap_server.hydra_complexmodels')

    return cli

def build_response(xml_string):
    parser = XMLParser(remove_blank_text=True, huge_tree=True)
    parser.set_element_class_lookup(objectify.ObjectifyElementClassLookup())
    objectify.set_default_parser(parser)
    etree_obj = etree.fromstring(xml_string)
    resp = etree_obj.getchildren()[0].getchildren()[0]
    res  = resp.getchildren()[0]
    dict_resp = get_as_dict(res)[1]
    #import pudb; pudb.set_trace()
    obj = objectify.fromstring(xml_string)
    resp = obj.Body.getchildren()[0]
    res  = resp.getchildren()[0]

    return res

def get_as_dict(element):
    return element.tag[element.tag.find('}')+1:], \
            dict(map(get_as_dict, element)) or element.text

def temp_ids(n=-1):
    """
    Create an iterator for temporary IDs for nodes, links and other entities
    that need them. You need to initialise the temporary id first and call the
    next element using the ``.next()`` function::

        temp_node_id = PluginLib.temp_ids()

        # Create a node
        # ...

        Node.id = temp_node_id.next()
    """
    while True:
        yield n
        n -= 1

def create_xml_response(plugin_name, network_id, scenario_ids,
                        errors=[], warnings=[], message=None, files=[]):
    xml_string = """<plugin_result>
    <message>%(message)s</message>
    <plugin_name>%(plugin_name)s</plugin_name>
    <network_id>%(network_id)s</network_id>
    %(scenario_list)s
    <errors>
        %(error_list)s
    </errors>
    <warnings>
        %(warning_list)s
    </warnings>
    <files>
        %(file_list)s
    </files>
</plugin_result>"""

    scenario_string = "<scenario_id>%s</scenario_id>"
    error_string = "<error>%s</error>"
    warning_string = "<warning>%s</warning>"
    file_string = "<file>%s<file>"

    if scenario_ids is None:
        scenario_ids = []

    xml_string = xml_string % dict(
        plugin_name  = plugin_name,
        network_id   = network_id,
        scenario_list = "\n".join([scenario_string % scen_id
                                   for scen_id in scenario_ids]),
        message      = message if message is not None else "",
        error_list   = "\n".join([error_string%error for error in errors]),
        warning_list = "\n".join([warning_string%warning for warning in warnings]),
        file_list = "\n".join([file_string % f for f in files]),
    )

    return xml_string


def write_xml_result(plugin_name, xml_string, file_path=None):
    if file_path is None:
        file_path = config.get('plugin', 'result_file')

    home = os.path.expanduser('~')

    output_file = os.path.join(home, file_path, plugin_name)

    f = open(output_file, 'a')

    output_string = "%%%s%%%s%%%s%%" % (os.getpid(), xml_string, os.getpid())

    f.write(output_string)

    f.close()


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
            ref_key = 'NETWORK',
            ref_id  = network.id,
            type_id = type_ids[networktype],
        ))

    if network.nodes:
        for node in network.nodes.Node:
            for typename, node_name_list in nodetype_dict.items():
                if type_ids[typename] and node.name in node_name_list:
                    args.ResourceTypeDef.append(dict(
                        ref_key = 'NODE',
                        ref_id  = node.id,
                        type_id = type_ids[typename],
                    ))
    else:
        warnings.append("No nodes found when setting template types")

    if network.links:
        for link in network.links.Link:
            for typename, link_name_list in linktype_dict.items():
                if type_ids[typename] and link.name in link_name_list:
                    args.ResourceTypeDef.append(dict(
                        ref_key = 'LINK',
                        ref_id  = link.id,
                        type_id = type_ids[typename],
                    ))
    else:
       warnings.append("No links found when setting template types")

    if network.resourcegroups:
        for group in network.resourcegroups.ResourceGroup:
            for typename, group_name_list in grouptype_dict.items():
                if type_ids[typename] and group.name in group_name_list:
                    args.ResourceTypeDef.append(dict(
                        ref_key = 'GROUP',
                        ref_id  = group.id,
                        type_id = type_ids[typename],
                    ))
    else:
       warnings.append("No resourcegroups found when setting template types")

    client.service.assign_types_to_resources(args)
    return warnings

def parse_suds_array(arr):
    """
        Take a list of nested suds any types and return a python list containing
        a single value, a string or sub lists.
    """
    ret_arr = []
    if hasattr(arr, 'array'):
        sub_arr = arr.array
        if type(sub_arr) is list:
            for s in sub_arr:
                ret_arr.append(parse_suds_array(s))
        else:
            return parse_suds_array(sub_arr)
    elif hasattr(arr, 'item'):
        if type(arr.item) is list:
            for x in arr.item:
                try:
                    val = float(x)
                except:
                    val = str(x)
                ret_arr.append(val)
            return ret_arr
        else:
            return eval(str(arr.item))
    else:
        raise ValueError("Something has gone wrong parsing an array.")
    return ret_arr

def create_dict(arr):
    if type(arr) is not list:
        return arr
    return {'array': [create_sub_dict(arr)]}

def array_dict_to_list(arrdict):
    """Convert an array dict created by 'create_dict()' to a nested list."""
    if 'array' in arrdict.keys():
        arr = []
        for arrdata in arrdict['array']:
            arr.append(array_dict_to_list(arrdata))
    elif'arr_data' in arrdict.keys():
        arr = []
        for arrdata in arrdict['arr_data']:
            arr.append(array_dict_to_list(arrdata))
    elif 'item' in arrdict.keys():
        arr = arrdict['item']

    return arr

def create_sub_dict(arr):
    if arr is None:
        return None

    #Either the array contains sub-arrays or values
    vals = None
    sub_arrays = []
    for sub_val in arr:
        if type(sub_val) is list:
            sub_dict = create_sub_dict(sub_val)
            sub_arrays.append(sub_dict)
        else:
            #if any of the elements of the array is NOT a list,
            #then there are no sub arrays
            vals = arr
            break

    if vals:
        return {'item': vals}

    if sub_arrays:
        return {'array': sub_arrays}

def write_progress(x, y):
    """
        Format and print a progress message to stdout so that
        a UI or other can pick it up and use it.
    """
    msg = "!!Progress %s/%s"%(x, y)
    print msg

def write_output(text):
    """
        Format and print a freeform message to stdout so that
        the UI or other can pick it up and use it
    """
    msg = "!!Output %s"%(text,)
    print msg


def validate_plugin_xml(plugin_xml_file_path):
    log.info('Validating plugin xml file (%s).' % plugin_xml_file_path)

    try:
        with open(plugin_xml_file_path) as f:
            plugin_xml = f.read()
    except:
        raise HydraPluginError("Couldn't find plugin.xml.")

    try:
        plugin_xsd_path = os.path.expanduser(config.get('plugin', 'plugin_xsd_path'))
        log.info("Plugin Input xsd: %s",plugin_xsd_path)
        xmlschema_doc = etree.parse(plugin_xsd_path)
        xmlschema = etree.XMLSchema(xmlschema_doc)
        xml_tree = etree.fromstring(plugin_xml)
    except:
        raise HydraPluginError("Couldn't find xsd to validate plugin.xml! Please check config.")

    try:
        xmlschema.assertValid(xml_tree)
    except etree.DocumentInvalid as e:
        raise HydraPluginError('Plugin validation failed: ' + e.message)

    log.info("Plugin XML OK")

def validate_template(template_file, connection=None):
    """
        Validate a template file against an xsd found in config.
        Pass in a json connection to retrieve the ids for the attributes
        if they are needed.
    """

    log.info('Validating template file (%s).' % template_file)

    with open(template_file) as f:
        xml_template = f.read()

    template_xsd_path = os.path.expanduser(config.get('templates', 'template_xsd_path'))
    log.info("Template xsd: %s",template_xsd_path)
    xmlschema_doc = etree.parse(template_xsd_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)
    xml_tree = etree.fromstring(xml_template)

    try:
        xmlschema.assertValid(xml_tree)
    except etree.DocumentInvalid as e:
        raise HydraPluginError('Template validation failed: ' + e.message)

    template_dict = {'name':xml_tree.find('template_name').text,
                     'resources' : {}
                    }

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

            try:
                stored_attr = connection.call('get_attribute', 
                                                   {'name':attr_name, 
                                                    'dimension': attr_dict.get('dimension')})
                attr_dict['id'] = stored_attr['id']
                log.info("Attribute %s retrieved!", attr_name)
            except Exception, e:
                log.info("Attribute %s (%s) is not on the server", attr_name, attr_dict.get('dimension'))

            restction_xml = attr.find("restrictions")
            attr_dict['restrictions'] = util.get_restriction_as_dict(restction_xml)
            resource_dict['attributes'][attr_name] = attr_dict

        if template_dict['resources'].get(resource_type):
            template_dict['resources'][resource_type][resource_name] = resource_dict
        else:
            template_dict['resources'][resource_type] = {resource_name : resource_dict}

    log.info("Template OK")

    return template_dict



