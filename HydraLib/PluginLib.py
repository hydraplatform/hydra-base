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

from lxml import etree
from lxml.etree import XMLSyntaxError, ParseError

from HydraException import HydraPluginError
from hydra_dateutil import get_datetime, get_time_period 
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from units import Units

import requests
import json
import util
import re

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

class RequestError(HydraPluginError):
    pass

class JSONPlugin(object):
    def connect(self, args):
        self.session_id = args.session_id
        self.server_url = args.server_url
        self.app_name = self.__class__.__bases__[0].__name__
        
        self.connection = JsonConnection(self.server_url, self.session_id, self.app_name)

        if self.session_id is None:
            self.session_id = self.connection.login()

        self.units = Units()

    def parse_time_step(self, time_step, target='s'):
        """
            Read in the time step and convert it to seconds.
        """
        log.info("Parsing time step %s", time_step)
        # export numerical value from string using regex
        value = re.findall(r'\d+', time_step)[0]
        valuelen = len(value)

        try:
            value = float(value)
        except:
            HydraPluginError("Unable to extract number of time steps (%s) from time step %s"%(value, time_step))

        units = time_step[valuelen:].strip()

        period = get_time_period(units)

        converted_time_step = self.units.convert(value, period, target)

        return float(converted_time_step), value, units

    def get_time_axis(self, start_time, end_time, time_step, time_axis=None):
        """
            Create a list of datetimes based on an start time, end time and time step.
            If such a list is already passed in, then this is not necessary.

            Often either the start_time, end_time, time_step is passed into an app
            or the time_axis is passed in directly. This function returns a time_axis
            in both situations.
        """
        if time_axis is not None:
            return time_axis

        else:
            if start_time is None:
                raise HydraPluginError("A start time must be specified")
            if end_time is None:
                raise HydraPluginError("And end time must be specified")
            if time_step is None:
                raise HydraPluginError("A time-step must be specified")

            start_date = get_datetime(start_time)
            end_date   = get_datetime(end_time)
            delta_t, value, units = self.parse_time_step(time_step)
            
            time_axis = [start_date]
            
            value=int(value)
            while start_date <end_date:
                #Months and years are a special case, so treat them differently
                if(units.lower()== "mon"):
                    start_date=start_date+relativedelta(months=value)
                elif (units.lower()== "yr"):
                    start_date=start_date+relativedelta(years=value)
                else:
                    start_date += timedelta(seconds=delta_t)
                time_axis.append(start_date)
            return time_axis

class JsonConnection(object):

    def __init__(self, url=None, session_id=None, app_name=None):
        if url is None:
            port = config.getint('hydra_client', 'port', 80)
            domain = config.get('hydra_client', 'domain', '127.0.0.1')
            path = config.get('hydra_client', 'json_path', 'json')
            #The domain may or may not specify the protocol, so do a check.
            if domain.find('http') == -1:
                self.url = "http://%s:%s/%s" % (domain, port, path)
            else:
                self.url = "%s:%s/%s" % (domain, port, path)
        else:
            log.info("Using user-defined URL: %s", url)
            port = _get_port(url)
            hostname = _get_hostname(url)
            path = _get_path(url)
            protocol = _get_protocol(url)
            self.url = "%s://%s:%s%s/json"%(protocol,hostname,port,path)
        log.info("Setting URL %s", self.url)
        self.app_name = app_name

        self.session_id = session_id

    def call(self, func, args):
        log.info("Calling: %s"%(func))
        call = {func:args}
        headers = {
                    'Content-Type': 'application/json',
                    'session_id'  : self.session_id,
                    'app_name'    : self.app_name,
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
            raise RequestError(err)

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


class SOAPPlugin(object):
    def connect(self, args):
        self.session_id = args.session_id
        self.server_url = args.server_url
        self.app_name = self.__class__.__bases__[0].__name__
        
        self.connection = SoapConnection(self.server_url, self.session_id, self.app_name)
        self.service = self.connection.client.service
        self.factory = self.connection.client.factory

        if self.session_is is None:
            self.session_id = self.connection.login()

        self.units = Units()

class SoapConnection(object):

    def __init__(self, url=None, session_id=None, app_name=None):
        if url is None:
            port = config.getint('hydra_client', 'port', 80)
            domain = config.get('hydra_client', 'domain', '127.0.0.1')
            path = config.get('hydra_client', 'json_path', 'json')
            #The domain may or may not specify the protocol, so do a check.
            if domain.find('http') == -1:
                self.url = "http://%s:%s/%s" % (domain, port, path)
            else:
                self.url = "%s:%s/%s" % (domain, port, path)
        else:
            log.info("Using user-defined URL: %s", url)
            port = _get_port(url)
            hostname = _get_hostname(url)
            path = _get_path(url)
            protocol = _get_protocol(url)
            self.url = "%s://%s:%s%s/json"%(protocol,hostname,port,path)
        log.info("Setting URL %s", self.url)
        
        self.app_name = app_name
        self.session_id = session_id
        self.retxml = False
        self.client = Client(self.url, timeout=3600, plugins=[FixNamespace()], retxml=self.retxml)
        self.client.add_prefix('hyd', 'soap_server.hydra_complexmodels')

        cache = self.client.options.cache
        cache.setduration(days=10)

    def login(self):
        """Establish a connection to the specified server. If the URL of the server
        is not specified as an argument of this function, the URL defined in the
        configuration file is used."""

        # Connect
        token = self.client.factory.create('RequestHeader')
        if self.session_id is None:
            user = config.get('hydra_client', 'user')
            passwd = config.get('hydra_client', 'password')
            login_response = self.client.service.login(user, passwd)
            token.user_id  = login_response.user_id
            session_id     = login_response.session_id
            token.username = user

        token.session_id = session_id
        self.client.set_options(soapheaders=token)

        return session_id

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
    """
        Build the XML string required at the end of each plugin, describing
        the errors, warnings, messages and outputed files, if any of these
        are relevant.
    """
    
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
    except XMLSyntaxError, e:
        raise HydraPluginError("There is an error in your XML syntax: %s"%e)
    except ParseError, e:
        raise HydraPluginError("There is an error in your XML: %s"%e)
    except:
        raise HydraPluginError("Couldn't find xsd to validate plugin.xml! Please check config.")

    try:
        xmlschema.assertValid(xml_tree)
    except etree.DocumentInvalid as e:
        raise HydraPluginError('Plugin validation failed: ' + e.message)

    log.info("Plugin XML OK")

def validate_template(template_file, connection):

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

            attributes.append({'name': attr_name, 'dimen': attr_dict['dimension']})

            restction_xml = attr.find("restrictions")
            attr_dict['restrictions'] = util.get_restriction_as_dict(restction_xml)
            resource_dict['attributes'][attr_name] = attr_dict

        if template_dict['resources'].get(resource_type):
            template_dict['resources'][resource_type][resource_name] = resource_dict
        else:
            template_dict['resources'][resource_type] = {resource_name : resource_dict}

    stored_attrs = connection.call('get_attributes', {'attrs':attributes})
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
