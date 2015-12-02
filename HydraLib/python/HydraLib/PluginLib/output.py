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

__all__ = ['create_xml_response', 'write_progress', 'write_output',
           'validate_plugin_xml']

import os
import logging
log = logging.getLogger(__name__)

from lxml import etree
from lxml.etree import XMLSyntaxError, ParseError

from HydraLib import config
from HydraLib.HydraException import HydraPluginError


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
    file_string = "<file>%s</file>"

    if scenario_ids is None:
        scenario_ids = []

    xml_string = xml_string % dict(
        plugin_name = plugin_name,
        network_id = network_id,
        scenario_list = "\n".join([scenario_string % scen_id
                                   for scen_id in scenario_ids]),
        message = message if message is not None else "",
        error_list = "\n".join([error_string%error for error in errors]),
        warning_list = "\n".join([warning_string%warning for warning in warnings]),
        file_list = "\n".join([file_string % f for f in files]),
    )

    return xml_string


def write_progress(x, y):
    """
        Format and print a progress message to stdout so that
        a UI or other can pick it up and use it.
    """
    msg = "!!Progress %s/%s" % (x, y)
    print msg


def write_output(text):
    """
        Format and print a freeform message to stdout so that
        the UI or other can pick it up and use it
    """
    msg = "!!Output %s" % (text,)
    print msg


def validate_plugin_xml(plugin_xml_file_path):
    log.info('Validating plugin xml file (%s).' % plugin_xml_file_path)

    try:
        with open(plugin_xml_file_path) as f:
            plugin_xml = f.read()
    except:
        raise HydraPluginError("Couldn't find plugin.xml.")

    try:
        plugin_xsd_path = os.path.expanduser(config.get('plugin',
                                                        'plugin_xsd_path'))
        log.info("Plugin Input xsd: %s", plugin_xsd_path)
        xmlschema_doc = etree.parse(plugin_xsd_path)
        xmlschema = etree.XMLSchema(xmlschema_doc)
        xml_tree = etree.fromstring(plugin_xml)
    except XMLSyntaxError, e:
        raise HydraPluginError("There is an error in your XML syntax: %s" % e)
    except ParseError, e:
        raise HydraPluginError("There is an error in your XML: %s" % e)
    except Exception, e:
        log.exception(e)
        raise HydraPluginError("An unknown error occurred with the plugin xsd: %s"%e.message)

    try:
        xmlschema.assertValid(xml_tree)
    except etree.DocumentInvalid as e:
        raise HydraPluginError('Plugin validation failed: ' + e.message)

    log.info("Plugin XML OK")
