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

__all__ = ['JsonConnection', 'SoapConnection']

import requests
import json

import logging
log = logging.getLogger(__name__)

from suds.client import Client
from suds.plugin import MessagePlugin

from HydraLib import config

from exception import RequestError


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
        return "/%s" % ("/".join(hostname[1:]))


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
            self.url = "%s://%s:%s%s/json" % (protocol, hostname, port, path)
        log.info("Setting URL %s", self.url)
        self.app_name = app_name

        self.session_id = session_id

    def call(self, func, args):
        log.info("Calling: %s" % (func))
        call = {func: args}
        headers = {'Content-Type': 'application/json',
                   'session_id': self.session_id,
                   'app_name': self.app_name,
                   }
        r = requests.post(self.url, data=json.dumps(call), headers=headers)
        if not r.ok:
            try:
                resp = json.loads(r.content)
                err = "%s:%s" % (resp['faultcode'], resp['faultstring'])
            except:
                log.debug("Headers: %s"%headers)
                log.debug("Url: %s"%self.url)
                log.debug("Content: %s"%json.dumps(call))

                if r.content != '':
                    err = r.content
                else:
                    err = "An unknown server has occurred."

                if self.url.find('soap') > 0:
                    log.info('The URL %s contains "soap". Is the wrong URL being used?', self.url)
                    err.append(' -- A shot in the dark: the URL contains the word "soap".'+
                                ', but this is a JSON-based plugin.' +
                                ' Perhaps the wrong URL is being specified?')

            raise RequestError(err)

        ret_obj = json.loads(r.content, object_hook=object_hook)

        log.info('done')

        return ret_obj

    def login(self, username=None, password=None):
        if username is None:
            username = config.get('hydra_client', 'user')
        if password is None:
            password = config.get('hydra_client', 'password')
        login_params = {'username': username, 'password': password}

        resp = self.call('login', login_params)
        #set variables for use in request headers
        self.session_id = resp.session_id

        log.info("Session ID=%s", self.session_id)

        return self.session_id


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
            self.url = "%s://%s:%s%s/json" % (protocol, hostname, port, path)
        log.info("Setting URL %s", self.url)

        self.app_name = app_name
        self.session_id = session_id
        self.retxml = False
        self.client = Client(self.url, timeout=3600, plugins=[FixNamespace()],
                             retxml=self.retxml)
        self.client.add_prefix('hyd', 'soap_server.hydra_complexmodels')

        cache = self.client.options.cache
        cache.setduration(days=10)

    def login(self):
        """Establish a connection to the specified server. If the URL of the
        server is not specified as an argument of this function, the URL
        defined in the configuration file is used."""

        # Connect
        token = self.client.factory.create('RequestHeader')
        if self.session_id is None:
            user = config.get('hydra_client', 'user')
            passwd = config.get('hydra_client', 'password')
            login_response = self.client.service.login(user, passwd)
            token.user_id = login_response.user_id
            session_id = login_response.session_id
            token.username = user

        token.session_id = session_id
        self.client.set_options(soapheaders=token)

        return session_id


def object_hook(x):
    return JSONObject(x)


class JSONObject(dict):
    def __init__(self, obj_dict):
        for k, v in obj_dict.items():
            self[k] = v
            setattr(self, k, v)
