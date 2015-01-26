#!/usr/local/bin/python
# (c) Copyright 2013, 2014, University of Manchester
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
import sys
if "./python" not in sys.path:
    sys.path.append("./python")
if "../../HydraLib/trunk/" not in sys.path:
    sys.path.append("../../HydraLib/trunk/")

import os
import unittest
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
from HydraLib import config
from SOAPpy import WSDL
import SOAPpy
from pysimplesoap.client import SoapClient
from HydraLib import PluginLib
import requests
import json

class SoapTest(unittest.TestCase):
    def test_SOAPpy(self):
        url = config.get('hydra_client', 'url')
        print "Connecting to %s"%url
        SOAPpy.Config.debug = 1
        client = WSDL.Proxy(url, namespace="hydra.base")
        WSDL.Config.namespaceStyle = '2001'
        WSDL.Config.strictNamespaces = 0
        for m in client.methods.values():
            m.namespace = client.wsdl.targetNamespace
        #session_id = client.login('root','')
        #session_id = client.get_network(1)

        session_id = client.get_projects()

    def test_PySimpleSOAP(self):
        url = config.get('hydra_client', 'url')
        print "Connecting to %s"%url
        client = SoapClient(wsdl=url)
        client.namespace = 'hydra.base'
        response = client.login("root","")
        client.http_headers['session_id'] = response['loginResult']['session_id']
        client.http_headers['user_id'] = str(int(response['loginResult']['user_id']))
        client.http_headers['username'] = 'root'
        client['RequestHeader'] = client.http_headers
        import pudb; pudb.set_trace()
        net = client.get_network(107)

    def test_SUDS(self):
        client = PluginLib.connect()
        #net = client.service.get_network(network_id=2)
        #sd = client.service.get_scenario_data(2)
        #node_data = client.service.get_node_data(597, 28)
       # print node_data
        networks = client.service.get_networks(119, 'N')
        print(networks[0])
    
    def test_JSON(self):
        user = config.get('hydra_client', 'user')
        passwd = config.get('hydra_client', 'password')
        login_params = {'login':{'username':user, 'password':passwd}}
        r = requests.get('http://127.0.0.1:8080/json', data=json.dumps(login_params))
        r_dict = json.loads(r.content)
        headers = { 'content-type': 'application/json' , 'username':'root',
                   'user_id':r_dict['user_id'], 'session_id':r_dict['session_id']}
        network_call = {'get_node_data':{'node_id':67, 'scenario_id':2}}
        r = requests.get('http://localhost:8080/json', data=json.dumps(network_call), headers=headers)
        data = json.loads(r.content)
        import pudb; pudb.set_trace()

def run():

    unittest.main()


if __name__ == "__main__":
    run() # run all tests
