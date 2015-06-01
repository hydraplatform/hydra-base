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
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import logging

import util

global CLIENT
CLIENT = None

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)

def connect():
    return util.connect()

class SoapServerTest(unittest.TestCase):
    url = None
    def setUp(self):
        logging.getLogger('suds').setLevel(logging.ERROR)
        logging.getLogger('suds.client').setLevel(logging.CRITICAL)
        logging.getLogger('suds.metrics').setLevel(logging.CRITICAL)
        # Clear SUDS cache:
        #shutil.rmtree(os.path.join(tmp(), 'suds'), True)
        global CLIENT
        if CLIENT is None:
            CLIENT = util.connect(self.url)

        self.client = CLIENT

        self.create_user("UserA")
        self.create_user("UserB")
        self.create_user("UserC")
        self.project_id = self.create_project().id

    def tearDown(self):
        log.debug("Tearing down")
        self.logout('root')

    def login(self, username, password):
        return util.login(self.client, username, password)

    def logout(self, username):
        return util.logout(self.client, username)

    def create_user(self, name):
        return util.create_user(self.client, name)

    def create_template(self):
        return util.create_template(self.client)

    def create_project(self, name=None):
        return util.create_project(self.client, name)

    def create_link(self, link_id, node_1_name, node_2_name, node_1_id, node_2_id):
        return util.create_link(self.client, link_id, node_1_name, node_2_name, node_1_id, node_2_id)

    def create_node(self,node_id, attributes=None, node_name="Test Node Name"):
        return util.create_node(self.client,node_id, attributes=None, node_name="Test Node Name")
    def create_attr(self, name="Test attribute", dimension="dimensionless"):
        return util.create_attr(self.client, name, dimension)

    def build_network(self, project_id=None, num_nodes=10, new_proj=True,
                      map_projection='EPSG:4326'):
        return util.build_network(self.client, project_id, num_nodes, new_proj, map_projection)

    def create_network_with_data(self, project_id=None, num_nodes=10,
                                 ret_full_net=True, new_proj=False,
                                 map_projection='EPSG:4326'):
        if project_id is None and new_proj is False:
            project_id = self.project_id
        return util.create_network_with_data(self.client, project_id, num_nodes,ret_full_net, new_proj,map_projection)

    def check_network(self, request_net, response_net):
        return util.check_network(self.client, request_net, response_net)

    def create_scalar(self, ResourceAttr, val=1.234):
        return util.create_scalar(self.client, ResourceAttr, val)

    def create_descriptor(self, ResourceAttr, val="test"):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        return util.create_descriptor(self.client, ResourceAttr, val)

    def create_timeseries(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        #[[[1, 2, "hello"], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]]]

        return util.create_timeseries(self.client, ResourceAttr)

    def create_array(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        #[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        return util.create_array(self.client, ResourceAttr)

def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
