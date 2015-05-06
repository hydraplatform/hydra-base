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

from multiprocessing import Process
import server
import util

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)

url = "http://ec2-54-229-95-247.eu-west-1.compute.amazonaws.com/hydra-server/soap/?wsdl"

class ConcurrencyTest(server.SoapServerTest):
    def setUp(self):
        self.url = url
        super(ConcurrencyTest, self).setUp()

    def tearDown(self):
        pass

    def test_concurrency(self):
        p1 = Process(target=self.do_work, args=('UserA',))
        p1.start()
        p2 = Process(target=self.do_work, args=('UserB',))
        p2.start()
        p3 = Process(target=self.do_work, args=('UserC',))
        p3.start()
        
    def do_work(self, user):
        client = util.connect(url)
        login_response = client.service.login(user, 'password')
        #If connecting to the cookie-based server, the response is just "OK"
        token = self.client.factory.create('RequestHeader')
        if login_response != "OK":
            token.session_id = login_response.session_id
        token.app_name = "Unit Test"

        client.set_options(cache=None, soapheaders=token)
        n = util.create_network_with_data(client, new_proj=True)
        util.get_network(n.id)

        client.service.logout(user)

def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
