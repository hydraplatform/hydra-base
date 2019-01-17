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
import hydra_base as hb
from hydra_base.exceptions import HydraError
from hydra_base.lib.objects import JSONObject
from fixtures import *
import pytest
import util
import logging
log = logging.getLogger(__name__)
import util

import server
import suds

class TestLogin():
    """
        Test for logins & logouts etc.
    """

    #This relies on there being a user named 'root' with an empty password.
    def test_good_login(self, session):
        login_response = hb.login('root', '')
        assert login_response is not None, "Login did not work correctly!"

    def test_bad_login(self, session):
        login_response = None

        try:
            login_response = hb.login('root', 'invalid_password')
        except Exception as e:
            print(e)
            assert e.code == '0000', \
                            "An unexpected excepton was thrown!"

        assert login_response is None, "Unexpected successful login."

    def test_logout(self, session):
        msg = hb.logout('root')
        assert msg == 'OK', "Logout failed."
        #log back in so that the tear down need not be affected.
        hb.login('root', '')


class TestSharing():

    def test_share_network(self, session, network_with_data, second_network_with_data):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down.
        #old_client = self.client
        #new_client = server.connect()
        #self.client = new_client
        (current_user_id, session_id) = hb.login("UserA", 'password')

        network_1 =  network_with_data
        network_2 =  second_network_with_data
        #network_2 =  util.create_network_with_data(project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326')
        #network_1 = self.create_network_with_data()
        #network_2 = self.create_network_with_data()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        try:
            hb.share_network(network_1.id, ["UserB"], 'Y', 'N', user_id=pytest.root_user_id)
        except Exception as e:
            print(e)
            assert e.code != 300, "Error in sharing network_1"

        #Let User C view and edit network 2; (read_only is 'N')
        try:
            hb.share_network(network_2.id, ["UserC"], 'N', 'Y', user_id=pytest.root_user_id)
        except Exception as e:
            print(e)
            assert e.code != 300, "Error in sharing network_2"

        hb.logout("UserA")

        (current_user_id, session_id) = hb.login("UserB", 'password')
        # Note: UserB is an admin and can see everything

        try:
            net1 = hb.get_network(network_1.id, user_id=current_user_id)
        except Exception as e:
            print(e)
            net1 = None

        try:
            net2 = hb.get_network(network_2.id, user_id=current_user_id)
        except Exception as e:
            print(e)
            net2 = None

        assert net1 is not None
        assert net2 is not None

        hb.logout("UserB")

        (current_user_id, session_id) = hb.login("UserC", 'password')
        # Note: UserC is not an admin and cannot see everything

        try:
            net2 = hb.get_network(network_2.id, user_id=current_user_id)
        except Exception as e:
            print(e)
            net2 = None

        try:
            net1 = hb.get_network(network_1.id, user_id=current_user_id)
        except Exception as e:
            print(e)
            net1 = None

        assert net1 is None
        assert net2 is not None

        #Now try to set the permission on network 2, so that user A, the creator
        #of the network, has their rights revolked. This should not succeed.
        #self.assertRaises(suds.WebFault, hb.set_network_permission,network_2.id, ["UserA"], 'N', 'N')

        hb.set_network_permission(network_2.id, ["UserA"], 'N', 'N', 'N', user_id=current_user_id)

        hb.logout("UserC")

        #self.client = old_client

    def test_unshare_network(self, session, network_with_data):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down.
        # old_client = self.client
        # new_client = server.connect()
        # self.client = new_client

        (current_user_id, session_id) = hb.login("UserA", 'password')


        # network_1 = self.create_network_with_data()
        network_1 =  network_with_data

        hb.share_network(network_1.id, ["UserB"], 'Y', 'N', user_id=current_user_id)

        hb.logout("UserA")

        (current_user_id, session_id) = hb.login("UserB", 'password')

        net1 = hb.get_network(network_1.id, user_id=current_user_id)
        assert net1 is not None

        hb.logout("UserB")


        (current_user_id, session_id) = hb.login("UserA", 'password')
        hb.set_network_permission(network_1.id, ["UserB"], 'N', 'Y', 'N', user_id=current_user_id)
        hb.logout("UserA")

        #re-login as user B and try to access the formerly accessible project
        (current_user_id, session_id) = hb.login("UserB", 'password')

        try:
            hb.get_network(network_1.id, user_id=current_user_id)
        except Exception as e:
            print(e)
            assert e.fault.faultcode.find("HydraError") > 0
            assert e.fault.faultstring.find("Permission denied.") >= 0
        hb.logout("UserB")
        # self.client = old_client

    def test_share_project(self, session, network_with_data):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down.
        # old_client = self.client
        # new_client = server.connect()
        # self.client = new_client
        """
        Continue from here to change tests
        """
        pass

        (current_user_id, session_id) = hb.login("UserA", 'password')

        #create a project with two networks.
        # network_1 = self.create_network_with_data(new_proj=True)
        # network_2 = self.create_network_with_data(network_1.project_id)

        network_1 =  network_with_data
        network_2 =  util.create_network_with_data(network_1.project_id)

        #Share a project which is read only with User B
        hb.share_project(network_1.project_id, ["UserB"], 'Y', 'N')

        #share a network for editing with user C. THis should make
        #the project read accessible, but only one of the networks in the project.
        hb.share_network(network_2.id, ["UserC"], 'N', 'N')

        hb.logout("UserA")

        #User B should be able to see the project but not edit it or anything in it.
        (current_user_id, session_id) = hb.login("UserB", 'password')

        userb_networks = hb.get_networks(network_1.project_id)
        assert len(userb_networks.Network) == 2

        userb_networks.Network[0].description = "Updated description"
        try:
            hb.update_network(userb_networks.Network[0])
        except Exception as e:
            assert e.fault.faultcode.find("HydraError") > 0
            assert e.fault.faultstring.find("Permission denied.") >= 0

        hb.logout("UserB")

        #User C should be able to edit network 2
        (current_user_id, session_id) = hb.login("UserC", 'password')

        userc_networks = hb.get_networks(network_2.project_id)

        assert len(userc_networks.Network) == 1

        userc_networks.Network[0].description = "Updated description"
        updated_userc_net = hb.update_network(userc_networks.Network[0])
        assert updated_userc_net.description == "Updated description"


        #Now try to set the permission on network 2, so that user A, the creator
        #of the network, has their rights revolked. This should not succeed.
        self.assertRaises(suds.WebFault, hb.set_project_permission,network_2.project_id, ["UserA"], 'N', 'N')

        hb.logout("UserC")

        # self.client = old_client

    def test_unshare_project(self, session, network_with_data):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down.
        # old_client = self.client
        # new_client = server.connect()
        # self.client = new_client
        return

        hb.login("UserA", 'password')

        #create a project with two networks.
        # network_1 = self.create_network_with_data(new_proj=True)
        network_1 = network_with_data(new_proj=True)
        # self.create_network_with_data(project_id=network_1.project_id)
        network_with_data(project_id=network_1.project_id)

        #Share a project which is read only with User B
        hb.share_project(network_1.project_id, ["UserB"], 'Y')

        hb.logout("UserA")

        hb.login("UserB", 'password')

        userb_networks = hb.get_networks(network_1.project_id)
        assert len(userb_networks.Network) == 2

        hb.logout("UserB")

        #re-login as user A and un-share the project
        hb.login("UserA", 'password')
        hb.set_project_permission(network_1.project_id, ["UserB"], 'N', 'N')
        hb.logout("UserA")

        #re-login as user B and try to access the formerly accessible project
        hb.login("UserB", 'password')
        try:
            userb_networks = hb.get_networks(network_1.project_id)
        except Exception as e:
            assert e.fault.faultcode.find("HydraError") > 0
            assert e.fault.faultstring.find("Permission denied.") >= 0
        hb.logout("UserB")

        #reset the client to the 'root' client for a consistent tearDown
        # self.client = old_client

    def test_sharing_shared_network(self, session, network_with_data):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down.
        # old_client = self.client
        # new_client = server.connect()
        # self.client = new_client
        return


        hb.login("UserA", 'password')

        # network_1 = self.create_network_with_data(new_proj=True)

        network_1 = network_with_data(new_proj=True)

        #share the whole project with user B, and allow them to share.
        hb.share_project(network_1.project_id, ["UserB"], 'Y', 'Y')

        hb.logout("UserA")

        hb.login("UserB", 'password')

        self.assertRaises(suds.WebFault, hb.share_project, network_1.project_id, ["UserC"], 'Y', 'Y')
        self.assertRaises(suds.WebFault, hb.share_network, network_1.id, ["UserC"], 'Y', 'Y')

        hb.logout("UserB")

        # self.client = old_client



if __name__ == '__main__':
    pytest.main(['-v', __file__])
