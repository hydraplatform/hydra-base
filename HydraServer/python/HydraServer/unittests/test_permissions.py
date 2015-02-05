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

import server
import suds
import datetime

class LoginTest(server.SoapServerTest):

    #This relies on there being a user named 'root' with an empty password.
    def test_good_login(self):
        login_response = self.client.service.login('root', '')
        assert login_response is not None, "Login did not work correctly!"

    def test_bad_login(self):
        login_response = None
        try:
            login_response = self.client.service.login('root', 'invalid_password')
        except Exception, e:
            assert e.fault.faultcode.find("AuthenticationError") >= 0, \
                                        "An unexpected excepton was thrown!"

        assert login_response is None, "Unexpected successful login."

    def test_logout(self):
        msg = self.logout('root')
        assert msg == 'OK', "Logout failed."
        #log back in so that the tear down need not be affected.
        self.login('root', '')


#class PermissionTest(server.SoapServerTest):
#    def test_allow_add_network(self):
#        #root is an admin user, with add_network rights.
#        #As wit other tests, this should work fine.
#        network = self.create_network_with_data()
#        assert network is not None
#
#        #Add a user
#        user = self.create_user('manager')
#        #Give this user the role of 'manager' who does not have add_network rights.
#        role =  self.client.service.get_role_by_code('manager')
#
#        self.client.service.set_user_role(user.id, role.id)
#        #Check that permission is denied.
#        old_client = self.client
#        new_client = server.connect()
#        self.client = new_client
#        self.login("manager", 'password')
#        self.assertRaises(suds.WebFault, self.create_network_with_data)
#        self.client.service.logout("manager")
#        self.client = old_client
#
class SharingTest(server.SoapServerTest):

    def test_share_network(self):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client
        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()
        network_2 = self.create_network_with_data()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        self.client.service.share_network(network_1.id, ["UserB"], 'Y')
        #Let User C view and edit network 2; (read_only is 'N')
        self.client.service.share_network(network_2.id, ["UserC"], 'N', 'Y')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        net1 = self.client.service.get_network(network_1.id)
        try:
            net2 = self.client.service.get_network(network_2.id)
        except Exception, e:
            print e
            net2 = None

        assert net1 is not None
        assert net2 is None

        self.client.service.logout("UserB")

        self.login("UserC", 'password')
        
        net2 = self.client.service.get_network(network_2.id)
        try:
            net1 = self.client.service.get_network(network_1.id)
        except Exception, e:
            print e
            net1 = None

        assert net1 is None
        assert net2 is not None

        #Now try to set the permission on network 2, so that user A, the creator
        #of the network, has their rights revolked. This should not succeed.
        self.assertRaises(suds.WebFault, self.client.service.set_network_permission,network_2.id, ["UserA"], 'N', 'N')

        self.client.service.logout("UserC")

        self.client = old_client

    def test_unshare_network(self):
        
        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client
        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()

        self.client.service.share_network(network_1.id, ["UserB"], 'Y', 'N')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        net1 = self.client.service.get_network(network_1.id)
        assert net1 is not None

        self.client.service.logout("UserB")


        self.login("UserA", 'password')
        self.client.service.set_network_permission(network_1.id, ["UserB"], 'N', 'Y')
        self.client.service.logout("UserA")

        #re-login as user B and try to access the formerly accessible project
        self.login("UserB", 'password')
        try:
            self.client.service.get_network(network_1.id)
        except Exception, e:
            assert e.fault.faultcode.find("HydraError") > 0
            assert e.fault.faultstring.find("Permission denied.") >= 0
        self.client.service.logout("UserB")

        self.client = old_client

    def test_share_project(self):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client
        self.login("UserA", 'password')
        
        #create a project with two networks.
        network_1 = self.create_network_with_data(new_proj=True)
        network_2 = self.create_network_with_data(network_1.project_id)

        #Share a project which is read only with User B
        self.client.service.share_project(network_1.project_id, ["UserB"], 'Y', 'N')

        #share a network for editing with user C. THis should make
        #the project read accessible, but only one of the networks in the project.
        self.client.service.share_network(network_2.id, ["UserC"], 'N', 'N')

        self.client.service.logout("UserA")
        
        #User B should be able to see the project but not edit it or anything in it.
        self.login("UserB", 'password')
       
        userb_networks = self.client.service.get_networks(network_1.project_id)
        assert len(userb_networks.Network) == 2

        userb_networks.Network[0].description = "Updated description"
        try:
            self.client.service.update_network(userb_networks.Network[0])
        except Exception, e:
            assert e.fault.faultcode.find("HydraError") > 0
            assert e.fault.faultstring.find("Permission denied.") >= 0

        self.client.service.logout("UserB")

        #User C should be able to edit network 2
        self.login("UserC", 'password')
        
        userc_networks = self.client.service.get_networks(network_2.project_id)

        assert len(userc_networks.Network) == 1

        userc_networks.Network[0].description = "Updated description"
        updated_userc_net = self.client.service.update_network(userc_networks.Network[0])
        assert updated_userc_net.description == "Updated description"


        #Now try to set the permission on network 2, so that user A, the creator
        #of the network, has their rights revolked. This should not succeed.
        self.assertRaises(suds.WebFault, self.client.service.set_project_permission,network_2.project_id, ["UserA"], 'N', 'N')

        self.client.service.logout("UserC")

        self.client = old_client

    def test_unshare_project(self):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client
        self.login("UserA", 'password')
        
        #create a project with two networks.
        network_1 = self.create_network_with_data(new_proj=True)
        self.create_network_with_data(project_id=network_1.project_id)

        #Share a project which is read only with User B
        self.client.service.share_project(network_1.project_id, ["UserB"], 'Y')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        userb_networks = self.client.service.get_networks(network_1.project_id)
        assert len(userb_networks.Network) == 2

        self.client.service.logout("UserB")
        
        #re-login as user A and un-share the project
        self.login("UserA", 'password')
        self.client.service.set_project_permission(network_1.project_id, ["UserB"], 'N', 'N')
        self.client.service.logout("UserA")
      
        #re-login as user B and try to access the formerly accessible project
        self.login("UserB", 'password')
        try:
            userb_networks = self.client.service.get_networks(network_1.project_id)
        except Exception, e:
            assert e.fault.faultcode.find("HydraError") > 0
            assert e.fault.faultstring.find("Permission denied.") >= 0
        self.client.service.logout("UserB")

        #reset the client to the 'root' client for a consistent tearDown
        self.client = old_client

    def test_sharing_shared_network(self):

        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = server.connect()
        self.client = new_client
        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data(new_proj=True)

        #share the whole project with user B, and allow them to share.
        self.client.service.share_project(network_1.project_id, ["UserB"], 'Y', 'Y')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        self.assertRaises(suds.WebFault, self.client.service.share_project, network_1.project_id, ["UserC"], 'Y', 'Y')
        self.assertRaises(suds.WebFault, self.client.service.share_network, network_1.id, ["UserC"], 'Y', 'Y')

        self.client.service.logout("UserB")

        self.client = old_client



if __name__ == '__main__':
    server.run()
