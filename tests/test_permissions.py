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

import datetime
from .fixtures import *
import pytest
from hydra_base.exceptions import HydraError, PermissionError as HydraPermissionError

class TestLogin():
    """
        Test for logins & logouts etc.
    """

    #This relies on there being a user named 'root' with an empty password.
    def test_good_login(self, session, client):
        login_response = client.login('UserA', 'password')
        assert client.user_id == pytest.user_a.id

    def test_bad_login(self, session, client):
        with pytest.raises(HydraError):
            login_response = client.login('root', 'invalid_password')

    def test_logout(self, session, client):
        client.login('UserA', 'password')
        assert client.user_id is not None
        client.logout()
        assert client.user_id is None


class TestPermission():
    def test_allow_add_network(self, session, client, networkmaker):
        #root is an admin user, with add_network rights.
        #As wit other tests, this should work fine.
        network = networkmaker.create()
        assert network is not None

        #Give this user the role of 'manager' who does not have add_network rights.
        #mimic user d (manager)
        client.user_id = pytest.user_d.id

        with pytest.raises(HydraPermissionError):
            client.add_network({'name':'Test Network'})


class TestSharing():

    def test_share_network(self, session, client, networkmaker):

        network_1 = networkmaker.create()
        network_2 = networkmaker.create()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        client.share_network(network_1.id, ["UserD"], 'Y', 'N')
        #Let User C view and edit network 2; (read_only is 'N')
        client.share_network(network_2.id, ["UserC"], 'N', 'Y')

        #mimic user D (a non-admin)
        client.user_id = pytest.user_d.id
        client.get_network(network_1.id)
        with pytest.raises(HydraPermissionError):
            client.get_network(network_2.id)

        #mimimc user c (a non-admin)
        client.user_id = pytest.user_c.id
        client.get_network(network_2.id)
        with pytest.raises(HydraPermissionError):
            client.get_network(network_1.id)

        #Now try to set the permission on network 2, so that user A, the creator
        #of the network, has their rights revolked. This should not succeed.
        with pytest.raises(HydraPermissionError):
            client.set_network_permission(network_2.id, ["root"], 'N', 'N', 'N')

    def test_unshare_network(self, session, client, networkmaker):

        #mimic user1
        client.user_id = pytest.user_a.id
        network_1 = networkmaker.create()
        client.share_network(network_1.id, ["UserB"], 'Y', 'N')

        #mimic user b
        client.user_id = pytest.user_b.id
        #this should work
        net1 = client.get_network(network_1.id)

        #mimic user a
        client.user_id = pytest.user_a.id
        client.unshare_network(network_1.id, ["UserB"])

        session.commit()

        #mimic user b
        client.user_id = pytest.user_b.id
        #User B can no longer access the network
        with pytest.raises(HydraPermissionError):
            client.get_network(network_1.id)

    def test_share_project(self, session, client, projectmaker, networkmaker):

        #Creat a project
        project = projectmaker.create(share=False)
        #create a project with two networks.
        network_1 = networkmaker.create(project_id=project.id)
        network_2 = networkmaker.create(project_id=project.id)

        #Share a project which is read only with User B
        client.share_project(network_1.project_id, ["UserB"], 'Y', 'N')

        #share a network for editing with user C. THis should make
        #the project read accessible, but only one of the networks in the project.
        client.share_network(network_2.id, ["UserC"], 'N', 'N')

        #mimic user b
        client.user_id = pytest.user_b.id
        userb_networks = client.get_networks(network_1.project_id)
        assert len(userb_networks) == 2

        #not sure why, but delete doesnb't take effect until the session is committed
        session.commit()

        userb_networks[0].description = "Updated description"
        with pytest.raises(HydraPermissionError):
            client.update_network(userb_networks[0])

        #mimic user c
        client.user_id = pytest.user_c.id

        userc_networks = client.get_networks(network_2.project_id)

        assert len(userc_networks) == 1

        userc_networks[0].description = "Updated description"
        updated_userc_net = client.update_network(userc_networks[0])
        assert updated_userc_net.description == "Updated description"

        #Now try to set the permission on network 2, so that root, the creator
        #of the network, has their rights revolked. This should not succeed.
        with pytest.raises(HydraPermissionError):
            client.set_project_permission(network_2.project_id, ["root"], 'N', 'N', 'N')

    def test_unshare_project(self, session, client, projectmaker, networkmaker):
        #Creat a project
        project = projectmaker.create(share=False)
        #create a project with two networks.
        network_1 = networkmaker.create(project_id=project.id)
        network_2 = networkmaker.create(project_id=project.id)

        client.user_id = pytest.user_c.id
        #Make sure user C (a non-admin) can't access user A's project
        with pytest.raises(HydraPermissionError):
            client.get_project(project.id)
        with pytest.raises(HydraPermissionError):
            client.get_networks(project.id)
        with pytest.raises(HydraPermissionError):
            client.get_network(network_1.id)

        #mimic root user
        client.user_id = pytest.root_user_id
        #Share a project which is read only with User C
        client.share_project(project.id, ["UserC"], 'Y', 'N')

        #mimic user b
        client.user_id = pytest.user_c.id
        userb_networks = client.get_networks(project.id)
        assert len(userb_networks) == 2

        #mimic root user
        client.user_id = pytest.root_user_id
        client.unshare_project(project.id, ["UserC"])

        #not sure why, but delete doesnb't take effect until the session is committed
        session.commit()

        #mimic user c
        client.user_id = pytest.user_c.id
        with pytest.raises(HydraPermissionError):
            client.get_networks(project.id)
        with pytest.raises(HydraPermissionError):
            client.get_project(project.id)
        with pytest.raises(HydraPermissionError):
            client.get_network(network_1.id)

    def test_sharing_shared_network(self, session, client, networkmaker):

        network_1 = networkmaker.create()

        #share the whole project with user B, and setting it to read-only, so they
        #cannot share it.
        client.share_project(network_1.project_id, ["UserB"], 'Y', 'N')

        client.user_id = pytest.user_b.id
        with pytest.raises(HydraPermissionError):
            client.share_project(network_1.project_id, ["UserC"], 'Y', 'Y')
        with pytest.raises(HydraPermissionError):
            client.share_network(network_1.id, ["UserC"], 'Y', 'Y')
