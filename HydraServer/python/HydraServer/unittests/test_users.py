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
import bcrypt
import datetime

class UsersTest(server.SoapServerTest):

    def test_add_user(self):
        user = self.client.factory.create('hyd:User')
        user.username = "test_user @ %s" % (datetime.datetime.now())
        user.password = "test_user_password"
        user.display_name = "Tom"

        new_user = self.client.service.add_user(user)

        assert new_user.username == user.username, "Usernames are not the same!"

        assert bcrypt.hashpw(user.password, new_user.password.encode('utf-8')) == new_user.password
        assert new_user.display_name == user.display_name 

        new_user.display_name = 'Tom Update'

        updated_user = self.client.service.update_user_display_name(new_user)

        assert updated_user.display_name == new_user.display_name

        delete_result = self.client.service.delete_user(new_user.id)

        assert delete_result == 'OK', "User was not removed!"

    def test_update_user_password(self):
        user = self.client.factory.create('hyd:User')
        user.username = "test_user @ %s" % (datetime.datetime.now())
        user.password = "test_user_password"
        user.display_name = "Tom"
        new_user = self.client.service.add_user(user)

        old_client = self.client
        new_client = server.connect()
        self.client = new_client

        self.login(new_user.username, user.password)
        self.client.service.logout(user.username)

        self.client = old_client
        self.client.service.update_user_password(new_user.id, 'new_test_user_password')
        self.client = new_client

        self.assertRaises(suds.WebFault, self.login, user.username, user.password)

        self.login(user.username, 'new_test_user_password')
        self.client.service.logout(user.username)

        self.client = old_client

    def test_add_role(self):
        role = self.client.factory.create('hyd:Role')
        role.name = "Test Role"
        role.code = "test_role @ %s" % (datetime.datetime.now()) 

        new_role = self.client.service.add_role(role)

        assert new_role.id is not None, "Role does not have an ID!"
        assert new_role.name == role.name, "Role are not the same!"

        delete_result = self.client.service.delete_role(new_role.id)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "Role was not removed!"

    def test_add_perm(self):
        perm = self.client.factory.create('hyd:Perm')
        perm.name = "Test Perm"
        perm.code = "test_perm @ %s" % (datetime.datetime.now())

        new_perm = self.client.service.add_perm(perm)

        assert new_perm.id is not None, "Perm does not have an ID!"
        assert new_perm.name == perm.name, "Perm are not the same!"

        delete_result = self.client.service.delete_perm(new_perm.id)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "perm was not removed!"

    def test_set_user_role(self):

        user = self.client.factory.create('hyd:User')
        user.username = "test_user @ %s" % datetime.datetime.now()
        user.password = "test_user_password"
        user.display_name = "Tom"

        new_user = self.client.service.add_user(user)

        role = self.client.factory.create('hyd:Role')
        role.name = "Test Role"
        role.code = "test_role @ %s"  % (datetime.datetime.now()) 

        new_role = self.client.service.add_role(role)

        role_with_users = self.client.service.set_user_role(new_user.id, new_role.id)

        assert role_with_users is not None, "Role user was not set correctly"
        assert role_with_users.roleusers.RoleUser[0].user_id == new_user.id, "User was not added to role correctly."

        delete_result = self.client.service.delete_user_role(new_user.id, new_role.id)

        assert delete_result == 'OK', "Role User was not removed!"

        delete_result = self.client.service.delete_user(new_user.id)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "Role User was not removed!"


    def test_set_role_perm(self):

        role = self.client.factory.create('hyd:Role')
        role.name = "Test Role"
        role.code = "test_role @ %s"  % (datetime.datetime.now()) 

        new_role = self.client.service.add_role(role)

        perm = self.client.factory.create('hyd:Perm')
        perm.name = "Test Perm"
        perm.code = "test_perm @ %s" % (datetime.datetime.now()) 

        new_perm = self.client.service.add_perm(perm)

        role_with_perms = self.client.service.set_role_perm(new_role.id, new_perm.id)

        assert role_with_perms is not None, "Role perm was not set correctly"
        assert role_with_perms.roleperms.RolePerm[0].perm_id == new_perm.id, "Perm was not added to role correctly."

        delete_result = self.client.service.delete_role_perm(new_role.id, new_perm.id)

        assert delete_result == 'OK', "Role Perm was not removed!"

        delete_result = self.client.service.delete_perm(new_perm.id)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "Role Perm was not removed!"

    def test_get_users(self):
        users = self.client.service.get_all_users()
        assert len(users) > 0
        for user in users.User:
            if user.id == 1:
                assert user.username == 'root'

    def test_get_username(self):
        username = self.client.service.get_username(1)
        assert username == 'root'

    def test_get_perms(self):
        perms = self.client.service.get_all_perms()
        assert len(perms.Perm) >= 19

        check_perm = perms.Perm[0]

        single_perm = self.client.service.get_perm(check_perm.id)

        assert single_perm.id == check_perm.id
        assert single_perm.code == check_perm.code
        assert single_perm.name == check_perm.name
        self.assertRaises(suds.WebFault, self.client.service.get_perm, 99999)

    def test_get_roles(self):
        roles = self.client.service.get_all_roles()
        assert len(roles.Role) >=6

        role_codes = set([r.code for r in roles.Role])
        core_set = set(['admin', 'dev', 'modeller', 'manager', 'grad', 'decision'])
        assert core_set.issubset(role_codes)
        for r in roles.Role:
            if r.code == 'admin':
                assert len(r.roleperms.RolePerm) >= 10
                check_role = r

        single_role = self.client.service.get_role(check_role.id)
        assert check_role.id == single_role.id
        assert check_role.code == single_role.code
        assert check_role.name == single_role.name
        assert len(check_role.roleperms) == len(single_role.roleperms)
        self.assertRaises(suds.WebFault, self.client.service.get_role, 99999)

    def test_get_user_roles(self):
        roles = self.client.service.get_user_roles(1)
        assert len(roles.Role) == 1
        assert roles.Role[0].code == 'admin'

    def test_get_user_permissions(self):
        permissions = self.client.service.get_user_permissions(1)

        role = self.client.service.get_role_by_code('admin')

        assert len(permissions.Perm) == len(role.roleperms.RolePerm)

def setup():
    server.connect()

if __name__ == '__main__':
    server.run()
