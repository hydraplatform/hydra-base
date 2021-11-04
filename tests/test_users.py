#!/usr/bin/env python
# -*- coding: utf-8 -*-

# (c) Copyright 2013 to 2017 University of Manchester
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

import hydra_base
from hydra_base.exceptions import HydraError
from hydra_base.lib.objects import JSONObject
import datetime
import bcrypt
import pytest


@pytest.fixture
def user_json_object():
    user = JSONObject(dict(
        username="test_user @ %s" % (datetime.datetime.now()),
        password="test_user_password",
        display_name="Tom"
    ))
    return user


@pytest.fixture
def role_json_object():
    role = JSONObject(dict(
        name="Test Role",
        code="test_role @ %s" % (datetime.datetime.now())
    ))
    return role


@pytest.fixture
def perm_json_object():
    perm = JSONObject(dict(
        name="Test Perm",
        code="test_perm @ %s" % (datetime.datetime.now())
    ))
    return perm


class TestUser:
    """ A collection of tests of the User part of the DB.
    """

    def test_add_user(self, client, user_json_object):
        """ Test adding a new user to the DB """
        user = user_json_object  # Convenience renaming

        new_user = client.add_user(user)
        assert new_user.username == user.username, "Usernames are not the same!"
        #convert pwd to bytes if it's not bytes
        #Add user returns the password *hash*, not the plain-text password
        pwd_hash = new_user.password if isinstance(new_user.password, bytes) else new_user.password.encode('utf-8')
        assert bcrypt.checkpw(user.password.encode('utf-8'), pwd_hash)
        assert new_user.display_name == user.display_name

        new_user.display_name = 'Tom Update'

        updated_user = client.update_user_display_name(new_user)

        assert updated_user.display_name == new_user.display_name

        delete_result = client.delete_user(new_user.id)

        assert delete_result == 'OK', "User was not removed!"

    def test_update_user_password(self, client, user_json_object):
        """ Test changing a user's password """
        user = user_json_object

        new_user = client.add_user(user)

        # TODO create a new session here
        new_password = 'new_test_user_password'
        new_user = client.update_user_password(new_user.id, new_password)
        pwd = new_user.password
        pwd_hash = pwd if isinstance(pwd, bytes) else pwd.encode('utf-8')

        assert bcrypt.checkpw(new_password.encode('utf-8'), pwd_hash)

    def test_add_role(self, client, role_json_object):
        """ Test adding a new role """
        role = role_json_object
        new_role = client.add_role(role)

        assert new_role.id is not None, "Role does not have an ID!"
        assert new_role.name == role.name, "Role are not the same!"

        delete_result = client.delete_role(new_role.id)

        assert delete_result == 'OK', "Role was not removed!"

    def test_add_perm(self, client, perm_json_object):
        """ Test adding a new permission """
        perm = perm_json_object

        new_perm = client.add_perm(perm)

        assert new_perm.id is not None, "Perm does not have an ID!"
        assert new_perm.name == perm.name, "Perm are not the same!"

        delete_result = client.delete_perm(new_perm.id)

        assert delete_result == 'OK', "perm was not removed!"

    def test_set_user_role(self, client, user_json_object, role_json_object):
        """ Test assigning a role to a user """
        # Rename for convenience during test
        user = user_json_object
        role = role_json_object

        # Add the user and the role
        new_user = client.add_user(user)
        new_role = client.add_role(role)

        # Apply role to the user
        # NB `set_user_role` returns a `Role` instance not a `RoleUser` instance
        role_with_users = client.set_user_role(new_user.id, new_role.id)

        assert role_with_users is not None, "Role user was not set correctly"
        assert role_with_users.roleusers[0].user_id == new_user.id, "User was not added to role correctly."

        delete_result = client.delete_user_role(new_user.id, new_role.id)

        assert delete_result == 'OK', "Role User was not removed!"

        delete_result = client.delete_user(new_user.id)

        assert delete_result == 'OK', "Role User was not removed!"

    def test_set_role_perm(self, client, role_json_object, perm_json_object):
        """ Test assigning a perm to a user """
        # Rename for convenience during testing
        role = role_json_object
        perm = perm_json_object

        # Add the role and perm
        new_role = client.add_role(role)
        new_perm = client.add_perm(perm)

        role_with_perms = client.set_role_perm(new_role.id, new_perm.id)

        assert role_with_perms is not None, "Role perm was not set correctly"
        assert role_with_perms.roleperms[0].perm_id == new_perm.id, "Perm was not added to role correctly."

        delete_result = client.delete_role_perm(new_role.id, new_perm.id)

        assert delete_result == 'OK', "Role Perm was not removed!"

        delete_result = client.delete_perm(new_perm.id)

        assert delete_result == 'OK', "Role Perm was not removed!"

    def test_get_users(self, client, user_json_object):
        """ Test fetching all the users """

        # These are the users added for the tests.
        existing_users = {1: 'root', 2: 'UserA', 3: 'UserB', 4: 'UserC'}

        # Add one user
        client.add_user(user_json_object)
        # There should always be a root user
        users = client.get_all_users()
        assert len(users) >= 4
        assert set(existing_users.values()).issubset(set([u.username for u in users]))

    def test_get_username(self, client, user_json_object):
        """ Test retrieving a username """
        # Add one user
        client.add_user(user_json_object)
        username = client.get_username(1)
        assert username == 'root'

    def test_get_perms(self, client):
        """ Test fetching all the perms """
        perms = client.get_all_perms()
        assert len(perms) >= 19

        check_perm = perms[0]

        single_perm = client.get_perm(check_perm.id)

        assert single_perm.id == check_perm.id
        assert single_perm.code == check_perm.code
        assert single_perm.name == check_perm.name

        # Check invalid ID raises an error
        with pytest.raises(HydraError):
            client.get_perm(99999)

    def test_get_roles(self, client):
        roles = client.get_all_roles()
        assert len(roles) >= 6

        role_codes = set([r.code for r in roles])
        core_set = set(['admin', 'dev', 'modeller', 'manager', 'grad', 'decision'])
        assert core_set.issubset(role_codes)
        for r in roles:
            if r.code == 'admin':
                assert len(r.roleperms) >= 10
                check_role = r

        single_role = client.get_role(check_role.id)
        assert check_role.id == single_role.id
        assert check_role.code == single_role.code
        assert check_role.name == single_role.name
        assert len(check_role.roleperms) == len(single_role.roleperms)

        # Check invalid ID raises an error
        with pytest.raises(HydraError):
            client.get_role(99999)

    def test_get_user_roles(self, client):
        roles = client.get_user_roles(1)
        assert len(roles) == 1
        assert roles[0].code == 'admin'

    def test_get_user_permissions(self, client):
        permissions = client.get_user_permissions(1)

        role = client.get_role_by_code('admin')

        assert len(permissions) == len(role.roleperms)

    def test_user_has_failed_login_count(self, client, user_json_object):
        user = client.add_user(user_json_object)
        failed_logins = client.get_failed_login_count(user.username)

    def test_max_login_attempts_defined(self, client, user_json_object):
        user = client.add_user(user_json_object)
        max_attempts = client.get_max_login_attempts()
        assert max_attempts is not None

    def test_user_has_remaining_login_attempts(self, client, user_json_object):
        user = client.add_user(user_json_object)
        remaining_attempts = client.get_remaining_login_attempts(user.username)

    def test_inc_user_failed_logins(self, client, user_json_object):
        user = client.add_user(user_json_object)

        initial_failed_logins = client.get_failed_login_count(user.username)
        client.inc_failed_login_attempts(user.username)
        final_failed_logins = client.get_failed_login_count(user.username)

        assert final_failed_logins == initial_failed_logins + 1

    def test_reset_user_failed_logins(self, client, user_json_object):
        user = client.add_user(user_json_object)

        client.inc_failed_login_attempts(user.username)
        client.reset_failed_logins(user.username)
        failed_logins = client.get_failed_login_count(user.username)

        assert failed_logins == 0

    def test_login_count_totals(self, client, user_json_object):
        user = client.add_user(user_json_object)

        client.inc_failed_login_attempts(user.username)
        failed_logins = client.get_failed_login_count(user.username)
        remaining_attempts = client.get_remaining_login_attempts(user.username)
        max_attempts = client.get_max_login_attempts()

        assert max_attempts == failed_logins + remaining_attempts
