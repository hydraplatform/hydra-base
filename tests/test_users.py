import hydra_base
import hydra_base.exceptions
from hydra_base.lib.objects import JSONObject
from fixtures import *
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

    def test_add_user(self, session, user_json_object):
        """ Test adding a new user to the DB """
        user = user_json_object  # Convenience renaming

        new_user = hydra_base.add_user(user)

        assert new_user.username == user.username, "Usernames are not the same!"

        assert bcrypt.checkpw(user.password.encode('utf-8'), new_user.password)
        assert new_user.display_name == user.display_name

        new_user.display_name = 'Tom Update'

        updated_user = hydra_base.update_user_display_name(new_user)

        assert updated_user.display_name == new_user.display_name

        delete_result = hydra_base.delete_user(new_user.id)

        assert delete_result == 'OK', "User was not removed!"

    def test_update_user_password(self, session, user_json_object):
        """ Test changing a user's password """
        user = user_json_object

        new_user = hydra_base.add_user(user)

        # TODO create a new session here
        new_password = 'new_test_user_password'
        new_user = hydra_base.update_user_password(new_user.id, new_password)
        assert bcrypt.checkpw(new_password.encode('utf-8'), new_user.password)

    def test_add_role(self, session, role_json_object):
        """ Test adding a new role """
        role = role_json_object
        new_role = hydra_base.add_role(role)

        assert new_role.role_id is not None, "Role does not have an ID!"
        assert new_role.role_name == role.name, "Role are not the same!"

        delete_result = hydra_base.delete_role(new_role.role_id)

        assert delete_result == 'OK', "Role was not removed!"

    def test_add_perm(self, session, perm_json_object):
        """ Test adding a new permission """
        perm = perm_json_object

        new_perm = hydra_base.add_perm(perm)

        assert new_perm.perm_id is not None, "Perm does not have an ID!"
        assert new_perm.perm_name == perm.name, "Perm are not the same!"

        delete_result = hydra_base.delete_perm(new_perm.perm_id)

        assert delete_result == 'OK', "perm was not removed!"

    def test_set_user_role(self, session, user_json_object, role_json_object):
        """ Test assigning a role to a user """
        # Rename for convenience during test
        user = user_json_object
        role = role_json_object

        # Add the user and the role
        new_user = hydra_base.add_user(user)
        new_role = hydra_base.add_role(role)

        # Apply role to the user
        # NB `set_user_role` returns a `Role` instance not a `RoleUser` instance
        role_with_users = hydra_base.set_user_role(new_user.id, new_role.role_id)

        assert role_with_users is not None, "Role user was not set correctly"
        assert role_with_users.roleusers[0].user_id == new_user.id, "User was not added to role correctly."

        delete_result = hydra_base.delete_user_role(new_user.id, new_role.role_id)

        assert delete_result == 'OK', "Role User was not removed!"

        delete_result = hydra_base.delete_user(new_user.id)

        assert delete_result == 'OK', "Role User was not removed!"

    def test_set_role_perm(self, session, role_json_object, perm_json_object):
        """ Test assigning a perm to a user """
        # Rename for convenience during testing
        role = role_json_object
        perm = perm_json_object

        # Add the role and perm
        new_role = hydra_base.add_role(role)
        new_perm = hydra_base.add_perm(perm)

        role_with_perms = hydra_base.set_role_perm(new_role.role_id, new_perm.perm_id)

        assert role_with_perms is not None, "Role perm was not set correctly"
        assert role_with_perms.roleperms[0].perm_id == new_perm.perm_id, "Perm was not added to role correctly."

        delete_result = hydra_base.delete_role_perm(new_role.role_id, new_perm.perm_id)

        assert delete_result == 'OK', "Role Perm was not removed!"

        delete_result = hydra_base.delete_perm(new_perm.perm_id)

        assert delete_result == 'OK', "Role Perm was not removed!"

    def test_get_users(self, session, user_json_object):
        """ Test fetching all the users """
        # Add one user
        hydra_base.add_user(user_json_object)
        # There should always be a root user
        users = hydra_base.get_all_users()
        assert len(users) > 0
        for user in users:
            if user.id == 1:
                assert user.username == 'root'
            else:
                assert user.username == user_json_object.username

    def test_get_username(self, session, user_json_object):
        """ Test retrieving a username """
        # Add one user
        hydra_base.add_user(user_json_object)
        username = hydra_base.get_username(1)
        assert username == 'root'

    def test_get_perms(self, session):
        """ Test fetching all the perms """
        perms = hydra_base.get_all_perms()
        assert len(perms) >= 19

        check_perm = perms[0]

        single_perm = hydra_base.get_perm(check_perm.perm_id)

        assert single_perm.perm_id == check_perm.perm_id
        assert single_perm.perm_code == check_perm.perm_code
        assert single_perm.perm_name == check_perm.perm_name

        # Check invalid ID raises an error
        with pytest.raises(hydra_base.exceptions.ResourceNotFoundError):
            hydra_base.get_perm(99999)

    def test_get_roles(self, session):
        roles = hydra_base.get_all_roles()
        assert len(roles) >= 6

        role_codes = set([r.role_code for r in roles])
        core_set = set(['admin', 'dev', 'modeller', 'manager', 'grad', 'decision'])
        assert core_set.issubset(role_codes)
        for r in roles:
            if r.role_code == 'admin':
                assert len(r.roleperms) >= 10
                check_role = r

        single_role = hydra_base.get_role(check_role.role_id)
        assert check_role.role_id == single_role.role_id
        assert check_role.role_code == single_role.role_code
        assert check_role.role_name == single_role.role_name
        assert len(check_role.roleperms) == len(single_role.roleperms)

        # Check invalid ID raises an error
        with pytest.raises(hydra_base.exceptions.HydraError):
            hydra_base.get_role(99999)

    def test_get_user_roles(self, session):
        roles = hydra_base.get_user_roles(1)
        assert len(roles) == 1
        assert roles[0].role_code == 'admin'

    def test_get_user_permissions(self, session):
        permissions = hydra_base.get_user_permissions(1)

        role = hydra_base.get_role_by_code('admin')

        assert len(permissions) == len(role.roleperms)
