"""
    Tests for permission checking: check_perm / check_role / required_perms and the
    default role -> permission assignments.
"""
import logging

import pytest

import hydra_base as hb
from hydra_base import db
from hydra_base.db.model import Perm, Role, RolePerm
from hydra_base.exceptions import HydraError, PermissionError
from hydra_base.lib.objects import JSONObject
from hydra_base.util import permissions
from hydra_base.util.hdb import create_default_users_and_perms

log = logging.getLogger(__name__)


@pytest.fixture()
def no_cache():
    """ Positive results are cached per thread, so clear it to test the DB lookup """
    permissions._perm_cache.__dict__.pop('cache', None)
    yield
    permissions._perm_cache.__dict__.pop('cache', None)


@pytest.fixture()
def user_without_role(client):
    """ A user who has been given no role at all """
    name = "NoRoleUser"
    existing = client.get_user_by_name(name)
    if existing not in (None, {}):
        return existing
    return JSONObject(client.add_user(JSONObject(dict(username=name, password="password", display_name="no role"))))


class TestCheckPerm:

    def test_user_without_role_is_denied(self, client, user_without_role, no_cache):
        with pytest.raises(PermissionError):
            permissions.check_perm(user_without_role.id, "get_network")
        hb.db.close_session()

    def test_user_without_the_perm_is_denied(self, client, no_cache):
        # developer role has no user administration permissions
        with pytest.raises(PermissionError):
            permissions.check_perm(pytest.user_c.id, "edit_user")
        hb.db.close_session()

    def test_admin_is_allowed(self, client, no_cache):
        permissions.check_perm(pytest.root_user_id, "edit_user")
        hb.db.close_session()

    def test_missing_user_id_is_denied(self, client, no_cache):
        with pytest.raises(PermissionError):
            permissions.check_perm(None, "get_network")
        hb.db.close_session()

    def test_unknown_permission_is_denied(self, client, no_cache):
        with pytest.raises(PermissionError):
            permissions.check_perm(pytest.root_user_id, "no_such_permission")
        hb.db.close_session()

    def test_denial_is_not_cached(self, client, user_without_role, no_cache):
        for _ in range(2):
            with pytest.raises(PermissionError):
                permissions.check_perm(user_without_role.id, "get_network")
        hb.db.close_session()


class TestCheckRole:

    def test_user_without_role_is_denied(self, client, user_without_role):
        with pytest.raises(PermissionError):
            permissions.check_role(user_without_role.id, "admin")
        hb.db.close_session()

    def test_user_with_other_role_is_denied(self, client):
        with pytest.raises(PermissionError):
            permissions.check_role(pytest.user_c.id, "admin")
        hb.db.close_session()

    def test_user_with_role_is_allowed(self, client):
        permissions.check_role(pytest.user_c.id, "developer")
        permissions.check_role(pytest.root_user_id, "admin")
        hb.db.close_session()

    def test_missing_user_id_is_denied(self, client):
        with pytest.raises(PermissionError):
            permissions.check_role(None, "admin")
        hb.db.close_session()


class TestWarnMode:

    def test_warn_mode_logs_and_allows(self, client, user_without_role, no_cache, monkeypatch, caplog):
        monkeypatch.setattr(permissions, "_permission_mode", lambda: "warn")
        with caplog.at_level(logging.WARNING):
            permissions.check_perm(user_without_role.id, "get_network")
        assert "WOULD DENY" in caplog.text
        hb.db.close_session()

    def test_default_mode_is_enforce(self):
        assert permissions._permission_mode() == "enforce"


class TestRequiredPermsOnAPI:
    """ The decorator must stop an unprivileged user calling an administrative function.
        Over a remote connection the server wraps the error, so only its type family and text are checked. """

    def test_developer_cannot_change_another_users_password(self, client):
        client.user_id = pytest.user_c.id  # a plain 'developer'
        try:
            with pytest.raises(HydraError, match="Permission denied"):
                client.update_user_password(new_pwd_user_id=pytest.user_a.id, new_password="changed")
        finally:
            client.user_id = pytest.root_user_id

    def test_developer_cannot_delete_a_user(self, client):
        client.user_id = pytest.user_c.id
        try:
            with pytest.raises(HydraError, match="Permission denied"):
                client.delete_user(pytest.user_a.id)
        finally:
            client.user_id = pytest.root_user_id
        assert client.get_user_by_name(pytest.user_a.username) not in (None, {})

    def test_developer_cannot_assign_roles(self, client):
        admin_role = JSONObject(client.get_role_by_code("admin"))
        client.user_id = pytest.user_c.id
        try:
            with pytest.raises(HydraError, match="Permission denied"):
                client.set_user_role(new_user_id=pytest.user_c.id, role_id=admin_role.id)
        finally:
            client.user_id = pytest.root_user_id

    def test_developer_cannot_create_roles(self, client):
        client.user_id = pytest.user_c.id
        try:
            with pytest.raises(HydraError, match="Permission denied"):
                client.add_role(JSONObject(dict(name="Sneaky", code="sneaky")))
        finally:
            client.user_id = pytest.root_user_id

    def test_admin_can_change_password(self, client):
        result = client.update_user_password(new_pwd_user_id=pytest.user_b.id, new_password="password")
        assert result is not None


class TestDefaultRolePerms:

    NON_ADMIN_ROLES = ("dev", "developer", "modeller", "manager", "grad", "decision")
    ADMIN_ONLY_PERMS = ("add_user", "edit_user", "get_role", "add_role", "edit_role",
                        "get_perm", "add_perm", "edit_perm")
    BASELINE_PERMS = ("get_network", "get_template", "get_rules", "get_data", "get_project", "get_user")

    @staticmethod
    def _perms_of(role_code):
        rows = db.DBSession.query(Perm.code).join(RolePerm, RolePerm.perm_id == Perm.id)\
            .join(Role, Role.id == RolePerm.role_id).filter(Role.code == role_code).all()
        return {r.code for r in rows}

    def test_non_admin_roles_have_baseline_perms(self, client):
        for role in self.NON_ADMIN_ROLES:
            perms = self._perms_of(role)
            for p in self.BASELINE_PERMS:
                assert p in perms, f"{role} lacks {p}"
        hb.db.close_session()

    def test_non_admin_roles_cannot_administer_users(self, client):
        for role in self.NON_ADMIN_ROLES:
            perms = self._perms_of(role)
            for p in self.ADMIN_ONLY_PERMS:
                assert p not in perms, f"{role} must not have {p}"
        hb.db.close_session()

    def test_developer_can_use_common_read_calls(self, client, network_with_data):
        """ Reading calls that carry a read permission must work for a normal user """
        net = network_with_data
        scenario_id = net.scenarios[0].id
        # share the network so that the resource level check passes too
        client.share_network(net.id, [pytest.user_c.username], read_only='N', share='N')
        client.user_id = pytest.user_c.id
        try:
            assert client.get_scenario(scenario_id) is not None
            assert client.get_templates() is not None
            assert client.get_users_like("User") is not None
        finally:
            client.user_id = pytest.root_user_id

    def test_sync_restores_missing_links(self, client):
        """ Re-running the (additive) default setup re-adds a removed role permission """
        role = db.DBSession.query(Role).filter(Role.code == "developer").one()
        perm = db.DBSession.query(Perm).filter(Perm.code == "get_rules").one()
        link = db.DBSession.query(RolePerm).filter(RolePerm.role_id == role.id,
                                                   RolePerm.perm_id == perm.id).one()
        db.DBSession.delete(link)
        db.DBSession.flush()
        assert "get_rules" not in self._perms_of("developer")

        create_default_users_and_perms()
        db.DBSession.flush()
        assert "get_rules" in self._perms_of("developer")

        # and it is idempotent
        before = db.DBSession.query(RolePerm).count()
        create_default_users_and_perms()
        db.DBSession.flush()
        assert db.DBSession.query(RolePerm).count() == before
        hb.db.close_session()
