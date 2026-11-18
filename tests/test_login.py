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
import pytest

import hydra_base
import hydra_base.exceptions

@pytest.mark.order("last")
class TestLogin:
    """ A collection of tests of the User part of the DB.
    """

    def test_login(self, client):
        """ Test adding a new user to the DB """

        user_id, session_id = client.login('root', '')

        assert user_id == 1

        assert session_id is not None
        client.logout()

    def test_logout(self, client):

        user_id, session_id = client.login('root', '')

        assert user_id == 1

        assert session_id is not None

        logout_result = client.logout()

        assert logout_result == 'OK'

    def test_get_session_user(self, client):

        #only run this for local clients, as the remote client doesn't
        # user sessions (in the test suite)
        if client.test_server is not None:
            return

        user_id, session_id = client.login('root', '')

        assert user_id == 1

        assert session_id is not None

        retrieved_user_id = client.get_session_user(session_id='i_am_not_a_session')

        assert retrieved_user_id is None

        retrieved_user_id = client.get_session_user(session_id=session_id)

        assert retrieved_user_id == user_id
        client.logout()

    def test_login_wrong_user(self, client):

        with pytest.raises(hydra_base.exceptions.HydraError):
            user_id, session_id = client.login('wrong-user!', '')

    def test_login_wrong_password(self, client):

        with pytest.raises(hydra_base.exceptions.HydraError):
            user_id, session_id = client.login('root', 'wrong-password!')


    def test_login_too_many_attempts(self, client):
        from hydra_base.lib.users import reset_failed_logins, get_failed_login_count

        for i in range(8):
            if i < 7:
                with pytest.raises(hydra_base.exceptions.HydraError):
                    user_id, session_id = client.login('root', 'wrong-password!')
            if i == 7:
                with pytest.raises(hydra_base.exceptions.HydraLoginUserMaxAttemptsExceeded):
                    user_id, session_id = client.login('root', 'wrong-password!')

        reset_failed_logins("root", flush=True, user_id=pytest.root_user_id)
        assert get_failed_login_count("root", user_id=pytest.root_user_id) == 0


class TestLoginHardening:
    """ Failed logins must persist, and login must not depend on the caller's permissions """

    USER = "LoginHardeningUser"

    @pytest.fixture()
    def login_user_fixture(self, client):
        from hydra_base.lib.objects import JSONObject
        client.user_id = pytest.root_user_id  # earlier login tests clear the fake session
        existing = client.get_user_by_name(self.USER)
        if existing in (None, {}):
            existing = client.add_user(JSONObject(dict(username=self.USER, password="correct-password",
                                                       display_name="login test")))
        user = JSONObject(existing)
        yield user
        from hydra_base.lib.users import reset_failed_logins
        reset_failed_logins(self.USER, flush=True, user_id=pytest.root_user_id)
        hydra_base.db.commit_transaction()

    def test_failed_login_survives_rollback(self, client, login_user_fixture):
        from hydra_base.lib.users import get_failed_login_count
        before = get_failed_login_count(self.USER, user_id=pytest.root_user_id)
        with pytest.raises(hydra_base.exceptions.HydraLoginUserPasswordWrong):
            hydra_base.hdb.login_user(self.USER, "wrong")
        # the server rolls back when it sees the exception
        hydra_base.db.rollback_transaction()
        assert get_failed_login_count(self.USER, user_id=pytest.root_user_id) == before + 1

    def test_lockout_applies_even_with_correct_password(self, client, login_user_fixture):
        from hydra_base.lib.users import _max_login_attempts
        for _ in range(_max_login_attempts()):
            with pytest.raises(hydra_base.exceptions.HydraLoginUserPasswordWrong):
                hydra_base.hdb.login_user(self.USER, "wrong")
            hydra_base.db.rollback_transaction()
        with pytest.raises(hydra_base.exceptions.HydraLoginUserMaxAttemptsExceeded):
            hydra_base.hdb.login_user(self.USER, "correct-password")

    def test_login_works_for_user_without_any_role(self, client, login_user_fixture):
        """ Authenticating is not a permission checked operation """
        assert hydra_base.hdb.login_user(self.USER, "correct-password") == login_user_fixture.id

    def test_unknown_user_still_pays_for_a_password_check(self, client, monkeypatch):
        from hydra_base.util import hdb
        calls = []
        monkeypatch.setattr(hdb, "_burn_password_check", lambda pw: calls.append(pw))
        with pytest.raises(hydra_base.exceptions.HydraLoginUserNotFound):
            hdb.login_user("no-such-user-anywhere", "x")
        assert calls == ["x"]

    def test_guarded_helpers_still_require_permission(self, client, login_user_fixture):
        from hydra_base.lib.users import get_failed_login_count, inc_failed_login_attempts
        with pytest.raises(hydra_base.exceptions.PermissionError):
            get_failed_login_count(self.USER, user_id=login_user_fixture.id)
        with pytest.raises(hydra_base.exceptions.PermissionError):
            inc_failed_login_attempts(self.USER, user_id=login_user_fixture.id)
