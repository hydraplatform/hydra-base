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
import hydra_base.exceptions
import datetime
import bcrypt
import pytest

class TestLogin:
    """ A collection of tests of the User part of the DB.
    """

    def test_login(self, client):
        """ Test adding a new user to the DB """

        user_id, session_id = client.login('root', '')

        assert user_id == 1

        assert session_id is not None

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

    def test_login_wrong_user(self, client):

        with pytest.raises(hydra_base.exceptions.HydraLoginUserNotFound):
            user_id, session_id = client.login('wrong-user!', '')

    def test_login_wrong_password(self, client):

        with pytest.raises(hydra_base.exceptions.HydraLoginUserPasswordWrong):
            user_id, session_id = client.login('root', 'wrong-password!')


    def test_login_too_many_attempts(self, client):
        exception_raised=""
        for i in range(1,10):
            try:
                user_id, session_id = client.login('root', 'wrong-password!')
            except hydra_base.exceptions.HydraLoginUserMaxAttemptsExceeded as e:
                exception_raised="HydraLoginUserMaxAttemptsExceeded"
            except hydra_base.exceptions.HydraLoginUserPasswordWrong as e:
                pass

        assert exception_raised == "HydraLoginUserMaxAttemptsExceeded"
