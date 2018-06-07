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
from fixtures import *
import datetime
import bcrypt
import pytest

class TestLogin:
    """ A collection of tests of the User part of the DB.
    """

    def test_login(self, session):
        """ Test adding a new user to the DB """

        user_id, session_id = hydra_base.login('root', '')

        assert user_id == 1

        assert session_id is not None



    def test_logout(self, session):

        user_id, session_id = hydra_base.login('root', '')

        assert user_id == 1

        assert session_id is not None

        bad_logout_result = hydra_base.logout('i_am_not_a_session')

        assert bad_logout_result == 'OK'

        logout_result = hydra_base.logout(session_id)

        assert logout_result == 'OK'

    def test_get_session_user(self, session):

        user_id, session_id = hydra_base.login('root', '')

        assert user_id == 1

        assert session_id is not None

        retrieved_user_id = hydra_base.get_session_user('i_am_not_a_session')

        assert retrieved_user_id == None

        retrieved_user_id = hydra_base.get_session_user(session_id)

        assert retrieved_user_id == user_id
