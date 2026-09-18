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

from beaker import session
import sqlalchemy as sa

from .. import util
from .. import config
from .. import db

DEFAULT_VALIDATE_KEY = 'YxaDbzUUSo08b+'
DEFAULT_DATA_DIR = '/tmp'
DEFAULT_FILE_DIR = '/tmp/auth'


CACHE_TABLE = sa.Table('beaker_cache', db.DeclarativeBase.metadata,
                       sa.Column('namespace', sa.String(255), primary_key=True),
                       sa.Column('accessed', sa.DateTime, nullable=False),
                       sa.Column('created', sa.DateTime, nullable=False),
                       sa.Column('data', sa.PickleType, nullable=False),
                       schema=db.DeclarativeBase.metadata.schema)

def login(username, password, **kwargs):
    """
        Login a user, returning a dict containing their user_id and session_id

        This does the DB login to check the credentials, and then creates a session
        so that requests from apps do not need to perform a login

        args:
            username (string): The user's username
            password(string): The user's password (unencrypted)
        returns:
            A dict containing the user_id and session_id
        raises:
            HydraError if the login fails
    """

    user_id = util.hdb.login_user(username, password)

    hydra_session = session.Session(
        {}, #This is normally a request object, but in this case is empty
        validate_key=config.get('COOKIES', 'VALIDATE_KEY', DEFAULT_VALIDATE_KEY),
        type='file' if db.hydra_db_url.startswith('sqlite') else 'ext:sqla',
        cookie_expires=True,
        data_dir=config.get('COOKIES', 'DATA_DIR', DEFAULT_DATA_DIR),
        bind=db.engine,
        table=CACHE_TABLE
    )

    hydra_session['user_id'] = user_id
    hydra_session['username'] = username
    hydra_session.save()

    return (user_id, hydra_session.id)

def logout(session_id, **kwargs):
    """
        Logout a user, removing their cookie if it exists and returning 'OK'


        args:
            session_id (string): The session ID to identify the cookie to remove
        returns:
            'OK'
        raises:
            HydraError if the logout fails
    """

    hydra_session_object = session.SessionObject(
        {}, #This is normally a request object, but in this case is empty
        validate_key=config.get('COOKIES', 'VALIDATE_KEY', DEFAULT_VALIDATE_KEY),
        type='file' if db.hydra_db_url.startswith('sqlite') else 'ext:sqla',
        cookie_expires=True,
        data_dir=config.get('COOKIES', 'DATA_DIR', DEFAULT_DATA_DIR),
        bind=db.engine,
        table=CACHE_TABLE
    )

    hydra_session = hydra_session_object.get_by_id(session_id)

    if hydra_session is not None:
        hydra_session.delete()
        hydra_session.namespace.do_remove()

    return 'OK'

def get_session_user(session_id, **kwargs):
    """
        Given a session ID, get the user ID that it is associated with

        args:
            session_id (string): The user's ID to identify the cookie to remove

        returns:
            user_id (string) or None if the session does not exist
    """

    hydra_session_object = session.SessionObject(
        {}, #This is normally a request object, but in this case is empty
        validate_key=config.get('COOKIES', 'VALIDATE_KEY', DEFAULT_VALIDATE_KEY),
        type='file' if db.hydra_db_url.startswith('sqlite') else 'ext:sqla',
        cookie_expires=True,
        data_dir=config.get('COOKIES', 'DATA_DIR', DEFAULT_DATA_DIR),
        bind=db.engine,
        table=CACHE_TABLE
    )

    hydra_session = hydra_session_object.get_by_id(session_id)

    if hydra_session is not None:
        return hydra_session

    return None
