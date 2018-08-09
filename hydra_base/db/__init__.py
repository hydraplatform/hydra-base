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

from sqlalchemy.orm import scoped_session
from sqlalchemy import create_engine
from .. import config
from zope.sqlalchemy import ZopeTransactionExtension

import transaction
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import logging
log = logging.getLogger(__name__)

global DeclarativeBase
DeclarativeBase = declarative_base()

global DBSession
DBSession = None

global engine
engine = None

def create_mysql_db(db_url):
    """
        To simplify deployment, create the mysql DB if it's not thre.
        Accepts a URL with or without a DB name stated, and returns a db url
        containing the db name for use in the main sqlalchemy engine.
    """
    #automatically create the mysql DB if it's not there.
    db_name = config.get('mysqld', 'db_name', 'hydradb')
    if db_url.find('mysql') >= 0:
        if db_url.find(db_name) >= 0:
            no_db_url = "/".join(db_url.split("/")[0:-1])
        else:
            no_db_url = db_url
            db_url = no_db_url + "/" + db_name
        
        if config.get('mysqld', 'auto_create', 'Y') == 'Y':
            tmp_engine = create_engine(no_db_url)
            log.warn("Creating database {0} as it does not exist.".format(db_name))
            tmp_engine.execute("CREATE DATABASE IF NOT EXISTS {0}".format(db_name))

    return db_url

def connect(db_url=None):
    if db_url is None:
        db_url = config.get('mysqld', 'url')

    log.info("Connecting to database: %s", db_url)

    db_url = create_mysql_db(db_url)

    global engine
    engine = create_engine(db_url)

    maker = sessionmaker(bind=engine, autoflush=False, autocommit=False,
                     extension=ZopeTransactionExtension())
    global DBSession
    DBSession = scoped_session(maker)

    global DeclarativeBase
    DeclarativeBase.metadata.create_all(engine)

def get_session():
    global DBSession
    return DBSession

def commit_transaction():
    try:
        transaction.commit()
    except Exception as e:
        log.critical(e)
        transaction.abort()

def close_session():
    DBSession.remove()

def rollback_transaction():
    transaction.abort()
