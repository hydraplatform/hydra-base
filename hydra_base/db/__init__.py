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

import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy import create_engine

#Import these as a test for foreign key checking in
from sqlalchemy import event
from sqlalchemy.engine import Engine

from .. import config
from zope.sqlalchemy import register

from hydra_base.exceptions import HydraError

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

#logger_sqlalchemy = logging.getLogger('sqlalchemy')
#logger_sqlalchemy.setLevel(logging.DEBUG)

# @event.listens_for(Engine, "connect")
# def set_sqlite_pragma(dbapi_connection, connection_record):
#     cursor = dbapi_connection.cursor()
#     cursor.execute("PRAGMA foreign_keys=ON")
#     cursor.close()

def create_mysql_db(db_url):
    """
        To simplify deployment, create the mysql DB if it's not there.
        Accepts a URL with or without a DB name stated, and returns a db url
        containing the db name for use in the main sqlalchemy engine.

        THe formats can take the following form:

        mysql+driver://username:password@hostname
        mysql+driver://username:password@hostname/dbname

        if no DB name is specified, it is retrieved from config
    """

    #add a special case for a memory-based sqlite session
    if db_url == 'sqlite://':
        return db_url

    #Remove trailing whitespace and forwardslashes
    db_url = db_url.strip().strip('/')


    #Check this is a mysql URL
    if db_url.find('mysql') >= 0:

        #Get the DB name from config and check if it's in the URL
        db_name = config.get('mysqld', 'db_name', 'hydradb')
        if db_url.find(db_name) >= 0:
            no_db_url = db_url.rsplit("/", 1)[0]
        else:
            #Check that there is a hostname specified, as we'll be using the '@' symbol soon..
            if db_url.find('@') == -1:
                raise HydraError("No Hostname specified in DB url")

            #Check if there's a DB name specified that's different to the one in config.
            host_and_db_name = db_url.split('@')[1]
            if host_and_db_name.find('/') >= 0:
                no_db_url, db_name = db_url.rsplit("/", 1)
            else:
                no_db_url = db_url
                db_url = no_db_url + "/" + db_name

        db_url = "{}?charset=utf8mb4&use_unicode=1".format(db_url)

        if config.get('mysqld', 'auto_create', 'Y') == 'Y':
            tmp_engine = create_engine(no_db_url)
            log.debug("Creating database {0} as it does not exist.".format(db_name))
            tmp_engine.execute("CREATE DATABASE IF NOT EXISTS {0}".format(db_name))

    return db_url

def connect(db_url=None):
    if db_url is None:
        db_url = config.get('mysqld', 'url')

    log.info("Connecting to database")
    if db_url.find('@') >= 0:
        log.info("DB URL: %s", db_url.split('@')[1])
    else:
        log.info("DB URL: %s", db_url)

    db_url = create_mysql_db(db_url)

    global engine

    if db_url.startswith('sqlite'):
        engine = create_engine(db_url, encoding='utf8')
    else:

        db_pool_size = int(config.get('mysqld', 'pool_size', 5))
        db_pool_recycle = int(config.get('mysqld', 'pool_recycle', 300))
        db_max_overflow = int(config.get('mysqld', 'max_overflow', 10))
        ssl_ca = config.get('mysqld', 'ssl_ca', False)
        connect_args = {}
        if ssl_ca:
            connect_args['ssl'] = {'ssl_ca': ssl_ca}

        engine = create_engine(db_url,
                               encoding='utf8',
                               connect_args=connect_args,
                               pool_recycle=db_pool_recycle,
                               pool_size=db_pool_size,
                               max_overflow=db_max_overflow)

    global DBSession

    maker = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    DBSession = scoped_session(maker)
    register(DBSession)


    global DeclarativeBase
    try:
        DeclarativeBase.metadata.create_all(engine, checkfirst=True)
    except sqlalchemy.exc.OperationalError as err:
        log.warning("Error creating database: %s", err)

    return db_url

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
