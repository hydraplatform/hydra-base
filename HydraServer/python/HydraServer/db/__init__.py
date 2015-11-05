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
from sqlalchemy.orm import scoped_session
from sqlalchemy import create_engine
from HydraLib import config
from zope.sqlalchemy import ZopeTransactionExtension

import transaction
import logging
log = logging.getLogger(__name__)

from sqlalchemy.ext.declarative import declarative_base
global DeclarativeBase
DeclarativeBase = declarative_base()

db_url = config.get('mysqld', 'url')
log.info("Connecting to database: %s", db_url)
engine = create_engine(db_url) 
from sqlalchemy.orm import sessionmaker

maker = sessionmaker(bind=engine, autoflush=False, autocommit=False,
                     extension=ZopeTransactionExtension())

DBSession = scoped_session(maker)

def commit_transaction():
    try:
        transaction.commit()
    except Exception, e:
        log.critical(e)
        transaction.abort()
    DBSession.remove()

def rollback_transaction():
    transaction.abort()
