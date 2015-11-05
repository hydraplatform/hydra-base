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




#These are for creating the resource data view (see bottom of page)
from sqlalchemy import select
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from sqlalchemy.ext import compiler
from model import ResourceAttr, ResourceScenario, Attr, Dataset

class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

class DropView(DDLElement):
    def __init__(self, name):
        self.name = name

@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (element.name, compiler.sql_compiler.process(element.selectable))

@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)

def view(name, metadata, selectable):
    t = table(name)

    for c in selectable.c:
        c._make_proxy(t)

    CreateView(name, selectable).execute_at('after-create', metadata)
    DropView(name).execute_at('before-drop', metadata)
    return t


view_qry = select([
    ResourceAttr.resource_attr_id,
    ResourceAttr.attr_id,
    Attr.attr_name,
    ResourceAttr.resource_attr_id,
    ResourceAttr.network_id,
    ResourceAttr.node_id,
    ResourceAttr.link_id,
    ResourceAttr.group_id,
    ResourceScenario.scenario_id,
    ResourceScenario.dataset_id,
    Dataset.data_units,
    Dataset.data_dimen,
    Dataset.data_name,
    Dataset.data_type,
    Dataset.value]).where(ResourceScenario.resource_attr_id==ResourceAttr.attr_id).where(ResourceAttr.attr_id==Attr.attr_id).where(ResourceScenario.dataset_id==Dataset.dataset_id)

stuff_view = view("vResourceData", DeclarativeBase.metadata, view_qry)
