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
"""
    This code takes care of the migration status
"""
from __future__ import division

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import func
from sqlalchemy.orm import load_only

from .. import db

from ..db.model import MigrationStatus, Migration, MigrationMapping
from .objects import JSONObject
from ..exceptions import HydraError, ResourceNotFoundError

from ..util.permissions import required_perms

import numpy
import logging
log = logging.getLogger(__name__)

#@required_perms("migrate_project")
def get_project_for_migration(migration_name, target_server_url, source_project_id, **kwargs):
    project_status = None
    try:
        project_status = db.DBSession.query(MigrationStatus).filter(MigrationStatus.migration_name==migration_name).filter(MigrationStatus.target_server_url==target_server_url).filter(MigrationStatus.source_project_id==source_project_id).one()
    except NoResultFound:
        # The project has never been initialized does not exist
        project_status = MigrationStatus()
        project_status.migration_name = migration_name
        project_status.source_project_id = source_project_id
        project_status.target_server_url = target_server_url
        project_status.target_project_id = None
        db.DBSession.add(project_status)
        db.DBSession.flush()

    return project_status

#@required_perms("migrate_project")
def set_target_project_id(migration_name, target_url, source_project_id, target_project_id,**kwargs):
    project_status = get_project_for_migration(migration_name, target_url, source_project_id, **kwargs)

    project_status.target_project_id = target_project_id
    db.DBSession.flush()

    return project_status

#@required_perms("migrate_project")
def add_network_to_project_status(migration_name, source_url, target_url, source_project_id, target_project_id, source_network_id, target_network_id, **kwargs):
    """
    """
    project_status = set_target_project_id(migration_name, target_url, source_project_id, target_project_id,**kwargs)
    project_status.add_network_done({"source_network_id": source_network_id, "target_network_id": target_network_id})
    db.DBSession.flush()

#@required_perms("migrate_project")
def get_migration_id(migration_name, source_server_url, target_server_url, **kwargs):
    migration = None
    try:
        # Maybe the migration is already in the DB
        migration = db.DBSession.query(Migration)\
                    .filter(Migration.migration_name==migration_name)\
                    .filter(Migration.source_server_url==source_server_url)\
                    .filter(Migration.target_server_url==target_server_url).one()
    except NoResultFound:
        # The migration has never been initialized so does not exist
        migration = Migration()
        migration.migration_name = migration_name
        migration.source_server_url = source_server_url
        migration.target_server_url = target_server_url
        db.DBSession.add(migration)
        db.DBSession.flush()

    return migration.id

#@required_perms("migrate_project")
def set_migration_entity_mapping(migration_id, table_name, source_entity_id, target_entity_id, **kwargs):
    mapping = None
    try:
        # Maybe the mapping is already in the DB
        mapping = db.DBSession.query(MigrationMapping)\
                    .filter(MigrationMapping.migration_id==migration_id)\
                    .filter(MigrationMapping.table_name==table_name)\
                    .filter(MigrationMapping.source_entity_id==source_entity_id).one()
    except NoResultFound:
        # The mapping has never been initialized so does not exist
        mapping = MigrationMapping()
        mapping.migration_id = migration_id
        mapping.table_name = table_name
        mapping.source_entity_id = source_entity_id
        db.DBSession.add(mapping)
        db.DBSession.flush()

    mapping.target_entity_id = target_entity_id

    return mapping
