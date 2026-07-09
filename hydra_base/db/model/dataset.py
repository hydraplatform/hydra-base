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
from .base import *

from hydra_base.lib.storage import (
    MongoDatasetManager,
    MongoStorageAdapter
)

#***************************************************
# Classes definition
#***************************************************

mongo_config = MongoStorageAdapter.get_mongo_config()
mongo_storage_location_key = mongo_config["value_location_key"]
mongo_external = mongo_config["direct_location_token"]

__all__ = ['Dataset', 'DatasetCollection', 'DatasetCollectionItem', 'Metadata']

from .ownership import DatasetOwner

class Dataset(Base, Inspect, PermissionControlled, AuditMixin):
    """
        Table holding all the attribute values
    """
    __tablename__='tDataset'

    __ownerclass__ = DatasetOwner
    __ownerfk__    = 'dataset_id'

    id         = Column(Integer(), primary_key=True, index=True, nullable=False)
    name       = Column(String(200),  nullable=False)
    type       = Column(String(60),  nullable=False)
    unit_id    = Column(Integer(), ForeignKey('tUnit.id'),  nullable=True)
    hash       = Column(BIGINT(),  nullable=False, unique=True)
    cr_date    = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))
    hidden     = Column(String(1),  nullable=False, server_default=text(u"'N'"))
    value_ref  = Column('value', Text().with_variant(mysql.LONGTEXT, 'mysql'),  nullable=True)

    _value = MongoDatasetManager()

    @hybrid_property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        self._value = val

    @value.expression
    def value(cls):
        return cls.value_ref

    unit = relationship('Unit', backref=backref("dataset_unit", order_by=unit_id))

    _parents  = ['tResourceScenario', 'tUnit']
    _children = ['tMetadata']

    def get_value(self):
        """
            Get the value
        """
        return self.value

    def set_metadata(self, metadata_tree):
        """
        Set the metadata on a dataset.
        Note that the `mongo_storage_location_key` (MSLK) is not
        managed by this mechanism, it is instead set and deleted
        in the Dataset.value descriptor: see comments below.

        **metadata_tree**: A dictionary of metadata key-vals.
        Transforms this dict into an array of metadata objects for
        storage in the DB.
        """
        if metadata_tree is None:
            return
        if isinstance(metadata_tree, str):
            metadata_tree = json.loads(metadata_tree)

        """
        For a currently-external dataset whose value has shrunk beneath
        the size threshold, HWI will send metadata including the MSLK
        even though this is no longer applicable.
        This is deleted in the Dataset.value.__set__ action so remove
        here to avoid unwanted recreation.
        """
        metadata_tree.pop(mongo_storage_location_key, None)

        existing_metadata = []
        for m in self.metadata:
            existing_metadata.append(m.key)
            if m.key in metadata_tree:
                if m.value != metadata_tree[m.key]:
                    m.value = metadata_tree[m.key]


        for k, v in metadata_tree.items():
            if k not in existing_metadata:
                m_i = Metadata(key=str(k),value=str(v))
                self.metadata.append(m_i)

        metadata_to_delete =  set(existing_metadata).difference(set(metadata_tree.keys()))
        """
        For a dataset which is being created on external storage for the
        first time, the metadata sent by HWI will not include the MSLK,
        but this has already been added by the Dataset.value.__set__ action.
        Discard from that set here to avoid unwanted deletion.
        """
        metadata_to_delete.discard(mongo_storage_location_key)
        for m in self.metadata:
            if m.key in metadata_to_delete:
                get_session().delete(m)

    def get_val(self, timestamp=None):
        """
            If a timestamp is passed to this function,
            return the values appropriate to the requested times.

            If the timestamp is *before* the start of the timeseries data, return None
            If the timestamp is *after* the end of the timeseries data, return the last
            value.

            The raw flag indicates whether timeseries should be returned raw -- exactly
            as they are in the DB (a timeseries being a list of timeseries data objects,
            for example) or as a single python dictionary
        """
        val = get_val(self, timestamp)
        return val

    def set_hash(self,metadata=None):

        if metadata is None:
            metadata = self.get_metadata_as_dict()

        dataset_dict = {'unit_id'   : self.unit_id,
                        'type'      : self.type,
                        'value'     : self.value_ref,
                        'metadata'  : metadata}

        data_hash = generate_data_hash(dataset_dict)

        self.hash = data_hash

        return data_hash

    def get_metadata_as_dict(self):
        metadata = {}
        sortedmeta = sorted(self.metadata, key=lambda x:x.key.lower())
        for r in sortedmeta:
            val = str(r.value)

            metadata[str(r.key)] = val

        return metadata


    def _is_open(self):
        """
            Check if this dataset is globally open (the default). This negates the
            need to check for ownership
        """
        if self.hidden == 'N':
            return True

        return False


    def is_external(self):
        """
        Does the metadata indicate that this Dataset is stored in external storage?
        """
        for datum in self.metadata:
            if datum.key == mongo_storage_location_key and datum.value == mongo_external:
                return True

        return False


class DatasetCollection(Base, Inspect):
    """
    """

    __tablename__='tDatasetCollection'

    id = Column(Integer(), primary_key=True, nullable=False)
    name = Column(String(200),  nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    _parents  = ['tDataset']
    _children = ['tDatasetCollectionItem']

class DatasetCollectionItem(Base, Inspect):
    """
    """

    __tablename__='tDatasetCollectionItem'

    collection_id = Column(Integer(), ForeignKey('tDatasetCollection.id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.id'), primary_key=True, nullable=False)
    cr_date = Column(TIMESTAMP(),  nullable=False, server_default=text(u'CURRENT_TIMESTAMP'))

    collection = relationship('DatasetCollection', backref=backref("items", order_by=dataset_id, cascade="all, delete-orphan"))
    dataset = relationship('Dataset', backref=backref("collectionitems", order_by=dataset_id,  cascade="all, delete-orphan"))

    _parents  = ['tDatasetCollection']
    _children = []

class Metadata(Base, Inspect):
    """
    """

    __tablename__='tMetadata'

    dataset_id = Column(Integer(), ForeignKey('tDataset.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    key = Column(String(60), primary_key=True, nullable=False)
    value = Column(String(1000), nullable=False)

    dataset = relationship('Dataset', backref=backref("metadata", order_by=dataset_id, cascade="all, delete-orphan"))

    _parents  = ['tDataset']
    _children = []
