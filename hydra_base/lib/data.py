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

import datetime
import sys
from ..util.hydra_dateutil import get_datetime
import logging
from ..db.model import Dataset, Metadata, DatasetOwner, DatasetCollection,\
        DatasetCollectionItem, ResourceScenario, ResourceAttr, TypeAttr
from ..util import generate_data_hash
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import aliased, make_transient, joinedload_all
from sqlalchemy.sql.expression import case
from sqlalchemy import func
from sqlalchemy import null
from .. import db
from ..import config

from .objects import JSONObject

import pandas as pd
from ..exceptions import HydraError, PermissionError, ResourceNotFoundError
from sqlalchemy import and_, or_
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.expression import literal_column
from sqlalchemy import distinct

from collections import namedtuple

from decimal import Decimal
import copy

import json

from . import units as hydra_units

global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
global qry_in_threshold
qry_in_threshold = 999
#"2013-08-13T15:55:43.468886Z"

current_module = sys.modules[__name__]
NS = "soap_server.hydra_complexmodels"

log = logging.getLogger(__name__)

def get_dataset(dataset_id,**kwargs):
    """
        Get a single dataset, by ID
    """

    user_id = int(kwargs.get('user_id'))

    if dataset_id is None:
        return None
    try:
        dataset_rs = db.DBSession.query(Dataset.id,
                Dataset.type,
                Dataset.unit,
                Dataset.name,
                Dataset.hidden,
                Dataset.cr_date,
                Dataset.created_by,
                DatasetOwner.user_id,
                null().label('metadata'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.value).label('value')).filter(
                Dataset.id==dataset_id).outerjoin(DatasetOwner,
                                    and_(DatasetOwner.dataset_id==Dataset.id,
                                    DatasetOwner.user_id==user_id)).one()

        rs_dict = dataset_rs._asdict()

        #convert the value row into a string as it is returned as a binary
        if dataset_rs.value is not None:
            rs_dict['value'] = str(dataset_rs.value)

        if dataset_rs.hidden == 'N' or (dataset_rs.hidden == 'Y' and dataset_rs.user_id is not None):
            metadata = db.DBSession.query(Metadata).filter(Metadata.dataset_id==dataset_id).all()
            rs_dict['metadata'] = metadata
        else:
            rs_dict['metadata'] = []

    except NoResultFound:
        raise HydraError("Dataset %s does not exist."%(dataset_id))


    dataset = namedtuple('Dataset', rs_dict.keys())(**rs_dict)

    return dataset

def clone_dataset(dataset_id,**kwargs):
    """
        Get a single dataset, by ID
    """

    user_id = int(kwargs.get('user_id'))

    if dataset_id is None:
        return None

    dataset = db.DBSession.query(Dataset).filter(
            Dataset.id==dataset_id).options(joinedload_all('metadata')).first()

    if dataset is None:
        raise HydraError("Dataset %s does not exist."%(dataset_id))

    if dataset is not None and dataset.created_by != user_id:
        owner = db.DBSession.query(DatasetOwner).filter(
                                DatasetOwner.dataset_id==Dataset.id,
                                DatasetOwner.user_id==user_id).first()
        if owner is None:
            raise PermissionError("User %s is not an owner of dataset %s and therefore cannot clone it."%(user_id, dataset_id))

    db.DBSession.expunge(dataset)

    make_transient(dataset)

    dataset.name = dataset.name + "(Clone)"
    dataset.id = None
    dataset.cr_date = None

    #Try to avoid duplicate metadata entries if the entry has been cloned previously
    for m in dataset.metadata:
        if m.key in ("clone_of", "cloned_by"):
            del(m)

    cloned_meta = Metadata()
    cloned_meta.key = "clone_of"
    cloned_meta.value  = str(dataset_id)
    dataset.metadata.append(cloned_meta)
    cloned_meta = Metadata()
    cloned_meta.key = "cloned_by"
    cloned_meta.value  = str(user_id)
    dataset.metadata.append(cloned_meta)

    dataset.set_hash()
    db.DBSession.add(dataset)
    db.DBSession.flush()

    cloned_dataset = db.DBSession.query(Dataset).filter(
            Dataset.id==dataset.id).first()

    return cloned_dataset

def get_datasets(dataset_ids,**kwargs):
    """
        Get a single dataset, by ID
    """

    user_id = int(kwargs.get('user_id'))
    datasets = []
    if len(dataset_ids) == 0:
        return []
    try:
        dataset_rs = db.DBSession.query(Dataset.id,
                Dataset.type,
                Dataset.unit,
                Dataset.name,
                Dataset.hidden,
                Dataset.cr_date,
                Dataset.created_by,
                DatasetOwner.user_id,
                null().label('metadata'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)],
                        else_=Dataset.value).label('value')).filter(
                Dataset.id.in_(dataset_ids)).outerjoin(DatasetOwner,
                                    and_(DatasetOwner.dataset_id==Dataset.id,
                                    DatasetOwner.user_id==user_id)).all()

        #convert the value row into a string as it is returned as a binary
        for dataset_row in dataset_rs:
            dataset_dict = dataset_row._asdict()

            if dataset_row.value is not None:
                dataset_dict['value'] = str(dataset_row.value)

            if dataset_row.hidden == 'N' or (dataset_row.hidden == 'Y' and dataset_row.user_id is not None):
                metadata = db.DBSession.query(Metadata).filter(Metadata.dataset_id == dataset_row.id).all()
                dataset_dict['metadata'] = metadata
            else:
                dataset_dict['metadata'] = []

            datasets.append(namedtuple('Dataset', dataset_dict.keys())(**dataset_dict))


    except NoResultFound:
        raise ResourceNotFoundError("Datasets not found.")

    return datasets



def search_datasets(dataset_id=None,
                dataset_name=None,
                collection_name=None,
                data_type=None,
                unit=None,
                scenario_id=None,
                metadata_key=None,
                metadata_val=None,
                attr_id = None,
                type_id = None,
                unconnected = None,
                inc_metadata='N',
                inc_val = 'N',
                page_start = 0,
                page_size   = 2000,
                **kwargs):
    """
        Get multiple datasets, based on several
        filters. If all filters are set to None, all
        datasets in the DB (that the user is allowe to see)
        will be returned.
    """


    log.info("Searching datasets: \ndatset_id: %s,\n"
                                  "datset_name: %s,\n"
                                  "collection_name: %s,\n"
                                  "data_type: %s,\n"
                                  "unit: %s,\n"
                                  "scenario_id: %s,\n"
                                  "metadata_key: %s,\n"
                                  "metadata_val: %s,\n"
                                  "attr_id: %s,\n"
                                  "type_id: %s,\n"
                                  "unconnected: %s,\n"
                                  "inc_metadata: %s,\n"
                                  "inc_val: %s,\n"
                                  "page_start: %s,\n"
                                  "page_size: %s" % (dataset_id,
                dataset_name,
                collection_name,
                data_type,
                unit,
                scenario_id,
                metadata_key,
                metadata_val,
                attr_id,
                type_id,
                unconnected,
                inc_metadata,
                inc_val,
                page_start,
                page_size))

    if page_size is None:
        page_size = config.get('SEARCH', 'page_size', 2000)

    user_id = int(kwargs.get('user_id'))

    dataset_qry = db.DBSession.query(Dataset.id,
            Dataset.type,
            Dataset.unit,
            Dataset.name,
            Dataset.hidden,
            Dataset.cr_date,
            Dataset.created_by,
            DatasetOwner.user_id,
            null().label('metadata'),
            Dataset.value
    )

    #Dataset ID is unique, so there's no point using the other filters.
    #Only use other filters if the datset ID is not specified.
    if dataset_id is not None:
        dataset_qry = dataset_qry.filter(
            Dataset.id==dataset_id)

    else:
        if dataset_name is not None:
            dataset_qry = dataset_qry.filter(
                func.lower(Dataset.name).like("%%%s%%"%dataset_name.lower())
            )
        if collection_name is not None:
            dc = aliased(DatasetCollection)
            dci = aliased(DatasetCollectionItem)
            dataset_qry = dataset_qry.join(dc,
                        func.lower(dc.name).like("%%%s%%"%collection_name.lower())
                        ).join(dci,and_(
                            dci.collection_id == dc.id,
                            dci.dataset_id == Dataset.id))

        if data_type is not None:
            dataset_qry = dataset_qry.filter(
                func.lower(Dataset.type) == data_type.lower())

        #null is a valid unit, so we need a way for the searcher
        #to specify that they want to search for datasets with a null unit
        #rather than ignoring the unit. We use 'null' to do this.
        if unit is not None:
            unit = unit.lower()
            if unit == 'null':
                unit = None
            if unit is not None:
                dataset_qry = dataset_qry.filter(
                    func.lower(Dataset.unit) == unit)
            else:
                dataset_qry = dataset_qry.filter(
                    Dataset.unit == unit)

        if scenario_id is not None:
            dataset_qry = dataset_qry.join(ResourceScenario,
                                and_(ResourceScenario.dataset_id == Dataset.id,
                                ResourceScenario.scenario_id == scenario_id))

        if attr_id is not None:
            dataset_qry = dataset_qry.join(
                ResourceScenario, ResourceScenario.dataset_id == Dataset.id).join(
                ResourceAttr, and_(ResourceAttr.id==ResourceScenario.resource_attr_id,
                                  ResourceAttr.attr_id==attr_id))

        if type_id is not None:
            dataset_qry = dataset_qry.join(
                ResourceScenario, ResourceScenario.dataset_id == Dataset.id).join(
                ResourceAttr, ResourceAttr.id==ResourceScenario.resource_attr_id).join(
                TypeAttr, and_(TypeAttr.attr_id==ResourceAttr.attr_id, TypeAttr.type_id==type_id))

        if unconnected == 'Y':
            stmt = db.DBSession.query(distinct(ResourceScenario.dataset_id).label('dataset_id'),
                                literal_column("0").label('col')).subquery()
            dataset_qry = dataset_qry.outerjoin(
                stmt, stmt.c.dataset_id == Dataset.id)
            dataset_qry = dataset_qry.filter(stmt.c.col == None)
        elif unconnected == 'N':
            #The dataset has to be connected to something
            stmt = db.DBSession.query(distinct(ResourceScenario.dataset_id).label('dataset_id'),
                                literal_column("0").label('col')).subquery()
            dataset_qry = dataset_qry.join(
                stmt, stmt.c.dataset_id == Dataset.id)
        if metadata_key is not None and metadata_val is not None:
            dataset_qry = dataset_qry.join(Metadata,
                                and_(Metadata.dataset_id == Dataset.id,
                                func.lower(Metadata.key).like("%%%s%%"%metadata_key.lower()),
                                func.lower(Metadata.value).like("%%%s%%"%metadata_val.lower())))
        elif metadata_key is not None and metadata_val is None:
            dataset_qry = dataset_qry.join(Metadata,
                                and_(Metadata.dataset_id == Dataset.id,
                                func.lower(Metadata.key).like("%%%s%%"%metadata_key.lower())))
        elif metadata_key is None and metadata_val is not None:
            dataset_qry = dataset_qry.join(Metadata,
                                and_(Metadata.dataset_id == Dataset.id,
                                func.lower(Metadata.value).like("%%%s%%"%metadata_val.lower())))

    #All datasets must be joined on dataset owner so only datasets that the
    #user can see are retrieved.
    dataset_qry = dataset_qry.outerjoin(DatasetOwner,
                                and_(DatasetOwner.dataset_id==Dataset.id,
                                DatasetOwner.user_id==user_id))

    dataset_qry = dataset_qry.filter(or_(Dataset.hidden=='N', and_(DatasetOwner.user_id is not None, Dataset.hidden=='Y')))

    log.info(str(dataset_qry))

    datasets = dataset_qry.all()

    log.info("Retrieved %s datasets", len(datasets))

    #page the datasets:
    if page_start + page_size > len(datasets):
        page_end = None
    else:
        page_end = page_start + page_size

    datasets = datasets[page_start:page_end]

    log.info("Datasets paged from result %s to %s", page_start, page_end)

    datasets_to_return = []
    for dataset_row in datasets:

        dataset_dict = dataset_row._asdict()



        if inc_val == 'N':
            dataset_dict['value'] = None
        else:
            #convert the value row into a string as it is returned as a binary
            if dataset_row.value is not None:
                dataset_dict['value'] = str(dataset_row.value)

        if inc_metadata=='Y':
            metadata = db.DBSession.query(Metadata).filter(Metadata.dataset_id==dataset_row.dataset_id).all()
            dataset_dict['metadata'] = metadata
        else:
            dataset_dict['metadata'] = []

        dataset = namedtuple('Dataset', dataset_dict.keys())(**dataset_dict)

        datasets_to_return.append(dataset)

    return datasets_to_return

def update_dataset(dataset_id, name, data_type, val, units, metadata={}, flush=True, **kwargs):
    """
        Update an existing dataset
    """

    if dataset_id is None:
        raise HydraError("Dataset must have an ID to be updated.")

    user_id = kwargs.get('user_id')

    dataset = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()
    #This dataset been seen before, so it may be attached
    #to other scenarios, which may be locked. If they are locked, we must
    #not change their data, so new data must be created for the unlocked scenarios
    locked_scenarios = []
    unlocked_scenarios = []
    for dataset_rs in dataset.resourcescenarios:
        if dataset_rs.scenario.locked == 'Y':
            locked_scenarios.append(dataset_rs)
        else:
            unlocked_scenarios.append(dataset_rs)

    #Are any of these scenarios locked?
    if len(locked_scenarios) > 0:
        #If so, create a new dataset and assign to all unlocked datasets.
        dataset = add_dataset(data_type,
                                val,
                                units,
                                metadata=metadata,
                                name=name,
                                user_id=kwargs['user_id'])
        for unlocked_rs in unlocked_scenarios:
            unlocked_rs.dataset = dataset

    else:

        dataset.type  = data_type
        dataset.value = val
        dataset.set_metadata(metadata)

        dataset.unit = units
        dataset.name  = name
        dataset.created_by = kwargs['user_id']
        dataset.hash  = dataset.set_hash()

        #Is there a dataset in the DB already which is identical to the updated dataset?
        existing_dataset = db.DBSession.query(Dataset).filter(Dataset.hash==dataset.hash, Dataset.id != dataset.id).first()
        if existing_dataset is not None and existing_dataset.check_user(user_id):
            log.warn("An identical dataset %s has been found to dataset %s."
                     " Deleting dataset and returning dataset %s",
                     existing_dataset.id, dataset.id, existing_dataset.id)
            db.DBSession.delete(dataset)
            dataset = existing_dataset
    if flush==True:
        db.DBSession.flush()

    return dataset


def add_dataset(data_type, val, units, metadata={}, name="", user_id=None, flush=False):
    """
        Data can exist without scenarios. This is the mechanism whereby
        single pieces of data can be added without doing it through a scenario.

        A typical use of this would be for setting default values on types.
    """

    d = Dataset()

    d.type  = data_type
    d.value = val
    d.set_metadata(metadata)

    d.unit  = units
    d.name  = name
    d.created_by = user_id
    d.hash  = d.set_hash()

    try:
        existing_dataset = db.DBSession.query(Dataset).filter(Dataset.hash==d.hash).one()
        if existing_dataset.check_user(user_id):
            d = existing_dataset
        else:
            d.set_metadata({'created_at': datetime.datetime.now()})
            d.set_hash()
            db.DBSession.add(d)
    except NoResultFound:
        db.DBSession.add(d)

    if flush == True:
        db.DBSession.flush()
    return d

def bulk_insert_data(data, **kwargs):
    datasets = _bulk_insert_data(data, user_id=kwargs.get('user_id'), source=kwargs.get('app_name'))
    #This line exists to make the db.DBSession 'dirty',
    #thereby telling it to flush the bulk insert.
    datasets[0].name = datasets[0].name

    db.DBSession.flush()

    return datasets

def _make_new_dataset(dataset_dict):
    #If the user is not allowed to use the existing dataset, a new
    #one must be created. This means a unique hash must be created
    #To create a unique hash, add a unique piece of metadata.
    new_dataset = copy.deepcopy(dataset_dict)
    new_dataset['metadata']['created_at'] = datetime.datetime.now()
    new_hash = generate_data_hash(new_dataset)
    new_dataset['hash'] = new_hash

    return new_dataset

def _bulk_insert_data(bulk_data, user_id=None, source=None):
    """
        Insert lots of datasets at once to reduce the number of DB interactions.
        user_id indicates the user adding the data
        source indicates the name of the app adding the data
        both user_id and source are added as metadata
    """
    get_timing = lambda x: datetime.datetime.now() - x
    start_time=datetime.datetime.now()

    new_data = _process_incoming_data(bulk_data, user_id, source)
    log.info("Incoming data processed in %s", (get_timing(start_time)))

    existing_data = _get_existing_data(new_data.keys())

    log.info("Existing data retrieved.")

    #The list of dataset IDS to be returned.
    hash_id_map = {}
    new_datasets = []
    metadata         = {}
    #This is what gets returned.
    for d in bulk_data:
        dataset_dict = new_data[d.hash]
        current_hash = d.hash

        #if this piece of data is already in the DB, then
        #there is no need to insert it!
        if  existing_data.get(current_hash) is not None:

            dataset = existing_data.get(current_hash)
            #Is this user allowed to use this dataset?
            if dataset.check_user(user_id) == False:
                new_dataset = _make_new_dataset(dataset_dict)
                new_datasets.append(new_dataset)
                metadata[new_dataset['hash']] = dataset_dict['metadata']
            else:
                hash_id_map[current_hash] = dataset#existing_data[current_hash]
        elif current_hash in hash_id_map:
            new_datasets.append(dataset_dict)
        else:
            #set a placeholder for a dataset_id we don't know yet.
            #The placeholder is the hash, which is unique to this object and
            #therefore easily identifiable.
            new_datasets.append(dataset_dict)
            hash_id_map[current_hash] = dataset_dict
            metadata[current_hash] = dataset_dict['metadata']

    log.debug("Isolating new data %s", get_timing(start_time))
    #Isolate only the new datasets and insert them
    new_data_for_insert = []
    #keep track of the datasets that are to be inserted to avoid duplicate
    #inserts
    new_data_hashes = []
    for d in new_datasets:
        if d['hash'] not in new_data_hashes:
            new_data_for_insert.append(d)
            new_data_hashes.append(d['hash'])

    if len(new_data_for_insert) > 0:
    	#If we're working with mysql, we have to lock the table..
    	#For sqlite, this is not possible. Hence the try: except
        #try:
        #    db.DBSession.execute("LOCK TABLES tDataset WRITE, tMetadata WRITE")
        #except OperationalError:
        #    pass

        log.debug("Inserting new data %s", get_timing(start_time))
        db.DBSession.bulk_insert_mappings(Dataset, new_data_for_insert)
        log.debug("New data Inserted %s", get_timing(start_time))

        #try:
        #    db.DBSession.execute("UNLOCK TABLES")
        #except OperationalError:
        #    pass


        new_data = _get_existing_data(new_data_hashes)
        log.debug("New data retrieved %s", get_timing(start_time))

        for k, v in new_data.items():
            hash_id_map[k] = v

        _insert_metadata(metadata, hash_id_map)
        log.debug("Metadata inserted %s", get_timing(start_time))

    returned_ids = []
    for d in bulk_data:
        returned_ids.append(hash_id_map[d.hash])

    log.info("Done bulk inserting data. %s datasets", len(returned_ids))

    return returned_ids

def _insert_metadata(metadata_hash_dict, dataset_id_hash_dict):
    if metadata_hash_dict is None or len(metadata_hash_dict) == 0:
        return

    metadata_list = []
    for _hash, _metadata_dict in metadata_hash_dict.items():
        for k, v in _metadata_dict.items():
            metadata = {}
            metadata['key']  = str(k)
            metadata['value']  = str(v)
            metadata['dataset_id']      = dataset_id_hash_dict[_hash].id
            metadata_list.append(metadata)

    db.DBSession.execute(Metadata.__table__.insert(), metadata_list)

def _process_incoming_data(data, user_id=None, source=None):

    datasets = {}
    for d in data:
        val = d.parse_value()

        if val is None:
            log.info("Cannot parse data (dataset_id=%s). "
                         "Value not available.",d.id)
            continue

        data_dict = {
            'type': d.type,
            'name': d.name,
            'unit': d.unit,
            'created_by': user_id,
        }

        db_val = _get_db_val(d.type, val)
        data_dict['value'] = db_val

        if d.metadata is not None:
            if isinstance(d.metadata, dict):
                metadata_dict= d.metadata
            else:
                metadata_dict = json.loads(d.metadata)
        else:
            metadata_dict={}

        metadata_keys = [k.lower() for k in metadata_dict]
        if user_id is not None and 'user_id' not in metadata_keys:
            metadata_dict[u'user_id'] = str(user_id)
        if source is not None and 'source' not in metadata_keys:
            metadata_dict[u'source'] = str(source)

        data_dict['metadata'] = metadata_dict

        d.hash = generate_data_hash(data_dict)

        data_dict['hash'] = d.hash
        datasets[d.hash] = data_dict

    return datasets

def _get_db_val(data_type, val):
    if data_type in ('descriptor','scalar'):
        return str(val)
    elif data_type in ('timeseries', 'array', 'dataframe'):
        return val
    else:
        raise HydraError("Invalid data type %s"%(data_type,))

def get_metadata(dataset_ids, **kwargs):
    return _get_metadata(dataset_ids)

def _get_metadata(dataset_ids):
    """
        Get all the metadata for a given list of datasets
    """
    metadata = []
    if len(dataset_ids) == 0:
        return []
    if len(dataset_ids) > qry_in_threshold:
        idx = 0
        extent = qry_in_threshold
        while idx < len(dataset_ids):
            log.info("Querying %s metadatas", len(dataset_ids[idx:extent]))
            rs = db.DBSession.query(Metadata).filter(Metadata.dataset_id.in_(dataset_ids[idx:extent])).all()
            metadata.extend(rs)
            idx = idx + qry_in_threshold

            if idx + qry_in_threshold > len(dataset_ids):
                extent = len(dataset_ids)
            else:
                extent = extent +qry_in_threshold
    else:
        metadata_qry = db.DBSession.query(Metadata).filter(Metadata.dataset_id.in_(dataset_ids))
        for m in metadata_qry:
            metadata.append(m)

    return metadata

def _get_existing_data(hashes):

    str_hashes = [str(h) for h in hashes]

    hash_dict = {}

    datasets = []
    if len(str_hashes) > qry_in_threshold:
        idx = 0
        extent =qry_in_threshold
        while idx < len(str_hashes):
            log.info("Querying %s datasets", len(str_hashes[idx:extent]))
            rs = db.DBSession.query(Dataset).filter(Dataset.hash.in_(str_hashes[idx:extent])).all()
            datasets.extend(rs)
            idx = idx + qry_in_threshold

            if idx + qry_in_threshold > len(str_hashes):
                extent = len(str_hashes)
            else:
                extent = extent + qry_in_threshold
    else:
        datasets = db.DBSession.query(Dataset).filter(Dataset.hash.in_(str_hashes))


    for r in datasets:
        hash_dict[r.hash] = r

    log.info("Retrieved %s datasets", len(hash_dict))

    return hash_dict

def _get_datasets(dataset_ids):
    """
        Get all the datasets in a list of dataset IDS. This must be done in chunks of 999,
        as sqlite can only handle 'in' with < 1000 elements.
    """

    dataset_dict = {}

    datasets = []
    if len(dataset_ids) > qry_in_threshold:
        idx = 0
        extent =qry_in_threshold
        while idx < len(dataset_ids):
            log.info("Querying %s datasets", len(dataset_ids[idx:extent]))
            rs = db.DBSession.query(Dataset).filter(Dataset.id.in_(dataset_ids[idx:extent])).all()
            datasets.extend(rs)
            idx = idx + qry_in_threshold

            if idx + qry_in_threshold > len(dataset_ids):
                extent = len(dataset_ids)
            else:
                extent = extent + qry_in_threshold
    else:
        datasets = db.DBSession.query(Dataset).filter(Dataset.id.in_(dataset_ids))


    for r in datasets:
        dataset_dict[r.id] = r

    log.info("Retrieved %s datasets", len(dataset_dict))

    return dataset_dict

def get_all_dataset_collections(**kwargs):
    all_collections = db.DBSession.query(DatasetCollection).all()

    return all_collections

def _get_collection(collection_id):
    """
        Get a dataset collection by ID
        :param collection ID
    """
    try:
        collection = db.DBSession.query(DatasetCollection).filter(DatasetCollection.id==collection_id).one()
        return collection
    except NoResultFound:
        raise ResourceNotFoundError("No dataset collection found with id %s"%collection_id)

def _get_collection_item(collection_id, dataset_id):
    """
        Get a single dataset collection entry by collection ID and dataset ID
        :param collection ID
        :param dataset ID
    """
    collection_item = db.DBSession.query(DatasetCollectionItem).\
            filter(DatasetCollectionItem.collection_id==collection_id,
                   DatasetCollectionItem.dataset_id==dataset_id).first()
    return collection_item

def add_dataset_to_collection(dataset_id, collection_id, **kwargs):
    """
        Add a single dataset to a dataset collection.
    """
    collection_i = _get_collection(collection_id)
    collection_item = _get_collection_item(collection_id, dataset_id)
    if collection_item is not None:
        raise HydraError("Dataset Collection %s already contains dataset %s", collection_id, dataset_id)

    new_item = DatasetCollectionItem()
    new_item.dataset_id=dataset_id
    new_item.collection_id=collection_id

    collection_i.items.append(new_item)

    db.DBSession.flush()

    return 'OK'


def add_datasets_to_collection(dataset_ids, collection_id, **kwargs):
    """
        Add multiple datasets to a dataset collection.
    """
    collection_i = _get_collection(collection_id)

    for dataset_id in dataset_ids:
        collection_item = _get_collection_item(collection_id, dataset_id)
        if collection_item is not None:
            raise HydraError("Dataset Collection %s already contains dataset %s", collection_id, dataset_id)

        new_item = DatasetCollectionItem()
        new_item.dataset_id=dataset_id
        new_item.collection_id=collection_id

        collection_i.items.append(new_item)

    db.DBSession.flush()
    return 'OK'

def remove_dataset_from_collection(dataset_id, collection_id, **kwargs):
    """
        Add a single dataset to a dataset collection.
    """
    collection_i = _get_collection(collection_id)
    collection_item = _get_collection_item(collection_id, dataset_id)
    if collection_item is None:
        raise HydraError("Dataset %s is not in collection %s.",
                                                    dataset_id,
                                                    collection_id)
    db.DBSession.delete(collection_item)
    db.DBSession.flush()

    db.DBSession.expunge_all()

    return 'OK'


def check_dataset_in_collection(dataset_id, collection_id, **kwargs):
    """
        Check whether a dataset is contained inside a collection
        :param dataset ID
        :param collection ID
        :returns 'Y' or 'N'
    """

    _get_collection(collection_id)
    collection_item = _get_collection_item(collection_id, dataset_id)
    if collection_item is None:
        return 'N'
    else:
        return 'Y'



def get_dataset_collection(collection_id,**kwargs):
    try:
        collection = db.DBSession.query(DatasetCollection).filter(DatasetCollection.id==collection_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("No dataset collection found with id %s"%collection_id)

    return collection

def get_dataset_collection_by_name(collection_name,**kwargs):
    try:
        collection = db.DBSession.query(DatasetCollection).filter(DatasetCollection.name==collection_name).one()
    except NoResultFound:
        raise ResourceNotFoundError("No dataset collection found with name %s"%collection_name)

    return collection

def add_dataset_collection(collection,**kwargs):

    coln_i = DatasetCollection(name=collection.name)

    for dataset_id in collection.dataset_ids:
        datasetitem = DatasetCollectionItem(dataset_id=dataset_id)
        coln_i.items.append(datasetitem)
    db.DBSession.add(coln_i)
    db.DBSession.flush()
    return coln_i

def delete_dataset_collection(collection_id,**kwargs):

    try:
        collection = db.DBSession.query(DatasetCollection).filter(DatasetCollection.id==collection_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("No dataset collection found with id %s"%collection_id)

    db.DBSession.delete(collection)
    db.DBSession.flush()

def get_collections_like_name(collection_name,**kwargs):
    """
        Get all the datasets from the collection with the specified name
    """
    try:
        collections = db.DBSession.query(DatasetCollection).filter(DatasetCollection.name.like("%%%s%%"%collection_name.lower())).all()
    except NoResultFound:
        raise ResourceNotFoundError("No dataset collection found with name %s"%collection_name)

    return collections

def get_collection_datasets(collection_id,**kwargs):
    """
        Get all the datasets from the collection with the specified name
    """
    collection_datasets = db.DBSession.query(Dataset).filter(Dataset.id==DatasetCollectionItem.dataset_id,
                                        DatasetCollectionItem.collection_id==DatasetCollection.id,
                                        DatasetCollection.id==collection_id).all()
    return collection_datasets

def get_val_at_time(dataset_id, timestamps,**kwargs):
    """
    Given a timestamp (or list of timestamps) and some timeseries data,
    return the values appropriate to the requested times.

    If the timestamp is before the start of the timeseries data, return
    None If the timestamp is after the end of the timeseries data, return
    the last value.  """
    t = []
    for time in timestamps:
        t.append(get_datetime(time))
    dataset_i = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()
    #for time in t:
    #    data.append(td.get_val(timestamp=time))

    data = dataset_i.get_val(timestamp=t)
    if data is not None:
        dataset = JSONObject({'data': json.dumps(data)})
    else:
        dataset = JSONObject({'data': None})

    return dataset

def get_multiple_vals_at_time(dataset_ids, timestamps,**kwargs):
    """
    Given a timestamp (or list of timestamps) and a list of timeseries datasets,
    return the values appropriate to the requested times.

    If the timestamp is before the start of the timeseries data, return
    None If the timestamp is after the end of the timeseries data, return
    the last value.  """
    datasets = _get_datasets(dataset_ids)
    datetimes = []
    for time in timestamps:
        datetimes.append(get_datetime(time))

    return_vals = {}
    for dataset_i in datasets.values():
        data = dataset_i.get_val(timestamp=datetimes)
        ret_data = {}
        if type(data) is list:
            for i, t in enumerate(timestamps):
                ret_data[t] = data[i]
        return_vals['dataset_%s'%dataset_i.id] = ret_data

    return return_vals

def get_vals_between_times(dataset_id, start_time, end_time, timestep,increment,**kwargs):
    """
        Retrive data between two specified times within a timeseries. The times
        need not be specified in the timeseries. This function will 'fill in the blanks'.

        Two types of data retrieval can be done.

        If the timeseries is timestamp-based, then start_time and end_time
        must be datetimes and timestep must be specified (minutes, seconds etc).
        'increment' reflects the size of the timestep -- timestep = 'minutes' and increment = 2
        means 'every 2 minutes'.

        If the timeseries is float-based (relative), then start_time and end_time
        must be decimal values. timestep is ignored and 'increment' represents the increment
        to be used between the start and end.
        Ex: start_time = 1, end_time = 5, increment = 1 will get times at 1, 2, 3, 4, 5
    """
    try:
        server_start_time = get_datetime(start_time)
        server_end_time   = get_datetime(end_time)
        times = [server_start_time]

        next_time = server_start_time
        while next_time < server_end_time:
            if int(increment) == 0:
                raise HydraError("%s is not a valid increment for this search."%increment)
            next_time = next_time  + datetime.timedelta(**{timestep:int(increment)})
            times.append(next_time)
    except ValueError:
        try:
            server_start_time = Decimal(start_time)
            server_end_time   = Decimal(end_time)
            times = [float(server_start_time)]

            next_time = server_start_time
            while next_time < server_end_time:
                next_time = float(next_time) + increment
                times.append(next_time)
        except:
            raise HydraError("Unable to get times. Please check to and from times.")

    td = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()
    log.debug("Number of times to fetch: %s", len(times))

    data = td.get_val(timestamp=times)

    data_to_return = []
    if type(data) is list:
        for d in data:
            if d is not None:
                data_to_return.append(list(d))
    elif data is None:
        data_to_return = []
    else:
        data_to_return.append(data)

    dataset = JSONObject({'data' : json.dumps(data_to_return)})

    return dataset

def delete_dataset(dataset_id,**kwargs):
    """
        Removes a piece of data from the DB.
        CAUTION! Use with care, as this cannot be undone easily.
    """
    try:
        d = db.DBSession.query(Dataset).filter(Dataset.id==dataset_id).one()
    except NoResultFound:
        raise HydraError("Dataset %s does not exist."%dataset_id)

    dataset_rs = db.DBSession.query(ResourceScenario).filter(ResourceScenario.dataset_id==dataset_id).all()
    if len(dataset_rs) > 0:
        raise HydraError("Cannot delete %s. Dataset is used by one or more resource scenarios."%dataset_id)

    db.DBSession.delete(d)

    db.DBSession.flush()

    #Remove ORM references to children of this dataset (metadata, collection items)
    db.DBSession.expunge_all()

def read_json(json_string):
    pd.read_json(json_string)
