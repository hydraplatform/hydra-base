"""
Defines a descriptor which manages access to dataset storage
"""
from sqlalchemy.exc import NoResultFound

from hydra_base.db import get_session
from hydra_base.exceptions import HydraError
from hydra_base.lib.adaptors import (
    HydraMongoDatasetAdaptor,
    get_mongo_config
)

import logging
log = logging.getLogger(__name__)


class DatasetManager():
    def __init__(self, ref_key="value_ref", loc_key=None):
        mongo_config = get_mongo_config()
        self.ref_key = ref_key
        self.loc_key = loc_key if loc_key else mongo_config["value_location_key"]
        self.threshold = mongo_config["threshold"]
        self.loc_mongo_direct = mongo_config["direct_location_token"]
        self.mongo = HydraMongoDatasetAdaptor() # Default config from hydra.ini


    def __set_name__(self, dataset, attr):
        """ Always '_value' """
        self.instattr = attr


    def __get__(self, dataset, dtype=None):
        value = getattr(dataset, self.ref_key)
        log.debug(f"* Dataset read: on {dataset=} {dtype=}")
        if loc := self.get_storage_location(dataset):
            log.debug(f"* External storage {loc=} with id='{value}'")
            if loc == self.loc_mongo_direct:
                return self.mongo.get_value(value)

        return value


    def __set__(self, dataset, value):
        if not value:
            size = 0
        else:
            try:
                size = len(value)
            except TypeError as err:
                raise HydraError(f"{value=} written to dataset has invalid type {type(value)=}") from err

        log.debug(f"* Dataset write: {size=} {value=} on {dataset=}")

        loc = self.get_storage_location(dataset)
        is_mongo_direct = loc == self.loc_mongo_direct

        if is_mongo_direct:
            """ Already in external storage """
            if size <= self.threshold:
                """ Value has shrunk so restore to SQL DB """
                self.delete_storage_location(dataset)
                oid = getattr(dataset, self.ref_key)
                self.mongo.delete_value(oid)
                setattr(dataset, self.ref_key, value)
                log.debug(f"Deleted {oid=} on {dataset.id=} and restored {value=} to DB")
            else:
                """ Update in external storage """
                oid = getattr(dataset, self.ref_key)
                self.mongo.set_document_value(oid, value)
        elif size > self.threshold:
            """ Create in external storage """
            _id = self.mongo.create_value(value)
            self.set_storage_location(dataset, self.loc_mongo_direct)
            setattr(dataset, self.ref_key, str(_id))
            log.debug(f"* External create in {self.loc_mongo_direct=} as {_id=}")
        else:
            """ In SQL DB: set value directly """
            log.debug(f"* Direct set {value=}")
            setattr(dataset, self.ref_key, value)


    def _get_storage_location_lookup(self, dataset):
        if not dataset:
            return
        for datum in dataset.metadata:
            if datum.key == self.loc_key:
                return datum.value


    def _get_storage_location_query(self, dataset):
        if not dataset.id:
            return
        qry_txt = f"select `value` from tMetadata where dataset_id = {dataset.id} and `key` = '{self.loc_key}'"
        try:
            cols = get_session().execute(qry_txt).one()
        except NoResultFound:
            # The dataset's metadata does not have a location key
            return
        return cols[0]


    def _delete_storage_location_query(self, dataset):
        dataset_id = getattr(dataset, "id")
        qry_txt = f"delete from tMetadata where dataset_id = {dataset_id} and `key` = '{self.loc_key}'"
        get_session().execute(qry_txt)


    def _delete_storage_location_lookup(self, dataset):
        for idx, datum in enumerate(dataset.metadata):
            if datum.key == self.loc_key:
                break
        dataset.metadata.pop(idx)
        get_session().delete(datum)
        #get_session().flush(datum)


    def set_storage_location(self, dataset, location):
        from hydra_base.db.model import Metadata
        m = Metadata(key=self.loc_key, value=location)
        dataset.metadata.append(m)
        #get_session().flush()

    get_storage_location = _get_storage_location_lookup
    delete_storage_location = _delete_storage_location_lookup
