"""
Defines a descriptor which manages access to dataset storage
"""
from sqlalchemy.exc import NoResultFound

from hydra_base import config
from hydra_base.db import get_session
from hydra_base.exceptions import HydraError
from hydra_base.lib.adaptors import HydraMongoDatasetAdaptor

import logging
log = logging.getLogger(__name__)


class DatasetManager():
    def __init__(self, ref_key="value_ref", loc_key=None):
        self.ref_key = ref_key
        self.loc_key = loc_key if loc_key else config.get("mongodb", "value_location_key")
        self.loc_mongo_direct = config.get("mongodb", "direct_location_token")
        log.info(f"{self.loc_key=} {self.loc_mongo_direct=}")
        self.mongo = HydraMongoDatasetAdaptor() # Default config from hydra.ini


    def __set_name__(self, dataset, attr):
        """ Always 'value' """
        self.instattr = attr


    def __get__(self, dataset, dtype=None):
        log.info(f"* Dataset read: on {dataset=}")
        value = getattr(dataset, self.ref_key)
        #key = self.get_metadata_key(dataset, self.loc_key)
        if loc := self.get_storage_location(dataset):
            log.info(f"* External storage {loc=} with id='{value}'")
            if loc == self.loc_mongo_direct:
                return self.mongo.get_value(value)

        return value


    def __set__(self, dataset, value):
        try:
            size = len(value)
        except TypeError as err:
            raise HydraError(f"{value=} written to dataset has invalid type {type(value)=}") from err

        log.info(f"* Dataset write: {size=} {value=} on {dataset=}")
        if loc := self.get_storage_location(dataset):
            log.info(f"* External write to {loc=}")
            if loc == self.loc_mongo_direct:
                oid = getattr(dataset, self.ref_key)
                self.mongo.set_document_value(oid, value)
        else:
            setattr(dataset, self.ref_key, value)


    def _get_storage_location_lookup(self, dataset):
        for datum in dataset.metadata:
            if datum.key == self.loc_key:
                return datum.value


    def _get_storage_location_query(self, dataset):
        qry_txt = f"select `value` from tMetadata where dataset_id = {dataset.id} and `key` = '{self.loc_key}'"
        try:
            cols = get_session().execute(qry_txt).one()
        except NoResultFound:
            # The dataset's metadata does not have a location key
            return
        return cols[0]

    get_storage_location = _get_storage_location_query

    def set_storage_location(self, dataset):
        pass
