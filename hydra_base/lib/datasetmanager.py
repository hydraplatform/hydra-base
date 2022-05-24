"""
Defines a descriptor which manages access to dataset storage
"""
from hydra_base.exceptions import HydraError
from hydra_base.lib.adaptors import HydraMongoDatasetAdaptor

import logging
log = logging.getLogger(__name__)


class DatasetManager():
    def __init__(self, ref_key="value_ref", loc_key="value_storage_location"):
        self.ref_key = ref_key
        self.loc_key = loc_key
        self.mongo = HydraMongoDatasetAdaptor() # Default config from hydra.ini


    def __set_name__(self, dataset, attr):
        """ Always 'value' """
        self.instattr = attr


    def __get__(self, dataset, dtype=None):
        log.info(f"* Dataset read: on {dataset=}")
        value = getattr(dataset, self.ref_key)
        if loc := self.get_storage_location(dataset):
            log.info(f"* External storage {loc=} with id='{value}'")
            if loc == "mongodb":
                document = self.mongo.get_document_by_object_id(value)
                return document["value"]

        return value


    def __set__(self, dataset, value):
        try:
            size = len(value)
        except TypeError as err:
            raise HydraError(f"{value=} written to dataset has invalid type {type(value)=}") from err

        log.info(f"* Dataset write: {size=} {value=} on {dataset=}")
        if loc := self.get_storage_location(dataset):
            log.info(f"* External write to {loc=}")
            if loc == "mongodb":
                oid = getattr(dataset, self.ref_key)
                self.mongo.set_document_value(oid, value)
        else:
            setattr(dataset, self.ref_key, value)


    def get_storage_location(self, dataset):
        for datum in dataset.metadata:
            if datum.key == self.loc_key:
                return datum.value
