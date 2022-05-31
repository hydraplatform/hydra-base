from abc import (
    ABC,
    abstractmethod
)

from bson.objectid import ObjectId
from pymongo import MongoClient

from hydra_base import config


class DatasetAdaptor(ABC):

    @abstractmethod
    def get_value(self, *args, **kwargs):
        pass

    @abstractmethod
    def set_value(self, *args, **kwargs):
        pass

    @abstractmethod
    def create_value(self, *args, **kwargs):
        pass

    @abstractmethod
    def delete_value(self, *args, **kwargs):
        pass


class HydraMongoDatasetAdaptor(DatasetAdaptor):
    def __init__(self, config_key="mongodb"):
        self.host = config.get(config_key, "host")
        self.port = config.get(config_key, "port")
        self.db_name = config.get(config_key, "db_name")
        self.datasets = config.get(config_key, "datasets")
        # !!! NB BULK INSERTION TEST COLLECTION HERE
        #self.datasets = "bitest"
        # Todo: Add user and passwd

        self.client = MongoClient(f"mongodb://{self.host}:{self.port}")
        self.db = self.client[self.db_name]


    def get_document_by_object_id(self, object_id, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = path.find_one({"_id": ObjectId(object_id)})
        return doc

    def get_document_by_oid_inst(self, object_id, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = path.find_one({"_id": object_id})
        return doc

    def delete_document_by_object_id(self, object_id, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = {"_id": ObjectId(object_id)}
        path.delete_one(doc)

    def set_document_value(self, object_id, value, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = {"_id": ObjectId(object_id)}
        path.update_one(doc, {"$set": {"value": value}})

    def insert_document(self, value, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        result = path.insert_one({"value": value})
        return result.inserted_id

    def get_value(self, *args, **kwargs):
        object_id = args[0]
        collection = kwargs.get("collection")
        doc = self.get_document_by_object_id(object_id, collection)
        return doc["value"]

    def set_value(self, *args, **kwargs):
        object_id = args[0]
        value = args[1]
        collection = kwargs.get("collection")
        self.set_document_value(object_id, value, collection)

    def create_value(self, *args, **kwargs):
        value = args[0]
        collection = kwargs.get("collection")
        _id = self.insert_document(value, collection)
        return _id

    def delete_value(self, *args, **kwargs):
        object_id = args[0]
        collection = kwargs.get("collection")
        self.delete_document_by_object_id(object_id, collection)


    def bulk_insert_values(self, values, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        data = [{"value": value} for value in values]
        inserted = path.insert_many(data)
        return inserted # InsertManyResults, has .inserted_ids list


    @property
    def default_collection(self):
        return self.datasets


if __name__ == "__main__":
    adaptor = HydraMongoDatasetAdaptor()
    doc = adaptor.get_document_by_object_id("628cee2fe5e9c1f01dccb14f")
    print(doc)
    doc = adaptor.get_value("628cee2fe5e9c1f01dccb14f")
    print(doc)
