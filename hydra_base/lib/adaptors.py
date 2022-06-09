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
    def __init__(self):
        mongo_config = get_mongo_config()
        host = mongo_config["host"]
        port = mongo_config["port"]
        user = mongo_config["user"]
        passwd = mongo_config["passwd"]
        self.db_name = mongo_config["db_name"]
        self.datasets = mongo_config["datasets"]
        # !!! NB BULK INSERTION TEST COLLECTION HERE
        self.datasets = "bitest"

        authtext = f"{user}:{passwd}@" if (user and passwd) else ""
        self.client = MongoClient(f"mongodb://{authtext}{host}:{port}")

        self.db = self.client[self.db_name]

    def __del__(self):
        """ Close connection on object destruction """
        self.client.close()

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


def get_mongo_config(config_key="mongodb"):
    numeric = ("threshold",)
    mongo_keys = [k for k in config.CONFIG.options(config_key) if k not in config.CONFIG.defaults()]
    mongo_items = {k: config.CONFIG.get(config_key, k) for k in mongo_keys}
    for k in numeric:
        mongo_items[k] = int(mongo_items[k])

    return mongo_items
