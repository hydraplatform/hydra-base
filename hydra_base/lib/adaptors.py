from bson.objectid import ObjectId
from pymongo import MongoClient

from hydra_base import config


class HydraMongoDatasetAdaptor():
    def __init__(self, config_key="mongodb"):
        self.host = config.get(config_key, "host")
        self.port = config.get(config_key, "port")
        self.db_name = config.get(config_key, "db_name")
        self.datasets = config.get(config_key, "datasets")
        # Todo: Add user and passwd

        self.client = MongoClient(f"mongodb://{self.host}:{self.port}")
        self.db = self.client[self.db_name]


    def get_document_by_object_id(self, object_id, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = path.find_one({"_id": ObjectId(object_id)})
        return doc


    def set_document_value(self, object_id, value, collection=None):
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = {"_id": ObjectId(object_id)}
        path.update_one(doc, {"$set": {"value": value}})


    @property
    def default_collection(self):
        return self.datasets


if __name__ == "__main__":
    adaptor = HydraMongoDatasetAdaptor()
    doc = adaptor.get_document_by_object_id("628cee2fe5e9c1f01dccb14f")
    print(doc)
