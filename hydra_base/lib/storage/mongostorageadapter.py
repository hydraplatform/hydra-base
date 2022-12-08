import functools
import logging
import os
import time

from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import (
    AutoReconnect,
    ServerSelectionTimeoutError
)

from hydra_base import config

log = logging.getLogger(__name__)

MAX_RECONNECTION_ATTEMPTS = 3

def autoreconnect(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        for attempt in range(MAX_RECONNECTION_ATTEMPTS):
            try:
                return func(self, *args, **kwargs)
            except AutoReconnect as ar:
                wait = 1.1 * pow(2, attempt)
                log.warning("pymongo autoreconnect in progress... "
                            f"({attempt+1} of {MAX_RECONNECTION_ATTEMPTS})")
                time.sleep(wait)
    return wrapper


class MongoStorageAdapter():
    """
    Provides an interface to DatasetManager instances to access MongoDB storage
    """
    def __init__(self):
        mongo_config = self.__class__.get_mongo_config()
        host = mongo_config["host"]
        port = mongo_config["port"]
        user = mongo_config["user"]
        passwd = mongo_config["passwd"]
        self.db_name = mongo_config["db_name"]
        self.datasets = mongo_config["datasets"]

        """ Mongo usernames/passwds require percent encoding of `:/?#[]@` chars """
        user, passwd = percent_encode(user), percent_encode(passwd)
        authtext = f"{user}:{passwd}@" if (user and passwd) else ""

        client_kwargs = {}

        if mongo_config["use_replica_set"]:
            replSet_path = os.path.expanduser(mongo_config["replset_config_path"])
            try:
                import json
                with open(replSet_path, 'r') as fp:
                    replSet = json.load(fp)
            except:
                log.critical(f"Unable to read replica set config at {replSet_path}, "
                              "reverting to single-host MongoDB connection")
                conntext = f"mongodb://{authtext}{host}:{port}"
            else:
                hosts_txt = ",".join(f"{m['host']}:{m['port']}" for m in replSet["members"])
                conntext = f"mongodb://{hosts_txt}/?replicaSet={replSet['id']}"
                client_kwargs.update({'w': 2})  # Replica set write concern level
                self.replSet = replSet
        else:
            conntext = f"mongodb://{authtext}{host}:{port}"

        try:
            self.client = MongoClient(conntext, **client_kwargs)
        except ServerSelectionTimeoutError as sste:
            log.critical(f"Unable to connect to Mongo server {conntext}: {sste}")
            raise sste

        self.db = self.client[self.db_name]

    @staticmethod
    def get_mongo_config(config_key="mongodb"):
        numeric = ("threshold",)
        boolean = ("use_replica_set",)
        mongo_keys = [k for k in config.CONFIG.options(config_key) if k not in config.CONFIG.defaults()]
        mongo_items = {k: config.CONFIG.get(config_key, k) for k in mongo_keys}
        for k in numeric:
            mongo_items[k] = int(mongo_items[k])
        for k in boolean:
            mongo_items[k] = mongo_items[k].lower() in ("true", "yes", "y")

        return mongo_items

    def __del__(self):
        """ Close connection on object destruction """
        self.client.close()

    @autoreconnect
    def get_document_by_object_id(self, object_id: str, collection=None):
        """ Retrieve the document with the specified object_id from a collection """
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = path.find_one({"_id": ObjectId(object_id)})
        return doc

    @autoreconnect
    def get_document_by_oid_inst(self, object_id: ObjectId, collection=None):
        """ Retrieve the document with the specified object_id from a collection """
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = path.find_one({"_id": object_id})
        return doc

    @autoreconnect
    def delete_document_by_object_id(self, object_id: str, collection=None):
        """ Delete the document with the specified object_id from a collection """
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = {"_id": ObjectId(object_id)}
        path.delete_one(doc)

    @autoreconnect
    def set_document_value(self, object_id: str, value, collection=None):
        """
        Set the `value` key of the document with the specified object_id
        to the `value` argument
        """
        collection = collection if collection else self.datasets
        path = self.db[collection]
        doc = {"_id": ObjectId(object_id)}
        path.update_one(doc, {"$set": {"value": value}})

    @autoreconnect
    def insert_document(self, value, collection=None):
        """ Insert a document with the specified `value` into a collection """
        collection = collection if collection else self.datasets
        path = self.db[collection]
        result = path.insert_one({"value": value})
        return result.inserted_id

    @autoreconnect
    def bulk_insert_values(self, values, collection=None):
        """
        Insert a list of `values` into a collection.
        Returns an instance of InsertManyResults containing an .inserted_ids list
        """
        collection = collection if collection else self.datasets
        path = self.db[collection]
        data = [{"value": value} for value in values]
        inserted = path.insert_many(data)
        return inserted

    @property
    def default_collection(self):
        return self.datasets




def percent_encode(s, xchars=":/?#[]@"):
    return "".join(f"%{ord(char):2X}" if char in xchars else char for char in s)
