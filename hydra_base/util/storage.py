"""
Utilities to assist with managing datasets in external storage.

It should be noted that when exporting and importing datasets
to and from external storage, the Hydra config size threshold
is ignored, but this is still applied to any changes to datasets
performed via Hydra.
"""
import logging
import transaction

from bson.objectid import ObjectId
from pymongo import MongoClient
from sqlalchemy.sql.expression import func

from hydra_base import db
from hydra_base.db.model import (
    Dataset,
    Metadata
)
from hydra_base.lib.storage import get_mongo_config

log = logging.getLogger(__name__)

if not db.DBSession:
    db.connect()

mongo = None


def largest_datasets(count=20):
    """
    Identify the `count` largest datasets in the SQL db and return ids
    and sizes of these.
    """
    datasets = db.DBSession.query(Dataset.id, func.length(Dataset.value))\
                           .order_by(func.length(Dataset.value).desc())\
                           .limit(count).all()
    return datasets


def datasets_larger_than(size):
    """
    Identify any datasets larger than `size` chars and return the ids and
    sizes of these.
    """
    datasets = db.DBSession.query(Dataset.id, func.length(Dataset.value))\
                           .filter(func.length(Dataset.value) > size)\
                           .order_by(func.length(Dataset.value).desc()).all()
    return datasets


def export_dataset_to_external_storage(ds_id, db_name=None, collection=None):
    """
    Place the value of the dataset identified by `ds_id` in external
    storage, replace the value with an ObjectID reference, and
    update the dataset metadata to indicate the new location.
    """
    mongo_config = get_mongo_config()
    db_name = db_name if db_name else mongo_config["db_name"]
    collection = collection if collection else mongo_config["datasets"]

    mongo = get_mongo_client()
    path = mongo[db_name][collection]

    dataset = db.DBSession.query(Dataset).filter(Dataset.id == ds_id).one()
    """ Verify dataset does not already have external storage metadata """
    if dataset.is_external():
        raise LookupError(f"Dataset {dataset.id} has external storage metadata")

    result = path.insert_one({"value": dataset.value, "dataset_id": dataset.id})
    if not (hasattr(result, "inserted_id") and isinstance(result.inserted_id, ObjectId)):
        raise TypeError(f"Insertion of dataset {dataset.id} to path {db_name}:{collection} failed")

    dataset.value_ref = str(result.inserted_id)
    location_key = mongo_config["value_location_key"]
    external_token = mongo_config["direct_location_token"]
    md = Metadata(key=location_key, value=external_token)
    dataset.metadata.append(md)
    transaction.commit()

    return result


def import_dataset_from_external_storage(ds_id, db_name=None, collection=None):
    """
    Retrieve the value of the dataset identified by `ds_id` from
    external storage, and replace the SQL db dataset value with this.
    Remove any metadata associated with the external storage location.
    """
    dataset = db.DBSession.query(Dataset).filter(Dataset.id == ds_id).one()
    if not dataset.is_external():
        raise LookupError(f"Dataset {dataset.id} does not have external storage metadata")

    mongo_config = get_mongo_config()
    db_name = db_name if db_name else mongo_config["db_name"]
    collection = collection if collection else mongo_config["datasets"]

    mongo = get_mongo_client()
    path = mongo[db_name][collection]

    object_id = ObjectId(dataset.value_ref)
    doc = path.find_one({"_id": object_id})
    if not doc:
        raise LookupError(f"No external document {object_id} found for dataset {ds_id} in {db_name}:{collection}")

    """
    If the doc has a reverse reference to a dataset, ensure
    it refers to the correct dataset_id
    """
    if doc_ds_id := doc.get("dataset_id"):
        if doc_ds_id != ds_id:
            raise LookupError(f"External doc {object_id} referred to by\
                dataset {ds_id} claims to represent dataset {doc_ds_id}")

    dataset.value_ref = doc["value"]

    location_key = mongo_config["value_location_key"]
    for idx, m in enumerate(dataset.metadata):
        if m.key == location_key:
            break
    dataset.metadata.pop(idx)
    transaction.commit()

    result = path.delete_one({"_id": object_id})
    if result.deleted_count != 1:
        warntext = f"Unable to delete document {object_id} from {db_name}:{collection}"
        raise Warning(warntext)

    return result


def get_mongo_client():
    global mongo
    if mongo:
        return mongo
    mongo_config = get_mongo_config()
    mongo = MongoClient(f"mongodb://{mongo_config['host']}:{mongo_config['port']}")
    return mongo
