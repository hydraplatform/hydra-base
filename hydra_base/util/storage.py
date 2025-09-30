"""
Utilities to assist with managing datasets in external storage.

It should be noted that when exporting and importing datasets
to and from external storage, the Hydra config size threshold
is ignored, but this is still applied to any changes to datasets
performed via Hydra.
"""
import bz2
import io
import json
import logging
import os
import transaction

from bson.objectid import ObjectId
import pymongo
from pymongo import MongoClient
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound

from hydra_base import db
from hydra_base.db.model import (
    Dataset,
    Metadata
)
from hydra_base.lib.storage import MongoStorageAdapter

from typing import (
    Dict,
    List,
    Tuple
)

log = logging.getLogger(__name__)

if not db.DBSession:
    db.connect()

mongo = None


def largest_datasets(count: int=20) -> List[Tuple[int, int]]:
    """
    Identify the `count` largest datasets in the SQL db and return ids
    and sizes of these.
    """
    datasets = db.DBSession.query(Dataset.id, func.length(Dataset.value))\
                           .order_by(func.length(Dataset.value).desc())\
                           .limit(count).all()
    return datasets


def datasets_larger_than(size: int) -> List[Tuple[int, int]]:
    """
    Identify any datasets larger than `size` chars and return the ids and
    sizes of these.
    """
    datasets = db.DBSession.query(Dataset.id, func.length(Dataset.value))\
                           .filter(func.length(Dataset.value) > size)\
                           .order_by(func.length(Dataset.value).desc()).all()
    return datasets


def total_datasets() -> int:
    """
    Return the total number of datasets in the SQL db
    """
    return db.DBSession.query(Dataset).count()


def dataset_distribution(buckets: int=10) -> Dict:
    """
    Return a histogram of the distribution of dataset sizes in bytes
    consisting of <buckets> divisions
    """
    max_sz = largest_datasets(1)[0][1]
    bucket_sz = max_sz/buckets

    hist = [{"lower": int(i*bucket_sz), "upper": int((i+1)*bucket_sz)} for i in range(buckets)]

    for bucket in hist:
        lower, upper = bucket["lower"], bucket["upper"]
        bucket["count"] = db.DBSession.query(Dataset.id, func.length(Dataset.value))\
                           .filter(func.length(Dataset.value) > lower)\
                           .filter(func.length(Dataset.value) <= upper)\
                           .count()
    return hist


def dataset_report() -> Dict:
    """
    Return a report containing basic statistics describing datasets
    in the SQL database
    """
    total_sz = db.DBSession.query(func.sum(Dataset.value)).scalar()
    mean = db.DBSession.query(func.avg(Dataset.value)).scalar()
    report = {
        "count": total_datasets(),
        "total_size": int(total_sz),
        "mean_size": round(mean, 2),
        "distribution": dataset_distribution()
    }

    return report


def collection_report(db_name: str=None, collection: str=None) -> Dict:
    """
    Return a report containing basic statistics describing documents
    in the specified <collection> of <db_name> Mongo database
    """
    mdb, coll = get_db_and_collection(db_name, collection)
    stats = mdb.command("collstats", coll.name)

    report = {
        "count": stats["count"],
        "total_size": stats["size"],
        "mean_size": stats["avgObjSize"],
        "distribution": document_distribution(db_name, collection)
    }

    return report


def largest_documents(count: int=20, db_name: str=None, collection: str=None) -> List[Tuple[ObjectId, int]]:
    """
    Identifies the <count> largest documents in the specified <collection>
    of the Mongo database <db_name>
    """
    _, coll = get_db_and_collection(db_name, collection)

    pipeline = [
        {"$match": {"value": {"$exists": True}}},
        {"$match": {"value": {"$type": "string"}}},  # Must ensure string args to strLenCP below
        {"$project": {
            "length": {"$strLenCP": "$value"}
            }
        },
        {"$sort": {"length": pymongo.DESCENDING}},
        {"$limit": count}
    ]

    agg = coll.aggregate(pipeline)
    return [(d["_id"], d["length"]) for d in agg]


def document_distribution(db_name: str=None, collection: str=None, buckets: str=10) -> Dict:
    """
    Returns a histogram consisting of <buckets> bins describing the distribution of
    dataset sizes in documents of the <collection> in <db_name>
    """
    _, coll = get_db_and_collection(db_name, collection)

    _, max_sz = largest_documents(1, db_name, collection)[0]
    bucket_sz = max_sz/buckets
    hist = [{"lower": int(i*bucket_sz), "upper": int((i+1)*bucket_sz)} for i in range(buckets)]

    for bucket in hist:
        pipeline = [
            {"$match": {"value": {"$exists": True}}},
            {"$match": {"value": {"$type": "string"}}},  # Must ensure string args to strLenCP below
            {"$redact": {"$cond": [ {"$gt": [{"$strLenCP": "$value"}, bucket["lower"]]}, "$$KEEP", "$$PRUNE"]}},
            {"$redact": {"$cond": [ {"$lte": [{"$strLenCP": "$value"}, bucket["upper"]]}, "$$KEEP", "$$PRUNE"]}},
            {"$count": "count"}
        ]

        result = coll.aggregate(pipeline)
        rl = [*result]
        count = rl[0]["count"] if rl else 0
        bucket["count"] = count

    return hist


def export_dataset_to_external_storage(ds_id: int, db_name: str=None, collection: str=None):
    """
    Place the value of the dataset identified by `ds_id` in external
    storage, replace the value with an ObjectID reference, and
    update the dataset metadata to indicate the new location.
    """
    mongo_config = MongoStorageAdapter.get_mongo_config()
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


def import_dataset_from_external_storage(ds_id: int, db_name: str=None, collection: str=None):
    """
    Retrieve the value of the dataset identified by `ds_id` from
    external storage, and replace the SQL db dataset value with this.
    Remove any metadata associated with the external storage location.
    """
    dataset = db.DBSession.query(Dataset).filter(Dataset.id == ds_id).one()
    if not dataset.is_external():
        raise LookupError(f"Dataset {dataset.id} does not have external storage metadata")

    mongo_config = MongoStorageAdapter.get_mongo_config()
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


def write_dataset_as_bz2(dataset_id: int, path: str='.'):
    """
    Makes a compressed copy of dataset with id <dataset_id> in a
    file named "<dataset_id>.bz2". An optional target directory may
    be provided in <path>
    """
    if not os.path.isdir(path):
        raise ValueError(f"Invalid path {path}; must be an existing directory")

    try:
        dataset = db.DBSession.query(Dataset).filter(Dataset.id == dataset_id).one()
    except NoResultFound:
        raise ValueError(f"No dataset found with id {dataset_id}")

    filename = os.path.join(path, f"{dataset.id}.bz2")
    if os.path.exists(filename):
        raise ValueError(f"File {filename} already exists")

    with bz2.BZ2File(filename, 'wb') as outfile:
        with io.TextIOWrapper(outfile, encoding='utf-8') as tiw:
            ds_size = tiw.write(dataset.value)

    f_size = os.stat(filename).st_size
    log.info(f"Written {filename} {ds_size} => {f_size} bytes ({(1-f_size/ds_size)*100.0:.2f}%)")
    return filename, f_size


def write_oid_as_bz2(oid: str, path: str='.', db_name=None, collection=None):
    """
    Makes a compressed copy of the 'value' attribute of a MongoDB document
    with oid <oid> in a file named "<oid>.bz2". An optional target directory
    may be provided in <path>
    """
    if not os.path.isdir(path):
        raise ValueError(f"Invalid path {path}; must be an existing directory")

    filename = os.path.join(path, f"{oid}.bz2")
    if os.path.exists(filename):
        raise ValueError(f"File {filename} already exists")

    mongo_config = MongoStorageAdapter.get_mongo_config()
    db_name = db_name if db_name else mongo_config["db_name"]
    collection = collection if collection else mongo_config["datasets"]

    mongo = get_mongo_client()
    path = mongo[db_name][collection]

    object_id = ObjectId(oid)
    doc = path.find_one({"_id": object_id})
    if not doc:
        raise LookupError(f"No external document {object_id} found in {db_name}:{collection}")

    with bz2.BZ2File(filename, 'wb') as outfile:
        with io.TextIOWrapper(outfile, encoding='utf-8') as tiw:
            ds_size = tiw.write(doc["value"])

    f_size = os.stat(filename).st_size
    log.info(f"Written {filename} {ds_size} => {f_size} bytes ({(1-f_size/ds_size)*100.0:.2f}%)")
    return filename, f_size


def bz2_file_equal_to_dataset(filename: str) -> bool:
    """
    Verifies that the bz2-compressed dataset contained in <filename> is
    equal to the SQL db dataset with the same dataset_id, as determined
    by the filename prefix.
    """
    _, ds_file = os.path.split(filename)
    dataset_id, _ = os.path.splitext(ds_file)

    try:
        dataset = db.DBSession.query(Dataset).filter(Dataset.id == dataset_id).one()
    except NoResultFound:
        raise ValueError(f"No dataset found with id {dataset_id}")

    with bz2.BZ2File(filename, 'rb') as infile:
        with io.TextIOWrapper(infile, encoding='utf-8') as tiw:
            file_dso = json.loads(tiw.read())

    db_dso = json.loads(dataset.value)
    return db_dso == file_dso


def bz2_file_equal_to_oid(filename: str, db_name=None, collection=None) -> bool:
    """
    Verifies that the bz2-compressed dataset contained in <filename> is
    equal to the MongoDB document with the same dataset_id, as determined
    by the filename prefix.
    """
    _, oid_file = os.path.split(filename)
    oid, _ = os.path.splitext(oid_file)

    mongo_config = MongoStorageAdapter.get_mongo_config()
    db_name = db_name if db_name else mongo_config["db_name"]
    collection = collection if collection else mongo_config["datasets"]

    mongo = get_mongo_client()
    path = mongo[db_name][collection]

    object_id = ObjectId(oid)
    doc = path.find_one({"_id": object_id})
    if not doc:
        raise LookupError(f"No external document {object_id} found in {db_name}:{collection}")

    with bz2.BZ2File(filename, 'rb') as infile:
        with io.TextIOWrapper(infile, encoding='utf-8') as tiw:
            file_oid = json.loads(tiw.read())

    db_oid = json.loads(doc["value"])
    return db_oid == file_oid


def get_mongo_client() -> MongoClient:
    global mongo
    if mongo:
        return mongo
    mongo_config = MongoStorageAdapter.get_mongo_config()
    mongo = MongoClient(f"mongodb://{mongo_config['host']}:{mongo_config['port']}")
    return mongo


def get_db_and_collection(db_name: str=None, collection: str=None):
    mongo_config = MongoStorageAdapter.get_mongo_config()
    db_name = db_name if db_name else mongo_config["db_name"]
    collection = collection if collection else mongo_config["datasets"]

    mongo = get_mongo_client()
    mdb = mongo[db_name]
    coll = mdb[collection]

    return mdb, coll
