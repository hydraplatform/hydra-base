"""
Utilities to assist with managing datasets in external storage.

It should be noted that when exporting and importing datasets
to and from external storage, the Hydra config size threshold
is ignored, but this is still applied to any changes to datasets
performed via Hydra.
"""
import bz2
from collections import defaultdict
import io
import json
import logging
import os
import transaction
import datetime

from bson.objectid import ObjectId
import pymongo
from pymongo import MongoClient
from sqlalchemy.sql.expression import func
from sqlalchemy import not_
from sqlalchemy.exc import NoResultFound
from zope.sqlalchemy import mark_changed

from hydra_base import db
from hydra_base.db.model import (
    Network,
    Scenario,
    Dataset,
    Node,
    Link,
    Attr,
    ResourceScenario,
    ResourceGroup,
    Metadata,
    ResourceAttr
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

def export_network_datasets_to_hdf(network_id: int=None, bucket_name: str=None) -> None:
    """
    Find all the dataframe results (aka resource attribtues with an attr_is_var which have resource scenarios
    where the dataset is type dataframe)
    1. compile all of all output attributes which are dataframes
    2. group all attributes togetether (all simulated_flows, for example) into one h5 document (simulated_flow.h5) keyed on the node names
    3. Save the file to the specified s3 bucket
    4. Update each dataset to point to the new location.
    """

    scenarios = db.DBSession.query(Scenario)\
        .filter(Scenario.network_id==network_id).all()
    
    export_scenario_datasets_to_hdf(scenarios, bucket_name)

def export_project_datasets_to_hdf(project_id: int, bucket_name: str=None) -> None:
    """
    Find all the dataframe results (aka resource attribtues with an attr_is_var which have resource scenarios
    where the dataset is type dataframe)
    1. compile all of all output attributes which are dataframes
    2. group all attributes togetether (all simulated_flows, for example) into one h5 document (simulated_flow.h5) keyed on the node names
    3. Save the file to the specified s3 bucket
    4. Update each dataset to point to the new location.
    """

    scenarios = db.DBSession.query(Scenario)\
        .filter(Scenario.network_id==Network.id)\
        .filter(Network.project_id==project_id).all()
    
    export_scenario_datasets_to_hdf(scenarios, bucket_name)


def export_scenario_datasets_to_hdf(scenarios: list, bucket_name: str=None) -> None:
    from random import randbytes
    import tempfile
    import hashlib
    import hmac
    import pandas as pd
    import boto3

    hashkey = hashlib.sha256(randbytes(56)).hexdigest().encode('utf-8')

    baseqry = db.DBSession.query(Attr, ResourceAttr, Dataset, ResourceScenario)\
        .filter(ResourceScenario.dataset_id==Dataset.id)\
        .filter(ResourceScenario.resource_attr_id==ResourceAttr.id)\
        .filter(ResourceAttr.attr_is_var == 'Y')\
        .filter(Attr.id==ResourceAttr.attr_id)\
        .filter(func.lower(Dataset.type)=='dataframe')\
        .filter(not_(Dataset.value.ilike('%url%')))

    attribute_lookup = defaultdict(dict)

    scenario_ids = [s.id for s in scenarios]


    db.DBSession.expire_on_commit = False

    for scenario_id in scenario_ids:

        resourcenamelookup = {}

        scenarioresults = baseqry.filter(ResourceScenario.scenario_id == scenario_id).all()
        if len(scenarioresults) == 0:
            log.info("No datasets to migrate from scenario %s Exiting.", scenario_id)
            continue
        log.info("Processing scenario %s", scenario_id)
        #categorise the datasets by their attribute
        log.info("Categorising datasets by attribute")
        for attr, ra, dataset, rs in scenarioresults:
            resource_id = ra.get_resource_id()

            datasetvalue = json.loads(dataset.value)

            #Is the value already an external file ref?
            if datasetvalue.get('data', {}).get('url', '').startswith('s3://'):
                continue

            df = pd.read_json(io.StringIO(dataset.value))
            attribute_lookup[attr.name][resource_id] = df
            resourcenamelookup[ra.id] = {'attr_name':attr.name, 'resource_id': resource_id, 'dataset': dataset}

        if len(resourcenamelookup) == 0:
            log.info("No datasets to migrate from scenario %s Exiting.", scenario_id)
            continue

        #write results to h5 files
        results_location = os.path.join(tempfile.gettempdir(), str(scenario_id))
        os.makedirs(results_location, exist_ok=True)
        log.info("Saving datasets to h5 files in %s", results_location)
        for attr_name, resultdict in attribute_lookup.items():
            filename = f'{attr_name}.h5'
            resultstore = pd.HDFStore(os.path.join(results_location, filename), mode='w')
            for resourceid, df in resultdict.items():
                resultstore.put(f"_{resourceid}", df)
                resultstore[f"_{resourceid}"].attrs['pandas_type'] = 'frame'
            resultstore.close()

        log.info("Uploading files to s3")
        now = datetime.datetime.now().toordinal()
        key = f"${scenario_id}_{now}".encode('utf-8')
        s3_path =  hmac.digest(hashkey, key, hashlib.sha256).hex()

        #upload files to s3
        for f in os.listdir(results_location):
            log.info("Saving %s to bucket %s s3", f, bucket_name)
            s3 = boto3.client('s3')
            s3.upload_file(os.path.join(results_location, f), Bucket=bucket_name, Key=f"{s3_path}/{f}")
            log.info("%s saved to s3 bucket %s", f, bucket_name)

        log.info("Updating datasets...")
        # log.info("Updating datasets to point to external datasets")
        # #update the datasets to point to the data within the h5 files
        for namemap in resourcenamelookup.values():
            attr_name = namemap['attr_name']
            resource_id = namemap['resource_id']
            dataset = namemap['dataset']
            newvalue = json.dumps({
                "data":
                {
                    "url": f"s3://{bucket_name}/{s3_path}/{attr_name}.h5",
                    "group": f"_{resource_id}"
                }
            })
            dataset.value = newvalue

        db.DBSession.flush()
        db.commit_transaction()


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
