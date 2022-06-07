"""
Utilities to assist with managing datasets in external storage.
"""
import logging

from sqlalchemy.sql.expression import func

import hydra_base
from hydra_base import db
from hydra_base.db import get_session
from hydra_base.db.model import Dataset
from hydra_base.lib.adaptors import (
    HydraMongoDatasetAdaptor,
    get_mongo_config
)

log = logging.getLogger(__name__)

if not db.DBSession:
    db.connect()


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


def export_dataset_to_external_storage(ds_id):
    """
    Place the value of the dataset identified by `ds_id` in external
    storage, replace the value with an ObjectID reference, and
    update the dataset metadata to indicate the new location.
    """
    pass


def import_dataset_from_external_storage(ds_id):
    """
    Retrieve the value of the dataset identified by `ds_id` from
    external storage, and replace the SQL db dataset value with this.
    Remove any metadata associated with the external storage location.
    """
    pass


if __name__ == "__main__":
    breakpoint()
