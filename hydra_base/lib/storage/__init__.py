"""
Package for DatasetManagers and StorageAdapters.

"""

""" Dataset managers """
from .mongodatasetmanager import MongoDatasetManager

""" Values in a datasets store (S3), read back by the dataset managers """
from .datasetstore import DatasetStore, DatasetStoreError, is_store_pointer, resolve_store_value

""" Storage adapters """
from .mongostorageadapter import MongoStorageAdapter
from .hdfstorageadapter import HdfStorageAdapter
