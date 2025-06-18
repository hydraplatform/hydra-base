"""
Package for DatasetManagers and StorageAdapters.

"""

""" Dataset managers """
from .mongodatasetmanager import MongoDatasetManager

""" Storage adapters """
from .mongostorageadapter import MongoStorageAdapter
from .hdfstorageadapter import HdfStorageAdapter
