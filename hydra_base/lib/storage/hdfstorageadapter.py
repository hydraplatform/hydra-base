import fsspec
import h5py
import inspect
import logging
import numpy as np
import os
import pandas as pd
import s3fs

from botocore.exceptions import ClientError
from datetime import datetime
from urllib.parse import urlparse
from functools import wraps

from hydra_base import config
from hydra_base.util import NullAdapter

from hydra_base.lib.storage.readers import (
    FrameGroupReader,
    FrameTableGroupReader
)

log = logging.getLogger(__name__)


def filestore_url(argname, rewrite_func="url_to_filestore_path"):
    """
      Decorator which rewrites the <argname> argument to the decorated function
      to include the appropriate path according to the <rewrite_func> argument.
    """
    def dfunc(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            fas = inspect.getfullargspec(func)
            idx = fas.args.index(argname)
            av = inspect.getargvalues(inspect.currentframe())
            """
              NB. self appears in args from getfullargspec, but not in
              getargvalues().locals(). Adjust the index to account for
              this difference.
            """
            idx -= 1
            pre_arg = av.locals["args"][idx]
            try:
                rw_func = getattr(self, rewrite_func)
            except AttributeError as ae:
                raise ValueError(f"Invalid rewrite_func '{rewrite_func}' for @filestore_url") from ae
            # rw_func is a bound method so may be called directly
            post_arg = rw_func(pre_arg)
            insert = (*av.locals["args"][:idx], post_arg, *av.locals["args"][idx+1:])
            return func(self, *insert, **kwargs)

        return wrapper
    return dfunc


class HdfStorageAdapter():
    """
       Utilities to describe and retrieve data from HDF storage
    """

    group_readers = {
        "frame": FrameGroupReader,
        "frame_table": FrameTableGroupReader
    }

    def __init__(self):
        self.config = self.__class__.get_hdf_config()
        self.filestore_path = self.config.get("hdf_filestore")
        if self.filestore_path and not os.path.exists(self.filestore_path):
            self.filestore_path = None

    @staticmethod
    def get_hdf_config(config_key="storage_hdf", **kwargs):
        numeric = ()
        boolean = ("disable_hdf", )
        hdf_keys = [k for k in config.CONFIG.options(config_key) if k not in config.CONFIG.defaults()]
        hdf_items = {k: config.CONFIG.get(config_key, k) for k in hdf_keys}
        for k in numeric:
            hdf_items[k] = int(hdf_items[k])
        for k in boolean:
            hdf_items[k] = hdf_items[k].lower() in ("true", "yes")

        return hdf_items

    @filestore_url("url")
    def open_hdf_url(self, url, **kwargs):
        try:
            with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
                return h5py.File(fp.fs.open(url), mode='r')
        except (ClientError, FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Unable to access url: {url}") from e

    def url_to_filestore_path(self, url, do_raise=True, check_exists=False, **kwargs):
        u = urlparse(url)
        if u.path == "" and do_raise:
            raise ValueError(f"Invalid URL: {url}")
        if self.filestore_path and u.scheme in ("", "file"):
            relpath = u.path.lstrip("/")
            relurl = os.path.join(self.filestore_path, relpath)
        elif u.scheme == "path":
            relurl = u.path
        else:
            relurl = url

        return relurl

    def file_exists_at_url(self, url, **kwargs):
        try:
            url = self.url_to_filestore_path(url)
            with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
                return fp.fs.exists(url)
        except (ValueError, FileNotFoundError, PermissionError):
            return False

    #@filestore_url("filepath")
    #def get_dataset_info_file(self, filepath, dsname, **kwargs):
    @filestore_url("url")
    def get_group_info_file(self, url, group, **kwargs):
        """
          Todo rewrite with no pandas.read_hdf
        """
        df = pd.read_hdf(filepath)
        series = df[dsname]
        index = df.index
        info = {
          "index":  {"name": index.name,
                     "length": len(index),
                     "dtype": str(index.dtype)
                    },
          "series": {"name": series.name,
                     "length": len(series),
                     "dtype": str(series.dtype)
                    }
        }
        return info

    @filestore_url("url")
    def size(self, url, **kwargs):
        with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
            size_bytes = fp.fs.size(fp.path)

        return size_bytes

    @filestore_url("url")
    def get_hdf_groups(self, url, **kwargs):
        h5f = self.open_hdf_url(url)
        return [*h5f.keys()]

    @filestore_url("url")
    def get_group_columns(self, url, groupname, **kwargs):
        h5f = self.open_hdf_url(url)
        try:
            group = h5f[groupname]
        except KeyError as ke:
            raise ValueError(f"Data source {url} does not contain specified group: {groupname}") from ke
        # todo

    def hdf_get_columns_as_dataframe(self, url, groupname=None, columns=None, start, end, **kwargs):
        json_opts = {"date_format": "iso"}
        hf = self.open_hdf_url(url)

        if not groupname:
            try:
                groupname = [*hf.keys()][0]
            except IndexError as ie:
                raise ValueError(f"Data source {url} contains no groups") from ie
        try:
            group = hf[groupname]
        except KeyError as ke:
            raise ValueError(f"Data source {url} does not contain specified group: {groupname}") from ke

        if not columns:
            columns = reader.get_columns_of_group()

        reader = self.make_reader(url, groupname)
        df = reader.get_columns_as_dataframe(columns, start=start, end=end)

        return df.to_json(**json_opts)

    def get_dataset_info_url(self, url, dsname, **kwargs):
        """
          Todo: preserve return
        """

        return {
            "name": dsname,
            "size": val_rows,
            "dtype": str(val_ds.dtype)
        }

    def equivalent_local_path(self, url, **kwargs):
        u = urlparse(url)
        filesrc = f"{u.netloc}{u.path}"
        if u.scheme == "path":
            return os.path.dirname(filesrc), os.path.basename(filesrc)
        filedir = os.path.dirname(filesrc)

        if self.filestore_path is not None:
            destdir = os.path.join(self.filestore_path, filedir.strip('/'))
        else:
            destdir = filedir
        return destdir, os.path.basename(filesrc)

    """
      Make this explicitly callable to force file
      to local cache at user's discretion
    """
    def retrieve_s3_file(self, url, **kwargs):
        u = urlparse(url)
        if not u.scheme == "s3":
            return
        filesrc = f"{u.netloc}{u.path}"
        destdir, filename = self.equivalent_local_path(url)

        if not os.path.exists(destdir):
            try:
                os.makedirs(destdir)
            except OSError as err:
                raise OSError(f"Unable to create local path at {destdir}: {err}")

        destfile = os.path.join(destdir, filename)
        fs = s3fs.S3FileSystem(anon=True)
        log.info(f"Retrieving {url} to {destfile} ...")
        fs.get(filesrc, destfile)
        file_sz = os.stat(destfile).st_size
        log.info(f"Retrieved {destfile} ({file_sz} bytes)")
        return destfile, file_sz

    def make_reader(self, url, groupname):
        Reader = self.get_group_reader(url, groupname)
        hf = self.open_hdf_url(url)

        return Reader(hf, groupname)

    def get_group_reader(self, url, groupname):
        group_type = self.identify_group_format(url, groupname)
        try:
            Reader = self.__class__.group_readers[group_type]
        except KeyError:
            # Not-None group_type was returned, but we don't have a reader for it
            raise ValueError(f"Error: No reader available for group of type {group_type}")

        return Reader

    def identify_group_format(self, url, groupname):
        hf = self.open_hdf_url(url)
        try:
            group = hf[groupname]
        except KeyError as ke:
            raise ValueError(f"Error: file at {url} contains no group {groupname}") from ke

        try:
            pandas_type = group.attrs["pandas_type"]
        except KeyError as ke:
            raise ValueError(f"Error: file at {url} has invalid format") from ke

        return pandas_type.decode()


"""
  If config defines [hdf_storage]::disable_hdf as "True",
  the above HdfStorageAdapter is replaced with a Null object.
"""
if HdfStorageAdapter.get_hdf_config().get("disable_hdf"):
    HdfStorageAdapter = NullAdapter


if __name__ == "__main__":
    filepath = "grid_data.h5"
    #groupname = "central_south_essex_results"
    groupname = "ESW_Essex_results"
    #groupname = "daily_profiles"
    hsa = HdfStorageAdapter()

    pt = hsa.identify_group_format(filepath, groupname)
    hf = hsa.open_hdf_url(filepath)
    if pt == "frame_table":
        reader = FrameTableGroupReader(hf, groupname)
        gc = reader.get_columns_of_group()
        df = reader.get_columns_as_dataframe(gc[:4], start=2, end=10)
    elif pt == "frame":
        reader = FrameGroupReader(hf, groupname)
        gc = reader.get_columns_of_group()
        df = reader.get_columns_as_dataframe(gc[:4], start=2, end=10)
    s = reader.get_group_shape()
    breakpoint()
