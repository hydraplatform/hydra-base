import fsspec
import h5py
import inspect
import logging
import os
import pandas as pd
import s3fs

from botocore.exceptions import ClientError
from datetime import datetime
from urllib.parse import urlparse
from functools import wraps

from hydra_base import config
from hydra_base.util import NullAdapter

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

    def __init__(self):
        self.config = self.__class__.get_hdf_config()
        self.filestore_path = self.config.get("hdf_filestore")
        if self.filestore_path and not os.path.exists(self.filestore_path):
            self.filestore_path = None

    @staticmethod
    def get_hdf_config(config_key="storage_hdf"):
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
    def open_hdf_url(self, url):
        try:
            with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
                return h5py.File(fp.fs.open(url), mode='r')
        except (ClientError, FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Unable to access url: {url}") from e

    def url_to_filestore_path(self, url, do_raise=True, check_exists=False):
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

    def file_exists_at_url(self, url):
        try:
            url = self.url_to_filestore_path(url)
            with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
                return fp.fs.exists(url)
        except (ValueError, FileNotFoundError, PermissionError):
            return False


    @filestore_url("filepath")
    def get_dataset_info_file(self, filepath, dsname):
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

    @filestore_url("filepath")
    def get_dataset_block_file(self, filepath, dsname, start, end):
        df = pd.read_hdf(filepath)
        section = df[dsname][start:end]
        block_index = section.index.map(str).tolist()
        block_values = section.values.tolist()

        return {
            "index": block_index,
            "series": block_values
        }

    @filestore_url("url")
    def size(self, url):
        with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
            size_bytes = fp.fs.size(fp.path)

        return size_bytes

    def get_hdf_groups(self, url):
        h5f = self.open_hdf_url(url)
        return [*h5f.keys()]

    def get_group_columns(self, url, groupname):
        h5f = self.open_hdf_url(url)
        try:
            group = h5f[groupname]
        except KeyError as ke:
            raise ValueError(f"Data source {url} does not contain specified group: {groupname}") from ke

        localpath, filename = self.equivalent_local_path(url)
        localfile = os.path.join(localpath, filename)
        if not os.path.exists(localfile):
            self.retrieve_s3_file(url)
        df = pd.read_hdf(localfile, key=groupname)
        return df.columns.to_list()

    def hdf_group_to_pandas_dataframe(self, url, groupname=None, series=None, as_json=True):
        json_opts = {"date_format": "iso"}

        localpath, filename = self.equivalent_local_path(url)
        localfile = os.path.join(localpath, filename)
        if not os.path.exists(localfile):
            self.retrieve_s3_file(url)
        df = pd.read_hdf(localfile, key=groupname)  # pd uses key=None as equivalent to no kwarg
        if series:
            if as_json:
                return df[series].to_json(**json_opts)
            else:
                return df[series]
        if as_json:
            return df.to_json(**json_opts)
        else:
            return df

    def hdf_dataset_to_pandas_dataframe(self, url, dsname, start, end, groupname=None):
        json_opts = {"date_format": "iso"}
        h5f = self.open_hdf_url(url)
        """
          Pywr uses hdf group names implicitly and these vary by dataset type.
          Assume the first key of the hdf doc represents a group containing
          the [axis0, axis1, block0_index, block0_values] constituents of a
          Pandas dataframe.
          Alternately, a groupname may be specified for cases where a data
          source contains multiple groups.
        """
        if not groupname:
            try:
                groupname = [*h5f.keys()][0]
            except IndexError as ie:
                raise ValueError(f"Data source {url} contains no groups") from ie
        try:
            group = h5f[groupname]
        except KeyError as ke:
            raise ValueError(f"Data source {url} does not contain specified group: {groupname}") from ke
        try:
            bcols = group["axis0"][:]
        except KeyError as ke:
            try:
                df = self.hdf_group_to_pandas_dataframe(url, groupname=groupname, series=dsname, as_json=False)
                return df[start:end].to_json(**json_opts)
            except:
                pass
            raise ValueError(f"Data source {url} has invalid structure") from ke

        cols = [*map(bytes.decode, bcols)]

        try:
            series_col = cols.index(dsname)
        except ValueError as ve:
            raise ValueError(f"No series '{dsname}' in {url}") from ve

        val_ds = group["block0_values"]
        val_rows = val_ds.shape[0]

        if start < 0 or start >= val_rows or start >= end or end < 0 or end >= val_rows:
            raise ValueError(f"Invalid section in dataset of size {val_rows}: {start=}, {end=}")

        val_sect = val_ds[start:end]
        section = [ i[0] for i in val_sect[:, series_col:series_col+1].tolist() ]

        ts_nano = group["axis1"][start:end]
        ts_sec = [*map(nscale, ts_nano)]
        timestamps = [*map(str, ts_sec)]

        h5f.close()

        df = pd.DataFrame({dsname: section}, index=pd.DatetimeIndex(timestamps))
        return df.to_json(**json_opts)

    def get_dataset_info_url(self, url, dsname):
        h5f = self.open_hdf_url(url)

        groupname = [*h5f.keys()][0]
        try:
            group = h5f[groupname]
            bcols = group["axis0"][:]
        except KeyError as ke:
            raise ValueError(f"Data source {url} has invalid structure") from ke

        cols = [*map(bytes.decode, bcols)]

        try:
            _ = cols.index(dsname)
        except ValueError as ve:
            raise ValueError(f"No series '{dsname}' in {url}") from ve

        val_ds = group["block0_values"]
        val_rows = val_ds.shape[0]

        return {
            "name": dsname,
            "size": val_rows,
            "dtype": str(val_ds.dtype)
        }

    def equivalent_local_path(self, url):
        u = urlparse(url)
        filesrc = f"{u.netloc}{u.path}"
        if u.scheme == "path":
            return os.path.dirname(filesrc), os.path.basename(filesrc)
        filedir = os.path.dirname(filesrc)
        destdir = os.path.join(self.filestore_path, filedir.strip('/'))
        return destdir, os.path.basename(filesrc)

    def retrieve_s3_file(self, url):
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


def nscale(ts):
    """
      Transforms integers representing nanoseconds past the epoch
      into instances of datetime.timestamp
    """
    return datetime.fromtimestamp(ts/1e9)


"""
  If config defines [hdf_storage]::disable_hdf as "True",
  the above HdfStorageAdapter is replaced with a Null object.
"""
if HdfStorageAdapter.get_hdf_config().get("disable_hdf"):
    HdfStorageAdapter = NullAdapter
