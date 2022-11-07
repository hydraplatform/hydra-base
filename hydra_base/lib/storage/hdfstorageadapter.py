import fsspec
import h5py
import inspect
import logging
import os
import pandas as pd

from botocore.exceptions import ClientError
from datetime import datetime
from urllib.parse import urlparse
from functools import wraps

from hydra_base import config

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
        hdf_keys = [k for k in config.CONFIG.options(config_key) if k not in config.CONFIG.defaults()]
        hdf_items = {k: config.CONFIG.get(config_key, k) for k in hdf_keys}
        for k in numeric:
            hdf_items[k] = int(hdf_items[k])

        return hdf_items

    @filestore_url("url")
    def open_hdf_url(self, url):
        try:
            with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
                return h5py.File(fp.fs.open(url), mode='r')
        except (ClientError, FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Unable to access url: {url}") from e

    def url_to_filestore_path(self, url):
        u = urlparse(url)
        if self.filestore_path and u.scheme in ("", "file"):
            relpath = u.path.lstrip("/")
            url = os.path.join(self.filestore_path, relpath)
            print(f"{url=}")
        elif u.scheme == "path":
            url = u.path

        return url

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

    def hdf_dataset_to_pandas_dataframe(self, url, dsname, start, end):
        h5f = self.open_hdf_url(url)
        """
          Pywr uses hdf group names implicitly and these vary by dataset type.
          Assume the first key of the hdf doc represents a group containing
          the [axis0, axis1, block0_index, block0_values] constituents of a
          Pandas dataframe.
        """

        groupname = [*h5f.keys()][0]
        try:
            group = h5f[groupname]
            bcols = group["axis0"][:]
        except KeyError as ke:
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
        return df.to_json()

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
            series_col = cols.index(dsname)
        except ValueError as ve:
            raise ValueError(f"No series '{dsname}' in {url}") from ve

        val_ds = group["block0_values"]
        val_rows = val_ds.shape[0]

        return {
            "name": dsname,
            "size": val_rows,
            "dtype": str(val_ds.dtype)
        }


def nscale(ts):
    """
      Transforms integers representing nanoseconds past the epoch
      into instances of datetime.timestamp
    """
    return datetime.fromtimestamp(ts/1e9)




if __name__ == "__main__":
    bad_url = "does_not_exist.h5"
    bad_aws_url = "s3://modelers-data-bucket/eapp/single/does_not_exist.h5"
    path_url = "path:///home/paul/data/eapp_new/data/ETH_flow_sim.h5"
    fs_url = "ETH_flow_sim.h5"
    remote_url = "s3://terrafusiondatasampler/P233/TERRA_BF_L1B_O12236_20020406135439_F000_V001.h5"
    aws_url = "s3://modelers-data-bucket/eapp/single/ETH_flow_sim.h5"
    dsn = "BR_Kabura"

    hsa = HdfStorageAdapter()
    print(f"{hsa.config=}")
    print()

    block_info = hsa.get_dataset_info_url(bad_aws_url, dsn)
    print(block_info)
    block_info = hsa.get_dataset_info_file(fs_url, dsn)
    print(block_info)
    block_info = hsa.get_dataset_info_url(fs_url, dsn)
    print(block_info)
    print()
    block_data = hsa.get_dataset_block_file(fs_url, dsn, 8, 16)
    print(block_data)
    block_data = hsa.get_dataset_block_file(fs_url, dsn, 12008, 12016)
    print(block_data)
    df = hsa.hdf_dataset_to_pandas_dataframe(aws_url, dsn, 8, 16)
    print(df)
    df = hsa.hdf_dataset_to_pandas_dataframe(aws_url, dsn, 12008, 12016)
    print(df)
    print(f"{hsa.size(path_url)=}")
    print(f"{hsa.size(remote_url)=}")
    print(f"{hsa.size(aws_url)=}")
    #import pudb; pudb.set_trace()
