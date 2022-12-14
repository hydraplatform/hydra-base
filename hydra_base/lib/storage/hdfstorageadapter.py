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


    @filestore_url("filepath")
    def get_dataset_info_file(self, filepath, dsname, **kwargs):
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
    def get_dataset_block_file(self, filepath, dsname, start, end, **kwargs):
        df = pd.read_hdf(filepath)
        section = df[dsname][start:end]
        block_index = section.index.map(str).tolist()
        block_values = section.values.tolist()

        return {
            "index": block_index,
            "series": block_values
        }

    @filestore_url("url")
    def size(self, url, **kwargs):
        with fsspec.open(url, mode='rb', anon=True, default_fill_cache=False) as fp:
            size_bytes = fp.fs.size(fp.path)

        return size_bytes

    def get_hdf_groups(self, url, **kwargs):
        h5f = self.open_hdf_url(url)
        return [*h5f.keys()]

    def get_group_columns(self, url, groupname, **kwargs):
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

    def hdf_group_to_pandas_dataframe(self, url, groupname=None, series=None, as_json=True, **kwargs):
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

    def hdf_dataset_to_pandas_dataframe(self, url, dsname, start, end, groupname=None, **kwargs):
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

    def get_dataset_info_url(self, url, dsname, **kwargs):
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


class FrameGroupReader():
    """
      pandas_type == "frame"
    """
    def __init__(self, hf, groupname):
        self.hf = hf
        try:
            self.group = hf[groupname]
        except KeyError as ke:
            raise ValueError(f"Error: file {hf.filename} contains no group {groupname}") from ke

    def get_columns_of_group(self):
        try:
            columns_raw = [*self.group["axis0"]]
        except KeyError as ke:
            raise ValueError(f"Error: {self.group.name} has invalid format") from ke
        return [col.decode() for col in columns_raw]

    def get_columns_by_block(self):
        try:
            nblocks = self.group.attrs["nblocks"]
        except KeyError as ke:
            raise KeyError(f"Error: group {self.group.name} contains no blocks") from ke
        blocks_columns = []
        for block_idx in range(nblocks):
            columns_raw = [*self.group[f"block{block_idx}_items"]]
            blocks_columns.append([col.decode() for col in columns_raw])
        return blocks_columns

    def make_column_map_of_group(self, blocks):
        group_cols = self.get_columns_of_group()
        return [column_to_block_coord(column, blocks) for column in group_cols]

    def get_series_by_column_names(self, column_names, start=None, end=None):
        columns = self.get_columns_of_group()
        block_columns = self.get_columns_by_block()

        column_map = self.make_column_map_of_group(block_columns)
        named_map = {cname: cmap for cname, cmap in zip(columns, column_map)}

        start = start or 0
        end = end or len(table)
        column_series = {}
        for column_name in column_names:
            block_idx, col_idx = named_map[column_name]
            block_values_name = f"block{block_idx}_values"
            block_values = self.group[block_values_name]
            section = np.array([row[col_idx] for row in block_values[start:end]])
            column_series[column_name] = section

        return column_series

    def find_index_axis_index(self):
        for ent in self.group:
            try:
                index_class = self.group[ent].attrs["index_class"]
                return ent
            except KeyError:
                continue

        raise ValueError(f"Group {self.group.name} of pandas_type "
                          "'frame' contains no index axis")

    def get_index_range(self, start=None, end=None):
        start = start or 0
        end = end or len(table)

        index_axis = self.find_index_axis_index()
        index = self.group[index_axis]
        return [nscale(row) for row in index[start:end]]

    def get_columns_as_dataframe(self, columns, start=None, end=None):
        index_range = self.get_index_range(start, end)
        column_series = self.get_series_by_column_names(columns, start, end)
        return make_pandas_dataframe(index_range, column_series)

    def get_group_shape(self):
        row_sz = len(self.group["axis1"])
        col_sz = len(self.get_columns_of_group())
        return (row_sz, col_sz)


class FrameTableGroupReader():
    """
      pandas_type == "frame_table"
    """
    def __init__(self, hf, groupname):
        self.hf = hf
        try:
            self.group = hf[groupname]
        except KeyError as ke:
            raise ValueError(f"Error: file {hf.filename} contains no group {groupname}") from ke
        try:
            self.table = self.group["table"]
        except KeyError as ke:
            raise ValueError(f"Error: file {hf.filename} has invalid format, no 'table'") from ke

    def get_index_column_index(self):
        max_index_depth = 128
        try:
            index_cols_raw = self.group.attrs["index_cols"].decode().split('\n')[2:-2:2]
        except KeyError:
            # No index columns present
            return None
        index_cols = trim_x_first_y_rest(1, 2, index_cols_raw)
        index_name = index_cols[0]  # Assume single index

        for field_idx in range(max_index_depth):
            try:
                field_name = self.table.attrs[f"FIELD_{field_idx}_NAME"]
            except KeyError:
                # No field has the name of the stated index column
                return None
            if field_name.decode() == index_name:
                return field_idx

    def get_values_start_block_index(self):
        max_value_depth = 128
        try:
            value_cols_raw = self.group.attrs["values_cols"].decode().split('\n')[1:-1:2]
        except KeyError:
            # Group contains no value columns
            return None
        value_cols = trim_x_first_y_rest(1, 2, value_cols_raw)
        first_value_col = value_cols[0]

        for field_idx in range(max_value_depth):
            try:
                field_name = self.table.attrs[f"FIELD_{field_idx}_NAME"]
            except KeyError:
                # No more fields
                return None
            if field_name.decode() == first_value_col:
                return field_idx

    def get_index_range(self, start=None, end=None):
        start = start or 0
        end = end or len(table)

        index_idx = self.get_index_column_index()
        if index_idx is None:
            return []
        return [nscale(row[index_idx]) for row in self.table[start:end]]

    def get_columns_by_block(self):
        value_cols_raw = self.group.attrs["values_cols"].decode().split('\n')[1:-1:2]
        value_cols = trim_x_first_y_rest(1, 2, value_cols_raw)

        block_columns = []
        for field_idx, field_name in enumerate(value_cols, start=1):
            kind = self.table.attrs[f"{field_name}_kind"]
            kind_cols_raw = kind.decode().split('\n')[1:-1:2]
            kind_cols = trim_x_first_y_rest(1, 2, kind_cols_raw)
            block_columns.append(kind_cols)

        return block_columns

    def get_series_by_column_names(self, column_names, start=None, end=None):
        columns = self.get_columns_of_group()
        block_columns = self.get_columns_by_block()

        column_map = self.make_column_map_of_group(block_columns)
        named_map = {cname: cmap for cname, cmap in zip(columns, column_map)}

        first_value_block_idx = self.get_values_start_block_index()

        start = start or 0
        end = end or len(table)
        column_series = {}
        for column_name in column_names:
            block_idx, col_idx = named_map[column_name]
            block_rows = np.array([row[block_idx+first_value_block_idx] for row in self.table[start:end]])
            section = block_rows[:, col_idx]
            column_series[column_name] = section

        return column_series

    def get_columns_as_dataframe(self, columns, start=None, end=None):
        index_range = self.get_index_range(start, end)
        column_series = self.get_series_by_column_names(columns, start, end)
        return make_pandas_dataframe(index_range, column_series)

    def make_column_map_of_group(self, blocks):
        non_index_cols = self.get_columns_of_group()
        return [column_to_block_coord(column, blocks) for column in non_index_cols]

    def get_columns_of_group(self):
        non_index_cols_raw = self.group.attrs["non_index_axes"].decode().split("\n")[3:-3:2]
        return trim_x_first_y_rest(1, 2, non_index_cols_raw)

    def get_group_shape(self):
        row_sz = len(self.table)
        col_sz = len(self.get_columns_of_group())
        return (row_sz, col_sz)


"""
  Auxiliary Functions
"""

def nscale(ts):
    """
      Transforms integers representing nanoseconds past the epoch
      into instances of datetime.timestamp
    """
    return datetime.fromtimestamp(ts/1e9)

def trim_x_first_y_rest(x, y, names):
    return [names[0][x:], *[name[y:] for name in names[1:]] ]

def column_to_block_coord(column, blocks):
    for block_idx, block in enumerate(blocks):
        try:
            col_idx = block.index(column)
        except ValueError:
            continue
        return (block_idx, col_idx)
    raise ValueError(f"Column {column} not found in any block")

def make_pandas_dataframe(index, series):
    return pd.DataFrame(
        {name: values for name, values in series.items()},
        index=pd.DatetimeIndex(index)
    )


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
