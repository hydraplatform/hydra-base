import fsspec
import h5py
import logging
import pandas as pd
from datetime import datetime

from hydra_base import config

log = logging.getLogger(__name__)


class HdfStorageAdapter():
    """
       Utilities to describe and retrieve data from HDF storage
    """

    def __init__(self):
        pass


    def open_hdf_url(self, url):
        """
          Add local filestore from config
        """
        s3f = fsspec.open(url, mode='rb', anon=True, default_fill_cache=False)
        return h5py.File(s3f.open(), mode='r')


    def get_dataset_info(self, url, dsname):
        df = pd.read_hdf(url)
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


    def get_dataset_block(self, url, dsname, start, end):
        df = pd.read_hdf(url)
        section = df[dsname][start:end]
        block_index = section.index.map(str).tolist()
        block_values = section.values.tolist()

        return {
            "index": block_index,
            "series": block_values
        }


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

        ts_nano = group["axis1"][start:end]
        ts_sec = [*map(nscale, ts_nano)]
        timestamps = [*map(str, ts_sec)]

        val_ds = group["block0_values"]
        val_rows = val_ds.shape[0]

        if start < 0 or start >= val_rows or start >= end or end < 0 or end >= val_rows:
            raise ValueError(f"Invalid section in dataset of size {val_rows}: {start=}, {end=}")

        val_sect = val_ds[start:end]
        section = [ i[0] for i in val_sect[:, series_col:series_col+1].tolist() ]

        df = pd.DataFrame({dsname: section}, index=pd.DatetimeIndex(timestamps))
        return df


def nscale(ts):
    """
      Transforms integers representing nanoseconds past the epoch
      into instances of datetime.timestamp
    """
    return datetime.fromtimestamp(ts/1e9)


if __name__ == "__main__":
    url = "/home/paul/data/eapp_new/data/ETH_flow_sim.h5"
    dsn = "BR_Kabura"
    hsa = HdfStorageAdapter()

    block_info = hsa.get_dataset_info(url, dsn)
    print(block_info)
    block_data = hsa.get_dataset_block(url, dsn,8, 16)
    print(block_data)
    df = hsa.hdf_dataset_to_pandas_dataframe(url, dsn, 8, 16)
    print(df)
    #import pudb; pudb.set_trace()
