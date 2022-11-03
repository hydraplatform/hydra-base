import json
import numpy as np
import pandas as pd
import pytest
import random

from packaging import version

import hydra_base
from hydra_base.lib.storage import HdfStorageAdapter

min_lib_versions = {
    "fsspec": version.parse("0.9.0"),
    "h5py": version.parse("3.7.0"),
    "s3fs": version.parse("0.6.0")
}

@pytest.fixture
def hdf():
    hdf = HdfStorageAdapter()
    return hdf

@pytest.fixture
def hdf_config():
    return HdfStorageAdapter.get_hdf_config()

@pytest.fixture
def aws_file():
    return {
        "path": "s3://modelers-data-bucket/eapp/single/ETH_flow_sim.h5",
        "file_size": 7374454,
        "dataset_name": "BR_Kabura",
        "dataset_size": 12784,
        "dataset_type": "float64"
    }


class TestHdf():
    def test_exists(self, hdf, hdf_config):
        """
          Can the HDF adapter be instantiated, and does the config exist?
        """
        assert hdf
        assert hdf_config

    def test_lib_versions(self):
        """
          Are the required libraries present and adequate versions?
        """
        import importlib
        for libname, semver in min_lib_versions.items():
            lib = importlib.import_module(libname)
            assert version.parse(lib.__version__) >= semver

    def test_hdf_size(self, hdf, aws_file):
        """
          Does the reported file size match an expected value?
        """
        assert hdf.size(aws_file["path"]) == aws_file["file_size"]

    def test_hdf_info(self, hdf, aws_file):
        """
          Do the reported properties of a dataset match expected values?
        """
        info = hdf.get_dataset_info_url(aws_file["path"], aws_file["dataset_name"])

        assert info["name"] == aws_file["dataset_name"]
        assert info["size"] == aws_file["dataset_size"]
        assert info["dtype"] == aws_file["dataset_type"]

    def test_hdf_dataset(self, hdf, aws_file):
        """
          Does a specified subset of a dataset match its expected
          index and series values?
        """
        expected = {
            aws_file["dataset_name"]: {
                (8,16): { "1972-01-09": 0.65664,
                          "1972-01-10": 0.65664,
                          "1972-01-11": 0.65664,
                          "1972-01-12": 0.65664,
                          "1972-01-13": 0.65664,
                          "1972-01-14": 0.65664,
                          "1972-01-15": 0.65664,
                          "1972-01-16": 0.65664},

                (12008,12016): { "2004-11-16": 1.2528,
                                 "2004-11-17": 1.2528,
                                 "2004-11-18": 1.2528,
                                 "2004-11-19": 1.2528,
                                 "2004-11-20": 1.2528,
                                 "2004-11-21": 1.2528,
                                 "2004-11-22": 1.2528,
                                 "2004-11-23": 1.2528}
            }
        }
        for dataset_name, ranges in expected.items():
            for bounds, data in ranges.items():
                df_json = hdf.hdf_dataset_to_pandas_dataframe(aws_file["path"], dataset_name, *bounds)
                df = pd.read_json(df_json)
                for ts, val in data.items():
                    assert np.isclose(df[dataset_name][ts], val)
