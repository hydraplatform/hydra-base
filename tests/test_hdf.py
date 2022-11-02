import json
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


class TestHdf():
    def test_exists(self, hdf, hdf_config):
        assert hdf
        assert hdf_config

    def test_lib_versions(self):
        import importlib
        for libname, semver in min_lib_versions.items():
            lib = importlib.import_module(libname)
            assert version.parse(lib.__version__) >= semver
