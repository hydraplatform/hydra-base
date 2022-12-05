import numpy as np
import pandas as pd
import pytest

from packaging import version

from hydra_base.lib import data
from hydra_base.lib.storage import HdfStorageAdapter
from hydra_base.util import NullAdapter

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

@pytest.fixture
def multigroup_file():
    return {
        "path": "s3://modelers-data-bucket/grid_data.h5",  # NB rot13 strings
        "groups": ['RFJ_Rffrk_erfhygf', 'prageny_fbhgu_rffrk_erfhygf', 'qnvyl_cebsvyrf', 'yvapbyafuver_erfhygf', 'zbaguyl_cebsvyrf', 'gvzrfrevrf']
    }

@pytest.fixture(params=["s3://modelers-data-bucket/does_not_exist.h5", "does_not_exist"])
def bad_url(request):
    return request.param


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

    @pytest.mark.requires_hdf
    def test_hdf_size(self, hdf, aws_file):
        """
          Does the reported file size match an expected value?
        """
        assert hdf.size(aws_file["path"]) == aws_file["file_size"]

    @pytest.mark.requires_hdf
    def test_hdf_info(self, hdf, aws_file):
        """
          Do the reported properties of a dataset match expected values?
        """
        info = hdf.get_dataset_info_url(aws_file["path"], aws_file["dataset_name"])

        assert info["name"] == aws_file["dataset_name"]
        assert info["size"] == aws_file["dataset_size"]
        assert info["dtype"] == aws_file["dataset_type"]

    @pytest.mark.requires_hdf
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

    @pytest.mark.requires_hdf
    def test_hydra_hdf_size(self, aws_file):
        """
          Does the reported file size match an expected value when
          accessed via hydra.lib?
        """
        assert data.get_hdf_filesize(aws_file["path"]) == aws_file["file_size"]

    @pytest.mark.requires_hdf
    def test_hydra_hdf_info(self, aws_file):
        """
          Do the reported properties of a dataset match expected values when
          accessed via hydra.lib?
        """
        info = data.get_hdf_dataset_info(aws_file["path"], aws_file["dataset_name"])

        assert info["name"] == aws_file["dataset_name"]
        assert info["size"] == aws_file["dataset_size"]
        assert info["dtype"] == aws_file["dataset_type"]

    @pytest.mark.requires_hdf
    def test_hydra_hdf_dataset(self, aws_file):
        """
          Does a specified subset of a dataset match its expected
          index and series values when accessed via hydra.lib?
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
            for bounds, series in ranges.items():
                df_json = data.get_hdf_dataframe(aws_file["path"], dataset_name, *bounds)
                df = pd.read_json(df_json)
                for ts, val in series.items():
                    assert np.isclose(df[dataset_name][ts], val)

    @pytest.mark.requires_hdf
    def test_bad_url(self, bad_url):
        """
          Does an inaccessible url raise ValueError?
        """
        with pytest.raises(ValueError):
            info = data.get_hdf_dataset_info(bad_url, "dataset_name")

    @pytest.mark.requires_hdf
    def test_bad_dataset_name(self, aws_file):
        """
          Does a nonexistent dataset name raise ValueError both for
          info and series retrieval?
        """
        with pytest.raises(ValueError):
            info = data.get_hdf_dataset_info(aws_file["path"], "nonexistent_dataset")

        with pytest.raises(ValueError):
            df_json = data.get_hdf_dataframe(aws_file["path"], "nonexistent_dataset", 8, 16)

    @pytest.mark.requires_hdf
    def test_bad_bounds(self, aws_file):
        """
          Do invalid bounds (start<0, start>end, end<0, end>size) raise ValueError?
        """
        with pytest.raises(ValueError):
            df_json = data.get_hdf_dataframe(aws_file["path"], aws_file["dataset_name"], -1, 16)

        with pytest.raises(ValueError):
            df_json = data.get_hdf_dataframe(aws_file["path"], aws_file["dataset_name"], 16, 8)

        with pytest.raises(ValueError):
            df_json = data.get_hdf_dataframe(aws_file["path"], aws_file["dataset_name"], 8, -1)

        with pytest.raises(ValueError):
            df_json = data.get_hdf_dataframe(aws_file["path"], aws_file["dataset_name"], 8, 1e72)

    @pytest.mark.requires_hdf
    def test_bad_url_does_not_exist(self, bad_url):
        """
          Does data.file_exists_at_url() return False for nonexistent
          files on remote or local filesystem?
        """
        assert not data.file_exists_at_url(bad_url)

    @pytest.mark.requires_hdf
    def test_existing_file_at_url_exists(self, aws_file):
        """
          Does data.file_exists_at_url() return True for existing
          files on remote or local filesystem?
        """
        assert data.file_exists_at_url(aws_file["path"])

    def test_nulladapter_is_null(self):
        """
          If HDF storage were to be disabled in config, does the NullAdapter
          replacement class transparently allow the API to do nothing?
        """
        # Static method
        NullAdapter.method()
        NullAdapter.nested.method()

        # Instance method
        na = NullAdapter()
        na.do.nothing()

        # Is iterable?
        for i in na:
            pass

        # Is subscriptable?
        assert na[2] == None

    @pytest.mark.requires_hdf
    def test_multigroup_dataframe(self, multigroup_file):
        """
          Is the correct dataframe returned when requesting a whole
          dataframe from a remote multigroup file?

          NB rot13 strings
        """
        df = data.get_hdf_group_as_dataframe(multigroup_file["path"], groupname="RFJ_Rffrk_erfhygf")
        assert df[:96] == '{"Ynatunz Vagnxr.Fhccyl.Nzbhag":{"1910-01-01T00:00:00.000Z":40.0,"1910-01-02T00:00:00.000Z":40.0'

    @pytest.mark.requires_hdf
    def test_multigroup_series(self, multigroup_file):
        """
          Is the correct series returned when requesting a particular
          series from a dataframe in a multigroup file?

          NB rot13 strings
        """
        df = data.get_hdf_group_as_dataframe(multigroup_file["path"], groupname="RFJ_Rffrk_erfhygf", series="Jbezvatsbeq Vagnxr.Fhccyl.Nzbhag")
        assert df[:96] == '{"1910-01-01T00:00:00.000Z":0.0,"1910-01-02T00:00:00.000Z":0.0,"1910-01-03T00:00:00.000Z":0.0,"1'

    @pytest.mark.requires_hdf
    def test_hdf_multigroups(self, multigroup_file):
        """
          Are the expected groups returned when querying an HDF file for its root groups?

          NB rot13 strings
        """
        groups = data.get_hdf_groups(multigroup_file["path"])
        assert set(groups) == set(multigroup_file["groups"])

    @pytest.mark.requires_hdf
    def test_get_hdf_multigroup_subset(self, multigroup_file):
        df_json = data.get_hdf_dataframe(multigroup_file["path"], "Qraire Vagnxr.Fhccyl.Nzbhag", 4, 8, groupname="RFJ_Rffrk_erfhygf")
        assert df_json == '{"Qraire Vagnxr.Fhccyl.Nzbhag":{"1910-01-05T00:00:00.000Z":61.19238,"1910-01-06T00:00:00.000Z":77.95459,"1910-01-07T00:00:00.000Z":106.8699,"1910-01-08T00:00:00.000Z":119.7732}}'

    @pytest.mark.requires_hdf
    def test_hdf_group_columns(self, multigroup_file):
        """
          Are the correct columns in the correct order returned from a group dataframe?

          NB rot13 strings
        """
        columns = data.get_hdf_group_columns(multigroup_file["path"], groupname="RFJ_Rffrk_erfhygf")
        assert len(columns) == 109
        assert columns[2] == 'Qraire Vagnxr.Fhccyl.Nzbhag'
        assert 'Unaavatsvryq Erfreibve.Fgbentr.Pnyphyngrq (%)' in columns
