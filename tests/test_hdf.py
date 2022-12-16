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
        "series_name": "BR_Kabura",
        "series_size": 12784,
        "series_type": "float64"
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
        assert hdf.file_size(aws_file["path"]) == aws_file["file_size"]

    @pytest.mark.requires_hdf
    def test_hdf_info(self, hdf, aws_file):
        """
          Do the reported properties of a dataset match expected values?
        """
        info = hdf.get_series_info(aws_file["path"], columns=aws_file["series_name"])

        assert info[0]["name"] == aws_file["series_name"]
        assert info[0]["length"] == aws_file["series_size"]
        assert info[0]["dtype"] == aws_file["series_type"]

    @pytest.mark.requires_hdf
    def test_hdf_dataset(self, hdf, aws_file):
        """
          Does a specified subset of a dataset match its expected
          index and series values?
        """
        expected = {
            aws_file["series_name"]: {
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
        for series_name, ranges in expected.items():
            for bounds, data in ranges.items():
                df_json = hdf.get_columns_as_dataframe(aws_file["path"], columns=[series_name], start=bounds[0], end=bounds[1])
                df = pd.read_json(df_json)
                for ts, val in data.items():
                    assert np.isclose(df[series_name][ts], val)

    @pytest.mark.requires_hdf
    def test_hydra_hdf_size(self, aws_file):
        """
          Does the reported file size match an expected value when
          accessed via hydra.lib?
        """
        assert data.get_hdf_file_size(aws_file["path"]) == aws_file["file_size"]

    @pytest.mark.requires_hdf
    def test_hydra_hdf_info(self, aws_file):
        """
          Do the reported properties of a series match expected values when
          accessed via hydra.lib?
        """
        info = data.get_hdf_group_info(aws_file["path"])

        assert info["index"] == {'name': 'timestamp', 'length': 12784, 'dtype': 'datetime64'}
        assert len(info["series"]) == 71
        assert info["series"][0] == {'name': 'BR_Bendera (Ruzizi 0.035)', 'length': 12784, 'dtype': 'float64'}

    @pytest.mark.requires_hdf
    def test_hydra_hdf_dataset(self, aws_file):
        """
          Does a specified subset of a dataset match its expected
          index and series values when accessed via hydra.lib?
        """
        expected = {
            aws_file["series_name"]: {
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
        for series_name, ranges in expected.items():
            for bounds, series in ranges.items():
                df_json = data.get_hdf_columns_as_dataframe(aws_file["path"], groupname=None, columns=[series_name], start=bounds[0], end=bounds[1])
                df = pd.read_json(df_json)
                for ts, val in series.items():
                    assert np.isclose(df[series_name][ts], val)

    @pytest.mark.requires_hdf
    def test_bad_url(self, bad_url):
        """
          Does an inaccessible url raise ValueError?
        """
        with pytest.raises(ValueError):
            info = data.get_hdf_series_info(bad_url, "series_name")

    @pytest.mark.requires_hdf
    def test_bad_series_name(self, aws_file):
        """
          Does a nonexistent dataset name raise ValueError both for
          info and series retrieval?
        """
        with pytest.raises(ValueError):
            info = data.get_hdf_series_info(aws_file["path"], "nonexistent_series")

        with pytest.raises(ValueError):
            df_json = data.get_hdf_columns_as_dataframe(aws_file["path"], columns=["nonexistent_series"], start=8, end=16)

    @pytest.mark.requires_hdf
    def test_bad_bounds(self, aws_file):
        """
          Do invalid bounds (start<0, start>end, end<0, end>size) raise ValueError?
        """
        with pytest.raises(ValueError):
            df_json = data.get_hdf_group_as_dataframe(aws_file["path"], start=-1, end=16)

        with pytest.raises(ValueError):
            df_json = data.get_hdf_group_as_dataframe(aws_file["path"], start=16, end=8)

        with pytest.raises(ValueError):
            df_json = data.get_hdf_group_as_dataframe(aws_file["path"], start=8, end=-1)

        with pytest.raises(ValueError):
            df_json = data.get_hdf_group_as_dataframe(aws_file["path"], start=8, end=1e72)

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
    def test_multigroup_series(self, multigroup_file):
        """
          Is the correct series returned when requesting a particular
          series from a dataframe in a multigroup file?

          NB rot13 strings
        """
        df = data.get_hdf_columns_as_dataframe(multigroup_file["path"],
                                               groupname="RFJ_Rffrk_erfhygf",
                                               columns=["Jbezvatsbeq Vagnxr.Fhccyl.Nzbhag"],
                                               end=256)
        assert df[:92] == '{"Jbezvatsbeq Vagnxr.Fhccyl.Nzbhag":{"1910-01-01T00:00:00.000":0.0,"1910-01-02T00:00:00.000"'

    @pytest.mark.requires_hdf
    def test_hdf_multigroups(self, multigroup_file):
        """
          Are the expected groups returned when querying an HDF file for its root groups?

          NB rot13 strings
        """
        groups = data.get_hdf_groups(multigroup_file["path"])
        assert set(groups) == set(multigroup_file["groups"])

    @pytest.mark.requires_hdf
    def test_get_hdf_whole_group_as_dataframe(self, multigroup_file):
        """
          Does retrieving a selection of rows from a whole Group
          return the correct section?

          NB rot13 strings
        """
        df_json = data.get_hdf_group_as_dataframe(multigroup_file["path"],
                                                  groupname="RFJ_Rffrk_erfhygf",
                                                  start=4, end=8)
        assert df_json[:126] == '{"Ynatunz Vagnxr.Fhccyl.Nzbhag":{"1910-01-05T00:00:00.000":40.0,'\
                                '"1910-01-06T00:00:00.000":40.0,"1910-01-07T00:00:00.000":40.0,'

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
