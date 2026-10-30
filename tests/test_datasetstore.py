"""
Datasets store pointers (hydra_base.lib.storage.datasetstore) and the S3 sources map, without S3:
the store reads local files through a stand-in map that hands out file:// urls.
"""
import hashlib
import json
import os

import pandas as pd
import pytest

from hydra_base.lib.storage.datasetstore import DatasetStore, DatasetStoreError, TEXT_FORMAT, is_store_pointer
from hydra_base.lib.storage.s3sources import S3SourceMap, S3SourceError

SHA = "a" * 64


def pointer(url, group="dataset_1", sha256=SHA, fmt=None):
    data = {"url": url, "group": group, "sha256": sha256}
    if fmt is not None:
        data["format"] = fmt
    return json.dumps({"data": data})


class LocalSources():
    """ Stands in for S3SourceMap: s3://<bucket>/<path> is read from <root>/<bucket>/<path>. """
    active = True

    def __init__(self, root, buckets=("store",)):
        self.root = root
        self.buckets = buckets

    def storage_options(self, url, required=False):
        bucket, path = url[len("s3://"):].split("/", 1)
        if bucket not in self.buckets:
            raise S3SourceError(f"Bucket '{bucket}' is not in this deployment's S3 map")
        return f"file://{os.path.join(self.root, bucket, path)}", {}


class NoMap():
    active = False


def write_store_file(root, name="datasets/1.h5", group="dataset_1"):
    """ A store object like the migration wrote: one DataFrame in a fixed-format HDFStore. """
    df = pd.DataFrame({"0": [1.5, 2.25, None], "1": [3.0, 4.0, 5.0]},
                      index=["2020-01-01", "2020-01-02", "2020-01-03"])
    path = os.path.join(root, "store", name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_hdf(path, key=group, format="fixed")
    with open(path, "rb") as fp:
        sha256 = hashlib.sha256(fp.read()).hexdigest()
    return df, path, sha256


# The text a frame can't give back: a wrapper with other fields, a quoted number, date keys.
ORIGINAL = ('{"type": "dataframe", "scenario": "Demand Drought Measures", '
            '"pandas_kwargs": {"parse_dates": true}, '
            '"data": {"demand": {"2021-01-01": "0.34", "2021-01-02": 12.0}}}')


def write_text_file(root, name="datasets-v2/1.h5", group="dataset_1", text=ORIGINAL, nx_format=TEXT_FORMAT):
    """ A store object like the re-migration (2026-10-30) writes: the text in value_json, a frame in <group>. """
    import h5py
    import numpy as np
    path = os.path.join(root, "store", name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame({"demand": [0.34, 12.0]}, index=["2021-01-01", "2021-01-02"]).to_hdf(path, key=group, format="fixed")
    with h5py.File(path, "a") as f:
        body = np.frombuffer(text.encode("utf-8"), dtype="uint8")
        f.create_dataset("value_json", data=body, compression="gzip", chunks=(len(body),))
        if nx_format is not None:
            f.attrs["nx_format"] = nx_format
    with open(path, "rb") as fp:
        sha256 = hashlib.sha256(fp.read()).hexdigest()
    return path, sha256


@pytest.mark.parametrize("value", [
    pointer("s3://waterstrategy-datasets/datasets-v2/1.h5", fmt=TEXT_FORMAT),
    pointer("s3://waterstrategy-datasets/datasets/1.h5", fmt="unknown/9"),
    pointer("s3://waterstrategy-datasets/datasets/1.h5"),
    "  " + pointer("s3://b/x.h5", group="g") + "\n",
])
def test_pointer_shapes_that_match(value):
    assert is_store_pointer(value)


@pytest.mark.parametrize("value", [
    None, 12, 1.5, "", "12.5", "[1, 2, 3]",
    json.dumps({"0": {"a": 1}}),
    # model results: same url/group shape, no sha256
    '{"data":{"url":"s3:\\/\\/waterstrategy-results\\/abc\\/simulated_flow.h5","group":"g"}}',
    # pywr inputs
    json.dumps({"type": "dataframe", "url": "s3://modelers-data-bucket/x.h5", "sha256": SHA}),
    pointer("https://example.org/x.h5"),
    pointer("s3://b/x.h5", sha256="not-a-hash"),
    pointer("s3://b/x.h5", group=""),
    json.dumps({"data": {"url": "s3://b/x.h5", "group": "g", "sha256": SHA}, "other": 1}),
    "{" + " " * 3000 + '"sha256" s3:',
])
def test_values_that_are_not_pointers(value):
    assert not is_store_pointer(value)


def test_resolve_gives_back_the_original_text(tmp_path):
    df, _, sha256 = write_store_file(str(tmp_path))
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    text = store.resolve(pointer("s3://store/datasets/1.h5", sha256=sha256), dataset_id=1)
    assert text == df.to_json()
    assert os.path.exists(tmp_path / "cache" / f"{sha256}.h5")


def test_text_format_gives_back_value_json_not_the_frame(tmp_path):
    _, sha256 = write_text_file(str(tmp_path))
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    ptr = pointer("s3://store/datasets-v2/1.h5", sha256=sha256, fmt=TEXT_FORMAT)
    assert store.resolve(ptr, dataset_id=1) == ORIGINAL
    # From the disk cache too, in a new store.
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    assert store.resolve(ptr) == ORIGINAL


def test_same_file_without_format_is_read_as_a_frame(tmp_path):
    # Format and frame share the cache: the memory key includes the format.
    _, sha256 = write_text_file(str(tmp_path))
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    store.resolve(pointer("s3://store/datasets-v2/1.h5", sha256=sha256, fmt=TEXT_FORMAT))
    text = store.resolve(pointer("s3://store/datasets-v2/1.h5", sha256=sha256))
    assert text != ORIGINAL and json.loads(text) == {"demand": {"2021-01-01": 0.34, "2021-01-02": 12.0}}


def test_unknown_format_is_an_error(tmp_path):
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    with pytest.raises(DatasetStoreError, match=r"Dataset 7 .*unknown datasets store format 'json-text\+frame/2'"):
        store.resolve(pointer("s3://store/datasets-v2/1.h5", fmt="json-text+frame/2"), dataset_id=7)


@pytest.mark.parametrize("nx_format", [None, "other/1"])
def test_object_format_must_match_the_record(tmp_path, nx_format):
    _, sha256 = write_text_file(str(tmp_path), nx_format=nx_format)
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    with pytest.raises(DatasetStoreError, match="nx_format"):
        store.resolve(pointer("s3://store/datasets-v2/1.h5", sha256=sha256, fmt=TEXT_FORMAT), dataset_id=1)


def test_other_values_are_returned_untouched(tmp_path):
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    for value in (None, 3, "3.5", '{"0": {"a": 1}}'):
        assert store.resolve(value) == value


def test_cached_file_is_used_without_the_source(tmp_path):
    df, path, sha256 = write_store_file(str(tmp_path))
    ptr = pointer("s3://store/datasets/1.h5", sha256=sha256)
    DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path))).resolve(ptr)
    os.remove(path)
    # A new store (no texts in memory) reads the disk cache.
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    assert store.resolve(ptr) == df.to_json()


def test_hash_mismatch_is_an_error_and_nothing_is_cached(tmp_path):
    write_store_file(str(tmp_path))
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    with pytest.raises(DatasetStoreError, match=r"Dataset 7 .*content hash"):
        store.resolve(pointer("s3://store/datasets/1.h5", sha256="b" * 64), dataset_id=7)
    assert os.listdir(tmp_path / "cache") == []


def test_unknown_bucket_is_an_error(tmp_path):
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    with pytest.raises(DatasetStoreError, match="bucket 'elsewhere'"):
        store.resolve(pointer("s3://elsewhere/datasets/1.h5"), dataset_id=7)


def test_missing_object_is_an_error(tmp_path):
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=LocalSources(str(tmp_path)))
    with pytest.raises(DatasetStoreError, match="FileNotFoundError"):
        store.resolve(pointer("s3://store/datasets/404.h5"))


def test_without_a_map_pointers_stay_as_they_are(tmp_path):
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), sources=NoMap())
    ptr = pointer("s3://store/datasets/1.h5")
    assert store.resolve(ptr) == ptr


def test_cache_keeps_to_its_size(tmp_path):
    store = DatasetStore(cache_dir=str(tmp_path / "cache"), cache_max_mb=1, sources=LocalSources(str(tmp_path)))
    store.cache_max_bytes = 1  # anything over one byte: only the newest file stays
    for i in (1, 2):
        _, _, sha256 = write_store_file(str(tmp_path), name=f"datasets/{i}.h5", group=f"dataset_{i}")
        store.resolve(pointer(f"s3://store/datasets/{i}.h5", group=f"dataset_{i}", sha256=sha256))
    assert len(os.listdir(tmp_path / "cache")) == 1


def test_s3_source_map(tmp_path):
    buckets = tmp_path / "s3-buckets.json"
    creds = tmp_path / "s3-credentials.json"
    buckets.write_text(json.dumps({"buckets": {
        "waterstrategy-datasets": {"role": "datasets", "access": "ro", "region": "uk",
                                   "endpoint": "https://s3.example.org", "credentials_id": "c2"},
        "named-in-records": {"bucket": "real-name", "role": "results", "credentials_id": "c1"}}}))
    creds.write_text(json.dumps({"c1": {"aws_access_key_id": "A1", "aws_secret_access_key": "S1"},
                                 "c2": {"aws_access_key_id": "A2", "aws_secret_access_key": "S2"}}))
    m = S3SourceMap(str(buckets), str(creds))
    assert m.active

    url, opts = m.storage_options("s3://waterstrategy-datasets/datasets/1.h5")
    assert url == "s3://waterstrategy-datasets/datasets/1.h5"
    assert opts["key"] == "A2" and opts["client_kwargs"] == {"endpoint_url": "https://s3.example.org", "region_name": "uk"}

    url, opts = m.storage_options("s3://named-in-records/x/y.h5")
    assert url == "s3://real-name/x/y.h5" and opts["key"] == "A1" and opts["client_kwargs"] == {}

    assert m.storage_options("file:///tmp/x.h5") == ("file:///tmp/x.h5", None)
    # Buckets not in the map keep the previous behaviour, unless the map is required.
    assert m.storage_options("s3://public-bucket/x.h5") == ("s3://public-bucket/x.h5", None)
    with pytest.raises(S3SourceError):
        m.storage_options("s3://public-bucket/x.h5", required=True)


def test_no_map_file_means_no_map(tmp_path):
    m = S3SourceMap(buckets_file=None, credentials_file=None)
    m.buckets_file = None
    assert not m.active
    assert m.storage_options("s3://any/x.h5") == ("s3://any/x.h5", None)
    with pytest.raises(S3SourceError):
        m.storage_options("s3://any/x.h5", required=True)
