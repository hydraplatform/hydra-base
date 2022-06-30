import json
import pytest
import random

from packaging import version

import hydra_base
from hydra_base.lib.storage import MongoStorageAdapter
from hydra_base.lib.objects import Dataset

# Minimum required mongo version
mongo_min_version = version.parse("3.6.8")


@pytest.fixture
def mongo():
    mongo = MongoStorageAdapter()
    return mongo

@pytest.fixture
def mongo_config():
    """ This can be parametrized to allow different config sources to be tested """
    return MongoStorageAdapter.get_mongo_config()


@pytest.mark.externaldb
class TestMongo():
    def generate_datasets(self, client, mongo_config, num_datasets=12, large_step=3):
        """
        Returns `num_datasets` Datasets containing an array of random floats.
        Every `large_step`'th dataset (including the 0'th) is larger
        than the external storage threshold.
        """
        mongo_threshold = mongo_config["threshold"]
        datasets = []
        unit_id = client.get_unit_by_abbreviation("m s^-1").id
        for idx in range(num_datasets):
            ds = Dataset()
            ds.name = f"Dataset {idx}"
            ds.type = "ARRAY"
            ds.unit_id = unit_id
            if idx % large_step:
                data_sz = 10
            else:
                data_sz = mongo_threshold * random.randint(1, 3) + 1
            ds.value = [random.uniform(1, 100) for _ in range(data_sz)]
            datasets.append(ds)

        return datasets


    def test_server_status(self, mongo):
        """
        Is the server available and can return the getBuildInfo() dict?
        """
        status = mongo.client.server_info()
        assert isinstance(status, dict)


    def test_mongo_version(self, mongo):
        """
        Does the server's version meet the minimum requirement?
        """
        status = mongo.client.server_info()
        semver = status.get("version")
        assert semver, "No version key in build info"
        vp = version.parse(semver)
        assert vp >= mongo_min_version


    def test_mongo_config(self, mongo):
        required = ("threshold", "value_location_key", "direct_location_token")
        mongo_config = MongoStorageAdapter.get_mongo_config()

        for key in required:
            assert key in mongo_config, f"Mongo config missing `{key}` definition"


    def test_bulk_add_mongo_data(self, client, mongo_config, mongo):
        """
        Builds a non-trivial number of large datasets and adds *the values*
        of these directly to mongo using the adapter's `bulk_insert_values`
        func.
        Bulk insertion of datasets via Hydra is tested in test_scenario::bulk_add_data
        """

        mongo_threshold = mongo_config["threshold"]
        num_datasets = 32
        datasets = []
        unit_id = client.get_unit_by_abbreviation("m s^-1").id
        for idx in range(num_datasets):
            ds = Dataset()
            ds.name = f"Bulk dataset {idx}"
            ds.type = "ARRAY"
            ds.unit_id = unit_id
            data_sz = mongo_threshold * random.randint(1, 3) + 1
            ds.value = [random.uniform(1, 100) for _ in range(data_sz)]
            datasets.append(ds)

        inserted = mongo.bulk_insert_values([d.value for d in datasets])
        assert len(inserted.inserted_ids) == num_datasets, "Datasets insertion count mismatch"
        """ Remove data from test collection """
        for _id in inserted.inserted_ids:
            mongo.delete_document_by_object_id(_id)


    def test_add_datasets(self, client, mongo_config, mongo):
        """
        Are datasets with size exceeding `mongo_threshold` correctly routed to
        external storage with appropriate metadata added to indicate this?
        """
        mongo_threshold = mongo_config["threshold"]
        mongo_location_key = mongo_config["value_location_key"]
        mongo_location_external = mongo_config["direct_location_token"]

        def lookup_dataset_metadata(ds_id):
            for m in metadatas:
                if m["dataset_id"] == ds_id:
                    return m

        datasets = self.generate_datasets(client, mongo_config)
        added = []
        for ds in datasets:
            added.append(client.add_dataset(ds.type, ds.value, ds.unit_id, {}, ds.name, flush=True))

        metadatas = client.get_metadata([ds.id for ds in added])

        """ Do external datasets and only these have location metadata? """
        for ds in added:
            if len(ds.value) > mongo_threshold:
                m = lookup_dataset_metadata(ds.id)
                key = m.get("key")
                assert key == mongo_location_key, "Location key missing from large dataset"
                location = m.get("value")
                assert location == mongo_location_external, "Invalid location metadata value"
            else:
                m = lookup_dataset_metadata(ds.id)
                assert not m

        """ Are the added values transparently retrieved for every dataset? """
        for local_ds, added_ds in zip(datasets, added):
            assert local_ds.value == added_ds.value, "Dataset value mismatch"


    def test_shrink_dataset(self, client, mongo_config, mongo):
        """
        Is a large dataset initially placed in external storage, then relocated
        to the SQL db if its size is reduced beneath the external threshold?
        """
        mongo_location_key = mongo_config["value_location_key"]
        mongo_location_external = mongo_config["direct_location_token"]
        mongo_threshold = mongo_config["threshold"]
        unit_id = client.get_unit_by_abbreviation("m s^-1").id

        ds = Dataset()
        ds.name = "Shrinking dataset"
        ds.type = "ARRAY"
        ds.unit_id = unit_id
        data_sz = mongo_threshold * random.randint(1, 3) + 1
        ds.value = [random.uniform(1, 100) for _ in range(data_sz)]

        server_ds = client.add_dataset(ds.type, ds.value, ds.unit_id, {}, ds.name, flush=True)
        metadata = client.get_metadata([server_ds.id])

        key = metadata[0].get("key")
        assert key == mongo_location_key, "Location key missing from large dataset"
        location = metadata[0].get("value")
        assert location == mongo_location_external, "Invalid location metadata value"

        """ Shrink the dataset and confirm it has been relocated """
        server_ds.value = [random.uniform(1, 100) for _ in range(10)]
        shrunk_ds = client.update_dataset(server_ds.id, server_ds.name, server_ds.type, json.dumps(server_ds.value), server_ds.unit_id, {})
        shrunk_metadata = client.get_metadata([shrunk_ds.id])
        assert not shrunk_metadata


    def test_grow_dataset(self, client, mongo_config, mongo):
        """
        Is a small dataset initially placed in SQL db storage, and then relocated
        to external storage when its size grows to exceeds the external threshold?
        """
        mongo_location_key = mongo_config["value_location_key"]
        mongo_location_external = mongo_config["direct_location_token"]
        mongo_threshold = mongo_config["threshold"]
        unit_id = client.get_unit_by_abbreviation("m s^-1").id

        ds = Dataset()
        ds.name = "Grow dataset"
        ds.type = "ARRAY"
        ds.unit_id = unit_id
        ds.value = [random.uniform(1, 100) for _ in range(10)]

        server_ds = client.add_dataset(ds.type, ds.value, ds.unit_id, {}, ds.name, flush=True)
        metadata = client.get_metadata([server_ds.id])
        """ No storage location, therefore SQL db """
        assert not metadata

        """ Grow the dataset and confirm it has been relocated """
        data_sz = mongo_threshold * random.randint(1, 3) + 1
        server_ds.value = [random.uniform(1, 100) for _ in range(data_sz)]
        grown_ds = client.update_dataset(server_ds.id, server_ds.name, server_ds.type, json.dumps(server_ds.value), server_ds.unit_id, {})

        grown_metadata = client.get_metadata([grown_ds.id])
        key = grown_metadata[0].get("key")
        assert key == mongo_location_key, "Location key missing from large dataset"
        location = grown_metadata[0].get("value")
        assert location == mongo_location_external, "Invalid location metadata value"
