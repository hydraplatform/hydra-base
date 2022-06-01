import pytest
import random

import hydra_base
from hydra_base import config
from hydra_base.lib.adaptors import HydraMongoDatasetAdaptor
from hydra_base.lib.objects import Dataset

# Retrieve size threshold for mongo storage from config
mongo_threshold = int(config.get("mongodb", "threshold"))
# Collection for test data
mongo_collection = "bitest"


@pytest.fixture
def mongo():
    mongo = HydraMongoDatasetAdaptor()
    return mongo


class TestMongo():
    def test_bulk_add_mongo_data(self, client, mongo, dateformat):
        """
        Builds a non-trivial number of datasets and adds *the values*
        of these to mongo using the adaptors `bulk_insert_values` func.
        """

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

        inserted = mongo.bulk_insert_values([d.value for d in datasets], collection=mongo_collection)
        assert len(inserted.inserted_ids) == num_datasets, "Datasets insertion count mismatch"
        """ Verify correct data inserted """
        """
        for ds in inserted:
            #retrieved = client.get_dataset(ds.id)
            #assert ds.value == retrieved.value
        """
        """ Remove data from test collection """
        for _id in inserted.inserted_ids:
            mongo.delete_document_by_object_id(_id, collection=mongo_collection)
