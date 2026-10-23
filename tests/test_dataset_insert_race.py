"""
    Two concurrent imports can both find a dataset missing from the
    existing-data lookup in _bulk_insert_data before either has committed,
    and then both insert it. The second insert must not abort the batch with
    an IntegrityError on tDataset.hash.
"""
from unittest import mock

from hydra_base.lib.objects import Dataset


def _datasets(client, name):
    ds = Dataset()
    ds.name = name
    ds.type = "ARRAY"
    ds.unit_id = client.get_unit_by_abbreviation("m s^-1").id
    ds.value = [1.5, 2.5, 3.5]
    return [ds]


class TestDatasetInsertRace:

    def test_duplicate_hash_from_concurrent_import_is_skipped(self, client):
        import hydra_base.lib.data as data

        first = [Dataset(d) for d in client.bulk_insert_data(_datasets(client, "race"))]

        real_lookup = data._get_existing_data
        calls = []

        def lookup_misses_first_time(hashes):
            # The first lookup is the "has anyone already inserted this?" check:
            # report nothing found, as if the other import had not committed yet.
            calls.append(1)
            return {} if len(calls) == 1 else real_lookup(hashes)

        with mock.patch.object(data, '_get_existing_data', side_effect=lookup_misses_first_time):
            second = [Dataset(d) for d in client.bulk_insert_data(_datasets(client, "race"))]

        assert len(calls) >= 2, "the insert path was not exercised"
        assert second[0].id == first[0].id, "the existing dataset should be used, not a duplicate"
