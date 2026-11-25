"""
Unit tests for the `use_mongo` config gating added to handle MongoDB server
and config errors gracefully (see PR: graceful handling of MongoDB server and
config errors).

Unlike tests/test_mongo.py, these do not require a live MongoDB server --
they verify that Mongo-backed lookups are only *attempted* when `use_mongo`
is enabled, and that the correct value source is chosen otherwise. Mongo
itself is mocked out entirely.
"""
from unittest.mock import patch, PropertyMock

import pytest
from pymongo.errors import ServerSelectionTimeoutError

from hydra_base.lib.objects import JSONObject
from hydra_base.db.model.dataset import Dataset
from hydra_base.lib.storage.mongostorageadapter import MongoStorageAdapter


class FakeSqlAlchemyRow:
    """
    Stands in for a SQLAlchemy row proxy resulting from a `Class.column`
    query: has a callable `_asdict` (the attribute JSONObject.normalise_input
    checks for) returning a dict with a "value" key that may be a Mongo
    ObjectId reference.
    """
    def __init__(self, value):
        self._value = value

    def _asdict(self):
        return {"value": self._value}


class TestJSONObjectMongoGating:
    def test_asdict_branch_skips_mongo_lookup_when_use_mongo_false(self):
        """
        With use_mongo disabled, a SQLAlchemy-row-like object's "value" must
        be passed through unchanged, and no Mongo lookup should be attempted.
        """
        row = FakeSqlAlchemyRow("68d0c9f1a1b2c3d4e5f6a7b8")  # looks like a valid ObjectId

        with patch("hydra_base.lib.objects.use_mongo", False), \
             patch("hydra_base.lib.objects.mongo.get_document_by_oid_inst") as mock_lookup:
            result = JSONObject(row)

        mock_lookup.assert_not_called()
        assert result["value"] == "68d0c9f1a1b2c3d4e5f6a7b8"

    def test_asdict_branch_performs_mongo_lookup_when_use_mongo_true(self):
        """
        With use_mongo enabled, a "value" that looks like a Mongo ObjectId
        reference must be resolved via the Mongo adapter.
        """
        row = FakeSqlAlchemyRow("68d0c9f1a1b2c3d4e5f6a7b8")

        with patch("hydra_base.lib.objects.use_mongo", True), \
             patch("hydra_base.lib.objects.mongo.get_document_by_oid_inst") as mock_lookup:
            mock_lookup.return_value = {"value": "the real value from mongo"}
            result = JSONObject(row)

        mock_lookup.assert_called_once()
        assert result["value"] == "the real value from mongo"

    def test_asdict_branch_with_use_mongo_true_and_non_oid_value_is_left_unchanged(self):
        """
        A "value" which isn't a valid ObjectId string must be left as-is
        (TypeError/InvalidId from ObjectId() construction is swallowed),
        even with use_mongo enabled.
        """
        row = FakeSqlAlchemyRow("not-a-mongo-object-id")

        with patch("hydra_base.lib.objects.use_mongo", True), \
             patch("hydra_base.lib.objects.mongo.get_document_by_oid_inst") as mock_lookup:
            result = JSONObject(row)

        mock_lookup.assert_not_called()
        assert result["value"] == "not-a-mongo-object-id"


class TestDatasetSetHashMongoGating:
    def _make_dataset(self, value_ref, resolved_value):
        ds = Dataset()
        ds.name = "Test dataset"
        ds.type = "SCALAR"
        ds.unit_id = None
        ds.value_ref = value_ref
        return ds

    def test_set_hash_uses_value_ref_when_use_mongo_true(self):
        """
        When Mongo storage is in use, the hash must be computed from
        value_ref (the pointer stored locally), not from the resolved
        (potentially very large) value.
        """
        ds = self._make_dataset(value_ref="mongo-pointer-abc123", resolved_value="the huge resolved value")

        with patch("hydra_base.db.model.dataset.use_mongo", True), \
             patch.object(Dataset, "value", new_callable=PropertyMock) as mock_value, \
             patch.object(Dataset, "get_metadata_as_dict", return_value={}), \
             patch("hydra_base.db.model.dataset.generate_data_hash") as mock_hash:
            mock_value.return_value = "the huge resolved value"
            mock_hash.return_value = 12345
            ds.set_hash()

        _, kwargs = mock_hash.call_args
        hashed_dict = mock_hash.call_args[0][0]
        assert hashed_dict["value"] == "mongo-pointer-abc123"

    def test_set_hash_uses_value_when_use_mongo_false(self):
        """
        When Mongo storage is not in use, value_ref is not populated, so the
        hash must be computed from the actual local value.
        """
        ds = self._make_dataset(value_ref=None, resolved_value="the local value")

        with patch("hydra_base.db.model.dataset.use_mongo", False), \
             patch.object(Dataset, "value", new_callable=PropertyMock) as mock_value, \
             patch.object(Dataset, "get_metadata_as_dict", return_value={}), \
             patch("hydra_base.db.model.dataset.generate_data_hash") as mock_hash:
            mock_value.return_value = "the local value"
            mock_hash.return_value = 54321
            ds.set_hash()

        hashed_dict = mock_hash.call_args[0][0]
        assert hashed_dict["value"] == "the local value"

    def test_set_hash_includes_name(self):
        """
        set_hash's hashed dict must include the dataset name (this branch
        added `name` to the hashed fields, which master's version lacked).
        """
        ds = self._make_dataset(value_ref="v", resolved_value="v")

        with patch("hydra_base.db.model.dataset.use_mongo", False), \
             patch.object(Dataset, "value", new_callable=PropertyMock, return_value="v"), \
             patch.object(Dataset, "get_metadata_as_dict", return_value={}), \
             patch("hydra_base.db.model.dataset.generate_data_hash") as mock_hash:
            mock_hash.return_value = 1
            ds.set_hash()

        hashed_dict = mock_hash.call_args[0][0]
        assert hashed_dict["name"] == "Test dataset"


class TestMongoStorageAdapterErrorHandling:
    def test_del_does_not_raise_when_client_was_never_set(self):
        """
        If the server is unreachable, MongoClient(...) construction itself
        doesn't raise (pymongo connects lazily) but a real connection
        attempt does, at which point __init__ re-raises. The partially
        constructed instance still gets __del__ called on it during
        garbage collection, and that must not raise a second, unrelated
        AttributeError on top of the original connection failure.
        """
        with patch("hydra_base.lib.storage.mongostorageadapter.MongoClient",
                  side_effect=ServerSelectionTimeoutError("no server")):
            adapter = object.__new__(MongoStorageAdapter)
            with pytest.raises(ServerSelectionTimeoutError):
                adapter.__init__()

        assert not hasattr(adapter, "client")
        adapter.__del__()  # must not raise
