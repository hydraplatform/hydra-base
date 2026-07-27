# -*- coding: utf-8 -*-

"""
Tests that demonstrate the template cache-poisoning bug.

Root cause: _save_template_to_cache() is called from add_template() and
import_template_dict() using JSONObject(orm_object) directly, without first
calling get_types(). This produces a cached entry where 'templatetypes' is
either absent or contains raw ORM-derived data rather than the fully-resolved
list that get_types() produces.

When get_template() subsequently hits that cache entry it returns a JSONObject
whose .templatetypes is None (via JSONObject.__getattr__'s default of None for
missing keys). Any caller that then iterates over .templatetypes — most notably
complexmodels.Template.__init__ in hydra-server — raises:

    TypeError: 'NoneType' object is not iterable

The three tests below each isolate a different aspect of the bug:

1. test_poisoned_cache_entry_yields_none_templatetypes
   Directly poisons the cache (simulating what add_template/import_template_dict
   do) and asserts that get_template returns a template with templatetypes=None,
   which then raises TypeError on iteration.

2. test_add_template_poisons_cache
   Calls the real add_template() and confirms it writes a cache entry that is
   missing or has fewer types than get_types() would return, so that a fresh
   get_template() call on the same id returns incorrect data.

3. test_import_template_dict_poisons_cache
   Same as above but via the import_template_dict() code path, which has the
   identical _save_template_to_cache call without get_types().
"""

import datetime
import json
import tempfile
import time
import pytest

import hydra_base as hb
from hydra_base.lib.objects import JSONObject
from hydra_base.lib.cache import cache, clear_cache
from hydra_base.lib import template as template_lib
from hydra_base.lib import attributes as attr_lib
from hydra_base.lib.template import CACHE_KEY
from hydra_base.util.hdb import (
    create_default_users_and_perms,
    make_root_user,
    create_default_units_and_dimensions,
)


# ---------------------------------------------------------------------------
# Session-scoped DB fixture (self-contained SQLite, no conftest.py dependency)
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def db():
    millis = int(round(time.time() * 1000))
    db_url = f'sqlite:///{tempfile.gettempdir()}/test_template_cache_{millis}.db'
    hb.db.connect(db_url)
    create_default_users_and_perms()
    make_root_user()
    create_default_units_and_dimensions()
    yield hb.db
    clear_cache()
    hb.db.close_session()


@pytest.fixture(autouse=True)
def _clear_template_cache():
    """Wipe template cache entries before each test for isolation."""
    yield
    clear_cache()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

USER_ID = 1  # root user created by make_root_user()


def _create_attr(name=None):
    if name is None:
        name = f"cache_test_attr_{datetime.datetime.now().timestamp()}"
    attr = JSONObject()
    attr.name = name
    attr.dimension_id = None
    return attr_lib.add_attribute(attr, user_id=USER_ID)


def _make_template_input(attr_id, name=None):
    """Return a JSONObject suitable for template_lib.add_template()."""
    if name is None:
        name = f"CacheBugTest {datetime.datetime.now()}"
    tmpl = JSONObject()
    tmpl.name = name
    tmpl.templatetypes = []

    t = JSONObject()
    t.name = "NodeType"
    t.resource_type = 'NODE'
    t.typeattrs = [JSONObject({'attr_id': attr_id})]
    tmpl.templatetypes.append(t)
    return tmpl


def _cached_entry(template_id):
    return cache.get(f"{CACHE_KEY}_{template_id}")


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

class TestTemplateCachePoisoning:

    def test_poisoned_cache_entry_is_bypassed(self, db):
        """
        Verifies the protection in get_template: when the cache contains an
        entry with templatetypes absent (the poisoned state produced by
        add_template / import_template_dict), get_template detects this and
        falls back to the DB path rather than returning None and causing:

            TypeError: 'NoneType' object is not iterable   (complexmodels.py:739)
        """
        attr = _create_attr()
        new_tmpl = template_lib.add_template(
            _make_template_input(attr.id), user_id=USER_ID
        )
        template_id = new_tmpl.id

        # Poison the cache with an entry that has no 'templatetypes' key —
        # this is what add_template / import_template_dict can write when the
        # ORM relationship is not present in __dict__ at caching time.
        poisoned = JSONObject({'id': template_id, 'name': new_tmpl.name})
        assert 'templatetypes' not in poisoned
        cache.set(f"{CACHE_KEY}_{template_id}", poisoned)

        # The fix: get_template detects the missing key and re-fetches from DB.
        result = template_lib.get_template(template_id, user_id=USER_ID)

        # Protection holds: templatetypes is not None even though the cache was poisoned
        assert result.templatetypes is not None, (
            "get_template should fall back to DB when the cached entry has no "
            "templatetypes — the protection in get_template is not working."
        )

        # Iteration must not raise TypeError (the production crash is prevented)
        types = list(result.templatetypes)
        assert len(types) == 1

    def test_add_template_poisons_cache(self, db):
        """
        add_template() calls _save_template_to_cache(JSONObject(tmpl)) without
        calling get_types() first (lib/template/__init__.py lines 521-522).

        The cached entry may not contain the same 'templatetypes' data that a
        fresh get_template() DB query would produce via get_types().  After
        add_template the cache is populated; a subsequent get_template() hits
        that entry and should return properly-populated templatetypes — but
        the bug means it may not.
        """
        attr = _create_attr()
        tmpl_input = _make_template_input(attr.id)
        expected_type_count = len(tmpl_input.templatetypes)

        new_tmpl = template_lib.add_template(tmpl_input, user_id=USER_ID)
        template_id = new_tmpl.id

        # Confirm add_template DID write a cache entry
        cached = _cached_entry(template_id)
        assert cached is not None, (
            "add_template should populate the cache via _save_template_to_cache"
        )

        # get_template hits the cache entry written by add_template
        # (not the fully-resolved get_types() path)
        result = JSONObject(template_lib.get_template(template_id, user_id=USER_ID))

        # Bug: templatetypes may be absent (None) from the cached JSONObject
        # because _save_template_to_cache was called without get_types()
        assert result.templatetypes is not None, (
            "templatetypes is None after get_template — the cache entry written "
            "by add_template is missing templatetypes because get_types() was "
            "not called before _save_template_to_cache (lines 521-522)"
        )

        actual_count = len(list(result.templatetypes))
        assert actual_count == expected_type_count, (
            f"Expected {expected_type_count} type(s) but got {actual_count}. "
            "The cache entry from add_template differs from what get_types() returns."
        )

        # Each type must have typeattrs — get_types() fetches these explicitly,
        # but JSONObject(orm_obj) only includes them if the relationship was loaded.
        for tt in result.templatetypes:
            assert tt.typeattrs is not None, (
                f"Type '{tt.name}' has typeattrs=None in the cached entry. "
                "get_types() loads typeattrs explicitly; the raw ORM-based "
                "JSONObject may not."
            )

    def test_import_template_dict_poisons_cache(self, db):
        """
        import_template_dict() has the identical bug: it calls
        _save_template_to_cache(JSONObject(template_i)) without get_types()
        (lib/template/__init__.py lines 459-460).

        After import, get_template hits the poisoned entry and may return a
        template where templatetypes is None or typeattrs is missing.
        """
        attr = _create_attr()

        template_dict = {
            "attributes": {
                str(attr.id): {"id": attr.id, "name": attr.name}
            },
            "datasets": {},
            "template": {
                "name": f"ImportCacheBugTest {datetime.datetime.now()}",
                "templatetypes": [
                    {
                        "name": "ImportedNodeType",
                        "resource_type": "NODE",
                        "typeattrs": [{"attr_id": attr.id}]
                    }
                ]
            }
        }

        imported = template_lib.import_template_dict(
            template_dict, allow_update=True, user_id=USER_ID
        )
        template_id = imported.id

        # Confirm import_template_dict DID write a cache entry (lines 459-460)
        cached = _cached_entry(template_id)
        assert cached is not None, (
            "import_template_dict should populate the cache via "
            "_save_template_to_cache (lines 459-460)"
        )

        # get_template hits the cache entry written by import_template_dict
        result = JSONObject(template_lib.get_template(template_id, user_id=USER_ID))

        assert result.templatetypes is not None, (
            "templatetypes is None after get_template following "
            "import_template_dict — the cache was poisoned because "
            "_save_template_to_cache was called without get_types() "
            "(lib/template/__init__.py lines 459-460)"
        )

        types = list(result.templatetypes)
        assert len(types) == 1

        for tt in types:
            assert tt.typeattrs is not None, (
                f"Type '{tt.name}' has typeattrs=None — import_template_dict "
                "caches without calling get_types(), so typeattrs may not be "
                "present in the cached JSONObject."
            )
