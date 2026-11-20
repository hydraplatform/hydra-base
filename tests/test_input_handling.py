"""
    Tests that values supplied by callers are parsed, never evaluated or used as paths.
"""
import pytest

from hydra_base.exceptions import HydraError
from hydra_base.lib import plugins
from hydra_base.lib.objects import JSONObject
from hydra_base.lib.template.xml import _get_layout_as_etree


class TestTemplateLayout:

    def test_json_layout(self):
        layout = _get_layout_as_etree('{"color": "red", "shapefile": "blah.shp"}')
        names = sorted(i.find("name").text for i in layout.findall("item"))
        assert names == ["color", "shapefile"]

    def test_python_literal_layout(self):
        layout = _get_layout_as_etree("{'color': 'red'}")
        assert [i.find("value").text for i in layout.findall("item")] == ["red"]

    def test_dict_layout(self):
        layout = _get_layout_as_etree({"color": "red"})
        assert len(layout.findall("item")) == 1

    def test_none_layout(self):
        assert _get_layout_as_etree(None) is None

    @pytest.mark.parametrize("value", ["[1, 2]", "'text'", "3"])
    def test_non_dictionary_layout_is_rejected(self, value):
        with pytest.raises(HydraError):
            _get_layout_as_etree(value)

    def test_layout_is_not_evaluated(self, tmp_path):
        marker = tmp_path / "must_not_exist"
        expr = "{'a': __import__('pathlib').Path(%r).write_text('x')}" % str(marker)
        with pytest.raises(HydraError):
            _get_layout_as_etree(expr)
        assert not marker.exists()


class TestRunPlugin:

    def _plugin(self, location):
        return JSONObject(dict(name="p", location=location, params=[]))

    def test_disabled_by_default(self):
        with pytest.raises(HydraError):
            plugins.run_plugin(self._plugin("anything.py"))

    @pytest.mark.parametrize("location", ["/bin/true", "../../../bin/true", "/etc/passwd"])
    def test_location_outside_plugin_directory_is_rejected(self, monkeypatch, location):
        monkeypatch.setattr(plugins.config, "get", lambda section, option, default=None: "Y")
        started = []
        monkeypatch.setattr(plugins.subprocess, "Popen", lambda *a, **k: started.append(a))
        with pytest.raises(HydraError):
            plugins.run_plugin(self._plugin(location))
        assert started == []
