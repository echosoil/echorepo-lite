import importlib.util
import unittest
from pathlib import Path

MOD = Path(__file__).resolve().parents[1] / "tools/apply_loess_overlay.py"
spec = importlib.util.spec_from_file_location("loess_installer_v3", MOD)
inst = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inst)


class InstallerTests(unittest.TestCase):
    def test_clean_anchors_and_idempotency(self):
        js = """  // Created only after the current translations have been loaded.
  let baseLayerControl = null;
    baseLayerControl = L.control.layers(
      {},
      biogeoOverlay
        ? { [T('biogeographicalRegions', {}, 'Biogeographical regions')]: biogeoOverlay }
        : null,
      {});
"""
        template = """    biogeo_overlay_url: {{ url_for('static', filename='geo/biogeo_regions_2016.geojson') | tojson }},
<script src="{{ url_for('static', filename='js/map.js') }}?v=43"></script>"""
        once_js = inst.apply_map(js)
        once_html = inst.apply_template(template)
        self.assertIn("loessOverlay", once_js)
        self.assertIn("LOESS soil observations", once_js)
        self.assertIn("loess_surveys.geojson", once_html)
        self.assertIn("loess_overlay.js", once_html)
        self.assertEqual(inst.apply_map(once_js), once_js)
        self.assertEqual(inst.apply_template(once_html), once_html)

    def test_update_from_previous_patch(self):
        template = """    loess_overlay_enabled: true,
    loess_overlay_url: {{ url_for('static', filename='geo/loess_surveys.geojson') | tojson }},
<script src="{{ url_for('static', filename='js/loess_overlay.js') }}?v=1"></script>
<script src="{{ url_for('static', filename='js/map.js') }}?v=44"></script>"""
        updated = inst.apply_template(template)
        self.assertIn("geojson') | tojson }} + '?v=3'", updated)
        self.assertIn("loess_overlay.js') }}?v=3", updated)
        self.assertIn("map.js') }}?v=45", updated)
        self.assertEqual(inst.apply_template(updated), updated)

    def test_unexpected_anchor_refuses(self):
        with self.assertRaises(RuntimeError):
            inst.apply_map("// incompatible map source")


if __name__ == "__main__":
    unittest.main()
