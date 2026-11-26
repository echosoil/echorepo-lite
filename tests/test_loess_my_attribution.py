"""Guard the LOESS overlay on public and personal maps (develop branch)."""

import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "echorepo/templates"


class LoessOnMyTests(unittest.TestCase):
    def test_same_overlay_is_allowed_on_personal_map(self):
        source = (ROOT / "static/js/map.js").read_text(encoding="utf-8")
        self.assertIn("(cfg.public_mode === true || PERSONAL_MAP)", source)
        self.assertIn("cfg.loess_overlay_enabled &&", source)
        self.assertIn("? window.ECHOREPO_LOESS.createLayer(map", source)
        self.assertIn("'LOESS soil observations'", source)
        self.assertIn("Never include in ECHO's ownership index", source)

    def test_personal_map_receives_flag_and_optional_js(self):
        html = (TEMPLATES / "results.html").read_text(encoding="utf-8")
        self.assertIn("'personal_map': true", html)
        self.assertIn("'loess_overlay_enabled': config.get('LOESS_OVERLAY_ENABLED', False)", html)
        self.assertIn("'loess_overlay_url': url_for('static', filename='geo/loess_surveys.geojson')", html)
        self.assertIn("{% if config.get('LOESS_OVERLAY_ENABLED', False) %}", html)
        self.assertIn("js/loess_overlay.js", html)
        self.assertLess(html.index("js/loess_overlay.js"), html.index("js/map.js"))

    def test_attribution_is_shared_and_flag_guarded(self):
        for filename in ("explore.html", "results.html"):
            with self.subTest(filename=filename):
                source = (TEMPLATES / filename).read_text(encoding="utf-8")
                self.assertIn("{% include 'partials/loess_map_attribution.html' %}", source)
                self.assertIn("css/loess_attribution.css", source)
                self.assertIn(
                    "{% if config.get('LOESS_OVERLAY_ENABLED', False) %}\n"
                    "{% include 'partials/loess_map_attribution.html' %}",
                    source,
                )
                self.assertLess(source.index('id="map"'), source.index("{% include 'partials/loess_map_attribution.html' %}"))
                self.assertIn("js/map.js') }}?v=47", source)

    def test_shared_attribution_has_external_source_and_separate_dataset_label(self):
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATES)),
            autoescape=select_autoescape(["html"]),
        )
        markup = env.get_template("partials/loess_map_attribution.html").render(
            _=lambda message: message
        )
        self.assertIn("https://loess-project.eu/", markup)
        self.assertIn("https://www.loess-soil-map.eu/map", markup)
        self.assertIn("separately from ECHO soil samples", markup)
        self.assertIn("aria-labelledby", markup)
        self.assertIn('rel="noopener noreferrer"', markup)


if __name__ == "__main__":
    unittest.main()
