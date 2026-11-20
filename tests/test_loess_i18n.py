"""LOESS overlay uses the shared ECHOREPO/Babel label registry on both maps."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class LoessI18nTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static/js/loess_overlay.js").read_text(encoding="utf-8")
        cls.i18n_source = (ROOT / "echorepo/i18n.py").read_text(encoding="utf-8")

    def test_every_loess_translation_key_is_registered(self):
        registry = set(re.findall(r'"(loess[A-Za-z0-9]+)"\s*:\s*N_\(', self.i18n_source))
        direct = set(re.findall(r"T\('(loess[A-Za-z0-9]+)'", self.js))
        category = set(re.findall(r"labelKey:\s*'(loess[A-Za-z0-9]+)'", self.js))
        self.assertTrue(category)
        self.assertTrue(direct)
        self.assertEqual(registry - {"loessObservations"}, direct | category)

    def test_category_labels_are_not_fixed_at_registration(self):
        self.assertIn("function categoryLabel(category)", self.js)
        self.assertIn("T(meta.labelKey, {}, meta.label)", self.js)
        self.assertIn("title.textContent = T('loessPopupTitle'", self.js)
        self.assertIn("labelText.textContent = categoryLabel(key)", self.js)
        self.assertIn("title: T('loessMarkerTitle'", self.js)

    def test_english_fallback_and_variable_substitution(self):
        self.assertIn("const labels = window.I18N?.labels || {};", self.js)
        self.assertIn("const byMsgid = window.I18N?.by_msgid || {};", self.js)
        self.assertIn("has(vars, name) ? String(vars[name])", self.js)
        self.assertIn("'LOESS observations ({n})'", self.js)

    def test_script_includes_updated_on_both_maps(self):
        for name in ("explore.html", "results.html"):
            with self.subTest(page=name):
                page = (ROOT / "echorepo/templates" / name).read_text(encoding="utf-8")
                self.assertIn("{% if config.get('LOESS_OVERLAY_ENABLED', False) %}", page)
                self.assertIn("js/loess_overlay.js') }}?v=5", page)
                self.assertLess(page.index("js/loess_overlay.js"), page.index("js/map.js"))

    def test_source_text_is_not_machine_translated_or_evaluated(self):
        self.assertIn("props.description", self.js)
        self.assertIn("props.potential_cause", self.js)
        self.assertIn("props.category_original", self.js)
        self.assertNotIn("innerHTML = props.", self.js)


if __name__ == "__main__":
    unittest.main()
