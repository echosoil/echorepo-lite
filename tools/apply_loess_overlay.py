#!/usr/bin/env python3
"""Add /explore LOESS map layer to echosoil/echorepo-lite (fresh or v2).

Safe and idempotent: stages both files before writing; never touches /my.
"""

import sys
from pathlib import Path

ROOT = Path.cwd()
MAP = ROOT / "static/js/map.js"
EXPLORE = ROOT / "echorepo/templates/explore.html"


def apply_map(body):
    marker = "  // LOESS: optional, separately sourced public crowd-observation overlay."
    if marker not in body:
        anchor = "  // Created only after the current translations have been loaded.\n  let baseLayerControl = null;"
        if body.count(anchor) != 1:
            raise RuntimeError("map.js LOESS insertion anchor not found exactly once")
        insertion = """  // LOESS: optional, separately sourced public crowd-observation overlay.
  // Never include in ECHO's ownership index, filters, QA or sample exports.
  const loessOverlay = cfg.public_mode && cfg.loess_overlay_enabled &&
    window.ECHOREPO_LOESS?.createLayer
    ? window.ECHOREPO_LOESS.createLayer(map, {
      url: cfg.loess_overlay_url || '/static/geo/loess_surveys.geojson?v=3'
    }) : null;

"""
        body = body.replace(anchor, insertion + anchor)
    if "[T('loessObservations', {}, 'LOESS soil observations')]" not in body:
        anchor = """      biogeoOverlay
        ? { [T('biogeographicalRegions', {}, 'Biogeographical regions')]: biogeoOverlay }
        : null,"""
        replacement = """      {
        ...(biogeoOverlay
          ? { [T('biogeographicalRegions', {}, 'Biogeographical regions')]: biogeoOverlay }
          : {}),
        ...(loessOverlay
          ? { [T('loessObservations', {}, 'LOESS soil observations')]: loessOverlay }
          : {})
      },"""
        if body.count(anchor) != 1:
            raise RuntimeError("map.js native Leaflet control anchor not found exactly once")
        body = body.replace(anchor, replacement)
    return body


def apply_template(body):
    config_line = "    loess_overlay_url: {{ url_for('static', filename='geo/loess_surveys.geojson') | tojson }},"
    config_new = "    loess_overlay_url: {{ url_for('static', filename='geo/loess_surveys.geojson') | tojson }} + '?v=3',"
    if "loess_overlay_enabled:" not in body:
        anchor = "    biogeo_overlay_url: {{ url_for('static', filename='geo/biogeo_regions_2016.geojson') | tojson }},"
        if body.count(anchor) != 1:
            raise RuntimeError("explore.html base overlay config anchor not found exactly once")
        body = body.replace(anchor, anchor + "\n    loess_overlay_enabled: true,\n" + config_new)
    elif config_line in body:
        body = body.replace(config_line, config_new)
    elif config_new not in body:
        raise RuntimeError("explore.html LOESS URL config looks different; manual review needed")
    older = """<script src="{{ url_for('static', filename='js/loess_overlay.js') }}?v=1"></script>
<script src="{{ url_for('static', filename='js/map.js') }}?v=44"></script>"""
    newer = """<script src="{{ url_for('static', filename='js/loess_overlay.js') }}?v=3"></script>
<script src="{{ url_for('static', filename='js/map.js') }}?v=45"></script>"""
    original = """<script src="{{ url_for('static', filename='js/map.js') }}?v=43"></script>"""
    if newer in body:
        return body
    if older in body:
        return body.replace(older, newer)
    if original in body:
        return body.replace(original, newer)
    raise RuntimeError("explore.html script import changed; manual review needed")


def main():
    if not MAP.exists() or not EXPLORE.exists():
        raise RuntimeError("Run from the root of echorepo-lite")
    if not (ROOT / "static/js/loess_overlay.js").is_file():
        raise RuntimeError("Copy static/js/loess_overlay.js from patch first")
    a = MAP.read_text(encoding="utf-8")
    b = EXPLORE.read_text(encoding="utf-8")
    next_a, next_b = apply_map(a), apply_template(b)
    if a != next_a:
        MAP.write_text(next_a, encoding="utf-8")
    if b != next_b:
        EXPLORE.write_text(next_b, encoding="utf-8")
    print("LOESS /explore integration ready (idempotent).")
    print("Updated:" if (a != next_a or b != next_b) else "Already current:")
    print("  static/js/map.js")
    print("  echorepo/templates/explore.html")
    print("The LOESS GeoJSON and overlay JS use v3 cache-busting; hard-refresh /explore.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
