# echorepo/services/translate_en.py

from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from typing import Dict, List, Tuple

import requests

# ---------------------------------------------------------------------------
# Config / globals
# ---------------------------------------------------------------------------

COUNTRY_TO_LANG = {
    "ES": "es", "PT": "pt", "FR": "fr", "IT": "it", "DE": "de", "PL": "pl", "CZ": "cs",
    "SK": "sk", "RO": "ro", "HU": "hu", "BG": "bg", "EL": "el", "GR": "el", "FI": "fi",
    "SE": "sv", "DK": "da", "NO": "no", "NL": "nl", "BE": "fr", "CH": "de", "LT": "lt",
    "LV": "lv", "EE": "et", "HR": "hr", "SI": "sl", "UA": "uk", "RU": "ru", "TR": "tr",
    "IE": "en", "GB": "en", "UK": "en",
}

# (text, CC) -> translated_en
_translate_cache: dict[tuple[str, str], str] = {}

# have we already seen LT /languages respond OK?
_lt_ready: bool = False


def _lt_base_url() -> str | None:
    """
    Resolve the base URL for LibreTranslate.

    Priority:
      1) LT_ENDPOINT_INSIDE
      2) LT_ENDPOINT
      3) LT_URL    (for legacy/i18n container configs)
    """
    return (
        os.getenv("LT_ENDPOINT_INSIDE")
        or os.getenv("LT_ENDPOINT")
        or os.getenv("LT_URL")
    )


# ---------------------------------------------------------------------------
# LT readiness / detection
# ---------------------------------------------------------------------------

def _ensure_lt_ready(timeout_sec: int | None = None) -> bool:
    """
    Poll LibreTranslate /languages until it responds, or timeout.
    """
    global _lt_ready

    if _lt_ready:
        return True

    LT = _lt_base_url()
    if not LT:
        return False

    timeout_sec = int(os.getenv("LT_WAIT_SECS", "60")) if timeout_sec is None else timeout_sec
    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        try:
            r = requests.get(f"{LT}/languages", timeout=3)
            if r.ok:
                _lt_ready = True
                return True
        except Exception:
            pass
        time.sleep(2)

    return False


def _lt_detect_batch(texts: list[str]) -> list[tuple[str | None, float]]:
    LT = _lt_base_url()
    if not LT or not texts:
        return [(None, 0.0)] * len(texts)

    _ensure_lt_ready()

    try:
        data = []
        for t in texts:
            data.append(("q", t))
        r = requests.post(f"{LT}/detect", data=data, timeout=12)
        r.raise_for_status()
        resp = r.json()

        # Normalize shapes
        if isinstance(resp, list) and resp and isinstance(resp[0], dict):
            ans = [
                (row.get("language"), float(row.get("confidence", 0.0) or 0.0))
                for row in resp
            ]
        elif isinstance(resp, list) and resp and isinstance(resp[0], list):
            ans = []
            for row in resp:
                if row and isinstance(row[0], dict):
                    ans.append(
                        (row[0].get("language"), float(row[0].get("confidence", 0.0) or 0.0))
                    )
                else:
                    ans.append((None, 0.0))
        else:
            ans = []

        # If count mismatches, redo per-item so we keep alignment
        if len(ans) != len(texts):
            per: list[tuple[str | None, float]] = []
            for t in texts:
                try:
                    rr = requests.post(f"{LT}/detect", data={"q": t}, timeout=6)
                    rr.raise_for_status()
                    arr = rr.json() or []
                    if isinstance(arr, list) and arr:
                        per.append(
                            (
                                arr[0].get("language"),
                                float(arr[0].get("confidence", 0.0) or 0.0),
                            )
                        )
                    else:
                        per.append((None, 0.0))
                except Exception:
                    per.append((None, 0.0))
            return per

        return ans
    except Exception:
        # Full fallback per-item
        out: list[tuple[str | None, float]] = []
        for t in texts:
            try:
                rr = requests.post(f"{LT}/detect", data={"q": t}, timeout=6)
                rr.raise_for_status()
                arr = rr.json() or []
                if isinstance(arr, list) and arr:
                    out.append(
                        (
                            arr[0].get("language"),
                            float(arr[0].get("confidence", 0.0) or 0.0),
                        )
                    )
                else:
                    out.append((None, 0.0))
            except Exception:
                out.append((None, 0.0))
        return out


# ---------------------------------------------------------------------------
# Translation (batch + single)
# ---------------------------------------------------------------------------

def translate_many_to_en(
    pairs: list[tuple[str, str | None]]
) -> dict[tuple[str, str], str]:
    """
    Batch translate many (text, country_code) pairs.

    Returns:
        {(text, CC): translated_en}
    """
    LT = _lt_base_url()
    result: dict[tuple[str, str], str] = {}
    if not pairs:
        return result

    # De-dupe and hit cache
    todo: list[tuple[str, str]] = []
    for text, cc in pairs:
        t = (text or "").strip()
        CC = (cc or "").upper()
        key = (t, CC)
        if not t:
            result[key] = ""
            continue
        if key in _translate_cache:
            result[key] = _translate_cache[key]
        else:
            todo.append(key)

    # If nothing left or LT missing, just return what we have
    if not todo or not LT:
        return result

    _ensure_lt_ready()

    # Detect sources in batch (with alignment-safe fallback)
    detect_threshold = float(os.getenv("LT_DETECT_CONF", "0.60"))
    texts = [t for (t, _CC) in todo]
    det = _lt_detect_batch(texts)

    # Choose sources & group
    by_source: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for (t, CC), (lang, conf) in zip(todo, det):
        if lang == "en" and conf >= 0.85:
            _translate_cache[(t, CC)] = t
            result[(t, CC)] = t
            continue
        if conf >= detect_threshold and lang:
            src = lang
        else:
            src = COUNTRY_TO_LANG.get(CC, "auto")
        by_source[src].append((t, CC))

    # Translate each group; if multi-q fails, go per-item
    for src, items in by_source.items():
        if not items:
            continue

        outs: list[str] | None = None

        try:
            payload = []
            for (t, _CC) in items:
                payload.append(("q", t))
            payload.extend([("source", src or "auto"), ("target", "en")])

            rr = requests.post(f"{LT}/translate", data=payload, timeout=25)
            rr.raise_for_status()
            resp = rr.json()

            if isinstance(resp, list):
                outs = [
                    (x.get("translatedText") if isinstance(x, dict) else "")
                    for x in resp
                ]
                if len(outs) != len(items):
                    outs = None  # mismatch -> fallback
            elif isinstance(resp, dict) and "translatedText" in resp:
                # server only supports single-q
                outs = None
        except Exception:
            outs = None

        # Fallback per-item if needed
        if outs is None:
            outs = []
            for (t, CC) in items:
                try:
                    r1 = requests.post(
                        f"{LT}/translate",
                        data={"q": t, "source": src or "auto", "target": "en"},
                        timeout=12,
                    )
                    r1.raise_for_status()
                    jt = r1.json()
                    out = (jt.get("translatedText") if isinstance(jt, dict) else "") or t
                except Exception:
                    out = t
                _translate_cache[(t, CC)] = out
                result[(t, CC)] = out
                outs.append(out)

        # Store results from batch branch
        if outs is not None:
            for (t, CC), out in zip(items, outs):
                out = out or t
                _translate_cache[(t, CC)] = out
                result[(t, CC)] = out

    return result


def translate_to_en(text: str, country_code: str | None = None) -> str:
    """
    Convenience single-item wrapper over translate_many_to_en.
    """
    text = (text or "").strip()
    CC = (country_code or "").upper()
    if not text:
        return ""
    key = (text, CC)
    if key in _translate_cache:
        return _translate_cache[key]
    m = translate_many_to_en([key])
    return m.get(key, text)
