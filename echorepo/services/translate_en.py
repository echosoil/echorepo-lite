# echorepo/services/translate_en.py

from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from typing import Dict, List, Tuple
import json 
from pathlib import Path
import requests

Pair = Tuple[str, str]  # (text, country_code)

# Path to JSON with manual overrides
_PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
_MANUAL_OVERRIDES_PATH = Path(os.getenv("MANUAL_OVERRIDES_PATH", _PROJECT_ROOT / "data" / "manual_overrides.json"))

# cache: norm_text -> en
_MANUAL_OVERRIDES_CACHE: Dict[str, str] | None = None


def _load_manual_overrides() -> Dict[str, str]:
    """
    Internal: load and flatten manual_overrides.json into
    { normalized_source_text: english_text }.

    Also always includes hard-coded CUSTOM_TRANSLATIONS_BY_TEXT.
    """
    out: Dict[str, str] = {}

    # 1) Seed with hard-coded soil mappings
    for src, en in CUSTOM_TRANSLATIONS_BY_TEXT.items():
        if not isinstance(src, str) or not isinstance(en, str):
            continue
        norm = _norm_text(src)
        if norm:
            out[norm] = en

    # 2) Then overlay JSON file, if present
    if not _MANUAL_OVERRIDES_PATH.exists():
        print(f"[TRANSLATE] MANUAL_OVERRIDES_PATH not found, only built-in overrides applied.")
        return out

    try:
        with _MANUAL_OVERRIDES_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"[TRANSLATE] manual overrides loaded OK from {_MANUAL_OVERRIDES_PATH}.")
    except Exception:
        print(f"[TRANSLATE] MANUAL_OVERRIDES_PATH found but not readable, only built-in overrides applied.")
        return out

    text_to_en = data.get("text_to_en", {})
    if isinstance(text_to_en, dict):
        for src, en in text_to_en.items():
            if not isinstance(src, str) or not isinstance(en, str):
                continue
            norm = _norm_text(src)
            if norm:
                out[norm] = en

    return out


def _get_manual_overrides() -> Dict[str, str]:
    """
    Returns cached overrides; loads from disk on first use.
    """
    global _MANUAL_OVERRIDES_CACHE
    if _MANUAL_OVERRIDES_CACHE is None:
        _MANUAL_OVERRIDES_CACHE = _load_manual_overrides()
    return _MANUAL_OVERRIDES_CACHE


def reload_manual_overrides() -> None:
    """
    Public helper: force reload from disk, used by admin endpoint after save.
    """
    global _MANUAL_OVERRIDES_CACHE
    _MANUAL_OVERRIDES_CACHE = _load_manual_overrides()


def _norm_text(text: str) -> str:
    """
    Normalise for lookup:
      - strip
      - lower-case
      - collapse internal whitespace
    """
    return " ".join((text or "").strip().lower().split())

# ---------------------------------------------------------------------------
# Hard-coded overrides:
#   key = normalised source text (see _norm_text)
#   value = desired English text for *_en columns
#
# You can freely extend / tweak this list.
# ---------------------------------------------------------------------------
CUSTOM_TRANSLATIONS_BY_TEXT: Dict[str, str] = {
    # --- Soil texture / structure examples (ES / IT style labels) ---
    "limoso": "Silty",
    "franco": "Loam",
    "franco limoso": "Loam, silty",
    "franco sabbioso": "Loam, sandy",
    "franco arenoso": "Loam, sandy",
    "franco arcilloso": "Loam, clayey",
    "arenoso": "Sandy",
    "sabbioso": "Sandy",
    "arcilloso": "Clayey",
    "argilloso": "Clayey",
    "siltoso": "Silty",
    "argilloso sabbioso": "Sandy clayey",

    # If you want to keep some terms *exactly as-is* in English,
    # just map them to themselves:
    # "limoso": "Limoso",
    # "franco": "Franco",
    # etc.
}

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
            print("[ensure LT ready] failed, new try...")
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
def _safe_translated(src: str, candidate: str) -> str:
    """
    Heuristic: if the translated text is absurdly longer than the source,
    treat it as broken and fall back to the original.
    """
    src = src or ""
    candidate = candidate or ""

    if not candidate:
        return src

    # If LT returns something >10x longer and >200 chars, assume it's garbage.
    if len(candidate) > max(200, 10 * len(src or "x")):
        return src

    return candidate

def _translate_many_to_en_core(
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
        except Exception as e:
            print(f"[_translate_many_to_en_core] failed with error: {e}")
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
                    raw_out = jt.get("translatedText") if isinstance(jt, dict) else ""
                    out = _safe_translated(t, raw_out or t)
                except Exception:
                    out = t
                _translate_cache[(t, CC)] = out
                result[(t, CC)] = out
                outs.append(out)

        # Store results from batch branch
        if outs is not None:
            for (t, CC), raw_out in zip(items, outs):
                out = _safe_translated(t, raw_out or t)
                _translate_cache[(t, CC)] = out
                result[(t, CC)] = out


    return result

def translate_many_to_en(pairs: List[Pair]) -> Dict[Pair, str]:
    """
    Wrapper that:
      1) Applies manual overrides from JSON by replacing the first occurrence
         of any override key (phrase) found in the *source* text.
         - Override keys are checked in descending length to avoid overlaps.
         - On first match, the phrase is replaced with the override's English
           value, and LibreTranslate is NOT called for that pair.
      2) Delegates the remaining texts to LibreTranslate.
    """
    overrides = _get_manual_overrides()  # {norm_source: en_value}
    prefilled: Dict[Pair, str] = {}
    to_translate: List[Pair] = []

    # Sort override keys by length (descending) to prefer longer phrases
    # e.g. "franco argilloso" before "franco"
    patterns: List[str] = sorted(overrides.keys(), key=len, reverse=True)

    for text, cc in pairs:
        raw = (text or "").strip()
        if not raw:
            prefilled[(text, cc)] = ""
            continue

        replaced = False

        for pat in patterns:
            if not pat:
                continue

            # pat is already normalized (lowercase, single spaces)
            tokens = pat.split()
            # Build a regex that:
            #  - matches tokens with flexible whitespace between them
            #  - is case-insensitive
            #  - respects word boundaries on both ends
            pat_re = re.compile(
                r"\b" + r"\s+".join(re.escape(t) for t in tokens) + r"\b",
                flags=re.IGNORECASE,
            )

            m = pat_re.search(raw)
            if not m:
                continue

            # Found the first override phrase in the *original* text.
            # Replace only the first occurrence with the English override.
            en = overrides[pat]
            new_raw = pat_re.sub(en, raw, count=1)

            prefilled[(text, cc)] = new_raw
            replaced = True
            break  # stop after the first matching override

        if not replaced:
            # No manual override applied → send to LibreTranslate later
            to_translate.append((text, cc))

    # If everything was handled by overrides, we're done
    if not to_translate:
        return prefilled

    # De-duplicate for the LT call
    unique: List[Pair] = []
    seen: set[Pair] = set()
    for p in to_translate:
        if p not in seen:
            seen.add(p)
            unique.append(p)

    core_map = _translate_many_to_en_core(unique)

    result: Dict[Pair, str] = {}
    result.update(core_map)   # LT results
    result.update(prefilled)  # manual overrides win for their pairs
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
