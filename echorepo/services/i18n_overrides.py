# echorepo/services/i18n_overrides.py
import json, os, threading

_LOCK = threading.Lock()

def _canon_locale(lang: str) -> str:
    if not lang:
        return "en"
    lang = lang.strip().lower().replace("-", "_")
    return lang.split("_", 1)[0]   # "es_es" → "es"

# One place on disk (must be RW). You mount ./data:/data so this works.
_OVERRIDES_PATH = os.environ.get("I18N_OVERRIDES_PATH", "/data/i18n_overrides.json")

def _load():
    try:
        with _LOCK:
            if os.path.exists(_OVERRIDES_PATH):
                with open(_OVERRIDES_PATH, "r", encoding="utf-8") as f:
                    return json.load(f) or {}
    except Exception:
        pass
    return {}

def _save(obj):
    os.makedirs(os.path.dirname(_OVERRIDES_PATH), exist_ok=True)
    with _LOCK, open(_OVERRIDES_PATH, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def get_overrides(locale: str) -> dict:
    data = _load()
    return data.get(_canon_locale(locale), {}).get("by_key", {})

def get_overrides_msgid(locale: str) -> dict:
    data = _load()
    return data.get(_canon_locale(locale), {}).get("by_msgid", {})

def set_override(locale: str, key: str, value: str):
    data = _load()
    loc = _canon_locale(locale)
    data.setdefault(loc, {}).setdefault("by_key", {})
    data[loc]["by_key"][key] = value
    _save(data)

def delete_override(locale: str, key: str):
    data = _load()
    loc = _canon_locale(locale)
    if loc in data and "by_key" in data[loc] and key in data[loc]["by_key"]:
        del data[loc]["by_key"][key]
        _save(data)

def set_override_msgid(locale: str, msgid: str, value: str):
    data = _load()
    loc = _canon_locale(locale)
    data.setdefault(loc, {}).setdefault("by_msgid", {})
    data[loc]["by_msgid"][msgid] = value
    _save(data)

def delete_override_msgid(locale: str, msgid: str):
    data = _load()
    loc = _canon_locale(locale)
    if loc in data and "by_msgid" in data[loc] and msgid in data[loc]["by_msgid"]:
        del data[loc]["by_msgid"][msgid]
        _save(data)
