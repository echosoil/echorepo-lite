# echorepo/services/i18n_labels.py
from __future__ import annotations

import os

from babel.support import Translations
from flask import current_app

from echorepo.i18n import BASE_LABEL_MSGIDS
from echorepo.services.i18n_overrides import _canon_locale, get_overrides, get_overrides_msgid


def _get_catalog(loc: str) -> Translations | None:
    """Load compiled translations for a locale, or None."""
    trans_dir = os.path.join(current_app.root_path, "translations")
    try:
        return Translations.load(dirname=trans_dir, locales=[loc], domain="messages")
    except Exception:
        return None


def _catalog_gettext(loc: str, msgid: str) -> str:
    cat = _get_catalog(loc)
    if cat:
        try:
            return cat.gettext(msgid)
        except Exception:
            pass
    return msgid


def make_labels(locale_code: str) -> dict:
    """
    Build JS labels payload (BASE_LABEL_MSGIDS) for /labels.js and /labels.json.
    Merges:
      1) Babel catalog translations (messages.mo)
      2) msgid overrides
      3) key overrides (wins)
    """
    loc = _canon_locale(locale_code)
    by_msgid = get_overrides_msgid(loc) or {}
    by_key = get_overrides(loc) or {}

    labels: dict[str, str] = {}
    for key, msgid in BASE_LABEL_MSGIDS.items():
        text = _catalog_gettext(loc, msgid)
        text = by_msgid.get(msgid, text)
        text = by_key.get(key, text)
        labels[key] = text
    return labels
