# echorepo/i18n.py
import os

from flask import Blueprint, current_app, redirect, request, url_for
from flask_babel import Babel, get_locale
from flask_babel import gettext as _real_gettext

from .services.i18n_overrides import get_overrides

SUPPORTED_LOCALES = ["en", "cs", "nl", "fi", "fr", "de", "el", "it", "pl", "pt", "ro", "sk", "es"]

# Raw English msgids used for JS labels
BASE_LABEL_MSGIDS = {
    "privacyRadius": "Privacy radius (~±{km} km)",
    "soilPh": "Soil pH",
    "acid": "Acidic (≤5.5)",
    "slightlyAcid": "Slightly acidic (5.5–6.5)",
    "neutral": "Neutral (6.5–7.5)",
    "slightlyAlkaline": "Slightly alkaline (7.5–8.5)",
    "alkaline": "Alkaline (≥8.5)",
    "yourSamples": "Your samples",
    "otherSamples": "Other samples",
    "export": "Export",
    "clear": "Clear",
    "exportFiltered": "Export filtered ({n})",
    "date": "Date",
    "qr": "QR code",
    "ph": "pH",
    "colour": "Colour",
    "soilOrganicMatter": "Soil organic matter",
    "structure": "Structure",
    "earthworms": "Earthworms",
    "plastic": "Plastic",
    "debris": "Debris",
    "contamination": "Contamination",
    "metals": "Metals",
    "elementalConcentrations": "Elemental concentrations",
    "drawRectangle": "Draw a rectangle",
    "drawRectangleHint": "Click and drag to draw a rectangle.",
    "cancelDrawing": "Cancel drawing",
    "cancel": "Cancel",
    "deleteLastPoint": "Delete last point",
}

LOCALE_FLAGS = {
    "en": "gb",
    "cs": "cz",
    "nl": "nl",
    "fi": "fi",
    "fr": "fr",
    "de": "de",
    "el": "gr",
    "it": "it",
    "pl": "pl",
    "pt": "pt",
    "ro": "ro",
    "sk": "sk",
    "es": "es",
}

babel = Babel()


def base_labels() -> dict:
    # Translate the raw English msgids for the current locale
    return {k: _real_gettext(v) for k, v in BASE_LABEL_MSGIDS.items()}


def _select_locale():
    # 1) cookie (persistent, survives logout)
    c = request.cookies.get("locale")
    if c in SUPPORTED_LOCALES:
        return c

    # 2) explicit URL param
    q = request.args.get("lang")
    if q in SUPPORTED_LOCALES:
        return q

    # 3) Accept-Language
    return request.accept_languages.best_match(SUPPORTED_LOCALES) or "en"


def build_i18n_labels(base: dict) -> dict:
    """
    base: dict of key -> already-translated strings (via _())
    """
    try:
        locale = str(get_locale() or "en")
    except Exception:
        locale = "en"

    overrides = get_overrides(locale)
    out = dict(base)
    out.update(overrides)
    return out


def init_i18n(app):
    app.config.setdefault("BABEL_DEFAULT_LOCALE", "en")
    app.config["BABEL_TRANSLATION_DIRECTORIES"] = os.path.join(app.root_path, "translations")

    babel.init_app(app, locale_selector=_select_locale)

    @app.context_processor
    def inject_i18n():
        base = base_labels()
        labels = build_i18n_labels(base)

        try:
            current_app.logger.warning(
                "inject_i18n: locale=%s cookie.locale=%r labels_count=%s keys_sample=%s",
                str(get_locale() or "NONE"),
                request.cookies.get("locale"),
                len(labels),
                list(labels.keys())[:5],
            )
        except Exception as e:
            current_app.logger.warning("inject_i18n log failed: %s", e)

        return {"I18N": {"labels": labels, "by_msgid": {}}}


# /lang/<code> route
lang_bp = Blueprint("lang", __name__, url_prefix="/lang")


@lang_bp.route("/<lang_code>")
def set_language(lang_code):
    if lang_code not in SUPPORTED_LOCALES:
        lang_code = "en"

    resp = redirect(request.referrer or url_for("web.explore"))
    resp.set_cookie("locale", lang_code, max_age=60 * 60 * 24 * 730, samesite="Lax")
    return resp
