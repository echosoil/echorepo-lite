# echorepo/i18n.py

import os

from flask import Blueprint, current_app, redirect, request, url_for
from flask_babel import Babel, get_locale
from flask_babel import gettext as _real_gettext

from .services.i18n_overrides import get_overrides


# ---------------------------------------------------------------------------
# Supported languages
#
# Web translations use language-level catalogues:
#
#   bg, cs, da, de, el, ...
#
# rather than region-specific catalogues such as:
#
#   bg_BG, cs_CZ, el_GR, el_CY, ...
#
# Consequently:
#   el_GR -> el
#   el_CY -> el
#   pt_PT -> pt
#   nb_NO -> nb
# ---------------------------------------------------------------------------

SUPPORTED_LOCALES = [
    "en",
    "bg",
    "cs",
    "da",
    "de",
    "el",
    "es",
    "et",
    "fi",
    "fr",
    "hr",
    "hu",
    "it",
    "lb",
    "lt",
    "lv",
    "nb",
    "nl",
    "pl",
    "pt",
    "ro",
    "sk",
    "sl",
    "sv",
]


# Flag-icon country codes.
#
# These are intentionally not always the same as the language code:
#   cs -> cz
#   da -> dk
#   et -> ee
#   lb -> lu
#   nb -> no
#   sl -> si
#   sv -> se
LOCALE_FLAGS = {
    "en": "gb",
    "bg": "bg",
    "cs": "cz",
    "da": "dk",
    "de": "de",
    "el": "gr",
    "es": "es",
    "et": "ee",
    "fi": "fi",
    "fr": "fr",
    "hr": "hr",
    "hu": "hu",
    "it": "it",
    "lb": "lu",
    "lt": "lt",
    "lv": "lv",
    "nb": "no",
    "nl": "nl",
    "pl": "pl",
    "pt": "pt",
    "ro": "ro",
    "sk": "sk",
    "sl": "si",
    "sv": "se",
}


# ---------------------------------------------------------------------------
# Raw English msgids used by JavaScript
# ---------------------------------------------------------------------------

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
    "streetMap": "Street map",
    "satellite": "Satellite",
    "selectionExport": "Selection export to a file",
    "selectionExport2": "Selection export",
    "selectionExportHintBefore": "Use selection tool",
    "selectionExportHintAfter": "to draw one or more areas",
    "exportSelection": "Export selection",
    "clearSelection": "Clear selection",
    "elementalConcentrationsHelp": (
        "Percentage values (%) can be converted to mg/kg by multiplying by 10000."
    ),
}


babel = Babel()


# ---------------------------------------------------------------------------
# Locale helpers
# ---------------------------------------------------------------------------

def canonical_locale(value: str | None) -> str:
    """
    Convert locale variants to the language code used by ECHOrepo.

    Examples:
        en_US -> en
        en-US -> en
        el_GR -> el
        el_CY -> el
        pt_PT -> pt
        nb_NO -> nb
    """
    if not value:
        return "en"

    value = str(value).strip().lower().replace("-", "_")
    language = value.split("_", 1)[0]

    if language in SUPPORTED_LOCALES:
        return language

    return "en"


def base_labels() -> dict:
    """
    Translate the raw English JavaScript msgids for the active locale.
    """
    return {
        key: _real_gettext(msgid)
        for key, msgid in BASE_LABEL_MSGIDS.items()
    }


def _select_locale():
    """
    Locale selection priority:

      1. persistent locale cookie
      2. ?lang= URL parameter
      3. browser Accept-Language
      4. English
    """

    # 1. Persistent cookie
    cookie_locale = request.cookies.get("locale")

    if cookie_locale:
        locale = canonical_locale(cookie_locale)
        if locale in SUPPORTED_LOCALES:
            return locale

    # 2. Explicit URL parameter
    query_locale = request.args.get("lang")

    if query_locale:
        locale = canonical_locale(query_locale)
        if locale in SUPPORTED_LOCALES:
            return locale

    # 3. Browser language preference
    best = request.accept_languages.best_match(SUPPORTED_LOCALES)

    if best:
        return canonical_locale(best)

    # 4. Fallback
    return "en"


# ---------------------------------------------------------------------------
# JS translation labels + overrides
# ---------------------------------------------------------------------------

def build_i18n_labels(base: dict) -> dict:
    """
    Build JavaScript labels for the active language.

    Priority:
        explicit JS-key override
        -> normal gettext translation
    """

    try:
        locale = canonical_locale(str(get_locale() or "en"))
    except Exception:
        locale = "en"

    overrides = get_overrides(locale) or {}

    out = dict(base)

    # Explicit JS-key overrides win over gettext.
    for key, value in overrides.items():
        if value not in (None, ""):
            out[key] = value

    return out


# ---------------------------------------------------------------------------
# Flask-Babel initialization
# ---------------------------------------------------------------------------

def init_i18n(app):
    app.config.setdefault("BABEL_DEFAULT_LOCALE", "en")

    app.config["BABEL_TRANSLATION_DIRECTORIES"] = os.path.join(
        app.root_path,
        "translations",
    )

    babel.init_app(
        app,
        locale_selector=_select_locale,
    )

    @app.context_processor
    def inject_i18n():
        base = base_labels()
        labels = build_i18n_labels(base)

        try:
            current_app.logger.debug(
                "i18n: locale=%s cookie.locale=%r labels_count=%s",
                canonical_locale(str(get_locale() or "en")),
                request.cookies.get("locale"),
                len(labels),
            )
        except Exception as exc:
            current_app.logger.debug(
                "i18n logging failed: %s",
                exc,
            )

        return {
            "I18N": {
                "labels": labels,
                "by_msgid": {},
            }
        }


# ---------------------------------------------------------------------------
# Language-selection route
#
# /lang/en
# /lang/es
# /lang/pt
#
# Region-specific forms also work:
#
# /lang/pt_PT -> pt
# /lang/el_GR -> el
# /lang/el_CY -> el
# ---------------------------------------------------------------------------

lang_bp = Blueprint(
    "lang",
    __name__,
    url_prefix="/lang",
)


@lang_bp.route("/<lang_code>")
def set_language(lang_code):
    locale = canonical_locale(lang_code)

    resp = redirect(
        request.referrer
        or url_for("web.home")
    )

    resp.set_cookie(
        "locale",
        locale,
        max_age=60 * 60 * 24 * 730,  # 2 years
        samesite="Lax",
    )

    return resp