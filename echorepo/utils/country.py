import re

import pycountry

ALIASES = {
    "uk": "United Kingdom",
    "great britain": "United Kingdom",
    "gb": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    "us": "United States",
    "u.s.": "United States",
    "usa": "United States",
    "u.s.a.": "United States",
    "russia": "Russian Federation",
    "czech republic": "Czechia",
    "ivory coast": "Côte d’Ivoire",
    "south korea": "Korea, Republic of",
    "north korea": "Korea, Democratic People’s Republic of",
    "swaziland": "Eswatini",
    "cape verde": "Cabo Verde",
    "macedonia": "North Macedonia",
    "moldova": "Moldova, Republic of",
    "bolivia": "Bolivia, Plurinational State of",
    "tanzania": "Tanzania, United Republic of",
    "palestine": "Palestine, State of",
    "laos": "Lao People’s Democratic Republic",
    "vietnam": "Viet Nam",
    "syria": "Syrian Arab Republic",
    "iran": "Iran, Islamic Republic of",
    "venezuela": "Venezuela, Bolivarian Republic of",
}


def country_to_iso2(name: str) -> str | None:
    if not name or not str(name).strip():
        return None
    s = str(name).strip()
    s_l = s.lower()
    if s_l in ALIASES:
        s = ALIASES[s_l]
    try:
        c = pycountry.countries.lookup(s)
        return c.alpha_2
    except LookupError:
        s2 = re.sub(r"[’`]", "'", s).strip()
        try:
            return pycountry.countries.lookup(s2).alpha_2
        except LookupError:
            return None
