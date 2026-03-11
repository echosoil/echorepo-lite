import re

# …these two are the distilled versions from your script…
_DMS_TOKEN = re.compile(
    r"""
^\s*(?P<sign>[+-])?\s*
(?P<deg>\d+(?:[.,]\d+)?)
(?:\s*[°ºdD]\s*
    (?P<min>\d+(?:[.,]\d+)?)?
    (?:\s*['′mM]\s*
        (?P<sec>\d+(?:[.,]\d+)?)?
        (?:\s*(?:"|″|”|s|S))?
    )?
)?
\s*(?P<hem>[NnSsEeWw])?\s*$""",
    re.VERBOSE,
)


def _to_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def parse_coord(value: str, *, is_lon: bool) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if re.fullmatch(r"[+-]?\d+(?:[.,]\d+)?", s):
        v = _to_float(s)
        return v
    m = _DMS_TOKEN.match(s)
    if not m:
        return None
    sign = -1.0 if (m.group("sign") == "-") else 1.0
    hem = (m.group("hem") or "").upper()
    deg = _to_float(m.group("deg"))
    minu = _to_float(m.group("min"))
    sec = _to_float(m.group("sec"))
    if deg is None:
        return None
    if minu is None and sec is None and re.search(r"[.,]", m.group("deg") or ""):
        deg_abs = abs(deg)
    else:
        deg_abs = abs(deg) + (minu or 0) / 60 + (sec or 0) / 3600
    if hem in ("N", "E"):
        signed = deg_abs
    elif hem in ("S", "W"):
        signed = -deg_abs
    else:
        signed = deg_abs if sign >= 0 else -deg_abs
    if is_lon and not -180 <= signed <= 180:
        return signed  # warn upstream if you want
    if not is_lon and not -90 <= signed <= 90:
        return signed
    return signed
