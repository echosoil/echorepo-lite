import html
import re

import pandas as pd

from ..config import settings
from ..services.personal_translations import row_key, text_value
from .geo import pick_lat_lon_cols


# ---- Format "METALS_info" into aligned monospace block ----
def _format_metals_block(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""

    txt = str(s).strip()
    if txt in {"", "0", "0.0", "nan", "NaN", "None", "null"}:
        return ""

    txt = (
        txt.replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("<br>", "\n")
        .replace("\\n", "\n")
        .replace(";", "\n")
    )

    lines_raw = [ln.strip() for ln in txt.split("\n") if ln.strip()]
    if not lines_raw:
        return ""

    rows = []
    for ln in lines_raw:
        # skip placeholder / junk lines entirely
        if ln in {"0", "0.0", "nan", "NaN", "None", "null"}:
            continue

        if "=" in ln or ":" in ln:
            name, rhs = re.split(r"[:=]", ln, maxsplit=1)
            name, rhs = name.strip(), rhs.strip()
        else:
            # If there is no explicit separator, ignore it.
            # This avoids rendering junk like "0  ="
            continue

        if not name or name in {"0", "0.0"}:
            continue

        m = re.match(r"\s*([+-]?\d+(?:[.,]\d+)?)(.*)$", rhs)
        if m:
            val_disp = m.group(1).strip()
            val_len_key = val_disp.replace(",", ".")
            unit = m.group(2).strip()

            try:
                numeric_value = float(val_len_key)
            except Exception:
                numeric_value = None

            if numeric_value == 0:
                # skip zero-valued fake entries entirely
                continue
        else:
            val_disp = rhs.strip()
            val_len_key = val_disp
            unit = ""

            if val_disp in {"", "0", "0.0", "nan", "NaN", "None", "null"}:
                continue

        rows.append((name, val_disp, unit, len(val_len_key)))

    if not rows:
        return ""

    name_w = max((len(r[0]) for r in rows), default=0)
    val_w = max((r[3] for r in rows), default=0)

    out_lines = []
    for name, val_disp, unit, _ in rows:
        name_pad = name.ljust(name_w)
        val_pad = val_disp.rjust(val_w) if val_w else val_disp
        out_lines.append(f"{name_pad}  = {val_pad}" + (f" {unit}" if unit else ""))

    safe = html.escape("\n".join(out_lines)).replace("\n", "<br>")
    return f'<div class="metals-block">{safe}</div>'


def strip_orig_cols(df: pd.DataFrame) -> pd.DataFrame:
    if not settings.HIDE_ORIG_COLS:
        return df
    DROP_SUFFIXES = {settings.ORIG_COL_SUFFIX} | {c for c in settings.HIDE_ORIG_LIST if c}
    drop = {c for c in df.columns if any(c.endswith(suf) for suf in DROP_SUFFIXES)}
    return df.drop(columns=list(drop), errors="ignore") if drop else df


def translation_html(original, english, display_html=None):
    """Native hover title plus keyboard/touch expandable, escaped English text."""
    original, english = text_value(original), text_value(english)
    display = html.escape(original or english) if display_html is None else display_html
    if not original or not english or original == english:
        return display
    title = html.escape("English translation: " + english, quote=True)
    return (
        '<details class="echo-translation">'
        f'<summary title="{title}">{display}'
        ' <i class="bi bi-translate" aria-hidden="true"></i>'
        ' <span class="visually-hidden">English translation available</span></summary>'
        f'<span class="echo-translation-text" lang="en">{html.escape(english)}</span>'
        "</details>"
    )


def make_table_html(df: pd.DataFrame, translations=None) -> str:
    if df.empty:
        return "<p>No data available.</p>"

    # Keep a copy with ALL columns so we can read *_option captions later
    df_full = df.copy()
    sample_translations, image_translations = translations or ({}, {})

    # Add lat/lon display columns
    lat_col, lon_col = pick_lat_lon_cols(df.columns)
    df = df.copy()
    if lat_col and lon_col:
        df["_lat_disp"] = pd.to_numeric(df[lat_col], errors="coerce").apply(
            lambda v: "" if pd.isna(v) else f"{float(v):.5f}"
        )
        df["_lon_disp"] = pd.to_numeric(df[lon_col], errors="coerce").apply(
            lambda v: "" if pd.isna(v) else f"{float(v):.5f}"
        )
    else:
        df["_lat_disp"] = ""
        df["_lon_disp"] = ""

    # ---- Select the columns we DO want to render (no *_option here) ----
    cols = [
        "sampleId",
        "collectedAt",
        "fs_createdAt",  # <-- use real column; pretty name later
        "_lat_disp",
        "_lon_disp",
        "QR_qrCode",
        "SOIL_STRUCTURE_structure",
        "SOIL_TEXTURE_texture",
        "SOIL_COLOR_color",
        "PH_ph",
        "SOIL_DIVER_earthworms",
        "SOIL_CONTAMINATION_plastic",
        "SOIL_CONTAMINATION_debris",
        "SOIL_CONTAMINATION_comments",
        "SOIL_DIVER_observations",
        "METALS_info",
        "PHOTO_photos_1_path",
        "PHOTO_photos_2_path",
        "PHOTO_photos_3_path",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].copy()  # <- slice to visible columns

    # Dates and integers
    if "collectedAt" in df.columns:
        df["collectedAt"] = pd.to_datetime(df["collectedAt"], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
    if "fs_createdAt" in df.columns:
        # Parse to UTC first, then strip tz → naive
        ts = pd.to_datetime(df["fs_createdAt"], errors="coerce", utc=True)
        df["fs_createdAt"] = ts.dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M")

    for c in ["SOIL_DIVER_earthworms", "SOIL_CONTAMINATION_plastic", "SOIL_CONTAMINATION_debris"]:
        if c in df.columns:
            df[c] = (
                pd.to_numeric(df[c], errors="coerce")
                .fillna(pd.NA)
                .astype("Int64")
                .astype(str)
                .replace("<NA>", "")
            )

    # Use original SQLite values, and attach English only if the canonical
    # original still matches (e.g. a duplicate QR or edited text must not borrow it).
    for col, field in (
        ("SOIL_CONTAMINATION_comments", "contamination_other"),
        ("SOIL_DIVER_observations", "observations"),
    ):
        if col not in df.columns:
            continue

        def format_text(row, column=col, prefix=field):
            original = text_value(row[column])
            canonical = sample_translations.get(row_key(df_full.loc[row.name]), {})
            english = (
                canonical.get(prefix + "_en")
                if original == text_value(canonical.get(prefix + "_orig"))
                else None
            )
            return translation_html(original, english)

        df[col] = df.apply(format_text, axis=1)

    # ---- Build thumbnails with ALT/TITLE and a tiny visible caption from *_option ----
    def fmt_img_with_cap(url, caption_text, comment="", english=""):
        if url is None or (isinstance(url, float) and pd.isna(url)):
            return ""
        url_s = str(url).strip()
        if not url_s:
            return ""
        safe_url = html.escape(url_s)
        cap = text_value(caption_text)
        safe_cap = html.escape(cap) if cap else "Sample photo"
        # figure+figcaption to show a small caption; alt+title for accessibility/tooltip
        return (
            f'<figure class="thumb">'
            f'  <a href="{safe_url}" target="_blank" rel="noopener">'
            f'    <img src="{safe_url}" alt="{safe_cap}" title="{safe_cap}" '
            f'         style="max-height:60px;border-radius:4px;object-fit:cover;">'
            f"  </a>"
            f"{f'<figcaption>{html.escape(cap)}</figcaption>' if cap else ''}"
            f"{f'<figcaption>{translation_html(comment, english)}</figcaption>' if comment else ''}"
            f"</figure>"
        )

    for n in (1, 2, 3):
        path_col = f"PHOTO_photos_{n}_path"
        opt_col = f"PHOTO_photos_{n}_option"
        if path_col in df.columns:
            def format_photo(row, slot=n, path=path_col, option=opt_col):
                raw = df_full.loc[row.name]
                comment = text_value(raw.get(f"PHOTO_photos_{slot}_comment"))
                canonical = image_translations.get((*row_key(raw), slot), {})
                english = (
                    canonical.get("image_description_en")
                    if comment == text_value(canonical.get("image_description_orig"))
                    else None
                )
                return fmt_img_with_cap(row[path], raw.get(option), comment, english)

            df[path_col] = df.apply(format_photo, axis=1)

    # Alignment in Metals column
    if "METALS_info" in df.columns:
        def format_metals(row):
            original = text_value(row["METALS_info"])
            block = _format_metals_block(original)
            canonical = sample_translations.get(row_key(df_full.loc[row.name]), {})
            source = text_value(canonical.get("metals_info_orig"))
            matches = original == source
            english = canonical.get("metals_info_en") if matches else None
            # Do not replace or translate structured lab measurements in the UI.
            if block:
                return block
            return translation_html(original, english)

        df["METALS_info"] = df.apply(format_metals, axis=1)

    # ---- Pretty headers and no-breaks where useful ----
    pretty = {
        "sampleId": ("Sample", True),
        "collectedAt": ("Date", True),
        "fs_createdAt": ("Uploaded to ECHO", False),  # pretty label here
        "_lat_disp": ("Latitude (±1 km)", True),
        "_lon_disp": ("Longitude (±1 km)", True),
        "QR_qrCode": ("QR code", True),
        "SOIL_STRUCTURE_structure": ("Structure", False),
        "SOIL_TEXTURE_texture": ("Texture", False),
        "SOIL_COLOR_color": ("Soil organic matter", False),
        "PH_ph": ("pH", True),
        "SOIL_DIVER_earthworms": ("Earthworms", False),
        "SOIL_CONTAMINATION_plastic": ("Plastic", False),
        "SOIL_CONTAMINATION_debris": ("Debris", False),
        "SOIL_CONTAMINATION_comments": ("Contamination", False),
        "SOIL_DIVER_observations": ("Observations", False),
        "METALS_info": ("Elemental concentrations", False),
        "PHOTO_photos_1_path": ("Photo 1", False),
        "PHOTO_photos_2_path": ("Photo 2", False),
        "PHOTO_photos_3_path": ("Photo 3", False),
    }
    for col, (_, no_break) in pretty.items():
        if col in df.columns and no_break:
            df[col] = df[col].apply(lambda s: f"<nobr>{s}</nobr>" if pd.notna(s) and s != "" else s)

    df.rename(columns={k: v[0] for k, v in pretty.items()}, inplace=True)

    fmt = {}
    if "Elemental concentrations" in df.columns:
        fmt["Elemental concentrations"] = lambda x: x

    return df.to_html(
        classes="table table-sm table-striped align-middle",
        index=False,
        escape=False,
        justify="left",
        formatters=fmt,
    )
