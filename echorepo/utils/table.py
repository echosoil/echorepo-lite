import html, pandas as pd
from ..config import settings
from .geo import pick_lat_lon_cols
import re

# ---- Format "METALS_info" into aligned monospace block ----
def _format_metals_block(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    # --- normalize separators to real newlines first
    txt = str(s)
    txt = (txt
           .replace("\r\n", "\n")
           .replace("\r", "\n")
           .replace("<br/>", "\n")
           .replace("<br />", "\n")
           .replace("<br>", "\n")
           .replace("\\n", "\n")
           .replace(";", "\n"))

    import re
    lines_raw = [ln.strip() for ln in txt.split("\n") if ln.strip()]
    if not lines_raw:
        return ""

    rows = []
    for ln in lines_raw:
        if "=" in ln or ":" in ln:
            name, rhs = re.split(r"[:=]", ln, maxsplit=1)
            name, rhs = name.strip(), rhs.strip()
        else:
            parts = ln.split(None, 1)
            if len(parts) == 2:
                name, rhs = parts[0].strip(), parts[1].strip()
            else:
                rows.append((ln.strip(), "", "", 0))
                continue

        m = re.match(r"\s*([+-]?\d+(?:[.,]\d+)?)(.*)$", rhs)
        if m:
            val_disp = m.group(1).strip()
            val_len_key = val_disp.replace(",", ".")
            unit = m.group(2).strip()
        else:
            val_disp = rhs
            val_len_key = val_disp
            unit = ""

        rows.append((name, val_disp, unit, len(val_len_key)))

    name_w = max((len(r[0]) for r in rows), default=0)
    val_w  = max((r[3] for r in rows), default=0)

    out_lines = []
    for name, val_disp, unit, _ in rows:
        name_pad = name.ljust(name_w)
        val_pad  = val_disp.rjust(val_w) if val_w else val_disp
        out_lines.append(f"{name_pad}  = {val_pad}" + (f" {unit}" if unit else ""))

    # Escape HTML, then replace newlines with <br> so pandas can't turn them into "\n"
    safe = html.escape("\n".join(out_lines)).replace("\n", "<br>")
    return f'<div class="metals-block">{safe}</div>'

def strip_orig_cols(df: pd.DataFrame) -> pd.DataFrame:
    if not settings.HIDE_ORIG_COLS:
        return df
    DROP_SUFFIXES = {settings.ORIG_COL_SUFFIX} | {c for c in settings.HIDE_ORIG_LIST if c}
    drop = {c for c in df.columns if any(c.endswith(suf) for suf in DROP_SUFFIXES)}
    return df.drop(columns=list(drop), errors="ignore") if drop else df

def make_table_html(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>No data available.</p>"

    # Keep a copy with ALL columns so we can read *_option captions later
    df_full = df.copy()

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
        "sampleId", "collectedAt", "fs_createdAt",   # <-- use real column; pretty name later
        "_lat_disp", "_lon_disp", "QR_qrCode",
        "SOIL_STRUCTURE_structure", "SOIL_TEXTURE_texture", "SOIL_COLOR_color", "PH_ph",
        "SOIL_DIVER_earthworms", "SOIL_CONTAMINATION_plastic", "SOIL_CONTAMINATION_debris",
        "SOIL_CONTAMINATION_comments", "METALS_info",
        "PHOTO_photos_1_path", "PHOTO_photos_2_path", "PHOTO_photos_3_path",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].copy()  # <- slice to visible columns

    # Dates and integers
    if "collectedAt" in df.columns:
        df["collectedAt"] = pd.to_datetime(df["collectedAt"], errors="coerce").dt.strftime("%Y-%m-%d")
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

    # ---- Build thumbnails with ALT/TITLE and a tiny visible caption from *_option ----
    def fmt_img_with_cap(url, caption_text):
        if url is None or (isinstance(url, float) and pd.isna(url)):
            return ""
        url_s = str(url).strip()
        if not url_s:
            return ""
        safe_url = html.escape(url_s)
        cap = (str(caption_text).strip() if caption_text is not None else "")
        safe_cap = html.escape(cap) if cap else "Sample photo"
        # figure+figcaption to show a small caption; alt+title for accessibility/tooltip
        return (
            f'<figure class="thumb">'
            f'  <a href="{safe_url}" target="_blank" rel="noopener">'
            f'    <img src="{safe_url}" alt="{safe_cap}" title="{safe_cap}" '
            f'         style="max-height:60px;border-radius:4px;object-fit:cover;">'
            f'  </a>'
            f'{f"<figcaption>{html.escape(cap)}</figcaption>" if cap else ""}'
            f'</figure>'
        )

    for n in (1, 2, 3):
        path_col = f"PHOTO_photos_{n}_path"
        opt_col  = f"PHOTO_photos_{n}_option"
        if path_col in df.columns:
            caps = df_full[opt_col] if opt_col in df_full.columns else None
            if caps is not None:
                # Keep index alignment so row.name matches caps index
                df[path_col] = df.apply(lambda row: fmt_img_with_cap(row[path_col], caps.loc[row.name]), axis=1)
            else:
                df[path_col] = df[path_col].apply(lambda url: fmt_img_with_cap(url, None))
    
    # Alignment in Metals column
    if "METALS_info" in df.columns:
        df["METALS_info"] = df["METALS_info"].apply(_format_metals_block)
    print("[DEBUG]2", df[["METALS_info"]])

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
