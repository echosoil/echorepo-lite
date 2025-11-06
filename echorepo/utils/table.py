import html, pandas as pd
from ..config import settings
from .geo import pick_lat_lon_cols

def strip_orig_cols(df: pd.DataFrame) -> pd.DataFrame:
    if not settings.HIDE_ORIG_COLS:
        return df
    DROP_SUFFIXES = {settings.ORIG_COL_SUFFIX} | {c for c in settings.HIDE_ORIG_LIST if c}
    drop = {c for c in df.columns if any(c.endswith(suf) for suf in DROP_SUFFIXES)}
    return df.drop(columns=list(drop), errors="ignore") if drop else df

def make_table_html(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>No data available.</p>"

    lat_col, lon_col = pick_lat_lon_cols(df.columns)
    df = df.copy()
    if lat_col and lon_col:
        df["_lat_disp"] = pd.to_numeric(df[lat_col], errors="coerce").apply(lambda v: "" if pd.isna(v) else f"{float(v):.5f}")
        df["_lon_disp"] = pd.to_numeric(df[lon_col], errors="coerce").apply(lambda v: "" if pd.isna(v) else f"{float(v):.5f}")
    else:
        df["_lat_disp"] = ""
        df["_lon_disp"] = ""

    cols = [
        "sampleId","collectedAt", "Uploaded to ECHO", "_lat_disp","_lon_disp","QR_qrCode",
        "SOIL_STRUCTURE_structure","SOIL_TEXTURE_texture","SOIL_COLOR_color","PH_ph",
        "SOIL_DIVER_earthworms","SOIL_CONTAMINATION_plastic","SOIL_CONTAMINATION_debris",
        "SOIL_CONTAMINATION_comments","METALS_info",
        "PHOTO_photos_1_path","PHOTO_photos_2_path","PHOTO_photos_3_path",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].copy()

    if "collectedAt" in df.columns:
        df["collectedAt"] = pd.to_datetime(df["collectedAt"], errors="coerce").dt.strftime("%Y-%m-%d")

    for c in ["SOIL_DIVER_earthworms","SOIL_CONTAMINATION_plastic","SOIL_CONTAMINATION_debris"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(pd.NA).astype("Int64").astype(str).replace("<NA>","")

    def fmt_img(url):
        if not url or pd.isna(url): return ""
        safe = html.escape(str(url))
        return f'<a href="{safe}" target="_blank" rel="noopener"><img src="{safe}" style="max-height:60px;border-radius:4px;"></a>'
    for c in ["PHOTO_photos_1_path","PHOTO_photos_2_path","PHOTO_photos_3_path"]:
        if c in df.columns:
            df[c] = df[c].apply(fmt_img)
    # prettify column names: (column_name: (Pretty Name, is_nolinebreak))
    pretty = {
        "sampleId":("Sample", True),
        "collectedAt":("Date", True),
        "fs_createdAt": ("Uploaded to ECHO", False), 
        "_lat_disp":("Latitude (±1 km)", True),
        "_lon_disp":("Longitude (±1 km)", True),
        "QR_qrCode":("QR code", True),
        "SOIL_STRUCTURE_structure":("Structure", False),
        "SOIL_TEXTURE_texture":("Texture", False),
        "SOIL_COLOR_color":("Colour", False),
        "PH_ph":("pH", True),
        "SOIL_DIVER_earthworms":("Earthworms", False),
        "SOIL_CONTAMINATION_plastic":("Plastic", False),
        "SOIL_CONTAMINATION_debris":("Debris", False),
        "SOIL_CONTAMINATION_comments":("Contamination", False),
        "METALS_info":("Metals", False),
        "PHOTO_photos_1_path":("Photo 1", False),
        "PHOTO_photos_2_path":("Photo 2", False),
        "PHOTO_photos_3_path":("Photo 3", False),
    }
    for col, (_, no_break) in pretty.items():
        if col in df.columns and no_break:
            df[col] = df[col].apply(lambda s: f"<nobr>{s}</nobr>" if pd.notna(s) and s != "" else s)
    df.rename(columns={k:v[0] for k, v in pretty.items()}, inplace=True)
    return df.to_html(classes="table table-sm table-striped align-middle", index=False, escape=False, justify="left")
