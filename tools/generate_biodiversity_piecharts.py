#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import math
import os
import re
import sys
import tempfile
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env exactly like pull_and_enrich_samples.py
# ---------------------------------------------------------------------------
env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path)
print(f"[INFO] Loaded environment from {env_path}")

# ---------------------------------------------------------------------------
# Make sure project root is importable
# ---------------------------------------------------------------------------
THIS_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = THIS_DIR.parent
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", str(DEFAULT_ROOT)))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
print(f"[INFO] Using PROJECT_ROOT={PROJECT_ROOT}")

# ---------------------------------------------------------------------------
# MinIO config: same style as pull_and_enrich_samples.py
# ---------------------------------------------------------------------------
try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None

    class S3Error(Exception):
        pass


MINIO_ENDPOINT = (
    os.getenv("MINIO_ENDPOINT_INSIDE")
    or os.getenv("MINIO_ENDPOINT_OUTSIDE")
    or os.getenv("MINIO_ENDPOINT")
    or "localhost:9000"
)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or ""
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or ""
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "echorepo-uploads")
PUBLIC_STORAGE_BASE = os.getenv("PUBLIC_STORAGE_BASE", "/storage")
FUNGUILD_DB_JSON = os.getenv(
    "FUNGUILD_DB_JSON",
    str(PROJECT_ROOT / "data" / "biodiversity" / "FUNGuild_db.json"),
)

GENERATE_FUNGAL_GUILDS = os.getenv("GENERATE_FUNGAL_GUILDS", "0") == "1"
BUILD_FAPROTAX_INPUTS = os.getenv("BUILD_FAPROTAX_INPUTS", "0") == "1"

GENERATE_BACTERIAL_GUILDS = os.getenv("GENERATE_BACTERIAL_GUILDS", "0") == "1"

FAPROTAX_FUNCTION_CSV = os.getenv(
    "FAPROTAX_FUNCTION_CSV",
    str(PROJECT_ROOT / "data" / "biodiversity" / "8_faprotax_samples_x_functions.csv"),
)

# ---------------------------------------------------------------------------
# Plot styling
# ---------------------------------------------------------------------------
PIE_BG = "#FFFFFF"
PIE_TEXT = "#000000"
PIE_EDGE = "#FFFFFF"
PIE_GRID = "#e0e0e0"

PIE_COLORS = [
    "#f0746a",  # salmon
    "#df9600",  # orange
    "#a6a800",  # olive
    "#41c400",  # green
    "#12bf80",  # teal-green
    "#1db7be",  # cyan-teal
    "#20a7df",  # blue
    "#8a83e6",  # lavender
    "#cc62dc",  # magenta-violet
    "#eb5bb3",  # pink
    "#999999",  # grey fallback for "Other"
]

mpl.rcParams["font.family"] = "DejaVu Sans"
mpl.rcParams["text.color"] = PIE_TEXT
mpl.rcParams["axes.labelcolor"] = PIE_TEXT
mpl.rcParams["xtick.color"] = PIE_TEXT
mpl.rcParams["ytick.color"] = PIE_TEXT

# ---------------------------------------------------------------------------
# Postgres config
# ---------------------------------------------------------------------------
try:
    import psycopg2
except ImportError:
    psycopg2 = None


def get_pg_conn():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed")

    host = (
        os.getenv("DB_HOST_OUTSIDE")
        or os.getenv("DB_HOST_INSIDE")
        or os.getenv("DB_HOST")
        or "localhost"
    )
    port = int(
        os.getenv("DB_PORT_OUTSIDE")
        or os.getenv("DB_PORT_INSIDE")
        or os.getenv("DB_PORT")
        or "5432"
    )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )


def init_minio():
    if Minio is None:
        print("[INFO] python-minio not installed; skipping MinIO upload.")
        return None

    secure = False
    endpoint = MINIO_ENDPOINT
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://") :]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://") :]

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print("[WARN] MinIO credentials not set; skipping chart upload.")
        return None

    client = Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )

    try:
        found = client.bucket_exists(MINIO_BUCKET)
        if not found:
            client.make_bucket(MINIO_BUCKET)
            print(f"[INFO] Created MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        print(f"[WARN] Could not ensure MinIO bucket: {e}")
        return None

    print(f"[INFO] MinIO ready at {MINIO_ENDPOINT}, bucket={MINIO_BUCKET}")
    return client


def upload_file_to_minio(
    mclient, local_path: Path, object_name: str, content_type: str = "image/png"
):
    if mclient is None:
        return None

    try:
        size = local_path.stat().st_size
        with local_path.open("rb") as f:
            mclient.put_object(
                MINIO_BUCKET,
                object_name,
                data=f,
                length=size,
                content_type=content_type,
            )
        print(f"[OK] uploaded to MinIO: {object_name}")
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except Exception as e:
        print(f"[WARN] could not upload {local_path} to MinIO as {object_name}: {e}")
        return None


def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s).strip())


def fetch_otu_data(marker: str = "16S") -> pd.DataFrame:
    sql = """
        SELECT sample_id, otu_id, count, taxa
        FROM sample_otu_counts
        WHERE marker = %s
    """
    with get_pg_conn() as conn:
        df = pd.read_sql(sql, conn, params=[marker])
    return df


TAX_PREFIX_TO_RANK = {
    "k": "Kingdom",
    "p": "Phylum",
    "c": "Class",
    "o": "Order",
    "f": "Family",
    "g": "Genus",
    "s": "Species",
}


def clean_taxon_value(value: str) -> str:
    """
    Clean values like:
      g__Fusarium
      f__Nectriaceae
      Capnodiales_fam_Incertae_sedis

    Returns a display-friendly value, or "" if it is not useful.
    """
    if value is None:
        return ""

    s = str(value).strip()
    if not s:
        return ""

    # Remove rank prefix if still present
    s = re.sub(r"^[a-zA-Z]__", "", s)

    # Convert underscores to spaces for display / matching
    s = s.replace("_", " ").strip()

    # Drop low-information labels
    if re.search(r"incertae|unclassified|uncultured|unknown", s, flags=re.I):
        return ""

    # Drop generic species placeholders such as "Capnodiales sp"
    if re.search(r"\bsp\.?$", s, flags=re.I):
        return ""

    return s


def parse_taxonomy_string(raw: str) -> dict:
    """
    Parse a single taxonomy string like:
      k__Fungi;p__Ascomycota;c__Dothideomycetes;o__Capnodiales;f__...;g__...;s__...

    Returns:
      {
        "Kingdom": "Fungi",
        "Phylum": "Ascomycota",
        "Class": "Dothideomycetes",
        "Order": "Capnodiales",
        "Family": "...",
        "Genus": "...",
        "Species": "..."
      }
    """
    out = {}

    if raw is None:
        return out

    s = str(raw).strip()
    if not s:
        return out

    # Accept semicolon, pipe, or comma-separated taxonomy strings
    parts = re.split(r"\s*[;|]\s*", s)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        m = re.match(r"^([kpcofgs])__?(.*)$", part, flags=re.I)
        if not m:
            continue

        prefix = m.group(1).lower()
        value = m.group(2).strip()
        rank = TAX_PREFIX_TO_RANK.get(prefix)
        if not rank:
            continue

        cleaned = clean_taxon_value(value)
        if cleaned:
            out[rank] = cleaned

    return out


def taxa_to_normalized_dict(taxa) -> dict:
    """
    Normalize taxa from Postgres sample_otu_counts.taxa.

    Supports:
      - dict with Taxonomy raw string
      - dict with A/B/C/D/E/F columns
      - dict with named ranks
      - JSON string
      - raw taxonomy string
    """
    d = _taxa_to_dict(taxa)

    # Case 1: taxa is a raw taxonomy string, not JSON
    if not d and isinstance(taxa, str):
        parsed = parse_taxonomy_string(taxa)
        if parsed:
            return parsed

    out = {}

    # Case 2: raw taxonomy column inside JSON/dict
    raw_tax = d.get("Taxonomy") or d.get("taxonomy") or d.get("taxon") or d.get("Taxon") or ""
    if raw_tax:
        out.update(parse_taxonomy_string(raw_tax))

    # Case 3: named rank columns already present
    for rank in ("Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"):
        val = d.get(rank) or d.get(rank.lower())
        cleaned = clean_taxon_value(val)
        if cleaned:
            out[rank] = cleaned

    # Case 4: old A/B/C/D/E/F style.
    # Based on your Excel: Taxonomy = kingdom, A=phylum, B=class,
    # C=order, D=family, E=genus, F=species.
    letter_map = {
        "A": "Phylum",
        "B": "Class",
        "C": "Order",
        "D": "Family",
        "E": "Genus",
        "F": "Species",
    }
    for key, rank in letter_map.items():
        cleaned = clean_taxon_value(d.get(key))
        if cleaned and rank not in out:
            out[rank] = cleaned

    return out


def extract_taxon_label(row: pd.Series, level: str) -> str:
    taxa = taxa_to_normalized_dict(row.get("taxa"))

    val = taxa.get(level)
    if val:
        return str(val).strip()

    return "Unclassified"


def make_piechart_for_sample(
    sample_df: pd.DataFrame, sample_id: str, marker: str, level: str, out_path: Path
):
    plot_df = sample_df.copy()
    plot_df["taxon"] = plot_df.apply(lambda r: extract_taxon_label(r, level), axis=1)
    plot_df["count"] = pd.to_numeric(plot_df["count"], errors="coerce").fillna(0)

    grouped = (
        plot_df.groupby("taxon", dropna=False)["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
    )

    grouped = grouped[grouped["count"] > 0].copy()
    if grouped.empty:
        return False

    # Keep top taxa, collapse the rest into "Other"
    top_n = 10
    if len(grouped) > top_n:
        top = grouped.iloc[:top_n].copy()
        other_sum = grouped.iloc[top_n:]["count"].sum()
        if other_sum > 0:
            top = pd.concat(
                [top, pd.DataFrame([{"taxon": "Other", "count": other_sum}])],
                ignore_index=True,
            )
        grouped = top

    total = grouped["count"].sum()
    grouped["pct"] = grouped["count"] / total * 100.0

    # ---------- Figure ----------
    fig, ax = plt.subplots(figsize=(14, 10), facecolor=PIE_BG)
    ax.set_facecolor(PIE_BG)

    colors = PIE_COLORS[: len(grouped)]
    if len(colors) < len(grouped):
        # fallback if ever needed
        extra = ["#777777"] * (len(grouped) - len(colors))
        colors = colors + extra

    wedges, _ = ax.pie(
        grouped["pct"],
        startangle=90,
        colors=colors,
        labels=None,  # no labels directly on the pie
        counterclock=True,
        wedgeprops={
            "edgecolor": PIE_EDGE,
            "linewidth": 2.0,
        },
        radius=1.0,
    )

    ax.axis("equal")

    # ---------- Title ----------
    fig.suptitle(
        f"Top {min(top_n, len(grouped))} {level} — {sample_id} ({marker})",
        fontsize=22,
        fontweight="bold",
        color=PIE_TEXT,
        y=0.96,
    )

    # ---------- Legend ----------
    legend_labels = [
        f"{taxon} ({pct:.1f}%)" for taxon, pct in zip(grouped["taxon"], grouped["pct"])
    ]

    leg = ax.legend(
        wedges,
        legend_labels,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
        fontsize=14,
        labelcolor=PIE_TEXT,
        handlelength=1.6,
        handleheight=1.6,
        borderaxespad=0.0,
    )

    # Some matplotlib versions ignore labelcolor above, so force it:
    for txt in leg.get_texts():
        txt.set_color(PIE_TEXT)

    # Remove axes junk
    ax.set_xticks([])
    ax.set_yticks([])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=[0.02, 0.02, 0.82, 0.93])
    fig.savefig(
        out_path,
        dpi=300,
        bbox_inches="tight",
        facecolor=fig.get_facecolor(),
    )
    plt.close(fig)
    return True


def build_clean_otu_and_taxonomy_files(
    marker: str = "16S",
    out_dir: Path | None = None,
    min_prev: int = 2,
    min_total: int = 50,
) -> tuple[Path, Path]:
    """
    Build R-compatible FAPROTAX input files from Postgres sample_otu_counts.

    Produces:
      - 6_otu_clean_counts_no_blanks.csv
      - 7_taxonomy_clean.csv

    This performs simple abundance filtering:
      - keep OTUs present in at least min_prev samples
      - keep OTUs with at least min_total total reads

    It does NOT perform decontam/blank filtering unless that has already happened
    before import.
    """
    if out_dir is None:
        out_dir = PROJECT_ROOT / "data" / "biodiversity" / "faprotax_work"

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Building FAPROTAX input files from marker={marker}")

    df = fetch_otu_data(marker=marker)

    if df.empty:
        raise RuntimeError(f"No OTU rows found in sample_otu_counts for marker={marker}")

    df["sample_id"] = df["sample_id"].astype(str).str.strip()
    df["otu_id"] = df["otu_id"].astype(str).str.strip()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)

    # ------------------------------------------------------------------
    # 1) Build OTU count matrix: rows = OTU IDs, columns = sample IDs
    # ------------------------------------------------------------------
    otu = df.pivot_table(
        index="otu_id",
        columns="sample_id",
        values="count",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()

    # Simple filtering equivalent to the early part of the R script
    prevalence = (otu > 0).sum(axis=1)
    total = otu.sum(axis=1)

    keep = (prevalence >= min_prev) & (total >= min_total)
    otu_clean = otu.loc[keep].copy()

    print(f"[INFO] OTUs before filtering: {len(otu)}")
    print(f"[INFO] OTUs after filtering : {len(otu_clean)}")

    # ------------------------------------------------------------------
    # 2) Build taxonomy table for same OTUs
    # ------------------------------------------------------------------
    tax_rows = []

    # one taxonomy row per OTU
    tax_source = df.drop_duplicates(subset=["otu_id"]).set_index("otu_id").loc[otu_clean.index]

    for otu_id, row in tax_source.iterrows():
        taxa = taxa_to_normalized_dict(row.get("taxa"))

        tax_rows.append(
            {
                "OTU_ID": otu_id,
                "Kingdom": taxa.get("Kingdom", ""),
                "Phylum": taxa.get("Phylum", ""),
                "Class": taxa.get("Class", ""),
                "Order": taxa.get("Order", ""),
                "Family": taxa.get("Family", ""),
                "Genus": taxa.get("Genus", ""),
                "Species": taxa.get("Species", ""),
            }
        )

    tax_df = pd.DataFrame(tax_rows).set_index("OTU_ID")

    # ------------------------------------------------------------------
    # 3) Write files in the format expected by the R/FAPROTAX script
    # ------------------------------------------------------------------
    otu_path = out_dir / "6_otu_clean_counts_no_blanks.csv"
    tax_path = out_dir / "7_taxonomy_clean.csv"

    otu_clean.to_csv(otu_path)
    tax_df.to_csv(tax_path, sep=";")

    print(f"[OK] Wrote {otu_path}")
    print(f"[OK] Wrote {tax_path}")

    return otu_path, tax_path


# ---------------------------------------------------------------------------
# Fungal ecological guild plots, based on FUNGuild genus-level assignments
# ---------------------------------------------------------------------------

FUNGUILD_KEEP_CONFIDENCE = {
    "Probable",
    "Highly Probable",
    "Higly Probable",  # typo present in some FUNGuild outputs
}

FUNGUILD_CONF_SCORE = {
    "Highly Probable": 3,
    "Higly Probable": 3,
    "Probable": 2,
    "Possible": 1,
}

FUNGAL_GUILD_MACRO_MAP = {
    "Ectomycorrhizal": "Ectomycorrhizal fungi",
    "Arbuscular Mycorrhizal": "Arbuscular mycorrhizal fungi",
    "Ericoid Mycorrhizal": "Mycorrhizal fungi",
    "Orchid Mycorrhizal": "Mycorrhizal fungi",
    "Wood Saprotroph": "Wood decomposers",
    "Litter Saprotroph": "Litter decomposers",
    "Plant Saprotroph": "Plant litter decomposers",
    "Dung Saprotroph": "Dung decomposers",
    "Undefined Saprotroph": "Decomposers (unspecified)",
    "Plant Pathogen": "Plant pathogens",
    "Animal Pathogen": "Animal pathogens",
    "Animal Parasite": "Animal pathogens",
    "Endophyte": "Endophytes",
    "Fungal Parasite": "Fungal parasites",
    "Lichen Parasite": "Lichen parasites",
    "Lichenized": "Lichenized fungi",
    "Nematophagous": "Nematophagous fungi",
    "Algal Parasite": "Algal parasites",
    "Insect Pathogen": "Insect pathogens",
    "Epiphyte": "Endophytes",
    "Pollen Saprotroph": "Decomposers (unspecified)",
}

FUNGAL_GUILD_ORDER = [
    "Ectomycorrhizal fungi",
    "Arbuscular mycorrhizal fungi",
    "Mycorrhizal fungi",
    "Wood decomposers",
    "Litter decomposers",
    "Plant litter decomposers",
    "Dung decomposers",
    "Decomposers (unspecified)",
    "Plant pathogens",
    "Animal pathogens",
    "Endophytes",
    "Fungal parasites",
    "Lichen parasites",
    "Lichenized fungi",
    "Nematophagous fungi",
    "Algal parasites",
    "Insect pathogens",
]

FUNGAL_GUILD_COLORS = {
    "Ectomycorrhizal fungi": "#264653",
    "Arbuscular mycorrhizal fungi": "#2A9D8F",
    "Mycorrhizal fungi": "#457B9D",
    "Wood decomposers": "#8B5E3C",
    "Litter decomposers": "#C9A96E",
    "Plant litter decomposers": "#E9C46A",
    "Dung decomposers": "#A8DADC",
    "Decomposers (unspecified)": "#BDB2A7",
    "Plant pathogens": "#E76F51",
    "Animal pathogens": "#F4A261",
    "Endophytes": "#6A994E",
    "Fungal parasites": "#BC6C25",
    "Lichen parasites": "#8D99AE",
    "Lichenized fungi": "#CDB4DB",
    "Nematophagous fungi": "#FFAFCC",
    "Algal parasites": "#D4E09B",
    "Insect pathogens": "#F08080",
}

# ---------------------------------------------------------------------------
# Bacterial ecological guild plots from FAPROTAX output
# ---------------------------------------------------------------------------

BACTERIAL_SOIL_CORE = [
    # Nitrogen
    "nitrogen_fixation",
    "nitrification",
    "aerobic_ammonia_oxidation",
    "nitrate_reduction",
    "nitrate_respiration",
    "nitrite_respiration",
    "nitrogen_respiration",
    "ureolysis",
    # Sulfur
    "sulfate_respiration",
    "sulfur_respiration",
    "sulfite_respiration",
    "respiration_of_sulfur_compounds",
    "dark_sulfide_oxidation",
    "dark_oxidation_of_sulfur_compounds",
    # Methane / C1
    "methanotrophy",
    "methanol_oxidation",
    "methylotrophy",
    "methanogenesis",
    "hydrogenotrophic_methanogenesis",
    "methanogenesis_by_reduction_of_methyl_compounds_with_H2",
    # Carbon degradation
    "cellulolysis",
    "xylanolysis",
    "aromatic_compound_degradation",
    "aromatic_hydrocarbon_degradation",
    "hydrocarbon_degradation",
    "aliphatic_non_methane_hydrocarbon_degradation",
    # Heterotrophy
    "aerobic_chemoheterotrophy",
    "anaerobic_chemoheterotrophy",
    "fermentation",
    # Mineral cycling
    "iron_respiration",
    "dark_iron_oxidation",
    "manganese_oxidation",
    # Pathogens / parasites / predation
    "plant_pathogen",
    "animal_parasite_or_symbiont",
    "predatory_or_exoparasitic",
    "chitinolysis",
    "nitrous_oxide_denitrification",
    "ligninolysis",
    "dark_hydrogen_oxidation",
    "phototrophy",
    "photoautotrophy",
    "cyanobacteria",
]

BACTERIAL_MACRO_MAP = {
    "chitinolysis": "Chitinolytic bacteria",
    "nitrogen_fixation": "Nitrogen fixers",
    "nitrification": "Nitrifiers",
    "aerobic_ammonia_oxidation": "Nitrifiers",
    "nitrate_reduction": "Denitrifiers",
    "nitrate_respiration": "Denitrifiers",
    "nitrite_respiration": "Denitrifiers",
    "nitrogen_respiration": "Denitrifiers",
    "nitrous_oxide_denitrification": "Denitrifiers",
    "ureolysis": "Ureolytic bacteria",
    "aromatic_compound_degradation": "Hydrocarbon degraders",
    "aromatic_hydrocarbon_degradation": "Hydrocarbon degraders",
    "hydrocarbon_degradation": "Hydrocarbon degraders",
    "aliphatic_non_methane_hydrocarbon_degradation": "Hydrocarbon degraders",
    "methanotrophy": "Methanotrophs",
    "methanol_oxidation": "Methanotrophs",
    "methylotrophy": "Methanotrophs",
    "methanogenesis": "Methanogens",
    "hydrogenotrophic_methanogenesis": "Methanogens",
    "methanogenesis_by_reduction_of_methyl_compounds_with_H2": "Methanogens",
    "dark_sulfide_oxidation": "Sulfur oxidizers",
    "dark_oxidation_of_sulfur_compounds": "Sulfur oxidizers",
    "sulfate_respiration": "Sulfate reducers",
    "sulfur_respiration": "Sulfate reducers",
    "sulfite_respiration": "Sulfate reducers",
    "respiration_of_sulfur_compounds": "Sulfate reducers",
    "iron_respiration": "Iron & Manganese cyclers",
    "dark_iron_oxidation": "Iron & Manganese cyclers",
    "manganese_oxidation": "Iron & Manganese cyclers",
    "fermentation": "Anaerobic heterotrophs",
    "aerobic_chemoheterotrophy": "Aerobic heterotrophs",
    "anaerobic_chemoheterotrophy": "Anaerobic heterotrophs",
    "plant_pathogen": "Plant pathogens",
    "animal_parasite_or_symbiont": "Animal parasites",
    "predatory_or_exoparasitic": "Predatory bacteria",
    "ligninolysis": "Lignocellulose degraders",
    "cellulolysis": "Lignocellulose degraders",
    "xylanolysis": "Lignocellulose degraders",
    "dark_hydrogen_oxidation": "Hydrogen oxidizers",
    "phototrophy": "Phototrophs",
    "photoautotrophy": "Phototrophs",
    "cyanobacteria": "Phototrophs",
}

BACTERIAL_GUILD_ORDER = [
    "Aerobic heterotrophs",
    "Anaerobic heterotrophs",
    "Nitrogen fixers",
    "Nitrifiers",
    "Denitrifiers",
    "Ureolytic bacteria",
    "Hydrocarbon degraders",
    "Methanotrophs",
    "Methanogens",
    "Sulfur oxidizers",
    "Sulfate reducers",
    "Iron & Manganese cyclers",
    "Plant pathogens",
    "Animal parasites",
    "Predatory bacteria",
    "Chitinolytic bacteria",
    "Lignocellulose degraders",
    "Hydrogen oxidizers",
    "Phototrophs",
]

BACTERIAL_GUILD_COLORS = {
    "Nitrogen fixers": "#264653",
    "Nitrifiers": "#2A9D8F",
    "Denitrifiers": "#457B9D",
    "Ureolytic bacteria": "#A8DADC",
    "Hydrocarbon degraders": "#C9A96E",
    "Methanotrophs": "#6A994E",
    "Methanogens": "#386641",
    "Sulfur oxidizers": "#FBF259",
    "Sulfate reducers": "#E9C46A",
    "Iron & Manganese cyclers": "#8D99AE",
    "Aerobic heterotrophs": "#E76F51",
    "Anaerobic heterotrophs": "#F4A261",
    "Plant pathogens": "#D62828",
    "Animal parasites": "#F08080",
    "Predatory bacteria": "#BC6C25",
    "Chitinolytic bacteria": "#CDB4DB",
    "Lignocellulose degraders": "#8B5E3C",
    "Hydrogen oxidizers": "#577590",
    "Phototrophs": "#D4E09B",
}


def _taxa_to_dict(taxa) -> dict:
    """
    sample_otu_counts.taxa may arrive as dict, JSON string, or None.
    """
    if isinstance(taxa, dict):
        return taxa
    if taxa is None:
        return {}
    if isinstance(taxa, str):
        s = taxa.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _strip_tax_prefix(value: str) -> str:
    """
    Convert g__Fusarium -> Fusarium, f__Nectriaceae -> Nectriaceae.
    Also replaces underscores with spaces.
    """
    if value is None:
        return ""
    s = str(value).strip()
    s = re.sub(r"^[a-zA-Z]__", "", s)
    s = s.replace("_", " ").strip()
    if not s:
        return ""
    if re.search(r"incertae|unclassified|uncultured", s, flags=re.I):
        return ""
    return s


def extract_fungal_genus(row: pd.Series) -> str:
    """
    Extract genus for FUNGuild matching.

    Supports both:
      - raw taxonomy string: k__;p__;c__;o__;f__;g__;s__
      - split fields: A/B/C/D/E/F
      - named rank fields: Genus
    """
    taxa = taxa_to_normalized_dict(row.get("taxa"))
    return taxa.get("Genus", "").strip()


def extract_primary_guild(guild_name: str) -> str:
    """
    FUNGuild guild names may contain a primary guild between pipes:
      Something-|Plant Pathogen|-Something
    If there are no pipes, fall back to the raw value or parts split by '-'.
    """
    if guild_name is None:
        return ""

    s = str(guild_name).strip()
    if not s or s.upper() == "NULL":
        return ""

    m = re.search(r"\|([^|]+)\|", s)
    if m:
        return m.group(1).strip()

    # fallback: try direct match first
    if s in FUNGAL_GUILD_MACRO_MAP:
        return s

    # fallback: split compound guilds
    for part in re.split(r"\s*-\s*", s):
        part = part.strip().replace("|", "")
        if part in FUNGAL_GUILD_MACRO_MAP:
            return part

    return s.replace("|", "").strip()


def load_funguild_best_by_genus() -> dict[str, dict]:
    """
    Load local FUNGuild_db.json and keep one best assignment per genus.

    Returns:
      {
        "Fusarium": {
          "guild": "...",
          "primary_guild": "...",
          "macro": "Plant pathogens",
          ...
        }
      }
    """
    path = Path(FUNGUILD_DB_JSON)
    if not path.exists():
        raise FileNotFoundError(
            f"FUNGuild DB JSON not found: {path}. Set FUNGUILD_DB_JSON or place the file there."
        )

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict):
        # Some JSON exports are dict-like. Try values.
        records = list(raw.values())
    elif isinstance(raw, list):
        records = raw
    else:
        raise ValueError(f"Unsupported FUNGuild JSON structure in {path}")

    best: dict[str, dict] = {}

    for rec in records:
        if not isinstance(rec, dict):
            continue

        taxon = str(rec.get("taxon") or rec.get("queried_taxon") or "").strip()
        if not taxon:
            continue

        confidence = str(rec.get("confidenceRanking") or "").strip()
        if confidence not in FUNGUILD_KEEP_CONFIDENCE:
            continue

        trophic = str(rec.get("trophicMode") or "").strip()
        guild = str(rec.get("guild") or "").strip()

        if not trophic or trophic.upper() == "NULL":
            continue
        if not guild or guild.upper() == "NULL":
            continue

        primary = extract_primary_guild(guild)
        macro = FUNGAL_GUILD_MACRO_MAP.get(primary)

        # Only keep guilds that map to citizen-friendly categories.
        if not macro:
            continue

        score = FUNGUILD_CONF_SCORE.get(confidence, 0)

        prev = best.get(taxon)
        if prev is None or score > prev["score"]:
            best[taxon] = {
                "taxon": taxon,
                "confidence": confidence,
                "score": score,
                "trophicMode": trophic,
                "guild": guild,
                "primary_guild": primary,
                "macro": macro,
            }

    print(f"[INFO] Loaded FUNGuild best assignments for {len(best)} genera")
    return best


def make_bacterial_guildplot_for_sample(
    sample_id: str,
    func_row: pd.Series,
    out_path: Path,
) -> bool:
    """
    Create one citizen-friendly bacterial ecological guild plot from one
    FAPROTAX sample row.

    Input values are expected to be FAPROTAX fractions, as in the R script.
    If values look like percentages already, the function handles that too.
    """
    values = {}

    for func_name, raw_val in func_row.items():
        if func_name not in BACTERIAL_SOIL_CORE:
            continue

        guild = BACTERIAL_MACRO_MAP.get(func_name)
        if not guild:
            continue

        try:
            v = float(raw_val)
        except Exception:
            continue

        if not math.isfinite(v) or v <= 0:
            continue

        values[guild] = values.get(guild, 0.0) + v

    if not values:
        return False

    df = pd.DataFrame([{"guild": k, "value": v} for k, v in values.items()])

    # R script does Percent = 100 * sum(Value).
    # But if the CSV already contains percentages, avoid multiplying again.
    max_v = df["value"].max()
    if max_v <= 1.5:
        df["percent"] = df["value"] * 100.0
    else:
        df["percent"] = df["value"]

    df = df[df["percent"] >= 1.0].copy()
    if df.empty:
        return False

    order_index = {name: i for i, name in enumerate(BACTERIAL_GUILD_ORDER)}
    df["order"] = df["guild"].map(lambda x: order_index.get(x, 999))
    df = df.sort_values(["order", "percent"], ascending=[True, False])

    # barh draws bottom-to-top, so reverse for top-to-bottom display.
    df = df.iloc[::-1].copy()

    labels = df["guild"].tolist()
    values_pct = df["percent"].tolist()
    colors = [BACTERIAL_GUILD_COLORS.get(label, "#999999") for label in labels]

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig_height = max(6.5, 0.55 * len(df) + 2.2)
    fig, ax = plt.subplots(figsize=(10, fig_height))

    # White background + black text style
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    y_pos = list(range(len(labels)))

    ax.barh(
        y_pos,
        values_pct,
        color=colors,
        height=0.75,
        edgecolor="none",
    )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=11, color="#333333")

    x_max = max(max(values_pct) * 1.12, 20)
    ax.set_xlim(0, x_max)

    if x_max <= 20:
        ticks = [0, 5, 10, 15, 20]
    else:
        step = 5
        ticks = list(range(0, int(x_max + step), step))

    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{t}%" for t in ticks], fontsize=10, color="#333333")

    ax.set_xlabel(
        "% of bacterial community",
        fontsize=12,
        color="#111111",
        labelpad=8,
    )

    ax.xaxis.grid(True, color="#dddddd", linewidth=1)
    ax.yaxis.grid(False)
    ax.set_axisbelow(True)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.tick_params(axis="both", length=0)

    for y, pct in zip(y_pos, values_pct, strict=False):
        ax.text(
            pct + x_max * 0.006,
            y,
            f"{pct:.1f}%",
            va="center",
            ha="left",
            fontsize=11,
            color="#222222",
        )

    ax.set_title(
        "Soil bacterial ecological guilds\n",
        loc="left",
        fontsize=15,
        fontweight="bold",
        color="#111111",
        pad=8,
    )

    ax.text(
        0,
        1.02,
        "Guild-level functional categories",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=11,
        color="#555555",
    )

    fig.suptitle(
        f"Your soil bacteria at a glance — Sample: {sample_id}",
        x=0.02,
        y=0.98,
        ha="left",
        va="top",
        fontsize=16,
        fontweight="bold",
        color="#111111",
    )

    caption = (
        "Values indicate the estimated percentage of the bacterial community associated with each ecological guild.\n"
        "Only guilds exceeding 1% are shown; absent categories may reflect low detection rather than true absence.\n"
        "Guild assignments are based on FAPROTAX (Louca et al. 2016)."
    )

    fig.text(
        0.02,
        0.025,
        caption,
        ha="left",
        va="bottom",
        fontsize=8.5,
        color="#666666",
    )

    fig.tight_layout(rect=[0.02, 0.09, 0.98, 0.92])
    fig.savefig(out_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

    return True


def make_fungal_guildplot_for_sample(
    sample_df: pd.DataFrame,
    sample_id: str,
    funguild_by_genus: dict[str, dict],
    out_path: Path,
) -> bool:
    """
    Create one citizen-friendly fungal guild horizontal bar plot.

    Percentages are relative to the full fungal community for the sample,
    matching the R script interpretation.

    Output style mirrors the R/ggplot example:
      - horizontal bars
      - fixed macro-category colours
      - title + subtitle
      - percentage labels at bar ends
      - explanatory caption
    """
    plot_df = sample_df.copy()
    plot_df["count"] = pd.to_numeric(plot_df["count"], errors="coerce").fillna(0)
    plot_df = plot_df[plot_df["count"] > 0].copy()

    if plot_df.empty:
        return False

    total = plot_df["count"].sum()
    if total <= 0:
        return False

    # Extract genus and map to citizen-friendly guild macro-category
    plot_df["genus"] = plot_df.apply(extract_fungal_genus, axis=1)
    plot_df["macro"] = plot_df["genus"].map(lambda g: funguild_by_genus.get(g, {}).get("macro", ""))

    annotated = plot_df[plot_df["macro"].astype(str).str.strip() != ""].copy()
    if annotated.empty:
        return False

    grouped = annotated.groupby("macro", dropna=False)["count"].sum().reset_index()
    grouped["percent"] = grouped["count"] / total * 100.0

    # Same communication threshold as the R script
    grouped = grouped[grouped["percent"] >= 1.0].copy()
    if grouped.empty:
        return False

    # Keep the same category order as the R script.
    order_index = {name: i for i, name in enumerate(FUNGAL_GUILD_ORDER)}
    grouped["order"] = grouped["macro"].map(lambda x: order_index.get(x, 999))
    grouped = grouped.sort_values(["order", "percent"], ascending=[True, False])

    # Matplotlib barh draws bottom-to-top, so reverse for top-to-bottom display.
    grouped = grouped.iloc[::-1].copy()

    labels = grouped["macro"].tolist()
    values = grouped["percent"].tolist()
    colors = [FUNGAL_GUILD_COLORS.get(label, "#999999") for label in labels]

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Similar aspect to the R output: wide, communication-oriented.
    fig_height = max(5.5, 0.55 * len(grouped) + 2.2)
    fig, ax = plt.subplots(figsize=(10, fig_height))

    # Warm, clean background like ggplot/theme_minimal
    fig.patch.set_facecolor("#f7f7f5")
    ax.set_facecolor("#f7f7f5")

    y_pos = list(range(len(labels)))

    ax.barh(
        y_pos,
        values,
        color=colors,
        height=0.75,
        edgecolor="none",
    )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=11, color="#4d4d4d")

    # Axis max: at least 20%, otherwise 12% extra headroom
    x_max = max(max(values) * 1.12, 20)
    ax.set_xlim(0, x_max)

    # Use 0/5/10/15/20 style ticks where possible
    if x_max <= 20:
        ticks = [0, 5, 10, 15, 20]
    else:
        step = 5
        ticks = list(range(0, int(x_max + step), step))

    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{t}%" for t in ticks], fontsize=10, color="#555555")

    ax.set_xlabel(
        "% of fungal community",
        fontsize=12,
        color="#111111",
        labelpad=8,
    )

    # Subtle vertical gridlines
    ax.xaxis.grid(True, color=PIE_GRID, linewidth=1)
    ax.yaxis.grid(False)
    ax.set_axisbelow(True)

    # Remove plot frame for ggplot-like look
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.tick_params(axis="both", length=0)

    # Percentage labels at bar ends
    for y, pct in zip(y_pos, values, strict=False):
        ax.text(
            pct + x_max * 0.006,
            y,
            f"{pct:.1f}%",
            va="center",
            ha="left",
            fontsize=11,
            color="#333333",
        )

    # Main chart title and subtitle
    ax.set_title(
        "Fungal ecological guilds\n",
        loc="left",
        fontsize=15,
        fontweight="bold",
        color="#111111",
        pad=8,
    )

    ax.text(
        0,
        1.02,
        "Guild macro-categories",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=11,
        color="#666666",
    )

    # Figure-level top title, like the R patchwork annotation
    fig.suptitle(
        f"Your soil fungi at a glance — Sample: {sample_id}",
        x=0.02,
        y=0.98,
        ha="left",
        va="top",
        fontsize=16,
        fontweight="bold",
        color="#111111",
    )

    # Footer caption
    caption = (
        "Values indicate the estimated percentage of the fungal community associated with each ecological guild.\n"
        "Only guilds exceeding 1% are shown; absent categories may reflect low detection rather than true absence."
    )
    fig.text(
        0.02,
        0.025,
        caption,
        ha="left",
        va="bottom",
        fontsize=8.5,
        color="#777777",
    )

    # Leave room for suptitle and footer
    fig.tight_layout(rect=[0.02, 0.08, 0.98, 0.92])

    fig.savefig(out_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return True


def generate_bacterial_guildplots_from_faprotax(mclient) -> tuple[int, int]:
    """
    Generate bacterial ecological guild plots from a FAPROTAX sample x function CSV.

    Expected input:
      rows    = sample IDs
      columns = FAPROTAX function names
      values  = fractions or percentages

    Uploads to:
      biodiversity/guildplots/bacteria/<sample_id>.png
    """
    path = Path(FAPROTAX_FUNCTION_CSV)
    if not path.exists():
        raise FileNotFoundError(
            f"FAPROTAX function CSV not found: {path}. "
            "Set FAPROTAX_FUNCTION_CSV or place the file there."
        )

    print(f"[INFO] Loading FAPROTAX functions from {path}")

    func_sxf = pd.read_csv(path, index_col=0)

    if func_sxf.empty:
        print("[INFO] FAPROTAX function matrix is empty; skipping bacterial guild plots.")
        return 0, 0

    print(f"[INFO] FAPROTAX matrix: {func_sxf.shape[0]} samples x {func_sxf.shape[1]} functions")

    available = set(func_sxf.columns)
    selected = [c for c in BACTERIAL_SOIL_CORE if c in available]

    print(f"[INFO] Relevant FAPROTAX functions present: {len(selected)}")
    if selected:
        print("[INFO] First relevant functions:", ", ".join(selected[:20]))

    if not selected:
        print("[WARN] No expected FAPROTAX soil functions found in the CSV.")
        return 0, 0

    out_dir = PROJECT_ROOT / "data" / "bacterial_guildplots"
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    uploaded = 0

    for sample_id, row in func_sxf.iterrows():
        sample_id = str(sample_id).strip()
        if not sample_id:
            continue

        safe_id = sanitize_filename(sample_id)
        local_png = out_dir / f"{safe_id}.png"

        ok = make_bacterial_guildplot_for_sample(
            sample_id=sample_id,
            func_row=row[selected],
            out_path=local_png,
        )
        if not ok:
            continue

        generated += 1

        object_name = f"biodiversity/guildplots/bacteria/{safe_id}.png"
        uploaded_url = upload_file_to_minio(
            mclient,
            local_png,
            object_name,
            content_type="image/png",
        )
        if uploaded_url:
            uploaded += 1

    print(f"[OK] Generated {generated} bacterial guild plots")
    print(f"[OK] Uploaded {uploaded} bacterial guild plots to MinIO")
    return generated, uploaded


def generate_fungal_guildplots(mclient) -> tuple[int, int]:
    """
    Generate fungal ecological guild plots from marker ITS.
    Uploads to:
      biodiversity/guildplots/fungi/<sample_id>.png
    """
    marker = "ITS"
    print("[INFO] Generating fungal ecological guild plots from ITS data")

    df = fetch_otu_data(marker=marker)
    if df.empty:
        print("[INFO] No ITS OTU rows found; skipping fungal guild plots.")
        return 0, 0

    funguild_by_genus = load_funguild_best_by_genus()

    all_genera = df.apply(extract_fungal_genus, axis=1)
    nonempty = all_genera[all_genera.astype(str).str.strip() != ""]
    matched = nonempty.map(lambda g: g in funguild_by_genus)

    print(f"[DEBUG] Extracted non-empty genera: {len(nonempty)}")
    print(f"[DEBUG] Unique extracted genera: {nonempty.nunique()}")
    print(f"[DEBUG] FUNGuild genus matches: {matched.sum()} / {len(nonempty)}")

    out_dir = PROJECT_ROOT / "data" / "biodiversity_guildplots" / "fungi"
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    uploaded = 0

    for sample_id, sample_df in df.groupby("sample_id"):
        sample_id = str(sample_id).strip()
        if not sample_id:
            continue

        safe_id = sanitize_filename(sample_id)
        local_png = out_dir / f"{safe_id}.png"

        ok = make_fungal_guildplot_for_sample(
            sample_df=sample_df,
            sample_id=sample_id,
            funguild_by_genus=funguild_by_genus,
            out_path=local_png,
        )
        if not ok:
            continue

        generated += 1

        object_name = f"biodiversity/guildplots/fungi/{safe_id}.png"
        uploaded_url = upload_file_to_minio(
            mclient,
            local_png,
            object_name,
            content_type="image/png",
        )
        if uploaded_url:
            uploaded += 1

    print(f"[OK] Generated {generated} fungal guild plots")
    print(f"[OK] Uploaded {uploaded} fungal guild plots to MinIO")
    return generated, uploaded


def main():
    marker = os.getenv("BIODIV_MARKER", "16S")
    level = os.getenv("BIODIV_LEVEL", "Family")
    out_dir = PROJECT_ROOT / "data" / "biodiversity_piecharts" / marker / level
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] marker={marker} level={level}")
    df = fetch_otu_data(marker=marker)

    mclient = init_minio()

    if BUILD_FAPROTAX_INPUTS:
        build_clean_otu_and_taxonomy_files(
            marker="16S",
            out_dir=Path("/tmp/faprotax_work"),
            min_prev=int(os.getenv("FAPROTAX_MIN_PREV", "2")),
            min_total=int(os.getenv("FAPROTAX_MIN_TOTAL", "50")),
        )

    generated = 0
    uploaded = 0

    if df.empty:
        print("[INFO] No OTU rows found for taxonomic piecharts.")
    else:
        for sample_id, sample_df in df.groupby("sample_id"):
            sample_id = str(sample_id).strip()
            if not sample_id:
                continue

            local_png = out_dir / f"{sanitize_filename(sample_id)}.png"
            ok = make_piechart_for_sample(sample_df, sample_id, marker, level, local_png)
            if not ok:
                continue

            generated += 1

            object_name = (
                f"biodiversity/piecharts/{marker}/{level}/{sanitize_filename(sample_id)}.png"
            )
            uploaded_url = upload_file_to_minio(
                mclient,
                local_png,
                object_name,
                content_type="image/png",
            )
            if uploaded_url:
                uploaded += 1

    print(f"[OK] Generated {generated} taxonomic charts")
    print(f"[OK] Uploaded {uploaded} taxonomic charts to MinIO")

    if GENERATE_FUNGAL_GUILDS:
        generate_fungal_guildplots(mclient)

    if GENERATE_BACTERIAL_GUILDS:
        generate_bacterial_guildplots_from_faprotax(mclient)


if __name__ == "__main__":
    main()
