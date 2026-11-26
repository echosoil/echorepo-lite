# Derived guild publication candidates (local only)

Prepare both datasets from the already generated outputs. No PostgreSQL, MinIO, R, network, or Zenodo access. Run from repository root with Python 3.10+.

```bash
DATA="$HOME/ECHO-STORE/echorepo-lite/data/biodiversity"
AUDIT="/absolute/path/to/guild-audit.json"
FUN="/absolute/path/to/funguild-export-directory"
OUT="$PWD/data/biodiversity/guild-publication-$(date -u +%Y%m%dT%H%M%SZ)"
python -m tools.prepare_guild_publication \
  --faprotax-dir "$DATA" --audit "$AUDIT" --funguild-dir "$FUN" --out-dir "$OUT"
cat "$OUT/preparation-report.json"
```

Use the complete FUNGuild export, not a pilot sample. Fingerprints must match the Phase A audit and full ITS manifest. A changed input requires a fresh reviewed audit/export, not manually editing checksums. Outputs go into independent faprotax/ and funguild/ directories containing numerical CSVs, README, CSVW file.json and provenance.json. The FAPROTAX format is dense long form with original rounded fractions and zero values. FUNGuild retains sparse positive rows and separate coverage; missing observations remain null. Guild definitions use primary_guild as key because several primary categories can map to one macro category. Function definitions list original identifiers only; no fabricated semantic definitions. The source inventory with internal upload associations is not copied into the candidates. Send only preparation-report.json for aggregate review.

The candidates are private local files, not approved publications. Reference versions/licences, public identifiers, source lineage and release metadata remain unresolved. CSVW is explicit local schema metadata, not evidence of SoilWise ingestion. Existing Zenodo deposits remain unchanged. A subsequent stage will reuse existing Zenodo deposition primitives with distinct state for each dataset and explicit release approval.

## Percentage schema v2

Public CSVs use otu_perc, annotated_read_perc, guild_perc and assigned_perc (0–100). The internal R and ITS inputs remain unchanged. Re-run local preparation from the same audited inputs; no scientific recomputation is necessary. Missing percentages stay empty and zero stays zero. CSVW and checksums are regenerated.
