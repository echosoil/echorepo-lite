# FUNGuild numerical export — local preparation

Run from the checkout with the same database/MinIO configuration and Python environment as the existing image generator. Use a fresh output directory. No image generation, database writes, MinIO writes or Zenodo requests are performed. Do not run during biodiversity imports or reference replacement. The live reconstruction has the same memory and temporary disk requirements as the image generator; it is not a frozen snapshot.

```bash
cd ~/ECHO-STORE/echorepo-lite-dev
git switch develop
git pull --ff-only origin develop
OUT="$PWD/data/biodiversity/funguild-export-$(date -u +%Y%m%dT%H%M%SZ)"
(umask 077; python -m tools.export_funguild_profiles --out-dir "$OUT" \
  --reference "$HOME/ECHO-STORE/echorepo-lite/data/biodiversity/FUNGuild_db.json")
cat "$OUT/provenance.local.json"
```

Activate the usual biodiversity Python virtual environment before the command. Optional repeated/comma-separated --sample-id filters allow a pilot. The production reference path above is an example; use the actual host-side path.

- guild_profiles.csv: sparse positive mapped macro-guild counts and fractions (0..1) of all positive reconstructed ITS reads. No 1% visualization cutoff. Absence of a row means zero for a particular mapped guild only if coverage status is observed.
- annotation_coverage.csv: one row per known current or legacy sample (plus explicitly requested missing IDs). Includes total, assigned, unassigned and assigned fraction. Missing source is not zero coverage; not_reconstructed can also mean no positive reads in the source, because the existing reconstruction does not distinguish the cases. zero_reconstructed_reads is only used when actual zero rows were available.
- guild_definitions.csv: ECHOREPO primary-to-macro mapping, not a redistribution of the FUNGuild reference database.
- provenance.local.json: aggregate counts, SHA-256 checksums, code commit, reference fingerprint and limitations. Exit code 2 means no observed samples; inspect the bundle.
- source_inventory.local.json: sensitive internal source associations and sample identifiers. Keep local; do not publish this file or assume labels are approved for public release.

The source reader preserves current-upload precedence over legacy rows. Annotation uses the existing genus matcher, confidence filters, primary-guild mapping and tie rules. Both the plots and numerical export call the same calculation; plotting alone hides fractions below 0.01. This is a predicted ecological guild profile, not directly measured activity.

Before Zenodo integration, verify source snapshot identity and reference version/terms, review identifiers and incomplete reconstruction, and add publication metadata, CSVW and a reviewed staged publisher. Existing deposits are untouched. FAPROTAX numerical outputs are separate; no R rerun is needed for this phase.
