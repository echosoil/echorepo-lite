# RO4-1 — observed publication inventory and proposed eligibility

Operator-generated inventory 2026-11-25T13:44:25.763372+00:00; reporting revision `af35342907a87d8957990594c8461128236ab63e`. Retain original inventory.json, inventory.md, versions.csv and dataset-review.csv. All four searches and four seeds OBSERVED. Unique union: 61 records (23 publications, 17 dataset releases, 14 videos, six software, one presentation). Five dataset concepts, zero unresolved groups; all five representative records declare open access and CC BY 4.0. Not an exhaustive project output census or an acceptance result.

| Concept DOI suffix | Assessed version suffix | Releases | Proposed scope | Prior harvesting evidence |
|---|---|---:|---|---|
| 19722513 | 23784504 | 12 | Core canonical dataset | SoilWise discovery supported by R7-3 Phase 4 capture |
| 22014976 | 22014977 | 1 | Microbial example; review coverage and scientific scope | NOT_ASSESSED |
| 23987337 | 24003680 | 2 | Paired field/laboratory validation dataset, strong candidate | NOT_ASSESSED |
| 10218824 | 10218825 | 1 | Inventory of initiatives, outside strict measurement scope | NOT_ASSESSED |
| 10598039 | 10598040 | 1 | Stakeholder mapping, outside strict measurement scope; no explicit ECHO grant match in API | NOT_ASSESSED |

DOI prefix 10.5281/zenodo. for every entry. The three soil data concepts have 15 releases collectively. One has existing record-specific SoilWise evidence; do not claim three qualifying harvested datasets. Neither unresolved external search nor inaccessible pages prove absence of harvesting. Direct EUSO discovery unverified. No independent acceptance or contractual count confirmed. Earlier planning proposed ten DOI datasets as a working proxy; seek consortium agreement on scope and counting before generating further deposits.

## Next: exact published coverage

Run from an updated development checkout using existing requests environment:

    cd ~/ECHO-STORE/echorepo-lite-dev
    git switch develop
    git pull --ff-only origin develop
    OUT="$PWD/ro41-evidence/phase2-$(date -u +%Y%m%dT%H%M%SZ)"
    python -m tools.ro41_coverage --ack yes-public-readonly \
      --commit "$(git rev-parse HEAD)" --out "$OUT/coverage.json"
    cat "$OUT/coverage.md"

The tool retrieves exact public versions (canonical samples CSV, biodiversity archive, validation CSV and README), verifies repository sizes/checksums, records DOI resolution separately, aggregates declared country codes, matrix marker header labels and paired validation observations. No live export, credentials, raw data storage or production deployment. Marker label counts are not independently verified physical sample counts; country codes are not independent geographic validation. The paired validation check matches explicitly expected header names only, records missing fields, and does not assess scientific accuracy. Reuse R7-3 Phase 4 file/CSVW checks for pinned canonical and biodiversity versions. Retain coverage.md/json.

Browser checks: search SoilWise for exact DOIs 10.5281/zenodo.22014977 and 10.5281/zenodo.24003680, record matching title, DOI and source link if present; otherwise report not found or not assessed with date. EDP and OpenAIRE evidence separate. No need to repeat already documented canonical discovery. Check with data owners whether full microbial publication and other standalone research datasets are planned; no arbitrary splitting or duplicate deposits merely to reach a count.
