# Guild Zenodo releases — staged workflow

The existing raw biodiversity and canonical publishers remain unchanged. This Linux-host tool reuses their Zenodo API primitives and token configuration, but keeps separate state for FAPROTAX and FUNGuild and for sandbox and production.

## 1. Offline plan (no credentials or network)

From the development checkout with the usual Python environment (requests installed):

```bash
BUNDLE="/absolute/path/to/guild-publication-directory"
RUN="$PWD/data/biodiversity/guild-release-$(date -u +%Y%m%dT%H%M%SZ)"
for KIND in faprotax funguild; do
  python -m tools.publish_guilds_to_zenodo plan --bundle "$BUNDLE" \
    --dataset "$KIND" --target sandbox --out-dir "$RUN/$KIND"
done
```

Each plan contains plan.json, review.local.json and files/. Send only plan.json for review. Initial plans are intentionally blocked. The template proposes ECHO Consortium, the ECHO grant and descriptive titles; confirm creators, licence, version and related identifiers. Do not upload review.local.json. Explicit review evidence must cover public sample IDs, reference version/citation and terms, source lineage, and scientific/release metadata. Record unknowns honestly; approval is an operator decision, not an automatic scientific certification.

## 2. Reviewed plan

Copy review.local.json outside the old plan, edit metadata and review evidence, add public provenance text, then repeat plan with --review /path/to/review.json and a new output directory. The plan's digest binds files, metadata, review and target. Read the public files before proceeding. The provenance preserves known unknowns from analysis; reviewed public notes supplement them. A production plan must be built separately with --target production. Never use sandbox approval for production.

## 3. Upload a draft, then publish separately

Use a persistent state directory (back it up, never share it across unrelated deployments). Existing .env_zenodo_biodiversity or equivalent token file is supported; sandbox uses ACCESS_TOKEN_SANDBOX, production ACCESS_TOKEN. Do not place secrets in plans.

```bash
PLAN="/absolute/path/to/reviewed-plan"
HASH=$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["plan_sha256"])' "$PLAN/plan.json")
STATE="$HOME/ECHO-STORE/guild-zenodo-state"
ENV="$HOME/ECHO-STORE/echorepo-lite/.env_zenodo_biodiversity"
python -m tools.publish_guilds_to_zenodo stage --plan-dir "$PLAN" \
  --approve-plan "$HASH" --state-dir "$STATE" --env-file "$ENV"
```

The stage action creates or versions only records tracked by the corresponding state file, updates metadata and uploads exact files. It verifies remote filename, size and MD5 and core metadata. Inspect all metadata and the draft in Zenodo before explicit publication:

```bash
python -m tools.publish_guilds_to_zenodo publish --plan-dir "$PLAN" \
  --approve-plan "$HASH" --state-dir "$STATE" --env-file "$ENV"
```

The publish action rechecks files and core metadata and records DOI, concept DOI and history on confirmed publication. No automatic adoption of existing deposition IDs. Identical public bytes and metadata skip a new version; regenerated provenance timestamps can change bytes. Concurrent actions for one dataset/target are locked. State is written before remote creation/publish; interruption during creation or upload intentionally blocks automatic retries. Inspect the state and remote record, reconcile manually rather than deleting state or risking duplicate depositions. A publish interruption can be checked by rerunning publish with the same plan, if state is publishing. Sandbox and production state are independent. No scheduled publication is enabled. Do not run against production before sandbox validation and project approval.

This is not a substitute for verifying authoritative reference terms, public identifier policy, exact lineage, or SoilWise discovery.

## Funding and draft corrections

Both ECHO awards are required in review metadata: 10.13039/501100000780::101112869 and 10.13039/100014013::10068004. Update older review JSON files accordingly. Use percentage-schema v2 candidates. If a tracked draft is still staged and unpublished, use restage with the new plan and --replace-plan OLD_PLAN_SHA256, the same state directory, and the new --approve-plan hash. It replaces files in place, verifies both grants, and never publishes. If already published, use stage for the next version (new metadata version). Do not delete state or manually edit hashes. An interrupted restage requires reconciliation.
