# Releasing ECHOREPO Lite

This procedure publishes a **GitHub source-code release**, not a new Zenodo **dataset** version and not a deployment to ECHOREPO production.

## Release identity

The previous published GitHub release is `v0.2.0` (1 July 2026). The proposed next version is `v0.3.0`. Source release notes are in [RELEASE_NOTES_v0.3.0.md](../RELEASE_NOTES_v0.3.0.md).

The repository currently has no automated GitHub Actions release job. GitHub releases must therefore be created manually through the website or GitHub CLI.

**Important:** `scripts/make_release.sh` is a *different operation*: it merges the remote `develop` branch into the local production checkout, modifies production environment configuration, and automatically creates and pushes a date-based Git tag. Do **not** invoke that script simply to create a public GitHub v0.3.0 release.

## Step 1 — Select and review the commit

Work in a normal development checkout, **not** the running production checkout:

~~~bash
git fetch origin --tags
git switch main
git pull --ff-only origin main
git status --short
git log -5 --oneline
~~~

Confirm the intended changes, committed documentation, licensing, and tests are present. Ensure the working tree is clean. Review the exact differences since the previous release:

~~~bash
git log --oneline v0.2.0..main
git diff --stat v0.2.0..main
~~~

Do not tag local changes that have not been pushed to `main`.

## Step 2 — Verify behaviour

At minimum check a development/test deployment, migration compatibility, access controls, the core map/search workflow, canonical CSV exports, and publication dry run.

~~~bash
python3 -m compileall -q echorepo tools scripts
./scripts/update_zenodo_bundle.sh --dry-run
~~~

The dry-run still downloads and validates live data, so it requires a correctly configured private `.env_zenodo` and API access. It should not publish a new Zenodo version.

Validate a downloaded export when possible:

~~~bash
python3 tools/validate_echorepo_bundle.py canonical_all.zip
~~~

Read its `--help` if your validator version expects different arguments. Avoid exposing API keys in shell history and logs. Make a backup before applying database migrations to production. Ensure no secrets or unpublished datasets are present in tracked files.

## Step 3 — Tag the tested commit

After merging the release-preparation documentation and selecting the reviewed `main` head:

~~~bash
git fetch origin --tags
git switch main
git pull --ff-only origin main
git status --short
git tag -l v0.3.0
git tag -a v0.3.0 -m "ECHOREPO Lite v0.3.0 — FAIR data workflows"
git push origin v0.3.0
~~~

Only execute the tag command if `v0.3.0` does not already exist. Tags are immutable release references in practice; do not retarget an already published tag.

## Step 4 — Publish the GitHub Release

Open https://github.com/echosoil/echorepo-lite/releases/new

- Choose existing tag `v0.3.0`.
- Title: **ECHOREPO Lite v0.3.0 — FAIR data publication and research data workflows**.
- Paste the reviewed contents of `RELEASE_NOTES_v0.3.0.md` into the release description.
- Select **Set as the latest release** if appropriate and leave **pre-release** unchecked only if this has passed release verification.
- Publish the release.

GitHub automatically offers the source code archive. You do not need to attach runtime databases, API secrets, unpublished soil data or snapshots.

GitHub CLI alternative (if `gh` is installed and authorised):

~~~bash
gh release create v0.3.0 \
  --repo echosoil/echorepo-lite \
  --title "ECHOREPO Lite v0.3.0 — FAIR data publication and research data workflows" \
  --notes-file RELEASE_NOTES_v0.3.0.md \
  --latest
~~~

## Step 5 — Verify Zenodo *software* archiving

If Zenodo's GitHub integration is enabled for this repository, verify a **software** record was created for tag `v0.3.0`. Record the version DOI in the GitHub Release and software citations. The *dataset* DOI is not a substitute for the software DOI.

If no software deposition appears, inspect the GitHub integration status in Zenodo rather than manufacturing or copying a DOI.

The dataset update script `./scripts/update_zenodo_bundle.sh` creates a new **dataset version**, and must **not** be run merely to make the GitHub source release.

## Step 6 — Optional deployment (separate decision)

Only after release verification and normal change-control should you deploy `v0.3.0` to production. Verify deployment-specific `.env` and external services; back up PostgreSQL, MinIO objects and other persistent state before applying migrations. You may use your established dev-to-production workflow after reviewing its merge and environment-copy behaviour.

## Rollback

Preserve the previous deployment image/configuration and database backups. A Git tag rollback alone does not undo schema migrations or data transformations. Make a rollback plan specific to the deployment before upgrading production.
