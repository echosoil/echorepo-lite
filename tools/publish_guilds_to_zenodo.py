"""Staged, independent Zenodo release of prepared ECHOREPO guild datasets."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import fcntl
import requests

from tools import prepare_guild_publication as prep
from tools import publish_biodiversity_to_zenodo as zen

KINDS = ("faprotax", "funguild")
CHECKS = ("public_sample_identifiers", "reference_version_and_terms",
          "source_lineage", "scientific_and_metadata_review")
ECHO_GRANTS = ("10.13039/501100000780::101112869", "10.13039/100014013::10068004")
COMMON = {"annotation_coverage.csv", "README.md", "file.json", "provenance.json"}
FILES = {"faprotax": COMMON | {"functional_profiles.csv", "function_definitions.csv"},
         "funguild": COMMON | {"guild_profiles.csv", "guild_definitions.csv"}}


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()


def checksums(path):
    meta = prep.file_meta(path)
    md5 = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            md5.update(block)
    if prep.file_meta(path) != meta:
        raise ValueError("File changed during hashing")
    return {**meta, "md5": md5.hexdigest()}


def validate_candidate(root, kind):
    directory = root / kind
    report = prep.read_json(root / "preparation-report.json")
    manifest_path = directory / "provenance.json"
    if prep.file_meta(manifest_path) != report["candidate_manifests"][kind]:
        raise ValueError("Candidate manifest fingerprint mismatch")
    manifest = prep.read_json(manifest_path)
    if set(manifest["files"]) != FILES[kind] - {"provenance.json"}:
        raise ValueError("Unexpected manifest file set")
    if {p.name for p in directory.iterdir()} != FILES[kind]:
        raise ValueError("Unexpected candidate file set")
    for name, expected in manifest["files"].items():
        if prep.file_meta(directory / name) != expected:
            raise ValueError(f"Candidate fingerprint mismatch: {name}")
    if manifest.get("publication_schema_version") != "2.0" or manifest.get("proportional_unit") != "percent":
        raise ValueError("Reprepare candidate with percentage schema v2")
    if manifest["statistics"] != report[kind]:
        raise ValueError("Candidate statistics disagree with preparation report")
    return manifest, report["candidate_manifests"][kind]


def default_metadata(kind):
    name = "FAPROTAX bacterial functional profiles" if kind == "faprotax" else "FUNGuild fungal ecological guild profiles"
    explanation = (
        "Percentages of present retained 16S OTUs assigned to predicted functions; "
        "overlap is allowed. Separate retained-read annotation coverage."
        if kind == "faprotax" else
        "Predicted ECHOREPO macro-guild profiles based on reconstructed ITS reads; "
        "one accepted mapped guild per genus, no visualization cutoff."
    )
    return {
        "title": f"ECHOREPO soil biodiversity: {name}",
        "upload_type": "dataset",
        "description": explanation + " Derived predictions, not direct measurements of activity.",
        "creators": [{"name": "ECHO Consortium"}],
        "access_right": "open",
        "keywords": ["soil biodiversity", "citizen science", "ECHOREPO", kind],
        "grants": [{"id": value} for value in ECHO_GRANTS],
    }


def review_template(kind):
    return {"dataset": kind, "metadata": {**default_metadata(kind), "license": "", "version": "1.0"},
            "review": {name: {"approved": False, "evidence": ""} for name in CHECKS},
            "public_provenance": {"reference_version": "", "reference_citation": "",
                                  "source_description": "", "limitations": ""}}


def review_blockers(kind, config):
    blockers = []
    if config.get("dataset") != kind:
        blockers.append("Review dataset mismatch")
    meta = config.get("metadata", {})
    allowed = {"title", "upload_type", "description", "creators", "access_right",
               "license", "version", "keywords", "grants", "communities",
               "related_identifiers", "subjects", "notes", "contributors"}
    if set(meta) - allowed:
        blockers.append("Unexpected metadata keys")
    if meta.get("upload_type") != "dataset" or meta.get("access_right") != "open":
        blockers.append("Expected an open dataset")
    for key in ("title", "description", "license", "version"):
        if not isinstance(meta.get(key), str) or not meta[key].strip():
            blockers.append(f"Missing metadata: {key}")
    creators = meta.get("creators")
    if not isinstance(creators, list) or not creators or any(
        not isinstance(c, dict) or not isinstance(c.get("name"), str) or not c["name"].strip()
        for c in creators
    ):
        blockers.append("Creator metadata missing")
    contributors = meta.get("contributors", [])
    if not isinstance(contributors, list) or any(
        not isinstance(c, dict) or not isinstance(c.get("name"), str)
        or not c["name"].strip() or not isinstance(c.get("type"), str)
        or not c["type"].strip()
        for c in contributors
    ):
        blockers.append("Invalid contributor metadata")
    grants = meta.get("grants", [])
    if not isinstance(grants, list) or any(not isinstance(g, dict) for g in grants) or (
        {g.get("id") for g in grants} != set(ECHO_GRANTS) or len(grants) != len(ECHO_GRANTS)
    ):
        blockers.append("Expected both ECHO funding awards")
    for name in CHECKS:
        item = config.get("review", {}).get(name, {})
        evidence = item.get("evidence")
        if item.get("approved") is not True or not isinstance(evidence, str) or not evidence.strip():
            blockers.append(f"Review required: {name}")
    public = config.get("public_provenance", {})
    for name in ("reference_version", "reference_citation", "source_description", "limitations"):
        if not isinstance(public.get(name), str) or not public[name].strip():
            blockers.append(f"Public provenance required: {name}")
    return blockers


def plan_release(root, kind, target, config, out):
    if out.exists():
        raise FileExistsError(out)
    manifest, source_meta = validate_candidate(root, kind)
    blockers = review_blockers(kind, config)
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".guild-release-", dir=out.parent) as tmp:
        stage = Path(tmp) / "plan"
        stage.mkdir(mode=0o700)
        payload = stage / "files"
        payload.mkdir()
        for name in sorted(FILES[kind] - {"provenance.json", "README.md", "file.json"}):
            prep.checked_copy(root / kind / name, payload / name, manifest["files"][name])
        for name in ("README.md", "file.json"):
            prep.checked_copy(root / kind / name, payload / name, manifest["files"][name])
        if not blockers:
            # Keep limitations in the original numerical provenance; review evidence
            # stays private. Public explanatory notes are explicitly supplied.
            readme = (payload / "README.md").read_text(encoding="utf-8")
            readme = readme.replace(" — local candidate", "")
            readme = readme.split("Public identifiers, reference version/terms, source lineage and release metadata")[0].rstrip()
            public = config["public_provenance"]
            readme += "\n\n## Release provenance\n\n" + "\n\n".join(
                f"**{key.replace('_', ' ')}:** {value}" for key, value in public.items()) + "\n"
            (payload / "README.md").write_text(readme, encoding="utf-8")
            csvw = prep.read_json(payload / "file.json")
            for table in csvw["tables"]:
                for column in table["tableSchema"]["columns"]:
                    if column["name"] == "sample_id":
                        column["dc:description"] = "ECHOREPO sample label."
            prep.write_json(payload / "file.json", csvw)
        provenance = {**manifest, "candidate_manifest": source_meta,
                      "public_release_review": config.get("public_provenance", {}) if not blockers else None,
                      "publication_ready": not blockers,
                      "files": {p.name: prep.file_meta(p) for p in sorted(payload.iterdir())}}
        prep.write_json(payload / "provenance.json", provenance)
        plan = {"schema": 1, "dataset": kind, "target": target,
                "metadata": config.get("metadata", {}), "blockers": blockers,
                "review_config_sha256": digest(config),
                "files": {p.name: checksums(p) for p in sorted(payload.iterdir())}}
        plan["plan_sha256"] = digest(plan)
        prep.write_json(stage / "plan.json", plan)
        prep.write_json(stage / "review.local.json", config)
        if out.exists():
            raise FileExistsError(out)
        stage.rename(out)
    return plan


def verify_plan(directory, approved):
    plan = prep.read_json(directory / "plan.json")
    expected = plan.pop("plan_sha256")
    if digest(plan) != expected or approved != expected:
        raise ValueError("Plan digest differs from explicit approval")
    plan["plan_sha256"] = expected
    kind, target = plan["dataset"], plan["target"]
    if kind not in KINDS or target not in {"sandbox", "production"}:
        raise ValueError("Invalid plan identity")
    config = prep.read_json(directory / "review.local.json")
    if (digest(config) != plan["review_config_sha256"] or plan["metadata"] != config["metadata"]
            or plan["blockers"] or review_blockers(kind, config)):
        raise ValueError("Release review incomplete or changed")
    payload = directory / "files"
    if set(plan["files"]) != FILES[kind] or {p.name for p in payload.iterdir()} != FILES[kind]:
        raise ValueError("Unexpected release file set")
    for name, expected in plan["files"].items():
        if checksums(payload / name) != expected:
            raise ValueError(f"Release file changed: {name}")
    return plan


def atomic_state(path, value):
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".state-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)


def get_record(base, token, ident):
    if not re.fullmatch(r"[0-9]+", str(ident)):
        raise ValueError("Invalid deposition ID")
    response = requests.get(f"{base}/{ident}", headers=zen.zenodo_auth_headers(token), timeout=120)
    zen.request_ok(response, {200}, "Zenodo deposition read")
    return response.json()


def verify_remote_files(base, token, ident, files):
    rows = zen.list_deposition_files(base, token, ident)
    by_name = {r.get("filename") or r.get("name"): r for r in rows}
    if len(rows) != len(by_name) or set(by_name) != set(files):
        raise ValueError("Remote file inventory mismatch")
    for name, expected in files.items():
        row = by_name[name]
        checksum = str(row.get("checksum", "")).lower().removeprefix("md5:")
        if checksum != expected["md5"] or int(row.get("filesize", row.get("size", -1))) != expected["bytes"]:
            raise ValueError(f"Remote checksum or size mismatch: {name}")


def verify_metadata(record, expected):
    actual = record.get("metadata", {})
    for key in ("title", "description", "access_right", "upload_type", "version", "license"):
        value = actual.get(key)
        if key == "license" and isinstance(value, dict):
            value = value.get("id")
        if value != expected[key]:
            raise ValueError(f"Remote metadata mismatch: {key}")
    if [c.get("name") for c in actual.get("creators", [])] != [
        c["name"] for c in expected["creators"]
    ]:
        raise ValueError("Remote creator mismatch")
    # Verify the role as well as the name. A data curator is not a creator.
    def contributor_key(item):
        return (item.get("name"), item.get("type"))

    actual_contributors = actual.get("contributors", [])
    expected_contributors = expected.get("contributors", [])
    if sorted(map(contributor_key, actual_contributors)) != sorted(
        map(contributor_key, expected_contributors)
    ):
        raise ValueError("Remote contributor mismatch")
    # Legacy deposition responses expose funder-prefixed grant IDs.
    if {g.get("id") for g in actual.get("grants", [])} != {g["id"] for g in expected["grants"]}:
        raise ValueError("Remote funding award mismatch; inspect Zenodo metadata")



def check_version_links(prior, base, ident, confirm_no_existing_draft=False):
    """A legacy latest_draft link may refer to the published record itself."""
    links = prior.get("links", {})
    latest = links.get("latest_draft")
    expected = f"{base}/{ident}"
    if latest:
        if latest.rstrip("/") != expected:
            raise ValueError("Prior concept already has a different draft link; reconcile manually")
        # Zenodo RDM issue #858: even a real new draft may be hidden by
        # a legacy self-link. Require an explicit operator check in the UI.
        if not confirm_no_existing_draft:
            raise ValueError(
                "Zenodo latest_draft points to the published record itself. "
                "Inspect My uploads (all versions); if no draft exists, "
                "retry stage with --confirm-no-existing-draft."
            )


def run_remote(action, directory, approved, state_root, env_file, replace_plan=None,
               confirm_no_existing_draft=False):
    plan = verify_plan(directory, approved)
    kind, target = plan["dataset"], plan["target"]
    base = zen.build_zenodo_base_url(target == "sandbox")
    token_name = "ACCESS_TOKEN_SANDBOX" if target == "sandbox" else "ACCESS_TOKEN"
    token = zen.env_or_config(token_name, None, zen.load_simple_env_file(str(env_file)))
    if not token:
        raise ValueError(f"Missing {token_name}")
    state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    state_path = state_root / f"{kind}-{target}.json"
    lock_path = state_path.with_suffix(".lock")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = prep.read_json(state_path) if state_path.exists() else {
            "dataset": kind, "target": target, "published": None, "pending": None, "history": []}
        if (state["dataset"], state["target"]) != (kind, target):
            raise ValueError("Release state identity mismatch")
        pending = state["pending"]
        previous = state["published"]
        # Review changes alone do not require a new version if the exact public
        # bytes and Zenodo metadata are unchanged.
        content_id = digest({"files": plan["files"], "metadata": plan["metadata"]})
        if action == "restage":
            if not pending or pending["status"] != "staged" or pending["plan_sha256"] != replace_plan:
                raise ValueError("Expected a staged draft matching the old plan digest")
            ident = pending["id"]
            draft = get_record(base, token, ident)
            if str(draft.get("id")) != ident or draft.get("submitted") is not False:
                raise ValueError("Refusing to modify anything except the tracked unpublished draft")
            if previous and str(draft.get("conceptrecid")) != previous["conceptrecid"]:
                raise ValueError("Draft concept identity mismatch")
            bucket = draft["links"]["bucket"]
            parsed = urlsplit(bucket)
            if parsed.scheme != "https" or parsed.netloc != urlsplit(base).netloc:
                raise ValueError("Unexpected Zenodo bucket host")
            rows = zen.list_deposition_files(base, token, ident)
            if {r.get("filename") or r.get("name") for r in rows} != set(plan["files"]):
                raise ValueError("Draft file set differs; reconcile manually")
            pending.update(status="restaging", prior_plan_sha256=replace_plan,
                           plan_sha256=approved, content_sha256=content_id)
            atomic_state(state_path, state)
            zen.update_metadata(base, token, ident, plan["metadata"])
            for name in sorted(plan["files"]):
                zen.upload_file_to_bucket(bucket, token, directory / "files" / name)
            verify_remote_files(base, token, ident, plan["files"])
            verify_metadata(get_record(base, token, ident), plan["metadata"])
            pending["status"] = "staged"
            atomic_state(state_path, state)
            return pending
        if action == "stage":
            if pending:
                raise ValueError("Pending release exists; inspect state and remote record before retrying")
            if previous and previous["content_sha256"] == content_id:
                return {"status": "unchanged", **previous}
            if previous:
                prior = get_record(base, token, previous["id"])
                if not prior.get("submitted") or str(prior.get("conceptrecid")) != previous["conceptrecid"]:
                    raise ValueError("Prior published concept identity mismatch")
                check_version_links(prior, base, previous["id"], confirm_no_existing_draft)
            # Persist intent before a non-idempotent API call. Ambiguous failure
            # must be reconciled manually rather than creating another record.
            pending = {"status": "creating", "plan_sha256": approved, "content_sha256": content_id}
            state["pending"] = pending
            atomic_state(state_path, state)
            draft = (zen.create_new_version_draft(base, token, previous["id"]) if previous
                     else zen.create_new_deposition(base, token))
            ident = str(draft["id"])
            pending.update(id=ident, status="created")
            atomic_state(state_path, state)
            if draft.get("submitted") or not re.fullmatch(r"[0-9]+", ident):
                raise ValueError("Expected a new unpublished draft")
            if previous and (ident == previous["id"] or str(draft.get("conceptrecid")) != previous["conceptrecid"]):
                raise ValueError("New draft concept identity mismatch")
            bucket = draft["links"]["bucket"]
            parsed = urlsplit(bucket)
            if parsed.scheme != "https" or parsed.netloc != urlsplit(base).netloc:
                raise ValueError("Unexpected Zenodo bucket host")
            if previous:
                zen.clear_deposition_files(base, token, ident)
            zen.update_metadata(base, token, ident, plan["metadata"])
            for name in sorted(plan["files"]):
                zen.upload_file_to_bucket(bucket, token, directory / "files" / name)
            verify_remote_files(base, token, ident, plan["files"])
            verify_metadata(get_record(base, token, ident), plan["metadata"])
            pending.update(status="staged", reserved_doi=zen.reserved_doi_from_draft(draft),
                           draft_url=draft.get("links", {}).get("html"))
            atomic_state(state_path, state)
            return pending
        if not pending or pending["status"] not in {"staged", "publishing"} or pending["plan_sha256"] != approved:
            raise ValueError("No matching staged draft")
        ident = pending["id"]
        record = get_record(base, token, ident)
        if record.get("submitted") and pending["status"] != "publishing":
            raise ValueError("Draft was published outside this workflow; reconcile manually")
        verify_remote_files(base, token, ident, plan["files"])
        verify_metadata(record, plan["metadata"])
        if not record.get("submitted"):
            pending["status"] = "publishing"
            atomic_state(state_path, state)
            zen.publish_deposition(base, token, ident)
            record = get_record(base, token, ident)
        if not record.get("submitted") or not record.get("doi") or not record.get("conceptrecid"):
            raise ValueError("Publication not confirmed; retain state and inspect before retrying")
        if previous and str(record["conceptrecid"]) != previous["conceptrecid"]:
            raise ValueError("Published concept identity mismatch")
        verify_remote_files(base, token, ident, plan["files"])
        result = {"id": ident, "conceptrecid": str(record["conceptrecid"]),
                  "doi": record["doi"], "concept_doi": zen.derive_concept_doi(record),
                  "content_sha256": content_id, "plan_sha256": approved,
                  "url": record.get("links", {}).get("html")}
        state["published"] = result
        state["history"].append(result)
        state["pending"] = None
        atomic_state(state_path, state)
        return {"status": "published", **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    plan = sub.add_parser("plan", help="Offline dry run; never calls Zenodo")
    plan.add_argument("--bundle", type=Path, required=True)
    plan.add_argument("--dataset", choices=KINDS, required=True)
    plan.add_argument("--target", choices=("sandbox", "production"), default="sandbox")
    plan.add_argument("--review", type=Path)
    plan.add_argument("--out-dir", type=Path, required=True)
    for action in ("stage", "restage", "publish"):
        cmd = sub.add_parser(action)
        cmd.add_argument("--plan-dir", type=Path, required=True)
        cmd.add_argument("--approve-plan", required=True)
        cmd.add_argument("--state-dir", type=Path, required=True)
        cmd.add_argument("--env-file", type=Path, required=True)
        if action == "restage":
            cmd.add_argument("--replace-plan", required=True, help="Old staged plan SHA256")
        if action == "stage":
            cmd.add_argument("--confirm-no-existing-draft", action="store_true",
                             help="Confirm no unpublished version exists after inspecting Zenodo UI")
    args = parser.parse_args()
    if args.action == "plan":
        config = prep.read_json(args.review) if args.review else review_template(args.dataset)
        result = plan_release(args.bundle, args.dataset, args.target, config, args.out_dir)
    else:
        result = run_remote(args.action, args.plan_dir, args.approve_plan, args.state_dir, args.env_file,
                            getattr(args, "replace_plan", None),
                            getattr(args, "confirm_no_existing_draft", False))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
