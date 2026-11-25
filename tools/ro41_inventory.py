#!/usr/bin/env python3
"""Read-only candidate inventory of ECHO Zenodo publications; no upload or data download."""
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

from tools.r73_interoperability import MIB, PublicReader, json_doc

GRANT = "101112869"
COMMUNITY = "echo_project"
SEEDS = ("23784504", "22014977", "10218825", "24485700")
ACK = "yes-public-readonly"
SEARCHES = (
    ("community", {"communities": COMMUNITY}),
    ("grant_number", {"q": '"101112869"'}),
    ("grant_structured", {"q": 'metadata.funding.award.number:"101112869"'}),
    ("echorepo_title", {"q": 'title:ECHOREPO'}),
)
PAGE_SIZE = 25
MAX_PAGES = 8
MAX_SEEDS = 30


def clean(value):
    return str(value or "").strip()


def code(value):
    if isinstance(value, dict):
        return clean(value.get("id") or value.get("type"))
    return clean(value)


def record_row(doc):
    md = doc.get("metadata") or {}
    record_id = clean(doc.get("id"))
    if not record_id.isdigit():
        raise ValueError("Missing record ID")
    grants = set()
    for item in md.get("grants") or []:
        if isinstance(item, dict):
            for key in ("id", "code", "number"):
                if item.get(key):
                    grants.add(clean(item[key]).split("::")[-1])
    for item in md.get("funding") or []:
        if isinstance(item, dict):
            award = item.get("award") or {}
            if isinstance(award, dict):
                for key in ("number", "id"):
                    if award.get(key):
                        grants.add(clean(award[key]).split("::")[-1])
    doi = clean(doc.get("doi") or md.get("doi")).lower()
    concept_doi = clean(doc.get("conceptdoi") or md.get("conceptdoi")).lower()
    concept_id = clean(doc.get("conceptrecid") or md.get("conceptrecid"))
    # Do not invent a concept ID from a version DOI. Where the API exposes a
    # concept record ID but no DOI, retain that exact ID for grouping.
    concept_key = concept_doi or ("zenodo-concept:" + concept_id if concept_id.isdigit() else "")
    fs = doc.get("files")
    files = []
    if isinstance(fs, list):
        for f in fs:
            if isinstance(f, dict):
                files.append({"name": clean(f.get("key")), "size": f.get("size"),
                              "checksum": clean(f.get("checksum"))})
    access = clean(md.get("access_right") or (doc.get("access") or {}).get("files"))
    license_id = code(md.get("license"))
    if not license_id and isinstance(md.get("rights"), list):
        license_id = "; ".join(code(x) for x in md["rights"] if code(x))
    kind = code(md.get("resource_type") or md.get("upload_type"))
    return {
        "record_id": record_id, "doi": doi, "concept_doi": concept_doi,
        "concept_id": concept_id, "concept_key": concept_key,
        "title": clean(md.get("title")), "resource_type": kind,
        "publication_date": clean(md.get("publication_date")),
        "version": clean(md.get("version")), "grant_numbers": sorted(grants),
        "echo_grant_confirmed": GRANT in grants,
        "access": access, "license": license_id,
        "file_inventory_present": isinstance(fs, list), "file_count": len(files),
        "csvw_filename_present": any(f["name"] == "file.json" for f in files),
        "files": files, "url": f"https://zenodo.org/records/{record_id}",
        "sources": [],
    }


def group_records(rows):
    groups = {}
    # A known concept DOI can bridge rows exposing only the concept record ID.
    bridges = {}
    for row in rows:
        if row["concept_id"] and row["concept_doi"]:
            bridges.setdefault(row["concept_id"], set()).add(row["concept_doi"])
    for row in rows:
        candidates = bridges.get(row["concept_id"], set())
        key = row["concept_doi"]
        if not key and len(candidates) == 1:
            key = next(iter(candidates))
        key = key or row["concept_key"] or ("unresolved-version:" + row["record_id"])
        groups.setdefault(key, []).append(row)
    result = []
    for key, members in sorted(groups.items()):
        members.sort(key=lambda r: (r["publication_date"], int(r["record_id"])), reverse=True)
        latest = members[0]
        kinds = {r["resource_type"] for r in members}
        result.append({
            "group_key": key, "concept_resolved": not key.startswith("unresolved-version:"),
            "representative_doi": latest["doi"], "representative_record_id": latest["record_id"],
            "title": latest["title"], "resource_types": sorted(kinds),
            "dataset_candidate": "dataset" in kinds,
            "echo_grant_confirmed": any(r["echo_grant_confirmed"] for r in members),
            "version_count_observed": len(members),
            "version_dois": [r["doi"] for r in members if r["doi"]],
            "record_ids": [r["record_id"] for r in members],
        })
    return result


def search(reader, label, params):
    results = []
    seen = set()
    outcome = {"source": label, "parameters": params, "status": "NOT_ASSESSED",
               "pages": 0, "reported_total": None, "records": 0}
    for page in range(1, MAX_PAGES + 1):
        query = dict(params, size=PAGE_SIZE, page=page, all_versions="true", sort="mostrecent")
        try:
            raw, _ = reader.get("https://zenodo.org/api/records?" + urlencode(query), 8 * MIB)
            doc = json_doc(raw)
            hits = doc["hits"]["hits"]
            if not isinstance(hits, list):
                raise ValueError("Unexpected search response")
            total = doc["hits"].get("total")
            if isinstance(total, dict):
                total = total.get("value") if total.get("relation") == "eq" else None
            outcome["reported_total"] = total if isinstance(total, int) else None
            outcome["pages"] += 1
            new = 0
            for hit in hits:
                row = record_row(hit)
                if row["record_id"] not in seen:
                    seen.add(row["record_id"])
                    results.append(row)
                    new += 1
            if not hits or len(hits) < PAGE_SIZE:
                outcome["status"] = ("OBSERVED" if outcome["reported_total"] is None
                                     or len(seen) >= outcome["reported_total"] else "PARTIAL")
                break
            if outcome["reported_total"] is not None and len(seen) >= outcome["reported_total"]:
                outcome["status"] = "OBSERVED"
                break
            if not new:
                outcome["status"] = "PARTIAL"
                outcome["reason"] = "Repeated page"
                break
        except (requests.RequestException, ValueError, TypeError, AttributeError, KeyError) as exc:
            outcome["status"] = "PARTIAL" if results else "NOT_ASSESSED"
            outcome["reason"] = type(exc).__name__
            break
    else:
        outcome["status"] = "PARTIAL"
        outcome["reason"] = "Page limit reached"
    outcome["records"] = len(results)
    return results, outcome


def inventory(reader, seeds):
    rows = {}
    queries = []

    def add(row, source):
        ident = row["record_id"]
        previous = rows.get(ident)
        sources = previous["sources"] if previous else []
        row["sources"] = sorted(set(sources + [source]))
        # Direct detail retrieval replaces search summaries; otherwise retain
        # the first observation and merge discovery provenance.
        if previous and not source.startswith("seed:"):
            previous["sources"] = row["sources"]
        else:
            rows[ident] = row

    for label, params in SEARCHES:
        found, outcome = search(reader, label, params)
        queries.append(outcome)
        for row in found:
            add(row, label)
    for ident in seeds:
        try:
            raw, _ = reader.get("https://zenodo.org/api/records/" + ident, 8 * MIB)
            row = record_row(json_doc(raw))
            if row["record_id"] != ident:
                raise ValueError("Unexpected record ID")
            add(row, "seed:" + ident)
            queries.append({"source": "seed:" + ident, "status": "OBSERVED", "records": 1})
        except (requests.RequestException, ValueError, TypeError, AttributeError, KeyError) as exc:
            queries.append({"source": "seed:" + ident, "status": "NOT_ASSESSED",
                            "records": 0, "reason": type(exc).__name__})
    records = sorted(rows.values(), key=lambda r: int(r["record_id"]))
    groups = group_records(records)
    return records, groups, queries


def review_rows(groups):
    for g in groups:
        if g["dataset_candidate"]:
            yield {
                "group_key": g["group_key"], "candidate_doi": g["representative_doi"],
                "title": g["title"], "eligibility": "REVIEW",
                "eligibility_reason": "", "assessed_version_doi": "",
                "doi_resolution": "NOT_ASSESSED", "resource_access": "NOT_ASSESSED",
                "harmonisation": "NOT_ASSESSED", "soilwise_status": "NOT_ASSESSED",
                "soilwise_url": "", "euso_status": "NOT_ASSESSED", "euso_url": "",
                "evidence_date_utc": "", "evidence_ref": "", "notes": "",
            }


def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8-sig", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            # Avoid spreadsheet formula interpretation of public metadata.
            writer.writerow({k: ("'" + v if isinstance(v, str) and v[:1] in ("=", "+", "-", "@", "\t", "\r") else v)
                             for k, v in row.items()})


def markdown(report):
    summary = report["summary"]
    lines = ["# RO4-1 — preliminary public publication inventory", "",
             f"- UTC: {report['generated_at_utc']}",
             f"- Reporting commit: {report['report_commit']}",
             f"- Unique version records returned: {summary['records']}",
             f"- Dataset-typed version records: {summary['dataset_records']}",
             f"- Resolved dataset candidate groups: {summary['resolved_dataset_groups']}",
             f"- Unresolved dataset group candidates: {summary['unresolved_dataset_groups']}",
             "- Qualifying harvested datasets: NOT ASSESSED by inventory alone.", "",
             "## Discovery queries", "",
             "| Source | Status | Records |", "|---|---|---:|"]
    for q in report["queries"]:
        lines.append(f"| {q['source']} | {q['status']} | {q['records']} |")
    lines += ["", "## Dataset candidates (not an acceptance count)", "",
              "| Concept/group | Representative DOI | Title | Versions observed | ECHO grant confirmed |",
              "|---|---|---|---:|---|"]
    for g in report["groups"]:
        if g["dataset_candidate"]:
            title = g["title"].replace("|", r"\|").replace("\n", " ")
            lines.append(f"| {g['group_key']} | {g['representative_doi']} | {title} | {g['version_count_observed']} | {g['echo_grant_confirmed']} |")
    lines += ["", "## Limitations", "", *["- " + x for x in report["limitations"]], ""]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ack", required=True, choices=[ACK])
    p.add_argument("--commit", default="not recorded")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--record-id", action="append", default=[])
    args = p.parse_args()
    seeds = list(dict.fromkeys([*SEEDS, *args.record_id]))
    if len(seeds) > MAX_SEEDS or any(not re.fullmatch(r"[0-9]{1,15}", s) for s in seeds):
        p.error("Supply at most 30 numeric Zenodo record IDs including built-in seeds")
    reader = PublicReader()
    reader.session.headers["User-Agent"] = "ECHOREPO-RO41-public-inventory/1.0"
    try:
        records, groups, queries = inventory(reader, seeds)
    finally:
        reader.session.close()
    candidates = [g for g in groups if g["dataset_candidate"]]
    report = {
        "kpi": "RO4-1", "phase": 1, "report_commit": args.commit,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {"records": len(records),
                    "dataset_records": sum(r["resource_type"] == "dataset" for r in records),
                    "resolved_dataset_groups": sum(g["concept_resolved"] for g in candidates),
                    "unresolved_dataset_groups": sum(not g["concept_resolved"] for g in candidates),
                    "qualifying_harvested_datasets": None},
        "queries": queries, "records": records, "groups": groups,
        "limitations": [
            "Public metadata candidates only; discovery may miss records or include unrelated hits.",
            "Check every query status and grant association; title/community matches alone do not prove eligibility.",
            "Group by concept DOI/record ID; unresolved versions and independently duplicated depositions require review.",
            "The representative is selected by publication date and record ID, not independently verified as latest.",
            "Files and licences are metadata observations; no payload access, checksum or DOI resolution test performed.",
            "No SoilWise/EUSO evidence inferred from a generated URL or HTTP status.",
            "Numerical target of ten is a proposed working criterion, subject to consortium confirmation.",
            "No credentials, publication, database access or dataset modification. Exit zero means report written."
        ]}
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "inventory.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (args.out / "inventory.md").write_text(markdown(report), encoding="utf-8")
    fields = ["record_id", "doi", "concept_doi", "concept_id", "title", "resource_type",
              "publication_date", "version", "echo_grant_confirmed", "access", "license",
              "file_count", "csvw_filename_present", "url", "sources"]
    write_csv(args.out / "versions.csv", records, fields)
    review = list(review_rows(groups))
    review_fields = ["group_key", "candidate_doi", "title", "eligibility", "eligibility_reason",
                     "assessed_version_doi", "doi_resolution", "resource_access", "harmonisation",
                     "soilwise_status", "soilwise_url", "euso_status", "euso_url",
                     "evidence_date_utc", "evidence_ref", "notes"]
    # Preserve operator review if the same output directory is reused.
    review_path = args.out / "dataset-review.csv"
    if not review_path.exists():
        write_csv(review_path, review, review_fields)
    print(f"Recorded {len(records)} version records and {len(candidates)} dataset candidate groups.")
    print("Review query completeness and dataset eligibility; no KPI attainment inferred.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
