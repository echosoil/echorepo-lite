#!/usr/bin/env python3
"""Consolidate R7-1 Phases 1–3 into a current-system evidence report.

No network connections or sensitive environment values are read. Evidence
is reconstructed from local JUnit XML where present; missing files become
MISSING, never PASS. External observations remain distinct from acceptance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from tools import r71_phase2_report, r71_phase3_report, r71_validation_report


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def digest_file(path: Path) -> dict:
    """Hash one local artifact without copying or printing file contents."""
    if not path.is_file():
        return {"path": str(path), "exists": False, "sha256": None, "size_bytes": None}
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": str(path),
        "exists": True,
        "sha256": digest.hexdigest(),
        "size_bytes": path.stat().st_size,
    }


def collect(
    *,
    phase1_xml: Path,
    phase2_xml: Path,
    phase3_xml: Path,
    snapshot: Path,
    phase1_matrix: dict,
    phase2_matrix: dict,
    phase3_matrix: dict,
    external: dict,
    links: dict,
    commit: str,
    phase2_environment: str,
    phase3_environment: str,
) -> dict:
    p1_available = phase1_xml.is_file()
    p2_available = phase2_xml.is_file()
    p3_available = phase3_xml.is_file()

    # Existing evaluators are the authoritative source for per-test criteria.
    p1 = r71_validation_report.evaluate(
        phase1_matrix,
        r71_validation_report.parse_junit(phase1_xml) if p1_available else {},
        commit=commit,
    )
    p2 = r71_phase2_report.evaluate(
        phase2_matrix["checks"],
        r71_phase2_report.junit_cases(phase2_xml) if p2_available else {},
        commit,
    )
    p3 = r71_phase3_report.evaluate(
        phase3_matrix,
        r71_phase3_report.load_junit(phase3_xml) if p3_available else {},
        mode=phase3_environment,
        commit=commit,
    )

    p1_ok = bool(p1_available and p1["mapped_test_results_seen"] == p1["mapped_test_count"]
                 and p1["automated_passed_components"] == len(
                     [c for c in phase1_matrix["components"] if c["tests"]]
                 ))
    p2_ok = bool(p2_available and p2["complete"])
    p3_ok = bool(p3_available and p3["all_checks_complete"])

    phase2_status = "COMPLETE" if p2_ok else "INCOMPLETE"
    phase3_status = "COMPLETE" if p3_ok else "INCOMPLETE"

    indexed = {
        c["id"]: c["result"] for c in p2["checks"] + p3["cases"]
    }
    linked = {item["id"]: item["supporting_checks"] for item in links["components"]}
    expected_ids = {c["id"] for c in phase1_matrix["components"]}
    if set(linked) != expected_ids:
        raise ValueError("Phase 4 links must cover the exact Phase 1 component inventory")

    components = []
    for component in p1["components"]:
        matches = linked[component["id"]]
        unknown = set(matches) - set(indexed)
        if unknown:
            raise ValueError(f"Unknown supporting tests for {component['id']}: {sorted(unknown)}")
        supporting = [{"id": case, "status": indexed[case]} for case in matches]
        components.append({
            "id": component["id"],
            "name": component["component"],
            "criterion": component["criterion"],
            "phase1_status": component["automated_result"],
            "phase1_passed": sum(c["status"] == "PASS" for c in component["tests"]),
            "phase1_total": len(component["tests"]),
            "later_supporting_checks": supporting,
            "remaining_evidence": component["additional_evidence_required"],
            # Forensic tests and supporting checks cannot imply formal acceptance.
            "accepted_against_requirements": False,
        })

    registers = external.get("items", [])
    accepted = sum(c["accepted_against_requirements"] for c in components)
    return {
        "kpi": "R7-1",
        "scope": "Current-system technical validation; NOT historical M40 proof",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "report_commit": commit or "not recorded",
        "phase1": {
            "status": "COMPLETE_AUTOMATED_SCOPE" if p1_ok else "INCOMPLETE",
            "junit_present": p1_available,
            "automated_passed": p1["automated_passed_components"],
            "component_total": p1["component_count"],
            "mapped_tests_seen": p1["mapped_test_results_seen"],
            "mapped_tests_total": p1["mapped_test_count"],
            "note": "A component with no mapped tests is NOT_COVERED, not validated.",
        },
        "phase2": {
            "status": phase2_status,
            "junit_present": p2_available,
            "passed": p2["passed"],
            "total": p2["total"],
            "reported_environment": phase2_environment,
            "note": (
                "If the Phase 2 staging guard was enabled while targeting production, "
                "label these as production observations with a mislabeled staging gate. "
                "Do not claim that staging was tested or that production operations "
                "were non-impacting solely because the tests passed."
            ),
        },
        "phase3": {
            "status": phase3_status,
            "junit_present": p3_available,
            "passed_required": p3["required_passed"],
            "required_total": p3["required_total"],
            "passed_optional": p3["optional_passed"],
            "optional_total": p3["optional_total"],
            "reported_environment": phase3_environment,
        },
        "technical_evidence_present_and_passing": bool(p1_ok and p2_ok and p3_ok),
        "external_evidence": registers,
        "accepted_components": accepted,
        "component_total": len(components),
        "components": components,
        "artifacts": [
            digest_file(phase1_xml),
            digest_file(phase2_xml),
            digest_file(phase3_xml),
            digest_file(snapshot),
        ],
        "overall_kpi_status": "NOT YET VERIFIED",
        "required_next_steps": [
            "Approve the requirements and component inventory against GA/D5.1.",
            "Attach and independently verify DOI-level SoilWise/EUSO provenance.",
            "Verify file.json ingestion and external interfaces (including Firebase) where in scope.",
            "Resolve remaining deployment/security and data QA acceptance evidence.",
            "Provide historical M40 release/requirements/deployment records if reporting for September 2026.",
            "Obtain dated WP5/WP7 acceptance/sign-off for each required component.",
        ],
    }


def markdown(report: dict) -> str:
    p1, p2, p3 = report["phase1"], report["phase2"], report["phase3"]
    lines = [
        "# ECHOREPO R7-1 — consolidated validation evidence",
        "",
        f"- Prepared UTC: {report['generated_at_utc']}",
        f"- Repository commit (report preparation): \`{report['report_commit']}\`",
        f"- Scope: {report['scope']}",
        "- **Overall status: NOT YET VERIFIED** — pending external evidence, requirements agreement and approval.",
        "",
        "## Automated validation",
        "",
        "| Phase | Source evidence | Observed outcome |",
        "|---|---|---|",
        f"| 1 — components | {p1['status']} | {p1['automated_passed']}/{p1['component_total']} components automated-pass; {p1['mapped_tests_seen']}/{p1['mapped_tests_total']} mapped cases present |",
        f"| 2 — integrations | {p2['status']} | {p2['passed']}/{p2['total']} checks; environment: {p2['reported_environment']} |",
        f"| 3 — browser | {p3['status']} | {p3['passed_required']}/{p3['required_total']} required + {p3['passed_optional']}/{p3['optional_total']} optional; environment: {p3['reported_environment']} |",
        "",
        "**Caveat (Phase 2):** " + p2["note"],
        "",
        "## Requirement-linked component coverage",
        "",
        "| Component | Isolated tests | Corroborating integration/browser checks | Formal acceptance |",
        "|---|---|---|---|",
    ]
    for c in report["components"]:
        linked = ", ".join(f"{x['id']} ({x['status']})" for x in c["later_supporting_checks"]) or "—"
        lines.append(
            f"| {c['id']} — {c['name']} | {c['phase1_status']} "
            f"({c['phase1_passed']}/{c['phase1_total']}) | {linked} | PENDING |"
        )
    lines.extend([
        "",
        "The presence of corroborating checks does not close a component's "
        "additional acceptance requirement. The Phase 1 NOT_COVERED "
        "external-dependencies component remains explicitly open.",
        "",
        "## External publication and interoperability register",
        "",
        "| ID | Subject | Evidence state | Source |",
        "|---|---|---|---|",
    ])
    for item in report["external_evidence"]:
        url = item.get("source_url") or ""
        lines.append(f"| {item['id']} | {item['subject']} | {item['assessment']} | {url} |")
    lines += [
        "",
        "**Important:** Zenodo publication is public; independent SoilWise "
        "harvesting of the precise version, actual file.json parsing and "
        "independent EUSO indexing have not all been demonstrated.",
        "",
        "## Retained file fingerprints",
        "",
        "| Input | Present | Size (bytes) | SHA-256 |",
        "|---|---|---:|---|",
    ]
    for item in report["artifacts"]:
        lines.append(
            f"| {item['path']} | {item['exists']} | {item['size_bytes'] or '—'} | "
            f"{item['sha256'] or '—'} |"
        )
    lines += ["", "## Remaining evidence and sign-off", ""]
    for line in report["required_next_steps"]:
        lines.append(f"- {line}")
    lines += [
        "",
        "## Sign-off (to be completed by authorized reviewers)",
        "",
        "- Requirements baseline / Grant Agreement version: **PENDING**",
        "- Component owner review: **PENDING**",
        "- WP5/WP7 representative(s): **PENDING**",
        "- Decision and approved exceptions: **PENDING**",
        "- Approval date: **PENDING**",
        "",
        "This report does not claim retrospective M40 validation or certify the KPI.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase1-xml", type=Path, default=Path("r71-evidence/results.xml"))
    parser.add_argument("--phase2-xml", type=Path, default=Path("r71-evidence/phase2-results.xml"))
    parser.add_argument("--phase3-xml", type=Path, default=Path("r71-evidence/phase3-results.xml"))
    parser.add_argument("--snapshot", type=Path, default=Path("r71-evidence/canonical_all.zip"))
    parser.add_argument("--phase1-matrix", type=Path,
                        default=Path("docs/R7_1_VALIDATION_MATRIX.json"))
    parser.add_argument("--phase2-matrix", type=Path,
                        default=Path("docs/R7_1_PHASE2_MATRIX.json"))
    parser.add_argument("--phase3-matrix", type=Path,
                        default=Path("docs/R7_1_PHASE3_MATRIX.json"))
    parser.add_argument("--links", type=Path, default=Path("docs/R7_1_PHASE4_LINKS.json"))
    parser.add_argument("--external", type=Path,
                        default=Path("docs/R7_1_PHASE4_EXTERNAL.json"))
    parser.add_argument("--phase2-environment", default="production observations with staging opt-in",
                        help="Describe where Phase 2 actually ran; no environment inferred.")
    parser.add_argument("--phase3-environment", default="production-readonly")
    parser.add_argument("--commit", default="")
    parser.add_argument("--output-json", type=Path,
                        default=Path("r71-evidence/phase4-consolidated.json"))
    parser.add_argument("--output-md", type=Path,
                        default=Path("r71-evidence/phase4-consolidated.md"))
    parser.add_argument("--require-technical", action="store_true",
                        help="Fail unless all available-matrix Phase 1 and all Phase 2/3 checks pass")
    args = parser.parse_args()

    result = collect(
        phase1_xml=args.phase1_xml, phase2_xml=args.phase2_xml,
        phase3_xml=args.phase3_xml, snapshot=args.snapshot,
        phase1_matrix=read_json(args.phase1_matrix),
        phase2_matrix=read_json(args.phase2_matrix),
        phase3_matrix=read_json(args.phase3_matrix),
        links=read_json(args.links), external=read_json(args.external),
        commit=args.commit,
        phase2_environment=args.phase2_environment,
        phase3_environment=args.phase3_environment,
    )
    for dest in (args.output_json, args.output_md):
        dest.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    args.output_md.write_text(markdown(result), encoding="utf-8")
    print(
        f"Phase 4: technical={result['technical_evidence_present_and_passing']}, "
        f"P1={result['phase1']['automated_passed']}/{result['phase1']['component_total']} components, "
        f"P2={result['phase2']['passed']}/{result['phase2']['total']} checks, "
        f"P3={result['phase3']['passed_required'] + result['phase3']['passed_optional']}/"
        f"{result['phase3']['required_total'] + result['phase3']['optional_total']} checks. "
        f"Overall R7-1: NOT YET VERIFIED."
    )
    return int(args.require_technical and not result["technical_evidence_present_and_passing"])


if __name__ == "__main__":
    raise SystemExit(main())
