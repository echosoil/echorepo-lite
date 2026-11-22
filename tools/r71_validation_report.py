#!/usr/bin/env python3
"""Convert R7-1 pytest JUnit results into conservative traceability evidence.

A passing automated component is NOT equivalent to fully validated software:
external/deployment acceptance evidence is independently required.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree


def parse_junit(path: Path) -> dict[str, list[dict[str, str]]]:
    found: dict[str, list[dict[str, str]]] = defaultdict(list)
    if not path.is_file():
        return found

    root = ElementTree.parse(path).getroot()
    for case in root.iter("testcase"):
        name = case.attrib.get("name", "")
        failed = case.find("failure") is not None or case.find("error") is not None
        skipped = case.find("skipped") is not None
        status = "FAIL" if failed else "SKIPPED" if skipped else "PASS"
        found[name].append({
            "status": status,
            "classname": case.attrib.get("classname", ""),
            "duration_s": case.attrib.get("time", ""),
        })
    return found


def evaluate(matrix: dict, cases: dict[str, list[dict[str, str]]], *, commit: str) -> dict:
    summary = []
    passed = 0
    observed = 0
    test_count = 0

    for requirement in matrix["components"]:
        entries = []
        for name in requirement["tests"]:
            matches = cases.get(name, [])
            if len(matches) == 1:
                status = matches[0]["status"]
                observed += 1
            elif len(matches) == 0:
                status = "NOT_RUN"
            else:
                status = "AMBIGUOUS"
            entries.append({"name": name, "status": status})

        test_count += len(entries)
        if not entries:
            result = "NOT_COVERED"
        elif all(entry["status"] == "PASS" for entry in entries):
            result = "AUTOMATED_PASS"
            passed += 1
        elif any(entry["status"] in {"FAIL", "AMBIGUOUS"} for entry in entries):
            result = "AUTOMATED_FAIL"
        elif any(entry["status"] == "SKIPPED" for entry in entries):
            result = "INCOMPLETE"
        else:
            result = "NOT_RUN"

        summary.append({
            "id": requirement["id"],
            "component": requirement["name"],
            "criterion": requirement["criterion"],
            "automated_result": result,
            "tests": entries,
            "additional_evidence_required": requirement["additional_evidence"],
        })

    return {
        "kpi": matrix["kpi"],
        "baseline": matrix["baseline"],
        "tested_commit": commit or "unknown",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "component_count": len(summary),
        "automated_passed_components": passed,
        "mapped_test_count": test_count,
        "mapped_test_results_seen": observed,
        "automated_component_coverage_pct": (
            round(100 * passed / len(summary), 2) if summary else 0
        ),
        "overall_kpi_status": "NOT YET VERIFIED",
        "warning": (
            "These results describe current automated checks only. "
            "They do not certify all components, prove external deployments, "
            "or retroactively establish validation at M40."
        ),
        "components": summary,
    }


def render_markdown(report: dict) -> str:
    lines = [
        "# ECHOREPO R7-1 — automated validation evidence",
        "",
        f"- Generated (UTC): {report['generated_at_utc']}",
        f"- Tested commit: {report['tested_commit']}",
        f"- Reporting baseline: {report['baseline']}",
        f"- Automated components passed: {report['automated_passed_components']} / {report['component_count']}",
        f"- Required test cases observed: {report['mapped_test_results_seen']} / {report['mapped_test_count']}",
        "- **Overall KPI status: NOT YET VERIFIED** — external/deployment acceptance is not inferred.",
        "",
        "| ID | Component | Automated result | Tests |",
        "|---|---|---|---|",
    ]
    for item in report["components"]:
        ok = sum(t["status"] == "PASS" for t in item["tests"])
        lines.append(
            f"| {item['id']} | {item['component']} | {item['automated_result']} "
            f"| {ok}/{len(item['tests'])} passed |"
        )
    lines.extend([
        "",
        "## Outstanding external validation",
        "",
    ])
    for item in report["components"]:
        lines.append(f"- **{item['id']} {item['component']}:** {item['additional_evidence_required']}")
    lines.extend(["", report["warning"], ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xml", type=Path, required=True, help="pytest --junitxml output")
    parser.add_argument("--matrix", type=Path, default=Path("docs/R7_1_VALIDATION_MATRIX.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--commit", default=os.environ.get("GITHUB_SHA", ""))
    args = parser.parse_args()

    matrix = json.loads(args.matrix.read_text(encoding="utf-8"))
    report = evaluate(matrix, parse_junit(args.xml), commit=args.commit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(report), encoding="utf-8")
    print(
        f"R7-1: {report['automated_passed_components']}/{report['component_count']} "
        f"components automated-pass; {report['mapped_test_results_seen']}/"
        f"{report['mapped_test_count']} mapped cases observed. Overall KPI: NOT YET VERIFIED."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
