#!/usr/bin/env python3
"""Produce an auditable, conservative report for R7-1 Phase 2 staging tests.

Missing/skipped tests never count as passed. No connection string or secret
is loaded, copied or emitted by this report generator.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree


def junit_cases(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"JUnit report does not exist: {path}")
    found: dict[str, list[str]] = defaultdict(list)
    for node in ElementTree.parse(path).getroot().iter("testcase"):
        name = node.get("name") or ""
        status = "FAIL" if node.find("failure") is not None or node.find("error") is not None else (
            "SKIPPED" if node.find("skipped") is not None else "PASS"
        )
        found[name].append(status)
    return found


def evaluate(checks: list[dict], cases: dict[str, list[str]], commit: str) -> dict:
    rows: list[dict] = []
    for check in checks:
        matched = cases.get(check["name"], [])
        status = matched[0] if len(matched) == 1 else "NOT_RUN" if not matched else "AMBIGUOUS"
        rows.append({
            "id": check["id"],
            "component": check["component"],
            "criterion": check["criterion"],
            "test_case": check["name"],
            "result": status,
        })
    passed = sum(row["result"] == "PASS" for row in rows)
    total = len(rows)
    return {
        "kpi": "R7-1",
        "phase": "Phase 2 staging integration (not historical M40 evidence)",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tested_commit": commit or "not provided",
        "passed": passed,
        "total": total,
        "complete": bool(total) and passed == total,
        "overall_kpi_status": "NOT YET VERIFIED",
        "warning": (
            "Phase 2 probes are operational evidence only. No automatic certification, "
            "external onboarding confirmation or retrospective M40 validation is implied."
        ),
        "checks": rows,
    }


def markdown(report: dict) -> str:
    lines = [
        "# R7-1 Phase 2 — staging integration evidence",
        "",
        f"- Run time (UTC): {report['generated_at_utc']}",
        f"- Commit: {report['tested_commit']}",
        f"- Staging checks passed: **{report['passed']} / {report['total']}**",
        "- Overall R7-1: **NOT YET VERIFIED** until signed-off requirements and deployment acceptance.",
        "",
        "| Check | Component | Result | Criterion |",
        "|---|---|---|---|",
    ]
    for item in report["checks"]:
        lines.append(
            f"| {item['id']} | {item['component']} | {item['result']} | {item['criterion']} |"
        )
    lines.extend(["", report["warning"], ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xml", type=Path, required=True)
    parser.add_argument("--matrix", type=Path, default=Path("docs/R7_1_PHASE2_MATRIX.json"))
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--commit", default=os.getenv("GITHUB_SHA", ""))
    parser.add_argument("--require-all", action="store_true", help="Fail if any check was not PASS")
    args = parser.parse_args()
    matrix = json.loads(args.matrix.read_text(encoding="utf-8"))
    report = evaluate(matrix["checks"], junit_cases(args.xml), args.commit)
    for output in (args.json, args.markdown):
        output.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.markdown.write_text(markdown(report), encoding="utf-8")
    print(f"R7-1 Phase 2: {report['passed']}/{report['total']} checks PASSED; R7-1 not yet verified.")
    return 1 if args.require_all and not report["complete"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
