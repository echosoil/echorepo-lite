#!/usr/bin/env python3
"""Create a conservative R7-1 Phase 3 browser acceptance report from JUnit XML.

No credentials, cookies, screenshots or personal sample data are printed.
Optional checks are not counted as required checks and remain visible as gaps.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from xml.etree import ElementTree


def load_junit(path: Path) -> dict[str, list[str]]:
    if not path.is_file():
        raise FileNotFoundError(f"JUnit XML not found: {path}")
    found: dict[str, list[str]] = defaultdict(list)
    for case in ElementTree.parse(path).getroot().iter("testcase"):
        name = case.get("name", "")
        if case.find("failure") is not None or case.find("error") is not None:
            status = "FAIL"
        elif case.find("skipped") is not None:
            status = "SKIPPED"
        else:
            status = "PASS"
        found[name].append(status)
    return found


def evaluate(matrix: dict, junit: dict[str, list[str]], *, mode: str, commit: str,
             origin: str = "") -> dict:
    cases = []
    for check in matrix["cases"]:
        found = junit.get(check["test"], [])
        result = "AMBIGUOUS" if len(found) > 1 else found[0] if found else "NOT_RUN"
        cases.append({
            "id": check["id"],
            "component": check["component"],
            "criterion": check["criterion"],
            "test": check["test"],
            "required": bool(check["required"]),
            "result": result,
        })
    core = [c for c in cases if c["required"]]
    optional = [c for c in cases if not c["required"]]
    core_pass = sum(c["result"] == "PASS" for c in core)
    optional_pass = sum(c["result"] == "PASS" for c in optional)
    return {
        "kpi": matrix["kpi"],
        "phase": matrix["phase"],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tested_commit": commit or "unknown",
        "environment_mode": mode or "NOT SPECIFIED",
        "target_origin": origin or "NOT PROVIDED",
        "required_passed": core_pass,
        "required_total": len(core),
        "optional_passed": optional_pass,
        "optional_total": len(optional),
        "required_suite_complete": bool(core) and core_pass == len(core),
        "all_checks_complete": bool(cases) and all(c["result"] == "PASS" for c in cases),
        "overall_kpi_status": "NOT YET VERIFIED",
        "note": (
            "This validates current browser flows only; optional authenticated and research "
            "services remain pending when not configured. No retroactive M40 proof, "
            "full UX sign-off or automatic R7-1 certification is implied."
        ),
        "cases": cases,
    }


def render_markdown(report: dict) -> str:
    lines = [
        "# ECHOREPO R7-1 — Phase 3 browser acceptance",
        "",
        f"- UTC: {report['generated_at_utc']}",
        f"- Commit: {report['tested_commit']}",
        f"- Environment: **{report['environment_mode']}**",
        f"- Target origin: {report['target_origin']}",
        f"- Required checks: **{report['required_passed']} / {report['required_total']}**",
        f"- Optional checks: **{report['optional_passed']} / {report['optional_total']}**",
        "- **Overall KPI: NOT YET VERIFIED** (consortium sign-off and external validation pending)",
        "",
        "| Check | Component | Scope | Outcome |",
        "|---|---|---|---|",
    ]
    for entry in report["cases"]:
        scope = "required" if entry["required"] else "optional"
        lines.append(
            f"| {entry['id']} | {entry['component']} | {scope} | {entry['result']} |"
        )
    lines.extend(["", report["note"], ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xml", type=Path, required=True)
    parser.add_argument("--matrix", type=Path, default=Path("docs/R7_1_PHASE3_MATRIX.json"))
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--commit", default=os.getenv("GITHUB_SHA", ""))
    parser.add_argument("--require-core", action="store_true",
                        help="Return nonzero unless all 14 required browser checks passed")
    parser.add_argument("--require-all", action="store_true",
                        help="Return nonzero unless required AND optional checks passed")
    args = parser.parse_args()

    # Only write the origin, never URLs that can contain credentials or query tokens.
    raw_origin = os.getenv("R71_PHASE3_BASE_URL", "")
    parsed = urlsplit(raw_origin)
    origin = (
        f"{parsed.scheme}://{parsed.hostname}{':' + str(parsed.port) if parsed.port else ''}"
        if parsed.hostname and parsed.scheme in {"http", "https"} else ""
    )
    matrix = json.loads(args.matrix.read_text(encoding="utf-8"))
    result = evaluate(
        matrix, load_junit(args.xml),
        mode=os.getenv("R71_PHASE3_RUN", ""), commit=args.commit,
        origin=origin,
    )
    for path in (args.json, args.markdown):
        path.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(result), encoding="utf-8")
    print(
        f"R7-1 Phase 3 ({result['environment_mode']}): "
        f"{result['required_passed']}/{result['required_total']} required and "
        f"{result['optional_passed']}/{result['optional_total']} optional checks PASSED."
    )
    if args.require-all and not result["all_checks_complete"]:
        return 1
    if args.require_core and not result["required_suite_complete"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
