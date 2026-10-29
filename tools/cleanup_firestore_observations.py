#!/usr/bin/env python3
"""Safely remove selected nested fields from ECHOREPO Firestore sample steps.

Firestore layout:
  collection_group('samples') / sample document / data[<index>]
    -> {'type': STEP_TYPE, 'info': {FIELD: TEXT, ...}, ...}

Examples:
  # Dry-run: METALS.info equal to a particular processing placeholder:
  python3 tools/cleanup_firestore_observations.py \
      --step-type METALS --field info \
      --equals 'to put in the queue to analyse'

  # Dry-run: SOIL_DIVER observations containing EchoMet:
  python3 tools/cleanup_firestore_observations.py \
      --step-type SOIL_DIVER --field observations --contains EchoMet

  # Apply after checking the CSV backup and confirming the project ID:
  python3 tools/cleanup_firestore_observations.py \
      --step-type METALS --field info \
      --equals 'to put in the queue to analyse' \
      --apply --confirm-project echosoil

  # Restore an earlier cleanup (dry-run first):
  python3 tools/cleanup_firestore_observations.py --restore-from PATH.csv
  python3 tools/cleanup_firestore_observations.py --restore-from PATH.csv \
      --apply --confirm-project echosoil

Dry-run is the default. A private CSV backup is written before every update.
The whole `data` array is replaced because Firestore does not support editing
one array element; a last-update-time precondition prevents concurrent clobbering.
Only the selected field is removed. No changes to Postgres/SQLite/MinIO.
"""
from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
BACKUP_COLUMNS = (
    "firebase_project", "document_path", "user_id", "sample_id",
    "step_type", "field", "step_index", "old_value",
    "document_update_time", "match_mode", "match_text", "ignore_case",
)
LEGACY_BACKUP_COLUMNS = (
    "firebase_project", "document_path", "user_id", "sample_id",
    "step_index", "old_observations", "document_update_time",
)


@dataclass(frozen=True)
class MatchRule:
    step_type: str
    field: str
    mode: str  # 'equals' or 'contains'
    text: str
    ignore_case: bool = False

    def matches(self, value: object) -> bool:
        if not isinstance(value, str):
            return False
        a, b = value, self.text
        if self.ignore_case:
            a, b = a.casefold(), b.casefold()
        return a == b if self.mode == "equals" else b in a


def matching_steps(data: object, rule: MatchRule) -> list[tuple[int, str]]:
    """Find all matching step.info[field] strings; do not modify data."""
    if not isinstance(data, list):
        return []
    found = []
    for index, step in enumerate(data):
        if not isinstance(step, dict) or step.get("type") != rule.step_type:
            continue
        info = step.get("info")
        if not isinstance(info, dict):
            continue
        value = info.get(rule.field)
        if rule.matches(value):
            found.append((index, value))
    return found


def remove_matching_fields(data: object, rule: MatchRule,
                           expected: list[tuple[int, str]]) -> list | None:
    """Return edited copy if every matched value is unchanged since scan."""
    if not isinstance(data, list):
        return None
    new_data = copy.deepcopy(data)
    for index, old_value in expected:
        if not isinstance(index, int) or index < 0 or index >= len(new_data):
            return None
        step = new_data[index]
        if (not isinstance(step, dict)
                or step.get("type") != rule.step_type
                or not isinstance(step.get("info"), dict)
                or step["info"].get(rule.field) != old_value
                or not rule.matches(old_value)):
            return None
        del step["info"][rule.field]
    return new_data


def restore_fields(data: object,
                   expected: list[tuple[int, str, str, str]]) -> list | None:
    """Restore only absent keys; never overwrite existing values."""
    if not isinstance(data, list):
        return None
    new_data = copy.deepcopy(data)
    seen = set()
    for index, step_type, field, old_value in expected:
        key = (index, step_type, field)
        if key in seen or index < 0 or index >= len(new_data):
            return None
        seen.add(key)
        step = new_data[index]
        if (not isinstance(step, dict)
                or step.get("type") != step_type
                or not isinstance(step.get("info"), dict)
                or field in step["info"]):
            return None
        step["info"][field] = old_value
    return new_data


def credentials_file() -> Path:
    # Same .env, env precedence, and host-side service-account path fallback as
    # tools/pull_and_enrich_samples.py.
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/echorepo/keys/firebase-sa.json")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    elif not path.exists():
        path = ROOT / path.relative_to("/")
    if not path.is_file():
        raise FileNotFoundError(f"Firebase service-account file not found: {path}")
    return path


def firebase_connection():
    import firebase_admin
    from firebase_admin import credentials, firestore
    path = credentials_file()
    sa = json.loads(path.read_text(encoding="utf-8"))
    service_project = str(sa.get("project_id") or "")
    env_project = str(os.getenv("FIREBASE_PROJECT_ID") or "")
    if service_project and env_project and service_project != env_project:
        raise RuntimeError(
            f"FIREBASE_PROJECT_ID={env_project!r} differs from service-account project {service_project!r}"
        )
    project = env_project or service_project
    if not project:
        raise RuntimeError("No Firebase project ID in .env or service-account file")
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(str(path)), {"projectId": project})
    db = firestore.client()
    return db, lambda updated_at: db.write_option(last_update_time=updated_at), project


def default_report_path(rule: MatchRule) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # Use a generic name: the report's columns record exact criteria.
    return ROOT / "data" / "maintenance" / f"firestore_fields_backup_{stamp}_{uuid4().hex[:8]}.csv"


def private_csv(path: Path, rows: list[dict], columns=BACKUP_COLUMNS):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def collect_matches(db, project: str, rule: MatchRule,
                    only_sample_id: str | None = None,
                    only_user_id: str | None = None):
    docs = {}
    scanned = 0
    print("Scanning Firestore collection_group('samples')...", flush=True)
    for snap in db.collection_group("samples").stream(timeout=300):
        scanned += 1
        if only_sample_id and snap.id != only_sample_id:
            continue
        parent = snap.reference.parent.parent
        uid = parent.id if parent else ""
        if only_user_id and uid != only_user_id:
            continue
        matches = matching_steps((snap.to_dict() or {}).get("data"), rule)
        if not matches:
            continue
        docs[snap.reference.path] = (uid, snap.id, matches, snap.update_time)
        if scanned % 500 == 0:
            print(f"Scanned {scanned} documents...", flush=True)
    count = sum(len(entry[2]) for entry in docs.values())
    print(f"Scanned {scanned} sample documents; matched {len(docs)} documents / {count} field values.")
    rows = []
    for path, (uid, sid, matches, updated) in sorted(docs.items()):
        for index, value in matches:
            rows.append({
                "firebase_project": project,
                "document_path": path,
                "user_id": uid,
                "sample_id": sid,
                "step_type": rule.step_type,
                "field": rule.field,
                "step_index": index,
                "old_value": value,
                "document_update_time": str(updated) if updated is not None else "",
                "match_mode": rule.mode,
                "match_text": rule.text,
                "ignore_case": str(rule.ignore_case),
            })
    return docs, rows


def apply_matches(db, LastUpdateOption, docs, rule: MatchRule) -> int:
    edited = removed = skipped = failed = 0
    for path, (_, _sid, expected, _updated) in sorted(docs.items()):
        ref = db.document(path)
        try:
            latest = ref.get()
            if not latest.exists:
                print(f"SKIP {path}: deleted since scan")
                skipped += 1
                continue
            new_data = remove_matching_fields((latest.to_dict() or {}).get("data"), rule, expected)
            if new_data is None:
                print(f"SKIP {path}: target field changed since scan")
                skipped += 1
                continue
            # Array elements cannot be patched separately in Firestore.
            # Guard the whole-array update against concurrent edits.
            ref.update({"data": new_data}, option=LastUpdateOption(latest.update_time))
            edited += 1
            removed += len(expected)
            print(f"UPDATED {path}: removed {len(expected)} {rule.step_type}.{rule.field} field(s)")
        except Exception as exc:
            failed += 1
            print(f"FAILED {path}: {type(exc).__name__}: {exc}", file=sys.stderr)
    print(f"Result: updated_documents={edited}, removed_fields={removed}, "
          f"skipped={skipped}, failed={failed}")
    return 1 if failed or skipped else 0


def load_backup(backup_path: Path, project: str):
    with backup_path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        headers = set(reader.fieldnames or [])
        new_style = set(BACKUP_COLUMNS).issubset(headers)
        old_style = set(LEGACY_BACKUP_COLUMNS).issubset(headers)
        if not (new_style or old_style):
            raise ValueError("Backup is missing required columns")
        records = list(reader)

    docs = defaultdict(list)
    seen = set()
    for row in records:
        if row["firebase_project"] != project:
            raise ValueError("Backup belongs to a different Firebase project")
        path = row["document_path"]
        # Path must be a Firestore document under a samples collection.
        parts = path.split("/")
        if len(parts) < 2 or len(parts) % 2 != 0 or parts[-2] != "samples" or not all(parts):
            raise ValueError(f"Invalid sample document path in backup: {path!r}")
        idx = int(row["step_index"])
        if idx < 0:
            raise ValueError("Negative step index in backup")
        step_type = row["step_type"] if new_style else "SOIL_DIVER"
        field = row["field"] if new_style else "observations"
        value = row["old_value"] if new_style else row["old_observations"]
        if not step_type or not field or not isinstance(value, str):
            raise ValueError("Missing field metadata in backup")
        key = (path, idx, step_type, field)
        if key in seen:
            raise ValueError(f"Duplicate backup entry for {path} data[{idx}].{field}")
        seen.add(key)
        docs[path].append((idx, step_type, field, value))
    return docs, len(records)


def restore_backup(db, LastUpdateOption, backup_path: Path, project: str,
                   dry_run: bool) -> int:
    docs, records_count = load_backup(backup_path, project)
    print(f"Restore candidates: {len(docs)} documents / {records_count} fields")
    if dry_run:
        print("DRY RUN: no Firebase documents modified")
        return 0
    restored = skipped = failed = 0
    for path, expected in sorted(docs.items()):
        try:
            ref = db.document(path)
            snap = ref.get()
            if not snap.exists:
                print(f"SKIP restore {path}: document missing")
                skipped += 1
                continue
            new_data = restore_fields((snap.to_dict() or {}).get("data"), expected)
            if new_data is None:
                print(f"SKIP restore {path}: target field now exists or step changed")
                skipped += 1
                continue
            ref.update({"data": new_data}, option=LastUpdateOption(snap.update_time))
            print(f"RESTORED {path}")
            restored += 1
        except Exception as exc:
            failed += 1
            print(f"FAILED restore {path}: {type(exc).__name__}: {exc}", file=sys.stderr)
    print(f"Restore result: restored={restored}, skipped={skipped}, failed={failed}")
    return 1 if failed or skipped else 0


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--step-type", "--category", dest="step_type",
                        help="Exact data[*].type, e.g. METALS or SOIL_DIVER")
    parser.add_argument("--field", help="Exact key inside data[*].info, e.g. info or observations")
    patterns = parser.add_mutually_exclusive_group()
    patterns.add_argument("--contains", help="Match a case-sensitive substring")
    patterns.add_argument("--equals", help="Match an entire string exactly")
    parser.add_argument("--ignore-case", action="store_true", help="Make string matching case-insensitive")
    parser.add_argument("--apply", action="store_true", help="Write to Firestore; otherwise dry-run")
    parser.add_argument("--confirm-project", help="Required with --apply, must equal Firebase project")
    parser.add_argument("--sample-id", help="Restrict to exact sample document ID")
    parser.add_argument("--user-id", help="Restrict to exact parent user document ID")
    parser.add_argument("--report", type=Path, help="New private CSV backup file (not overwritten)")
    parser.add_argument("--restore-from", type=Path, help="Restore from this backup CSV instead of scanning")
    return parser


def main(argv=None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)

    if args.restore_from:
        if any((args.step_type, args.field, args.contains is not None,
                args.equals is not None, args.ignore_case, args.sample_id,
                args.user_id, args.report)):
            parser.error("--restore-from cannot be combined with matching or scan options")
    else:
        if not args.step_type or not args.field:
            parser.error("--step-type and --field are required")
        if args.contains is None and args.equals is None:
            parser.error("Specify exactly one of --contains or --equals")
        if args.contains == "" or args.equals == "":
            parser.error("Empty matching strings are not allowed")
        if any(ch in args.field for ch in ".`/[]") or not args.field.strip():
            parser.error("--field must name one direct key of the step's info map")

    db, LastUpdateOption, project = firebase_connection()
    print(f"Firebase project: {project}")
    if args.apply and args.confirm_project != project:
        parser.error(f"--apply requires --confirm-project {project}")

    if args.restore_from:
        print(f"Operation: {'RESTORE' if args.apply else 'RESTORE DRY RUN'}")
        return restore_backup(db, LastUpdateOption, args.restore_from, project, not args.apply)

    rule = MatchRule(args.step_type, args.field,
                     "equals" if args.equals is not None else "contains",
                     args.equals if args.equals is not None else args.contains,
                     args.ignore_case)
    print(f"Operation: {'APPLY' if args.apply else 'DRY RUN'}; "
          f"target={rule.step_type}.info.{rule.field}; "
          f"{rule.mode}={rule.text!r}; ignore_case={rule.ignore_case}")

    docs, rows = collect_matches(db, project, rule, args.sample_id, args.user_id)
    report = args.report or default_report_path(rule)
    private_csv(report, rows)
    print(f"Backup/report created: {report} (mode 0600)")
    for row in rows[:15]:
        preview = row["old_value"].replace("\r", " ").replace("\n", " ")[:150]
        print(f"  {row['document_path']} data[{row['step_index']}].info.{row['field']}: {preview!r}")
    if len(rows) > 15:
        print(f"  ... {len(rows) - 15} additional matching fields in backup")

    if not args.apply:
        print("DRY RUN: nothing changed in Firebase. To proceed, rerun with "
              "--apply --confirm-project PROJECT_ID")
        return 0
    if not docs:
        print("No matches; nothing to update")
        return 0
    return apply_matches(db, LastUpdateOption, docs, rule)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
