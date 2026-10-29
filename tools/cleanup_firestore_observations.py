#!/usr/bin/env python3
"""One-time ECHOREPO Firestore SOIL_DIVER observations cleanup.

Only removes data[*].info.observations when the step type is SOIL_DIVER
and the observation contains the exact case-sensitive substring KEYWORD.

Dry run is the default. Writes a private CSV backup before any update.
Firestore's array API requires replacing the whole `data` array; this script
re-reads each document and uses a last-update precondition to avoid clobbering
concurrent edits. No PostgreSQL, MinIO, SQLite, or other Firebase fields touched.

Usage examples:
    # Dry run, scan all samples, create a backup CSV, get project ID from .env or service-account file:
    python3 tools/cleanup_firestore_observations.py
    # Apply changes to Firestore, but only if the project ID matches:
    python3 tools/cleanup_firestore_observations.py   --apply   --confirm-project my-project-id
"""
from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
KEYWORD = "JRC"  # Exact, case-sensitive substring. Never match JRC alone.
BACKUP_COLUMNS = (
    "firebase_project", "document_path", "user_id", "sample_id",
    "step_index", "old_observations", "document_update_time",
)


def matching_steps(data):
    """Return (array index, original text) for eligible SOIL_DIVER entries."""
    if not isinstance(data, list):
        return []
    result = []
    for index, step in enumerate(data):
        if not isinstance(step, dict) or step.get("type") != "SOIL_DIVER":
            continue
        info = step.get("info")
        if not isinstance(info, dict):
            continue
        observation = info.get("observations")
        if isinstance(observation, str) and KEYWORD in observation:
            result.append((index, observation))
    return result


def strip_observations(data, expected):
    """Create an edited copy only if every old text matches the scanned value."""
    if not isinstance(data, list):
        return None
    result = copy.deepcopy(data)
    for index, original in expected:
        if index >= len(result):
            return None
        step = result[index]
        if (not isinstance(step, dict) or step.get("type") != "SOIL_DIVER"
                or not isinstance(step.get("info"), dict)
                or step["info"].get("observations") != original):
            return None
        del step["info"]["observations"]
    return result


def restore_observations(data, expected):
    """Restore only fields that are currently missing; never overwrite new text."""
    if not isinstance(data, list):
        return None
    result = copy.deepcopy(data)
    for index, original in expected:
        if index >= len(result):
            return None
        step = result[index]
        if (not isinstance(step, dict) or step.get("type") != "SOIL_DIVER"
                or not isinstance(step.get("info"), dict)):
            return None
        # Avoid replacing legitimate observations introduced after cleanup.
        if "observations" in step["info"] and step["info"]["observations"] not in (None, ""):
            return None
        step["info"]["observations"] = original
    return result


def credentials_file():
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/echorepo/keys/firebase-sa.json")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    elif not path.exists():
        # Same host-side fallback as pull_and_enrich_samples.py.
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


def default_report_path():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return ROOT / "data" / "maintenance" / f"firebase_echomet_backup_{stamp}_{uuid4().hex[:8]}.csv"


def private_csv(path, rows, columns=BACKUP_COLUMNS):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # O_EXCL prevents accidentally replacing an older backup.
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


def collect_matches(db, project, only_sample_id=None, only_user_id=None):
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
        matches = matching_steps((snap.to_dict() or {}).get("data"))
        if not matches:
            continue
        docs[snap.reference.path] = (uid, snap.id, matches, snap.update_time)
        if scanned % 500 == 0:
            print(f"Scanned {scanned} documents...", flush=True)
    print(f"Scanned {scanned} sample documents; matched {len(docs)} documents / "
          f"{sum(len(x[2]) for x in docs.values())} observation values.")
    rows = []
    for path, (uid, sid, matches, updated) in sorted(docs.items()):
        for index, text in matches:
            rows.append({
                "firebase_project": project,
                "document_path": path,
                "user_id": uid,
                "sample_id": sid,
                "step_index": index,
                "old_observations": text,
                "document_update_time": str(updated) if updated is not None else "",
            })
    return docs, rows


def apply_matches(db, LastUpdateOption, docs):
    edited = 0
    fields_removed = 0
    skipped = 0
    failed = 0
    for path, (_, sid, expected, _) in sorted(docs.items()):
        ref = db.document(path)
        try:
            latest = ref.get()
            if not latest.exists:
                print(f"SKIP {path}: deleted since scan")
                skipped += 1
                continue
            current = (latest.to_dict() or {}).get("data")
            new_data = strip_observations(current, expected)
            if new_data is None:
                print(f"SKIP {path}: observations changed since scan")
                skipped += 1
                continue
            # Firestore does not support updating an individual array element.
            # Update only `data`, guarded by the current document update_time.
            ref.update({"data": new_data}, option=LastUpdateOption(latest.update_time))
            edited += 1
            fields_removed += len(expected)
            print(f"UPDATED {path} (removed {len(expected)} observation field(s))")
        except Exception as exc:
            failed += 1
            print(f"FAILED {path}: {type(exc).__name__}: {exc}", file=sys.stderr)
    print(f"Result: updated_documents={edited}, removed_fields={fields_removed}, "
          f"skipped={skipped}, failed={failed}")
    return failed


def restore_backup(db, LastUpdateOption, backup_path, project, dry_run):
    with backup_path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not set(BACKUP_COLUMNS).issubset(reader.fieldnames or []):
            raise ValueError("CSV is missing required backup columns")
        records = list(reader)
    docs = defaultdict(list)
    for row in records:
        if row["firebase_project"] != project:
            raise ValueError("Backup belongs to a different Firebase project")
        docs[row["document_path"]].append((int(row["step_index"]), row["old_observations"]))
    print(f"Restore candidates: {len(docs)} documents / {len(records)} observations")
    if dry_run:
        print("DRY RUN: no Firebase documents modified")
        return 0
    restored = 0
    skipped = 0
    failed = 0
    for path, expected in sorted(docs.items()):
        try:
            ref = db.document(path)
            snap = ref.get()
            if not snap.exists:
                skipped += 1
                continue
            data = restore_observations((snap.to_dict() or {}).get("data"), expected)
            if data is None:
                print(f"SKIP restore {path}: field is no longer empty or step changed")
                skipped += 1
                continue
            ref.update({"data": data}, option=LastUpdateOption(snap.update_time))
            print(f"RESTORED {path}")
            restored += 1
        except Exception as exc:
            failed += 1
            print(f"FAILED restore {path}: {exc}", file=sys.stderr)
    print(f"Restore result: restored={restored}, skipped={skipped}, failed={failed}")
    return failed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually modify Firestore; default is dry run")
    parser.add_argument("--confirm-project", help="Required with --apply; must equal the Firebase project ID")
    parser.add_argument("--sample-id", help="Optional exact sample document ID restriction")
    parser.add_argument("--user-id", help="Optional exact parent user document ID restriction")
    parser.add_argument("--report", type=Path, help="New private CSV backup/report filename; never overwritten")
    parser.add_argument("--restore-from", type=Path, help="Restore removed fields using a prior backup CSV")
    args = parser.parse_args(argv)

    db, LastUpdateOption, project = firebase_connection()
    print(f"Firebase project: {project}")
    print(f"Operation: {'APPLY' if args.apply else 'DRY RUN'}; keyword: {KEYWORD!r} (case-sensitive)")
    if args.apply and args.confirm_project != project:
        parser.error(f"--apply requires --confirm-project {project}")

    if args.restore_from:
        if args.sample_id or args.user_id or args.report:
            parser.error("--restore-from cannot be combined with --sample-id/--user-id/--report")
        return 1 if restore_backup(db, LastUpdateOption, args.restore_from, project, not args.apply) else 0

    docs, rows = collect_matches(db, project, args.sample_id, args.user_id)
    report = args.report or default_report_path()
    private_csv(report, rows)
    print(f"Backup/report created: {report} (mode 0600)")
    for row in rows[:15]:
        preview = row["old_observations"].replace("\r", " ").replace("\n", " ")[:150]
        print(f"  {row['document_path']} data[{row['step_index']}]: {preview!r}")
    if len(rows) > 15:
        print(f"  ... {len(rows)-15} more matches in CSV backup")
    if not args.apply:
        print("DRY RUN: nothing changed in Firebase. To proceed, rerun with --apply --confirm-project PROJECT_ID")
        return 0
    if not docs:
        print("No matching observations; nothing to update")
        return 0
    return 1 if apply_matches(db, LastUpdateOption, docs) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
