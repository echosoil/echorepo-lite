#!/usr/bin/env python3
"""Import an unsplit biodiversity matrix without buffering it or archiving it.

Modes:
  --file    Import a pre-joined CSV/TSV/XLSX file directly from a server path.
  --table + --taxonomy
            Join an original wide BIOM-style TSV and taxa_*.txt on the fly.
            A temporary SQLite index stores only taxonomy, not the giant matrix.

Run --plan first; --apply is required for database mutations.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import math
import sqlite3
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path.cwd() / ".env")

from echorepo.services.biodiversity_import import (  # noqa: E402
    _is_biodiversity_header,
    _open_biodiversity_rows,
    import_biodiversity_file,
)
from tools.prepare_split_biodiversity_for_echorepo_premin10 import (  # noqa: E402
    OUTPUT_TAXONOMY_COLUMNS,
    canonical_sample_columns,
    parse_taxonomy_annotation,
)

LOG = logging.getLogger("biodiversity-large-import")


def file_digest(path: Path, chunk_size: int = 8 * 1024 * 1024) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.digest()


def joined_identity(
    table: Path,
    taxonomy: Path,
    marker: str,
    min_total_reads: float,
) -> str:
    """Stable identity for the *pair* plus data-affecting preprocessing rules."""
    digest = hashlib.sha256(b"echorepo-biodiversity-joined-v1\0")
    digest.update(marker.encode("ascii"))
    digest.update(b"\0")
    digest.update(repr(min_total_reads).encode("ascii"))
    for label, path in (("table", table), ("taxonomy", taxonomy)):
        LOG.warning("Hashing %s (%s bytes): %s", label, path.stat().st_size, path)
        digest.update(label.encode("ascii") + b"\0")
        digest.update(file_digest(path))
    return digest.hexdigest()


class SourcedRow(list):
    """List-like virtual row retaining its original TSV line number."""

    def __init__(self, fields, line_number: int):
        super().__init__(fields)
        self.source_row_number = line_number


class JoinedTsvSource:
    """Lazy OTU + taxonomy rows; only the taxonomy index uses scratch space."""

    def __init__(
        self,
        table: Path,
        taxonomy: Path,
        marker: str,
        min_total_reads: float,
        fail_on_missing_taxonomy: bool,
        scratch_dir: Path | None,
    ):
        self.table = table
        self.taxonomy = taxonomy
        self.marker = marker
        self.min_total_reads = min_total_reads
        self.fail_on_missing_taxonomy = fail_on_missing_taxonomy
        self.scratch_dir = scratch_dir
        self.source_sample_column_numbers = {}
        self._table_stat = table.stat()
        self._taxonomy_stat = taxonomy.stat()

    def assert_unchanged(self):
        for path, original in (
            (self.table, self._table_stat),
            (self.taxonomy, self._taxonomy_stat),
        ):
            now = path.stat()
            if (now.st_size, now.st_mtime_ns) != (
                original.st_size, original.st_mtime_ns
            ):
                raise RuntimeError(f"Input changed during import: {path}")

    def _build_taxonomy_index(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE tax (otu TEXT PRIMARY KEY, "
            + ", ".join(f"{col.lower()} TEXT" for col in OUTPUT_TAXONOMY_COLUMNS)
            + ")"
        )
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA cache_size=-65536")
        columns = ", ".join(["otu", *(col.lower() for col in OUTPUT_TAXONOMY_COLUMNS)])
        placeholders = ", ".join("?" for _ in range(8))
        insert = f"INSERT OR IGNORE INTO tax ({columns}) VALUES ({placeholders})"
        select = "SELECT " + ", ".join(
            col.lower() for col in OUTPUT_TAXONOMY_COLUMNS
        ) + " FROM tax WHERE otu = ?"
        batch = []
        n = 0
        with self.taxonomy.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.reader(handle, delimiter="\t"):
                if not row or row[0].startswith("#"):
                    continue
                if len(row) < 2 or not row[0].strip():
                    raise ValueError(f"Invalid taxonomy line near row {n + 1}")
                ranks = parse_taxonomy_annotation(row[1])
                batch.append((row[0].strip(), *(
                    ranks.get(col, "") for col in OUTPUT_TAXONOMY_COLUMNS
                )))
                n += 1
                if len(batch) >= 2_000:
                    self._insert_taxonomy_batch(connection, batch, insert, select)
                    batch.clear()
                if n % 200_000 == 0:
                    LOG.warning("Indexed %s taxonomy rows", f"{n:,}")
        if batch:
            self._insert_taxonomy_batch(connection, batch, insert, select)
        connection.commit()
        if n == 0:
            raise ValueError("No taxonomy records found")
        LOG.warning("Taxonomy index ready (%s source rows)", f"{n:,}")

    @staticmethod
    def _insert_taxonomy_batch(connection, batch, insert, select):
        # Ignore an identical duplicate, but reject conflicting assignments.
        before = connection.total_changes
        connection.executemany(insert, batch)
        if connection.total_changes - before != len(batch):
            for row in batch:
                actual = connection.execute(select, (row[0],)).fetchone()
                if actual != tuple(row[1:]):
                    raise ValueError(
                        f"Conflicting taxonomy for OTU {row[0]!r}"
                    )

    def open_biodiversity_rows(self):
        temp = tempfile.TemporaryDirectory(
            prefix="echorepo-biodiv-tax-",
            dir=str(self.scratch_dir) if self.scratch_dir else None,
        )
        connection = None
        stream = None
        try:
            connection = sqlite3.connect(str(Path(temp.name) / "taxonomy.sqlite"))
            self._build_taxonomy_index(connection)
            stream = self.table.open("r", encoding="utf-8-sig", newline="")
            reader = csv.reader(stream, delimiter="\t")

            header = None
            for row in reader:
                if not row:
                    continue
                first = row[0].strip().lstrip("#").strip().lower()
                if first in {"otu id", "otu_id", "feature id", "feature_id"}:
                    header = [val.strip() for val in row]
                    break
            if header is None:
                raise ValueError("No OTU header found in source TSV")

            selected, _dropped, actual_marker = canonical_sample_columns(
                header, self.marker
            )
            if actual_marker != self.marker:
                raise ValueError("Molecular-marker mismatch")
            labels = [label.upper() for _, label in selected]
            self.source_sample_column_numbers = {
                label.upper(): idx + 1 for idx, label in selected
            }
            if len(labels) != len(set(labels)):
                raise ValueError("Duplicate normalized sample columns in source TSV")
            expected_lengths = {len(header)}
            taxonomy_field = any(c.lower() == "taxonomy" for c in header)
            if taxonomy_field:
                expected_lengths.add(len(header) - 1)
            lookup = "SELECT " + ", ".join(
                col.lower() for col in OUTPUT_TAXONOMY_COLUMNS
            ) + " FROM tax WHERE otu = ?"
            db_cursor = connection.cursor()
            stats = {"rows": 0, "below_min": 0, "missing_taxonomy": 0}

            def rows():
                for line in reader:
                    if not line or not line[0].strip() or line[0].startswith("#"):
                        continue
                    stats["rows"] += 1
                    if len(line) not in expected_lengths:
                        raise ValueError(
                            f"OTU {line[0]!r} has {len(line)} columns; "
                            f"expected {sorted(expected_lengths)}"
                        )
                    otu = line[0].strip()
                    sample_values = [line[idx] for idx, _ in selected]
                    total = 0.0
                    for raw in sample_values:
                        value = raw.strip()
                        if not value:
                            continue
                        try:
                            n = float(value)
                        except ValueError as exc:
                            raise ValueError(
                                f"Non-numeric abundance {raw!r} in OTU {otu!r}"
                            ) from exc
                        if not math.isfinite(n) or n < 0:
                            raise ValueError(
                                f"Invalid abundance {raw!r} in OTU {otu!r}"
                            )
                        total += n

                    if total < self.min_total_reads:
                        stats["below_min"] += 1
                        continue
                    taxonomy = db_cursor.execute(lookup, (otu,)).fetchone()
                    if taxonomy is None:
                        stats["missing_taxonomy"] += 1
                        if self.fail_on_missing_taxonomy:
                            raise ValueError(f"No taxonomy found for OTU {otu!r}")
                        taxonomy = ("",) * 7

                    if stats["rows"] % 50_000 == 0:
                        LOG.warning(
                            "Joined %s OTUs, removed %s below threshold",
                            f"{stats['rows']:,}",
                            f"{stats['below_min']:,}",
                        )
                    yield SourcedRow(
                        [otu, *sample_values, *taxonomy], reader.line_num
                    )

            def close():
                try:
                    if stream is not None:
                        stream.close()
                    if connection is not None:
                        connection.close()
                finally:
                    temp.cleanup()
                    LOG.warning("Joined TSV stats: %s", stats)

            return (
                ["OTU ID", *labels, *OUTPUT_TAXONOMY_COLUMNS],
                rows(),
                close,
                f"streamed TSV + SQLite taxonomy: {self.table.name}",
            )
        except Exception:
            if stream is not None:
                stream.close()
            if connection is not None:
                connection.close()
            temp.cleanup()
            raise


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--file", type=Path, help="Pre-joined CSV/TSV/XLSX")
    inputs.add_argument("--table", type=Path, help="Original wide OTU TSV")
    parser.add_argument("--taxonomy", type=Path, help="Matching taxa_*.txt (with --table)")
    parser.add_argument("--marker", choices=["16S", "ITS"], help="Required for --table")
    parser.add_argument("--scratch-dir", type=Path, help="Temporary taxonomy-index directory")
    parser.add_argument("--min-total-reads", type=float, default=10.0)
    parser.add_argument("--fail-on-missing-taxonomy", action="store_true")
    parser.add_argument(
        "--post-min10", action="store_true",
        help="For a *single unsplit* --file, apply the total<10 filter in importer",
    )
    parser.add_argument("--uploader", default="server-cli")
    parser.add_argument("--apply", action="store_true", help="Commit the import")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.table and (not args.taxonomy or not args.marker):
        raise SystemExit("--table requires --taxonomy and --marker")
    if args.file and (args.taxonomy or args.marker):
        raise SystemExit("--taxonomy/--marker apply only to --table")
    if args.table and args.post_min10:
        raise SystemExit("--post-min10 is only for a single --file")
    if args.min_total_reads < 0 or not math.isfinite(args.min_total_reads):
        raise SystemExit("--min-total-reads must be finite and nonnegative")
    paths = [p for p in (args.file, args.table, args.taxonomy) if p]
    for path in paths:
        if not path.is_file():
            raise SystemExit(f"File not found: {path}")
    if args.scratch_dir and not args.scratch_dir.is_dir():
        raise SystemExit(f"Scratch directory not found: {args.scratch_dir}")

    if args.file:
        source = args.file
        filename = source.name
        header, _rows, close, description = _open_biodiversity_rows(
            source, filename
        )
        try:
            if not _is_biodiversity_header(header):
                raise SystemExit("Input is not an ECHOREPO-compatible OTU matrix")
            sample_columns = [
                h for h in header if str(h).strip().upper().endswith(("-16S", "-ITS"))
            ]
        finally:
            close()
        print(
            f"[PLAN] Direct file: {source} ({source.stat().st_size:,} bytes); "
            f"{len(sample_columns)} candidate marker columns; {description}",
            flush=True,
        )
        if not args.apply:
            print("[PLAN] No database changes. Add --apply to import.", flush=True)
            return 0
        # This mode hashes the source in bounded chunks before import.
        kwargs = {"post_min10": args.post_min10}
    else:
        source = JoinedTsvSource(
            args.table, args.taxonomy, args.marker,
            args.min_total_reads, args.fail_on_missing_taxonomy,
            args.scratch_dir,
        )
        filename = args.table.name
        print(
            f"[PLAN] Join {args.table} + {args.taxonomy}, marker={args.marker}, "
            f"min-total-reads={args.min_total_reads:g}; no output matrix or "
            "MinIO archive will be written.",
            flush=True,
        )
        if not args.apply:
            print("[PLAN] No database changes. Add --apply to import.", flush=True)
            return 0
        kwargs = {
            "source_sha256": joined_identity(
                args.table, args.taxonomy, args.marker, args.min_total_reads
            ),
            "post_min10": False,  # Pre-filtered against complete source row.
        }

    print(
        "[WARN] Starting a single PostgreSQL transaction. "
        "Ensure the database volume has enough space for raw rows and WAL.",
        flush=True,
    )
    count = import_biodiversity_file(
        source,
        filename,
        args.uploader,
        archive_raw=False,
        **kwargs,
    )
    print(f"[OK] Imported {count:,} Phylum aggregate rows", flush=True)
    print("[OK] Original source was NOT copied to MinIO", flush=True)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    try:
        sys.exit(main())
    except Exception:
        LOG.exception("Large biodiversity import failed")
        raise
