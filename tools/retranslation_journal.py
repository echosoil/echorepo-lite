"""Durable CSV write-ahead journal for explicit free-text replacement."""

import csv
import fcntl
import json
import os
import stat
from pathlib import Path

COLUMNS = (
    "event", "table", "field", "keys_json", "original_json", "country_json",
    "previous_en_json", "new_en_json",
)


def encode(value):
    # JSON preserves NULL vs empty, quotes and newlines, and prevents formula
    # interpretation of source text in spreadsheet viewers.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class RetranslationJournal:
    def __init__(self, path, scope, resume=False):
        self.path = Path(path)
        self.scope = scope
        self.entries = {}
        flags = os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW
        if not resume:
            flags |= os.O_CREAT | os.O_EXCL
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
            ):
                raise ValueError("Journal must be an owner-only regular file")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.handle = os.fdopen(fd, "r+", encoding="utf-8", newline="")
        except BaseException:
            os.close(fd)
            raise
        try:
            self.writer = csv.writer(self.handle, lineterminator="\n")
            if resume:
                self._load()
            else:
                self.writer.writerow(COLUMNS)
                self.writer.writerow(("scope", "", "", encode(scope), "", "", "", ""))
                self._sync()
                # Persist the new directory entry before any database write.
                directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        except BaseException:
            self.close()
            raise

    def _sync(self):
        self.handle.flush()
        os.fsync(self.handle.fileno())

    def _load(self):
        self.handle.seek(0)
        raw = self.handle.read()
        if not raw.endswith("\n"):
            raise ValueError("Journal has an incomplete final record; inspect before resuming")
        self.handle.seek(0)
        reader = csv.reader(self.handle, strict=True)
        if tuple(next(reader, ())) != COLUMNS:
            raise ValueError("Invalid journal header")
        header = next(reader, ())
        if len(header) != len(COLUMNS) or header[0] != "scope":
            raise ValueError("Missing journal scope")
        if json.loads(header[3]) != self.scope:
            raise ValueError("Journal database, strategy or country scope does not match")
        for row in reader:
            if len(row) != len(COLUMNS) or row[0] not in {"pending", "committed", "unchanged", "reconciled"}:
                raise ValueError("Invalid journal record")
            event, table, field, keys, original, country, previous, new = row
            key = (table, field, tuple(json.loads(keys)))
            values = tuple(json.loads(value) for value in (original, country, previous, new))
            self.entries[key] = (event, *values)
        self.handle.seek(0, os.SEEK_END)

    def record(self, event, table, field, keys, original, country, previous, new):
        self.writer.writerow(
            (event, table, field, encode(keys), encode(original), encode(country),
             encode(previous), encode(new))
        )
        self._sync()
        self.entries[(table, field, tuple(keys))] = (event, original, country, previous, new)

    def resume_status(self, table, field, keys, original, country, current):
        """Skip completed rows; reconcile a crash between DB commit and journal event."""
        entry = self.entries.get((table, field, tuple(keys)))
        if entry is None:
            return "retry"
        event, old_source, old_country, previous, new = entry
        if (original, country) != (old_source, old_country):
            return "conflict"
        if current == new:
            if event == "pending":
                self.record("reconciled", table, field, keys, original, country, previous, new)
            return "done"
        if event == "pending" and current == previous:
            return "retry"
        return "conflict"

    def close(self):
        self.handle.close()
