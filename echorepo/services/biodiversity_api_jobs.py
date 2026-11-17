"""Bounded, disk-backed raw HTTP file staging for biodiversity imports.

This module intentionally does not import Flask or PostgreSQL: it can be tested
without starting the web application and never buffers the source in RAM.
"""
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from uuid import UUID, uuid4


VALID_NAME = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,198}\.(?:csv|tsv|txt)(?:\.gz)?$",
    re.I,
)
READ_SIZE = 4 * 1024 * 1024


class InvalidBiodiversityUpload(ValueError):
    """Malformed filename, empty request, or length mismatch."""


class BiodiversityUploadTooLarge(ValueError):
    """A multipart/streamed request exceeded the configured hard cap."""


@dataclass(frozen=True)
class StagedBiodiversityUpload:
    job_id: str
    filename: str
    path: Path
    size_bytes: int
    sha256: str


def validate_filename(filename: str) -> str:
    name = str(filename or "").strip()
    if not VALID_NAME.fullmatch(name):
        raise InvalidBiodiversityUpload(
            "X-Filename must end in CSV/TSV/TXT, optionally .gz, "
            "(letters, digits, underscores, periods or hyphens; no paths)."
        )
    return name


def staging_path(directory: Path, job_id: str, filename: str) -> Path:
    # UUID parsing prevents attacker-controlled paths even if a job ID were
    # somehow obtained from an external source.
    ident = UUID(job_id)
    name = validate_filename(filename)
    return directory / f"{ident.hex}{Path(name).suffix.lower()}"


def stage_biodiversity_stream(
    reader: BinaryIO,
    directory: Path,
    filename: str,
    *,
    max_bytes: int,
    content_length: int | None,
) -> StagedBiodiversityUpload:
    name = validate_filename(filename)
    if max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")
    if content_length is None:
        raise InvalidBiodiversityUpload(
            "Content-Length is required; use curl --data-binary @file."
        )
    if content_length <= 0:
        raise InvalidBiodiversityUpload("Upload body is empty")
    if content_length > max_bytes:
        raise BiodiversityUploadTooLarge(
            f"File is larger than configured limit of {max_bytes} bytes"
        )
    directory.mkdir(parents=True, exist_ok=True)
    job_id = str(uuid4())
    final = staging_path(directory, job_id, name)
    partial = directory / f"{UUID(job_id).hex}.partial"
    digest = hashlib.sha256()
    total = 0

    try:
        # exclusive creation: neither path may be attacker-selected
        with partial.open("xb") as out:
            while True:
                data = reader.read(min(READ_SIZE, max_bytes - total + 1))
                if not data:
                    break
                total += len(data)
                if total > max_bytes:
                    raise BiodiversityUploadTooLarge(
                        f"File exceeded configured limit of {max_bytes} bytes"
                    )
                out.write(data)
                digest.update(data)
            out.flush()
            os.fsync(out.fileno())

        if total != content_length:
            raise InvalidBiodiversityUpload(
                f"Upload incomplete: expected {content_length} bytes, received {total}"
            )

        os.replace(partial, final)
        return StagedBiodiversityUpload(
            job_id=job_id,
            filename=name,
            path=final,
            size_bytes=total,
            sha256=digest.hexdigest(),
        )
    except BaseException:
        partial.unlink(missing_ok=True)
        final.unlink(missing_ok=True)
        raise
