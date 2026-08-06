from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import PurePosixPath
from typing import Iterable

try:
    from minio import Minio
except ImportError:  # pragma: no cover - depends on deployment extras
    Minio = None


log = logging.getLogger(__name__)

DEFAULT_BUCKET = "echorepo-uploads"
_NOT_FOUND_CODES = {
    "NoSuchBucket",
    "NoSuchKey",
    "NoSuchObject",
    "NotFound",
    "XMinioInvalidObjectName",
}


class StorageError(RuntimeError):
    """Base error for MinIO-backed storage operations."""


class StorageNotConfigured(StorageError):
    """Raised when MinIO or its credentials are unavailable."""


class StorageObjectNotFound(StorageError):
    """Raised when a requested object does not exist."""


@dataclass(frozen=True)
class StoredObject:
    data: bytes
    content_type: str
    download_name: str


@dataclass(frozen=True)
class RawBiodiversityArchive:
    object_name: str
    sha256: str


def bucket_name() -> str:
    return os.getenv("MINIO_BUCKET", DEFAULT_BUCKET)


def get_client(*, required: bool = False):
    """Build a MinIO client from environment configuration."""
    if Minio is None:
        if required:
            raise StorageNotConfigured(
                "The MinIO Python package is not installed."
            )
        return None

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").strip()
    access_key = (
        os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
        or ""
    ).strip()
    secret_key = (
        os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
        or ""
    ).strip()

    secure = False
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint.removeprefix("https://")
    elif endpoint.startswith("http://"):
        endpoint = endpoint.removeprefix("http://")

    if not endpoint or not access_key or not secret_key:
        if required:
            raise StorageNotConfigured(
                "MinIO endpoint and credentials are not configured."
            )
        return None

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def _ensure_bucket(client) -> str:
    bucket = bucket_name()
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    except Exception as exc:
        raise StorageError(
            f"Cannot prepare MinIO bucket {bucket}: {exc}"
        ) from exc
    return bucket


def _is_not_found(exc: Exception) -> bool:
    return str(getattr(exc, "code", "")) in _NOT_FOUND_CODES


def _validate_object_name(object_name: str) -> str:
    cleaned = str(object_name or "").strip().lstrip("/")
    path = PurePosixPath(cleaned)

    if not cleaned or any(part in {"", ".", ".."} for part in path.parts):
        raise StorageError("Invalid MinIO object name")

    return cleaned


def get_canonical_object(obj_name: str) -> StoredObject:
    """Read a canonical CSV or ZIP object from MinIO."""
    relative_name = _validate_object_name(obj_name)
    object_name = f"canonical/{relative_name}"
    client = get_client(required=True)
    bucket = bucket_name()

    response = None
    try:
        response = client.get_object(bucket, object_name)
        data = response.read()
    except Exception as exc:
        if _is_not_found(exc):
            raise StorageObjectNotFound(
                f"Canonical object not found: {object_name}"
            ) from exc
        raise StorageError(
            f"Cannot read canonical object {object_name}: {exc}"
        ) from exc
    finally:
        if response is not None:
            try:
                response.close()
            finally:
                response.release_conn()

    content_type = (
        "application/zip"
        if relative_name.lower().endswith(".zip")
        else "text/csv"
    )

    return StoredObject(
        data=data,
        content_type=content_type,
        download_name=PurePosixPath(relative_name).name,
    )


def _put_bytes(
    *,
    client,
    bucket: str,
    object_name: str,
    data: bytes,
    content_type: str,
) -> None:
    object_name = _validate_object_name(object_name)
    try:
        client.put_object(
            bucket,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
    except Exception as exc:
        raise StorageError(
            f"Cannot upload MinIO object {object_name}: {exc}"
        ) from exc


def upload_canonical_csvs(
    csv_contents: dict[str, str],
    version_date: str,
) -> None:
    """Upload canonical CSVs to both dated and latest prefixes."""
    client = get_client()
    if client is None:
        log.warning("MinIO is not configured; skipping canonical CSV upload")
        return

    try:
        bucket = _ensure_bucket(client)
    except StorageError:
        log.exception("Cannot prepare MinIO for canonical CSV upload")
        return

    for filename, csv_text in csv_contents.items():
        if not csv_text:
            continue

        data = csv_text.encode("utf-8")
        for prefix in (
            f"canonical/{version_date}/",
            "canonical/latest/",
        ):
            object_name = prefix + filename
            try:
                _put_bytes(
                    client=client,
                    bucket=bucket,
                    object_name=object_name,
                    data=data,
                    content_type="text/csv",
                )
            except StorageError:
                log.exception("Cannot upload %s", object_name)


def upload_canonical_zip(
    zip_bytes: bytes,
    version_date: str,
) -> None:
    """Upload all.zip to both dated and latest prefixes."""
    client = get_client()
    if client is None:
        log.warning("MinIO is not configured; skipping canonical ZIP upload")
        return

    try:
        bucket = _ensure_bucket(client)
    except StorageError:
        log.exception("Cannot prepare MinIO for canonical ZIP upload")
        return

    for prefix in (
        f"canonical/{version_date}/",
        "canonical/latest/",
    ):
        object_name = prefix + "all.zip"
        try:
            _put_bytes(
                client=client,
                bucket=bucket,
                object_name=object_name,
                data=zip_bytes,
                content_type="application/zip",
            )
        except StorageError:
            log.exception("Cannot upload %s", object_name)


def latest_canonical_snapshot_date() -> str | None:
    """Return the latest valid YYYY-MM-DD canonical snapshot folder."""
    client = get_client()
    if client is None:
        return None

    bucket = bucket_name()
    dates: set[str] = set()

    try:
        objects = client.list_objects(
            bucket,
            prefix="canonical/",
            recursive=True,
        )
        for obj in objects:
            parts = obj.object_name.split("/")
            if (
                len(parts) != 3
                or parts[0] != "canonical"
                or parts[2] != "all.zip"
            ):
                continue

            candidate = parts[1]
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
                continue

            try:
                date.fromisoformat(candidate)
            except ValueError:
                continue

            dates.add(candidate)
    except Exception as exc:
        if _is_not_found(exc):
            return None
        log.error(
            "Error listing canonical snapshots from MinIO: %s",
            exc,
        )
        return None

    return max(dates) if dates else None


def archive_raw_biodiversity_upload(
    *,
    file_bytes: bytes,
    filename: str,
    uploader_id: str,
    aggregation_level: str = "Phylum",
) -> RawBiodiversityArchive:
    """Archive the original biodiversity upload as a ZIP in MinIO."""
    client = get_client(required=True)
    bucket = _ensure_bucket(client)

    original_filename = (
        os.path.basename(filename or "").strip()
        or "biodiversity_upload.xlsx"
    )
    safe_filename = re.sub(
        r"[^A-Za-z0-9._-]+",
        "_",
        original_filename,
    ).strip("._") or "biodiversity_upload.xlsx"

    sha256 = hashlib.sha256(file_bytes).hexdigest()
    now = datetime.now(timezone.utc)
    uploaded_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    date_part = now.date().isoformat()

    manifest = {
        "original_filename": original_filename,
        "sha256": sha256,
        "uploaded_at_utc": uploaded_at,
        "uploaded_by": uploader_id,
        "aggregation_level": aggregation_level,
        "markers": ["16S", "ITS"],
        "processing_version": "phylum-aggregation-v1",
    }

    zip_buffer = BytesIO()
    try:
        with zipfile.ZipFile(
            zip_buffer,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.writestr(safe_filename, file_bytes)
            archive.writestr(
                "manifest.json",
                json.dumps(
                    manifest,
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            archive.writestr(
                "README.txt",
                (
                    "This archive contains the original, unmodified biodiversity "
                    "laboratory upload.\n\n"
                    "The ECHOrepo operational database stores only Phylum-level "
                    "aggregate statistics. Full OTU-level source data are retained "
                    "in this archive for publication and preservation through Zenodo.\n"
                ),
            )
    except Exception as exc:
        raise StorageError(
            f"Cannot build raw biodiversity ZIP archive: {exc}"
        ) from exc

    zip_bytes = zip_buffer.getvalue()
    object_name = (
        f"biodiversity/raw/{date_part}/"
        f"{sha256[:16]}_{safe_filename}.zip"
    )
    _put_bytes(
        client=client,
        bucket=bucket,
        object_name=object_name,
        data=zip_bytes,
        content_type="application/zip",
    )

    return RawBiodiversityArchive(
        object_name=object_name,
        sha256=sha256,
    )


def biodiversity_chart_object_names(
    sample_id: str,
    marker: str,
    level: str = "Phylum",
) -> list[str]:
    objects = [
        f"biodiversity/piecharts/{marker}/{level}/{sample_id}.png"
    ]

    if marker == "ITS":
        objects.append(
            f"biodiversity/guildplots/fungi/{sample_id}.png"
        )
    elif marker == "16S":
        objects.append(
            f"biodiversity/guildplots/bacteria/{sample_id}.png"
        )

    return objects


def invalidate_biodiversity_charts(
    affected_sample_markers: Iterable[tuple[str, str]],
    level: str = "Phylum",
) -> None:
    """Best-effort removal of cached taxonomic and guild chart images."""
    client = get_client()
    if client is None:
        return

    bucket = bucket_name()
    for sample_id, marker in affected_sample_markers:
        for object_name in biodiversity_chart_object_names(
            sample_id,
            marker,
            level,
        ):
            try:
                client.remove_object(bucket, object_name)
            except Exception:
                log.exception(
                    "Could not invalidate biodiversity chart %s",
                    object_name,
                )


def object_exists(object_name: str) -> bool:
    """Return whether an object exists; raise on unexpected storage errors."""
    object_name = _validate_object_name(object_name)
    client = get_client()
    if client is None:
        return False

    try:
        client.stat_object(bucket_name(), object_name)
        return True
    except Exception as exc:
        if _is_not_found(exc):
            return False
        raise StorageError(
            f"Cannot inspect MinIO object {object_name}: {exc}"
        ) from exc
