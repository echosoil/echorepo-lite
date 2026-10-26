#!/usr/bin/env python3
"""Standalone, resumable ECHOREPO MinIO image redaction.

Run AFTER the frequent Firestore mirror has uploaded photos. This job accesses
MinIO directly (no Firestore calls, no canonical recomputation). It replaces
images in-place after local OCR/face/QR detection and records completed object
ETags for safe idempotency across daily runs. It never uploads a raw fallback on
detector/encoding failure.

Warning: if the MinIO bucket is public, new photos will be visible BEFORE this
job executes. For strict privacy, mirror into a private staging bucket and
publish only the sanitized image (not implemented by this standalone job).
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.privacy_redact import redact_pil_image

log = logging.getLogger("redact_minio_images")
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".jfif", ".png", ".webp"}
AUDIT_COLUMNS = (
    "processed_at_utc", "processing_version", "user_id", "sample_id", "field",
    "object_name", "storage_path", "mode", "text_regions", "faces",
    "qr_codes", "total_regions",
)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def path_from_env(name: str, default: str) -> Path:
    value = Path(os.getenv(name, default)).expanduser()
    if value.is_absolute():
        # Mirror the existing host-side '/data/...' convention.
        if str(value).startswith("/data/") and not value.parent.exists():
            return PROJECT_ROOT / value.relative_to("/")
        return value
    return PROJECT_ROOT / value


def etag_of(value) -> str:
    return str(value or "").strip('"').strip().lower()


def load_done(path: Path, version: str) -> set[tuple[str, str]]:
    """Only trust ETag-qualified records; older two-column records are unsafe."""
    done: set[tuple[str, str]] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 3 and parts[0] == version and parts[1] and parts[2]:
                done.add((parts[1], etag_of(parts[2])))
    return done


def record_done(path: Path, version: str, name: str, etag: str,
                done: set[tuple[str, str]]) -> None:
    item = (name, etag_of(etag))
    if item in done:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"{version}\t{name}\t{etag_of(etag)}\n")
        fh.flush()
        os.fsync(fh.fileno())
    done.add(item)


def audit_seen(path: Path) -> set[tuple[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    with path.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        if not rdr.fieldnames or not {"processing_version", "object_name"} <= set(rdr.fieldnames):
            raise ValueError(f"Unrecognized audit CSV format: {path}")
        return {(r["processing_version"], r["object_name"]) for r in rdr}


def record_audit(path: Path, version: str, name: str, mode: str,
                 counts: dict[str, int], public_base: str) -> None:
    counts = {k: int(counts.get(k, 0)) for k in ("text_regions", "faces", "qr_codes")}
    total = sum(counts.values())
    if total == 0:
        return
    parts = name.split("/")
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists() or path.stat().st_size == 0
    entry = dict(
        processed_at_utc=datetime.now(timezone.utc).isoformat(),
        processing_version=version,
        user_id=parts[0] if len(parts) > 0 else "",
        sample_id=parts[1] if len(parts) > 1 else "",
        field=Path(parts[-1]).stem,
        object_name=name,
        storage_path=f"{public_base.rstrip('/')}/{name}",
        mode=mode,
        **counts,
        total_regions=total,
    )
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=AUDIT_COLUMNS)
        if needs_header:
            writer.writeheader()
        writer.writerow(entry)
        fh.flush()
        os.fsync(fh.fileno())
    log.info("[affected] %s text=%d faces=%d qr=%d", name,
             counts["text_regions"], counts["faces"], counts["qr_codes"])


def minio_client():
    from minio import Minio
    endpoint = os.getenv("MINIO_ENDPOINT_OUTSIDE") or os.getenv("MINIO_ENDPOINT") or "localhost:9000"
    secure = endpoint.startswith("https://")
    endpoint = endpoint.removeprefix("https://").removeprefix("http://").rstrip("/")
    access = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
    secret = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
    if not access or not secret:
        raise RuntimeError("MinIO credentials are missing from .env")
    return Minio(endpoint, access_key=access, secret_key=secret, secure=secure)


def is_photo_object(name: str) -> bool:
    # User/sample/field.extension is how the mirror stores photos.
    return len(name.split("/")) >= 3 and Path(name).suffix.lower() in IMAGE_EXTENSIONS


def metadata_value(metadata: dict, key: str) -> str:
    desired = {key.lower(), "x-amz-meta-" + key.lower()}
    for k, v in (metadata or {}).items():
        if str(k).lower() in desired:
            return str(v)
    return ""


def redact_bytes(data: bytes, *, mode: str, max_side: int,
                 blur_text: bool, blur_faces: bool, blur_qr: bool,
                 max_pixels: int, jpeg_quality: int) -> tuple[bytes, dict[str, int]]:
    from PIL import Image, ImageOps
    with Image.open(io.BytesIO(data)) as image:
        image.load()
        if image.width * image.height > max_pixels:
            raise ValueError(f"Image too large: {image.width}x{image.height}")
        oriented = ImageOps.exif_transpose(image)
        obscured, counts = redact_pil_image(
            oriented, mode=mode, blur_text=blur_text,
            blur_faces=blur_faces, blur_qr=blur_qr,
            detect_max_side=max_side,
        )
    result = io.BytesIO()
    # Re-encoding drops EXIF/GPS even when no region was detected.
    obscured.convert("RGB").save(result, format="JPEG", quality=jpeg_quality, optimize=True)
    return result.getvalue(), counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="List pending images without OCR or writes")
    parser.add_argument("--max-images", type=int, default=0, help="Process only N pending images (0=all)")
    parser.add_argument("--prefix", default="", help="Limit object prefix for testing")
    parser.add_argument("--force", action="store_true", help="Reprocess checked images (may double-blur)")
    args = parser.parse_args(argv)
    if args.max_images < 0:
        parser.error("--max-images must be >= 0")

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s: %(message)s")
    version = os.getenv("PRIVACY_PROCESSING_VERSION", "v1").strip()
    mode = os.getenv("PRIVACY_REDACTION_MODE", "blur").strip().lower()
    max_side = int(os.getenv("PRIVACY_DETECT_MAX_SIDE", "2600"))
    max_bytes = int(os.getenv("PRIVACY_MAX_INPUT_BYTES", str(20 * 1024 * 1024)))
    max_pixels = int(os.getenv("PRIVACY_MAX_PIXELS", "45000000"))
    jpeg_quality = int(os.getenv("PRIVACY_JPEG_QUALITY", "90"))
    if not version or "\t" in version or "\n" in version:
        parser.error("PRIVACY_PROCESSING_VERSION must be a nonempty single token")
    if mode not in {"blur", "solid"} or max_side < 512 or max_bytes < 1 or max_pixels < 1:
        parser.error("Invalid redaction settings in .env")
    if not 1 <= jpeg_quality <= 95:
        parser.error("PRIVACY_JPEG_QUALITY must be between 1 and 95")
    blur_text = env_bool("PRIVACY_BLUR_TEXT", True)
    blur_faces = env_bool("PRIVACY_BLUR_FACES", True)
    blur_qr = env_bool("PRIVACY_BLUR_QR", True)
    if not any((blur_text, blur_faces, blur_qr)):
        parser.error("At least one privacy detector must be enabled")

    progress_path = path_from_env("PRIVACY_PROGRESS_FILE", "data/privacy_processed_images.txt")
    print("Privacy progress file:", progress_path.resolve(), file=sys.stderr)
    audit_path = path_from_env("PRIVACY_REDACTION_AUDIT_FILE", "data/privacy_redacted_images.csv")
    public_base = os.getenv("PUBLIC_STORAGE_BASE", "/storage")
    bucket = os.getenv("MINIO_BUCKET", "echorepo-uploads")
    done = load_done(progress_path, version)
    seen = audit_seen(audit_path)
    client = minio_client()
    if not client.bucket_exists(bucket):
        raise RuntimeError(f"MinIO bucket not found: {bucket}")
    log.info("MinIO %s; bucket=%s version=%s mode=%s existing_markers=%d dry_run=%s",
             os.getenv("MINIO_ENDPOINT_OUTSIDE", "localhost:9000"), bucket,
             version, mode, len(done), args.dry_run)
    stats = {"scanned": 0, "already_done": 0, "pending": 0,
             "processed": 0, "affected": 0, "failed": 0}
    for item in client.list_objects(bucket, prefix=args.prefix, recursive=True):
        name = item.object_name
        if not is_photo_object(name):
            continue
        stats["scanned"] += 1
        source_etag = etag_of(item.etag)
        if not args.force and (name, source_etag) in done:
            stats["already_done"] += 1
            continue
        try:
            stat = client.stat_object(bucket, name)
            latest_etag = etag_of(stat.etag)
            meta = stat.metadata or {}
            if latest_etag != source_etag:
                log.warning("Skipping concurrent change to %s", name)
                continue
            existing_version = metadata_value(meta, "privacy-version")
            if not args.force and existing_version == version:
                # Progress file was lost/never written, but MinIO has a durable marker.
                counts = {
                    "text_regions": int(metadata_value(meta, "privacy-text-regions") or 0),
                    "faces": int(metadata_value(meta, "privacy-faces") or 0),
                    "qr_codes": int(metadata_value(meta, "privacy-qr-codes") or 0),
                }
                if not args.dry_run:
                    if sum(counts.values()) and (version, name) not in seen:
                        record_audit(audit_path, version, name,
                                     metadata_value(meta, "privacy-mode") or mode,
                                     counts, public_base)
                        seen.add((version, name))
                    record_done(progress_path, version, name, latest_etag, done)
                stats["already_done"] += 1
                continue
            stats["pending"] += 1
            if args.dry_run:
                log.info("[pending] %s", name)
            else:
                response = client.get_object(bucket, name)
                try:
                    response_etag = etag_of(response.headers.get("etag"))
                    raw = response.read(max_bytes + 1)
                finally:
                    response.close()
                    response.release_conn()
                if response_etag and response_etag != latest_etag:
                    raise RuntimeError("Object changed while downloading")
                if len(raw) > max_bytes:
                    raise ValueError(f"Image larger than {max_bytes} bytes")
                redacted, counts = redact_bytes(
                    raw, mode=mode, max_side=max_side,
                    blur_text=blur_text, blur_faces=blur_faces, blur_qr=blur_qr,
                    max_pixels=max_pixels, jpeg_quality=jpeg_quality,
                )
                # A lightweight guard against the mirror changing the object
                # between download and redacted upload. Use cross-job flock
                # in crontab for strict mutual exclusion.
                if etag_of(client.stat_object(bucket, name).etag) != latest_etag:
                    raise RuntimeError("Object changed before redacted upload")
                metadata = {
                    "privacy-version": version,
                    "privacy-mode": mode,
                    "privacy-text-regions": str(counts["text_regions"]),
                    "privacy-faces": str(counts["faces"]),
                    "privacy-qr-codes": str(counts["qr_codes"]),
                }
                written = client.put_object(bucket, name, io.BytesIO(redacted),
                                            len(redacted), content_type="image/jpeg",
                                            metadata=metadata)
                # Audit after successful publication; MinIO metadata supports
                # recovery should audit append fail before progress append.
                record_audit(audit_path, version, name, mode, counts, public_base)
                record_done(progress_path, version, name, written.etag, done)
                stats["processed"] += 1
                if sum(counts.values()):
                    stats["affected"] += 1
            if args.max_images and stats["pending"] >= args.max_images:
                break
        except Exception as exc:
            stats["failed"] += 1
            log.exception("[failed] %s: %s", name, exc)
    log.info("RESULT %s", " ".join(f"{k}={v}" for k, v in stats.items()))
    log.info("Progress file: %s; affected image audit: %s", progress_path, audit_path)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
