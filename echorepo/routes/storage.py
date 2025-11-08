# echorepo/routes/storage.py
import os
from io import BytesIO
from flask import Blueprint, abort, send_file, current_app

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None
    class S3Error(Exception): ...
    
storage_bp = Blueprint("storage", __name__)

def _get_minio_client():
    if Minio is None:
        return None

    endpoint = (
        current_app.config.get("MINIO_ENDPOINT")
        or os.getenv("MINIO_ENDPOINT")
        or "echorepo-minio:9000"   # works in your docker network
    )
    access_key = (
        current_app.config.get("MINIO_ACCESS_KEY")
        or current_app.config.get("MINIO_ROOT_USER")
        or os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
    )
    secret_key = (
        current_app.config.get("MINIO_SECRET_KEY")
        or current_app.config.get("MINIO_ROOT_PASSWORD")
        or os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
    )

    secure = False
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[8:]
    elif endpoint.startswith("http://"):
        endpoint = endpoint[7:]

    if not access_key or not secret_key:
        current_app.logger.warning("MinIO creds missing")
        return None

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

@storage_bp.get("/storage/<path:relpath>")
def serve_storage(relpath):
    bucket = (
        current_app.config.get("MINIO_BUCKET")
        or os.getenv("MINIO_BUCKET")
        or "echorepo-uploads"
    )

    mclient = _get_minio_client()
    if mclient is not None:
        try:
            resp = mclient.get_object(bucket, relpath)
            data = resp.read()
            resp.close()
            resp.release_conn()

            name = relpath.lower()
            if name.endswith((".jpg", ".jpeg")):
                mimetype = "image/jpeg"
            elif name.endswith(".png"):
                mimetype = "image/png"
            elif name.endswith(".webp"):
                mimetype = "image/webp"
            else:
                mimetype = "application/octet-stream"

            return send_file(
                BytesIO(data),
                mimetype=mimetype,
                download_name=relpath.rsplit("/", 1)[-1],
            )
        except S3Error as e:
            current_app.logger.warning(f"MinIO S3Error for {bucket}/{relpath}: {e}")
        except Exception as e:
            current_app.logger.error(f"MinIO error for {bucket}/{relpath}: {e}")

    # fallback to local dir
    base = current_app.config.get("LOCAL_STORAGE_DIR", "/data/storage")
    full = os.path.join(base, relpath)
    if not os.path.isfile(full):
        abort(404)
    return send_file(full)
