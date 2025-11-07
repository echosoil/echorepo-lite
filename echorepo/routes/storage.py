# echorepo/routes/storage.py
from flask import Blueprint, abort, send_file, current_app
import os

storage_bp = Blueprint("storage", __name__)

@storage_bp.get("/storage/<path:relpath>")
def serve_storage(relpath):
    # base dir where you sync/minio-mirror files
    base = current_app.config.get("LOCAL_STORAGE_DIR", "/data/storage")
    full = os.path.join(base, relpath)

    if not os.path.isfile(full):
        abort(404)

    # you can add auth here if you want
    return send_file(full)
