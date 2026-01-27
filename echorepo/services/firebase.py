import os
from pathlib import Path

import firebase_admin
import requests
from firebase_admin import credentials, firestore
from flask_babel import gettext as _

from ..config import settings


def init_firebase_once():
    if firebase_admin._apps:
        return
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path or not Path(cred_path).exists():
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or file missing.")
    cred = credentials.Certificate(cred_path)
    if settings.FIREBASE_PROJECT_ID:
        firebase_admin.initialize_app(cred, {"projectId": settings.FIREBASE_PROJECT_ID})
    else:
        firebase_admin.initialize_app(cred)


def update_coords_by_user_sample(user_id: str, sample_id: str, lat: float, lon: float):
    """Write to users/{userId}/samples/{sampleId}: data[1].info.lat/long"""
    init_firebase_once()
    db = firestore.client()
    ref = db.document(f"users/{user_id}/samples/{sample_id}")
    snap = ref.get()
    if not snap.exists:
        return False, f"Document not found: {ref.path}"
    doc = snap.to_dict() or {}
    arr = doc.get("data", [])
    while len(arr) <= 1:
        arr.append({})
    step1 = arr[1] if isinstance(arr[1], dict) else {}
    info1 = step1.get("info") if isinstance(step1.get("info"), dict) else {}
    info1["lat"] = float(lat)
    info1["long"] = float(lon)
    step1["info"] = info1
    arr[1] = step1
    ref.update({"data": arr})
    return True, ref.path


def send_password_reset_email(email: str) -> tuple[bool, str]:
    """
    Ask Firebase Auth to send a password reset email.

    Returns (ok, public_message) where public_message is safe to show to the user.
    """
    api_key = settings.FIREBASE_WEB_API_KEY
    if not api_key:
        return False, _("Password recovery is not configured for this site.")

    endpoint = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={api_key}"
    payload = {"requestType": "PASSWORD_RESET", "email": email}

    try:
        r = requests.post(endpoint, json=payload, timeout=10)
    except requests.RequestException:
        # Log-only detail; user-facing message should be generic
        return False, _("Could not contact the password recovery service.")

    # Per Firebase behaviour, even invalid emails normally give 200 with a generic response
    # so we can always show a generic success.:contentReference[oaicite:2]{index=2}
    if r.status_code == 200:
        return True, _("If this address is registered, a reset link has been sent.") + "<br>" + _(
            "New password may take up to 30 minutes to activate in ECHOREPO."
        )

    # In case of non-200 (quota, config error, etc.)
    return False, _("Password recovery is temporarily unavailable. Please contact the organisers.")
