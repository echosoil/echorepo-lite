import os
from pathlib import Path
import firebase_admin
from firebase_admin import credentials, firestore

from ..config import settings

def init_firebase_once():
    if firebase_admin._apps: return
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
    info1["lat"]  = float(lat)
    info1["long"] = float(lon)
    step1["info"] = info1
    arr[1] = step1
    ref.update({"data": arr})
    return True, ref.path
