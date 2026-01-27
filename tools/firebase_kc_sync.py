#!/usr/bin/env python3
import csv
import datetime
import hashlib
import json
import os
import sys
import time
from typing import Any

import requests
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2 import service_account

# =========================================================
# ENV / CONFIG
# =========================================================
load_dotenv()

# Firebase
PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "echosoil")
SA_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", os.path.expanduser("keys/firebase-sa.json"))
SA_PATH = SA_PATH if not SA_PATH.startswith("/keys") else SA_PATH[1:]
print(f"Using Firebase SA key at: {SA_PATH}")

# Keycloak
KC_BASE = os.environ.get("KEYCLOAK_BASE_URL", "http://keycloak-dev.quanta-labs.com").rstrip("/")
REALM = os.environ.get("REALM", "echo_realm")
KC_ADMIN_USER = os.environ.get("KC_ADMIN_USER")
KC_ADMIN_PASSWORD = os.environ.get("KC_ADMIN_PASSWORD")
KC_CLIENT_ID = os.environ.get("KEYCLOAK_CLIENT_ID", "echo_client")
KC_CLIENT_SECRET = os.environ.get("KEYCLOAK_CLIENT_SECRET")

# files
HASH_PARAMS_FILE = os.environ.get("HASH_PARAMS", "data/hash_params.json")
LAST_SYNC_FILE = os.environ.get("LAST_SYNC_FILE", "data/last_sync.json")
ORPHAN_PROFILES = os.environ.get("ORPHAN_PROFILES", "data/orphan_profiles.csv")
PROFILES_JSON = os.environ.get("PROFILES_JSON", "data/profiles.json")
USERS_JSON = os.environ.get("USERS_JSON", "data/users.json")

# API
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
AUTH_EXPORT_URL = (
    "https://identitytoolkit.googleapis.com/v1/projects/{project_id}/accounts:batchGet"
)
FIRESTORE_LIST_PROFILES = "https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/profiles"

# which profile fields go to user.attributes in KC
PROFILE_FIELDS = [
    "name",
    "surname",
    "publicName",
    "publicCountry",
    "publicRegion",
    "publicStatus",
    "userType",
    "publicEmail",
    "createdAt",
    "updatedAt",
]

# prefix for KC attributes (empty = no prefix)
PROFILE_PREFIX = os.getenv("PROFILE_PREFIX", "").strip()

# Time settings
UTC = datetime.timezone.utc


# =========================================================
# SMALL HELPERS
# =========================================================


def to_iso_from_millis_str(s: str) -> str | None:
    if not s:
        return None
    try:
        ms = int(s)
    except ValueError:
        return None
    dt = datetime.datetime.fromtimestamp(ms / 1000.0, tz=UTC)
    return dt.isoformat()


def to_iso_from_seconds_str(s: str) -> str | None:
    if not s:
        return None
    try:
        sec = int(s)
    except ValueError:
        return None
    dt = datetime.datetime.fromtimestamp(sec, tz=UTC)
    return dt.isoformat()


def to_std_b64(s: str) -> str:
    if not s:
        return s
    s = s.replace("-", "+").replace("_", "/")
    pad = (4 - (len(s) % 4)) % 4
    return s + ("=" * pad)


# =========================================================
# FIREBASE PART
# =========================================================


def get_fb_token(sa_path: str) -> str:
    creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    creds.refresh(Request())
    return creds.token


def export_auth_users(project_id: str, token: str):
    url = AUTH_EXPORT_URL.format(project_id=project_id)
    headers = {"Authorization": f"Bearer {token}"}
    params = {"maxResults": 1000, "returnPasswordHash": "true"}
    users = []
    page = None

    while True:
        qp = dict(params)
        if page:
            qp["nextPageToken"] = page
        r = requests.get(url, headers=headers, params=qp, timeout=60)
        if r.status_code == 403:
            raise SystemExit("403 Forbidden: Identity Toolkit not enabled or SA lacks permission.")
        r.raise_for_status()
        data = r.json()
        users.extend(data.get("users", []))
        page = data.get("nextPageToken")
        if not page:
            break
        time.sleep(0.1)

    # add human timestamps
    for u in users:
        ca = u.get("createdAt")
        lla = u.get("lastLoginAt")
        vs = u.get("validSince")
        if ca:
            u["createdAtIso"] = to_iso_from_millis_str(ca)
        if lla:
            u["lastLoginAtIso"] = to_iso_from_millis_str(lla)
        if vs:
            u["validSinceIso"] = to_iso_from_seconds_str(vs)

    return users


def export_profiles(project_id: str, token: str) -> dict[str, dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    url = FIRESTORE_LIST_PROFILES.format(project_id=project_id)
    params = {"pageSize": 1000}
    profiles: dict[str, dict[str, Any]] = {}

    while True:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 403:
            print(
                "WARN: cannot read Firestore profiles (403). Skipping enrichment.", file=sys.stderr
            )
            return {}
        r.raise_for_status()
        data = r.json()

        for doc in data.get("documents", []):
            full_name = doc["name"]
            doc_id = full_name.rsplit("/", 1)[-1]
            fields = doc.get("fields", {})
            flat: dict[str, Any] = {}
            for f in PROFILE_FIELDS:
                if f in fields:
                    fv = fields[f]
                    if "timestampValue" in fv:
                        flat[f] = fv["timestampValue"]
                        flat[f + "Iso"] = fv["timestampValue"]
                    elif "stringValue" in fv:
                        val = fv["stringValue"]
                        flat[f] = val
                        iso = to_iso_from_millis_str(val) or to_iso_from_seconds_str(val)
                        if iso:
                            flat[f + "Iso"] = iso
            if flat:
                profiles[doc_id] = flat

        next_token = data.get("nextPageToken")
        if not next_token:
            break
        params["pageToken"] = next_token
        time.sleep(0.1)

    return profiles


# =========================================================
# KEYCLOAK PART
# =========================================================


def token_endpoints():
    return [
        f"{KC_BASE}/realms/{REALM}/protocol/openid-connect/token",
        f"{KC_BASE}/auth/realms/{REALM}/protocol/openid-connect/token",
    ]


def get_kc_token() -> str:
    last_err = None

    # password grant first
    if KC_ADMIN_USER and KC_ADMIN_PASSWORD:
        data = {
            "grant_type": "password",
            "client_id": KC_CLIENT_ID,
            "username": KC_ADMIN_USER,
            "password": KC_ADMIN_PASSWORD,
        }
        if KC_CLIENT_SECRET:
            data["client_secret"] = KC_CLIENT_SECRET
        for url in token_endpoints():
            r = requests.post(url, data=data)
            if r.status_code == 200:
                return r.json()["access_token"]
            last_err = (r.status_code, r.text, url)

    # client credentials
    if KC_CLIENT_SECRET:
        data = {
            "grant_type": "client_credentials",
            "client_id": KC_CLIENT_ID,
            "client_secret": KC_CLIENT_SECRET,
        }
        for url in token_endpoints():
            r = requests.post(url, data=data)
            if r.status_code == 200:
                return r.json()["access_token"]
            last_err = (r.status_code, r.text, url)

    code, body, url = last_err if last_err else (0, "no attempt", "n/a")
    print(f"ERROR: could not obtain KC token from {url}: {code} {body}", file=sys.stderr)
    sys.exit(1)


def admin_bases():
    return [
        f"{KC_BASE}/admin/realms/{REALM}",
        f"{KC_BASE}/auth/admin/realms/{REALM}",
    ]


def lookup_user(token: str, identifier: str) -> tuple[str | None, str | None]:
    headers = {"Authorization": f"Bearer {token}"}
    for base in admin_bases():
        # username
        r = requests.get(f"{base}/users", headers=headers, params={"username": identifier})
        if r.status_code == 200 and r.json():
            return base, r.json()[0]["id"]
        # email
        r = requests.get(f"{base}/users", headers=headers, params={"email": identifier})
        if r.status_code == 200 and r.json():
            return base, r.json()[0]["id"]
        # search
        r = requests.get(f"{base}/users", headers=headers, params={"search": identifier})
        if r.status_code == 200 and r.json():
            return base, r.json()[0]["id"]
    return None, None


def load_hash_params():
    with open(HASH_PARAMS_FILE, encoding="utf-8") as f:
        hp = json.load(f)
    return {
        "rounds": hp.get("rounds", 8),
    }


# =========================================================
# HASH-CACHE PART
# =========================================================


def load_last_sync() -> dict[str, str]:
    if not os.path.exists(LAST_SYNC_FILE):
        return {}
    with open(LAST_SYNC_FILE, encoding="utf-8") as f:
        return json.load(f)


def save_last_sync(d: dict[str, str]):
    tmp = LAST_SYNC_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    os.replace(tmp, LAST_SYNC_FILE)


def hash_payload(payload: dict[str, Any]) -> str:
    # only hash the parts we control
    minimal = {
        "username": payload.get("username"),
        "email": payload.get("email"),
        "firstName": payload.get("firstName"),
        "lastName": payload.get("lastName"),
        "attributes": payload.get("attributes", {}),
        "credentials": payload.get("credentials", []),
        "enabled": payload.get("enabled"),
        "emailVerified": payload.get("emailVerified"),
    }
    s = json.dumps(minimal, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# =========================================================
# MAIN SYNC
# =========================================================


def main():
    # --- 1. export from Firebase ---
    if not os.path.exists(SA_PATH):
        print(f"Service account key not found at {SA_PATH}", file=sys.stderr)
        sys.exit(1)

    fb_token = get_fb_token(SA_PATH)

    print("Exporting Firebase Auth…")
    auth_users = export_auth_users(PROJECT_ID, fb_token)
    print(f"Auth users: {len(auth_users)}")

    print("Exporting Firestore profiles…")
    profiles = export_profiles(PROJECT_ID, fb_token)
    print(f"Profiles: {len(profiles)}")

    # --- 2. enrich ---
    enriched = []
    auth_ids = {u.get("localId") for u in auth_users if u.get("localId")}
    auth_emails = set()
    for u in auth_users:
        if u.get("email"):
            auth_emails.add(u["email"].lower())
        for p in u.get("providerUserInfo", []):
            em = p.get("email")
            if em:
                auth_emails.add(em.lower())

    for u in auth_users:
        lid = u.get("localId")
        if lid and lid in profiles:
            cp = dict(u)
            cp["profile"] = profiles[lid]
            enriched.append(cp)
        else:
            enriched.append(u)

    # --- optional orphan report ---
    orphan_profiles = []
    for pid, prof in profiles.items():
        if pid in auth_ids:
            continue
        pub_email = prof.get("publicEmail")
        if pub_email and pub_email.lower() in auth_emails:
            continue
        rec = {"profileId": pid}
        rec.update(prof)
        orphan_profiles.append(rec)

    if orphan_profiles:
        fields = (
            ["profileId"]
            + [f for f in PROFILE_FIELDS if f not in ("createdAt", "updatedAt")]
            + ["createdAtIso", "updatedAtIso"]
        )
        with open(ORPHAN_PROFILES, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for row in orphan_profiles:
                w.writerow(row)
        print(f"Wrote {len(orphan_profiles)} orphan profiles → {ORPHAN_PROFILES}")

    # write debug files (nice to keep)
    with open(USERS_JSON, "w", encoding="utf-8") as f:
        json.dump({"users": enriched}, f, ensure_ascii=False, indent=2)
    with open(PROFILES_JSON, "w", encoding="utf-8") as f:
        json.dump({"profiles": profiles}, f, ensure_ascii=False, indent=2)

    # --- 3. import into Keycloak (with hash cache) ---
    kc_token = get_kc_token()
    rounds = load_hash_params()["rounds"]
    last_sync = load_last_sync()
    changed = 0
    total = 0

    for u in enriched:
        total += 1
        email = u.get("email") or u.get("localId")
        if not email:
            print("skip user without email/localId")
            continue

        password_hash = u.get("passwordHash")
        salt = u.get("salt")
        if salt:
            salt = to_std_b64(salt)

        payload: dict[str, Any] = {
            "username": email,
            "email": email,
            "enabled": not u.get("disabled", False),
            "emailVerified": u.get("emailVerified", False),
        }

        # profile → names
        prof = u.get("profile") or {}
        if prof.get("name"):
            payload["firstName"] = prof["name"].strip()
        if prof.get("surname"):
            payload["lastName"] = prof["surname"].strip()

        # credentials
        if password_hash and salt:
            secret_data = {"value": password_hash, "salt": salt}
            credential_data = {"algorithm": "firebase-scrypt", "hashIterations": rounds}
            payload["credentials"] = [
                {
                    "type": "password",
                    "userLabel": "firebase-scrypt",
                    "secretData": json.dumps(secret_data),
                    "credentialData": json.dumps(credential_data),
                }
            ]
        else:
            # no creds
            pass

        # attributes
        attrs = {}

        for f in PROFILE_FIELDS:
            if not f:
                continue
            if f in prof and prof[f] not in (None, ""):
                key = f"{PROFILE_PREFIX}{f}" if PROFILE_PREFIX else f
                attrs[key] = [str(prof[f])]

        # firebase timestamps
        if "createdAt" in u:
            attrs["fb_createdAt_raw"] = [str(u["createdAt"])]
            iso = to_iso_from_millis_str(str(u["createdAt"]))
            if iso:
                attrs["fb_createdAt_iso"] = [iso]
        if "lastLoginAt" in u:
            attrs["fb_lastLoginAt_raw"] = [str(u["lastLoginAt"])]
            iso = to_iso_from_millis_str(str(u["lastLoginAt"]))
            if iso:
                attrs["fb_lastLoginAt_iso"] = [iso]
        if "validSince" in u:
            attrs["fb_validSince_raw"] = [str(u["validSince"])]
            iso = to_iso_from_seconds_str(str(u["validSince"]))
            if iso:
                attrs["fb_validSince_iso"] = [iso]

        # profile timestamps we may have normalized on export
        if "createdAtIso" in prof:
            attrs["profile_createdAt_iso"] = [prof["createdAtIso"]]
        if "updatedAtIso" in prof:
            attrs["profile_updatedAt_iso"] = [prof["updatedAtIso"]]

        if attrs:
            payload["attributes"] = attrs

        # hash
        h = hash_payload(payload)
        old_h = last_sync.get(email)

        if old_h == h:
            # no change
            continue

        # need to create or update
        headers = {"Authorization": f"Bearer {kc_token}", "Content-Type": "application/json"}
        created_or_updated = False

        # try POST
        for base in admin_bases():
            r = requests.post(f"{base}/users", headers=headers, data=json.dumps(payload))
            if r.status_code == 201:
                print("created", email)
                created_or_updated = True
                break
            if r.status_code == 409:
                # need to update
                found_base, uid = lookup_user(kc_token, email)
                if found_base and uid:
                    r2 = requests.put(
                        f"{found_base}/users/{uid}", headers=headers, data=json.dumps(payload)
                    )
                    if r2.status_code in (204, 200):
                        print("updated", email)
                        created_or_updated = True
                    else:
                        print(f"ERROR updating {email}: {r2.status_code} {r2.text}")
                else:
                    print(f"ERROR: user {email} exists but cannot be looked up")
                break
            else:
                print(f"ERROR creating {email} at {base}: {r.status_code} {r.text}")
            # other errors → try next base

        if created_or_updated:
            last_sync[email] = h
            changed += 1

    save_last_sync(last_sync)
    print(f"done. processed {total} users; changed {changed}.")


if __name__ == "__main__":
    main()
