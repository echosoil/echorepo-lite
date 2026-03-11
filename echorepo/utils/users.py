import csv
import os

from ..config import settings


def user_exists_in_users_csv(user_input: str) -> bool:
    p = settings.USERS_CSV
    if not os.path.exists(p):
        return False
    try:
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                email = (row.get("email") or "").strip().lower()
                if email and email == user_input.strip().lower():
                    return True
    except Exception:
        return False
    return False
