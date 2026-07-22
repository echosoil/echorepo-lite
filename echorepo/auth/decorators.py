from functools import wraps

from flask import redirect, request, session, url_for


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("kc"):
            # Preserve the complete requested URL for normal page requests,
            # including query parameters.
            if request.method in {"GET", "HEAD"}:
                next_url = request.full_path.rstrip("?")
            else:
                # A POST request cannot safely be repeated after login.
                # After authentication, use the normal default destination.
                next_url = None

            return redirect(
                url_for(
                    "auth.login",
                    next=next_url,
                )
            )

        return f(*args, **kwargs)

    return wrapper