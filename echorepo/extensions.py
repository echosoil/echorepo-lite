from authlib.integrations.flask_client import OAuth
from flask_babel import Babel

oauth = OAuth()
babel = Babel()

__all__ = ["babel", "oauth"]