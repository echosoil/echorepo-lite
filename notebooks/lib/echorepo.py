import os
from io import BytesIO

import pandas as pd
import requests


class EchoRepo:
    def __init__(self, base_url=None):
        self.base_url = (
            base_url
            or os.getenv(
                "ECHOREPO_URL",
                "https://echorepo.quanta-labs.com",
            )
        ).rstrip("/")

        self.session = requests.Session()

    def _csv(self, path, params=None):
        url = f"{self.base_url}{path}"

        response = self.session.get(
            url,
            params=params,
            timeout=300,
        )
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")

        if "text/csv" not in content_type.lower():
            raise RuntimeError(
                f"Expected CSV from {response.url}, "
                f"got Content-Type={content_type!r}\n\n"
                f"{response.text[:1000]}"
            )

        return pd.read_csv(BytesIO(response.content))

    def _json(self, path, params=None):
        url = f"{self.base_url}{path}"

        response = self.session.get(
            url,
            params=params,
            timeout=300,
        )
        response.raise_for_status()

        return response.json()

    # ---------------------------------------------------------
    # Connectivity
    # ---------------------------------------------------------

    def ping(self):
        return self._json("/api/v1/ping")

    # ---------------------------------------------------------
    # Complete canonical datasets
    # ---------------------------------------------------------

    def samples(self, **filters):
        """
        Return the complete canonical samples dataset.

        Optional API filters can be supplied, e.g.:
            country="ES"
            bbox="-10,35,5,45"
            within="41.4,2.1,50"
        """
        params = {
            "format": "csv",
            **filters,
        }

        return self._csv(
            "/api/v1/canonical/samples",
            params=params,
        )

    def parameters(self, **filters):
        params = {
            "format": "csv",
            **filters,
        }

        return self._csv(
            "/api/v1/canonical/sample_parameters",
            params=params,
        )

    def images(self, **filters):
        params = {
            "format": "csv",
            **filters,
        }

        return self._csv(
            "/api/v1/canonical/sample_images",
            params=params,
        )

    def biodiversity(self, **filters):
        params = {
            "format": "csv",
            **filters,
        }

        return self._csv(
            "/api/v1/canonical/sample_biodiversity",
            params=params,
        )

    # ---------------------------------------------------------
    # Paginated sample query
    # ---------------------------------------------------------

    def query_samples(
        self,
        *,
        limit=100,
        offset=0,
        **filters,
    ):
        """
        Return a limited/paginated sample query.

        Unlike samples(), this uses JSON because CSV mode
        intentionally returns all matching records.
        """
        params = {
            "format": "json",
            "limit": limit,
            "offset": offset,
            **filters,
        }

        result = self._json(
            "/api/v1/canonical/samples",
            params=params,
        )

        return pd.DataFrame(result["data"])

    def sample_count(self, **filters):
        result = self._json(
            "/api/v1/canonical/samples/count",
            params=filters,
        )

        return result["count"]
